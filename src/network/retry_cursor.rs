//! Fair selection from an ordered set of retry jobs that may keep growing.

use std::collections::VecDeque;

/// Visit the jobs present at the start of a cycle before admitting new ones.
/// IDs for completion reports and policy witnesses are ordered tuples, so a
/// newly inserted ID can sort before the cycle's previous maximum.
pub(crate) struct RetryCursor<K> {
    remaining: VecDeque<K>,
}

impl<K> Default for RetryCursor<K> {
    fn default() -> Self {
        Self {
            remaining: VecDeque::new(),
        }
    }
}

impl<K: Ord + Clone> RetryCursor<K> {
    /// `keys` must be strictly increasing. All call sites iterate ordered
    /// outbox IDs or a sorted completion-report list.
    pub(crate) fn next(&mut self, keys: impl IntoIterator<Item = K>) -> Option<K> {
        let current: Vec<K> = keys.into_iter().collect();
        debug_assert!(current.windows(2).all(|pair| pair[0] < pair[1]));
        if current.is_empty() {
            self.remaining.clear();
            return None;
        }
        loop {
            if self.remaining.is_empty() {
                self.remaining.extend(current.iter().cloned());
            }
            while let Some(key) = self.remaining.pop_front() {
                // A completed job may disappear during a cycle. New jobs are
                // deferred until the next snapshot, regardless of key order.
                if current.binary_search(&key).is_ok() {
                    return Some(key);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continually_appended_jobs_cannot_starve_an_old_failure() {
        let mut cursor = RetryCursor::default();
        let mut visited_old = 0;
        for newest in 0..100 {
            let chosen = cursor.next(0..=newest).unwrap();
            visited_old += usize::from(chosen == 0);
        }
        assert!(visited_old > 1);
    }

    #[test]
    fn removed_jobs_and_empty_queue_are_handled() {
        let mut cursor = RetryCursor::default();
        assert_eq!(cursor.next([0, 1]), Some(0));
        assert_eq!(cursor.next([1]), Some(1));
        assert_eq!(cursor.next(std::iter::empty()), None);
        assert_eq!(cursor.next([7]), Some(7));
    }

    #[test]
    fn completion_reports_revisit_old_sender_as_new_senders_arrive() {
        let mut cursor = RetryCursor::default();
        let old = ("a".to_owned(), 0_u64);
        let mut visited_old = 0;
        for suffix in b'b'..=b'z' {
            let mut pending = vec![old.clone()];
            pending.extend((b'b'..=suffix).map(|id| ((id as char).to_string(), 0)));
            visited_old += usize::from(cursor.next(pending) == Some(old.clone()));
        }
        assert!(visited_old > 1);
    }

    #[test]
    fn new_keys_inside_old_ceiling_cannot_starve_existing_completion() {
        let mut cursor = RetryCursor::default();
        let oldest = ("a".to_owned(), 0_u64);
        let waiting = ("z".to_owned(), 0_u64);
        assert_eq!(
            cursor.next([oldest.clone(), waiting.clone()]),
            Some(oldest.clone())
        );
        let mut revisits = 0;
        for count in 0..100 {
            let mut pending = vec![oldest.clone(), waiting.clone()];
            pending.extend((0..=count).map(|id| (format!("m{id:03}"), 0)));
            pending.sort();
            revisits += usize::from(cursor.next(pending) == Some(waiting.clone()));
        }
        assert!(revisits > 1);
    }
}
