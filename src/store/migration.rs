use crate::error::CrdtError;

/// A migration that transforms persisted data from one format version to the next.
pub trait Migration {
    /// The format version this migration reads from.
    fn source_version(&self) -> u32;

    /// The format version this migration produces.
    fn target_version(&self) -> u32;

    /// Apply the migration to the given JSON data, returning the transformed data.
    fn migrate(&self, data: serde_json::Value) -> Result<serde_json::Value, CrdtError>;
}

/// Registry holding an ordered sequence of migrations.
///
/// Migrations must be registered in ascending order (v1->v2, v2->v3, etc.).
pub struct MigrationRegistry {
    migrations: Vec<Box<dyn Migration>>,
}

impl MigrationRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            migrations: Vec::new(),
        }
    }

    /// Register a migration. Migrations should be added in order.
    pub fn register(&mut self, migration: Box<dyn Migration>) {
        self.migrations.push(migration);
    }

    /// Apply all necessary migrations to transform data from `from_version` to `to_version`.
    ///
    /// Returns an error if:
    /// - `from_version > to_version` (forward incompatibility)
    /// - A required migration step is missing
    pub fn apply_migrations(
        &self,
        mut data: serde_json::Value,
        from_version: u32,
        to_version: u32,
    ) -> Result<serde_json::Value, CrdtError> {
        if from_version > to_version {
            return Err(CrdtError::IncompatibleVersion {
                data_version: from_version,
                code_version: to_version,
            });
        }

        // Nothing to do when versions already match. The while loop below
        // also handles this (it never executes when current == to_version),
        // but the early return makes the intent explicit and avoids a
        // registry lookup when no migration is needed.
        if from_version == to_version {
            return Ok(data);
        }

        let mut current = from_version;
        while current < to_version {
            let migration = self
                .migrations
                .iter()
                .find(|m| m.source_version() == current)
                .ok_or_else(|| CrdtError::MigrationFailed {
                    from: current,
                    to: current + 1,
                    reason: format!("no migration registered from v{current}"),
                })?;

            data = migration.migrate(data)?;
            let next = migration.target_version();
            if next <= current {
                return Err(CrdtError::MigrationFailed {
                    from: current,
                    to: next,
                    reason: format!(
                        "migration v{current}→v{next} does not advance version (infinite loop guard)"
                    ),
                });
            }
            current = next;
        }

        Ok(data)
    }
}

impl Default for MigrationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Built-in migrations
// ---------------------------------------------------------------------------

/// Placeholder migration from v1 to v2 (no-op, establishes the pattern).
pub struct V1ToV2;

impl Migration for V1ToV2 {
    fn source_version(&self) -> u32 {
        1
    }

    fn target_version(&self) -> u32 {
        2
    }

    fn migrate(&self, data: serde_json::Value) -> Result<serde_json::Value, CrdtError> {
        // No-op: v1 and v2 share the same schema for now.
        Ok(data)
    }
}

/// Migration from v2 to v3: the session-guarantee fields
/// (`applied_origins`, `merge_failed_keys`, `pruned_floor`,
/// `visible_origins`) were added to `Store`.
///
/// A JSON no-op: the new fields carry `#[serde(default)]` and old
/// snapshots simply omit them. (The bincode path cannot rely on serde
/// defaults and uses a versioned decode type instead — see
/// `Store::load_from_backend_bincode`.)
pub struct V2ToV3;

impl Migration for V2ToV3 {
    fn source_version(&self) -> u32 {
        2
    }

    fn target_version(&self) -> u32 {
        3
    }

    fn migrate(&self, data: serde_json::Value) -> Result<serde_json::Value, CrdtError> {
        // No-op for JSON: added fields default via serde.
        Ok(data)
    }
}

/// Build the default migration registry with all known migrations.
pub fn default_registry() -> MigrationRegistry {
    let mut registry = MigrationRegistry::new();
    registry.register(Box::new(V1ToV2));
    registry.register(Box::new(V2ToV3));
    registry
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn v1_to_v2_passes_data_through() {
        let migration = V1ToV2;
        let data = json!({"data": {"key": "value"}, "timestamps": {}});
        let result = migration.migrate(data.clone()).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn registry_applies_single_migration() {
        let registry = default_registry();
        let data = json!({"data": {"key": "value"}, "timestamps": {}});
        let result = registry.apply_migrations(data.clone(), 1, 2).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn registry_no_op_when_versions_equal() {
        let registry = default_registry();
        let data = json!({"data": {"key": "value"}});
        let result = registry.apply_migrations(data.clone(), 2, 2).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn registry_rejects_future_version() {
        let registry = default_registry();
        let data = json!({});
        let result = registry.apply_migrations(data, 99, 2);
        assert!(result.is_err());
        match result.unwrap_err() {
            CrdtError::IncompatibleVersion {
                data_version: 99,
                code_version: 2,
            } => {}
            other => panic!("expected IncompatibleVersion, got {:?}", other),
        }
    }

    #[test]
    fn registry_error_on_missing_migration() {
        let registry = MigrationRegistry::new(); // empty
        let data = json!({});
        let result = registry.apply_migrations(data, 1, 2);
        assert!(result.is_err());
        match result.unwrap_err() {
            CrdtError::MigrationFailed { from: 1, .. } => {}
            other => panic!("expected MigrationFailed, got {:?}", other),
        }
    }

    /// Verify registry actually invokes a transforming migration (not just skips it).
    /// Uses a mock V1→V2 that adds a sentinel field, proving the code path ran.
    #[test]
    fn registry_invokes_migration_with_transforming_mock() {
        struct V1ToV2Transforming;
        impl Migration for V1ToV2Transforming {
            fn source_version(&self) -> u32 {
                1
            }
            fn target_version(&self) -> u32 {
                2
            }
            fn migrate(&self, mut data: serde_json::Value) -> Result<serde_json::Value, CrdtError> {
                data["migration_ran"] = serde_json::Value::Bool(true);
                Ok(data)
            }
        }

        let mut registry = MigrationRegistry::new();
        registry.register(Box::new(V1ToV2Transforming));

        let data = json!({"data": {}});
        let result = registry.apply_migrations(data, 1, 2).unwrap();
        assert_eq!(
            result["migration_ran"], true,
            "migration must have been invoked"
        );
    }

    /// Verify the infinite loop guard fires when a migration does not advance version.
    #[test]
    fn registry_rejects_stuck_migration() {
        struct StuckMigration;
        impl Migration for StuckMigration {
            fn source_version(&self) -> u32 {
                1
            }
            // same as source — stuck!
            fn target_version(&self) -> u32 {
                1
            }
            fn migrate(&self, data: serde_json::Value) -> Result<serde_json::Value, CrdtError> {
                Ok(data)
            }
        }

        let mut registry = MigrationRegistry::new();
        registry.register(Box::new(StuckMigration));

        let result = registry.apply_migrations(json!({}), 1, 2);
        match result.unwrap_err() {
            CrdtError::MigrationFailed { from: 1, to: 1, .. } => {}
            other => panic!("expected MigrationFailed(1→1), got {:?}", other),
        }
    }

    /// Test a multi-step migration chain with transforming mocks (a fresh
    /// registry — the default one already covers 1→2→3 with no-ops).
    #[test]
    fn registry_applies_chain_v1_to_v3() {
        struct MockV1ToV2;
        impl Migration for MockV1ToV2 {
            fn source_version(&self) -> u32 {
                1
            }
            fn target_version(&self) -> u32 {
                2
            }
            fn migrate(&self, mut data: serde_json::Value) -> Result<serde_json::Value, CrdtError> {
                data["migrated_to_v2"] = serde_json::Value::Bool(true);
                Ok(data)
            }
        }
        struct MockV2ToV3;
        impl Migration for MockV2ToV3 {
            fn source_version(&self) -> u32 {
                2
            }
            fn target_version(&self) -> u32 {
                3
            }
            fn migrate(&self, mut data: serde_json::Value) -> Result<serde_json::Value, CrdtError> {
                // Add a marker to prove this ran
                data["migrated_to_v3"] = serde_json::Value::Bool(true);
                Ok(data)
            }
        }

        let mut registry = MigrationRegistry::new();
        registry.register(Box::new(MockV1ToV2));
        registry.register(Box::new(MockV2ToV3));

        let data = json!({"data": {}});
        let result = registry.apply_migrations(data, 1, 3).unwrap();
        assert_eq!(result["migrated_to_v2"], true);
        assert_eq!(result["migrated_to_v3"], true);
    }

    /// The default registry migrates v1 and v2 data all the way to the
    /// current version (both built-in steps are JSON no-ops).
    #[test]
    fn default_registry_covers_v1_and_v2_to_current() {
        let registry = default_registry();
        let data = json!({"data": {"key": "value"}, "timestamps": {}});
        for from in [1u32, 2] {
            let result = registry
                .apply_migrations(data.clone(), from, crate::store::kv::CURRENT_FORMAT_VERSION)
                .unwrap();
            assert_eq!(result, data, "v{from} migration chain must be a no-op");
        }
    }
}
