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
            current = migration.target_version();
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

/// Build the default migration registry with all known migrations.
pub fn default_registry() -> MigrationRegistry {
    let mut registry = MigrationRegistry::new();
    registry.register(Box::new(V1ToV2));
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

    /// Test a multi-step migration chain by adding a second migration.
    #[test]
    fn registry_applies_chain_v1_to_v3() {
        struct V2ToV3;
        impl Migration for V2ToV3 {
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

        let mut registry = default_registry();
        registry.register(Box::new(V2ToV3));

        let data = json!({"data": {}});
        let result = registry.apply_migrations(data, 1, 3).unwrap();
        assert_eq!(result["migrated_to_v3"], true);
    }
}
