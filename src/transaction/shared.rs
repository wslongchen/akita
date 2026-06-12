/*
 *
 *  *
 *  *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *  *
 *  *   Redistribution and use in source and binary forms, with or without
 *  *   modification, are permitted provided that the following conditions are met:
 *  *
 *  *   Redistributions of source code must retain the above copyright notice,
 *  *   this list of conditions and the following disclaimer.
 *  *   Redistributions in binary form must reproduce the above copyright
 *  *   notice, this list of conditions and the following disclaimer in the
 *  *   documentation and/or other materials provided with the distribution.
 *  *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *  *   contributors may be used to endorse or promote products derived from
 *  *   this software without specific prior written permission.
 *  *   Author: SnackCloud
 *  *
 *
 */

//! Shared transaction utilities for both sync and async implementations.
//!
//! This module contains helper functions and types for savepoint support
//! that are common to both `AkitaTransaction` (blocking) and
//! `AsyncAkitaTransaction` (non-blocking).

use crate::errors::{Result};

/// RAII guard for savepoints that automatically rolls back on drop if not released.
///
/// # Example
/// ```ignore
/// let mut tx = db.start_transaction()?;
/// {
///     let sp = SavepointGuard::new(&mut tx, "sp1")?;
///     // Do some operations...
///     sp.release()?; // Explicitly release the savepoint
/// } // If not released, automatically rolls back to savepoint
/// tx.commit()?;
/// ```
pub struct SavepointGuard<'a, E: SavepointExecutor> {
    /// A mutable reference to the executor that owns the database connection.
    executor: &'a mut E,
    /// The savepoint name used in SQL statements.
    name: String,
    /// Whether the savepoint has been explicitly released or rolled back.
    released: bool,
}

impl<'a, E: SavepointExecutor> SavepointGuard<'a, E> {
    /// Create a new savepoint with the given name.
    ///
    /// Issues a `SAVEPOINT <name>` statement immediately. If the guard is
    /// dropped without calling [`release()`](Self::release), the savepoint
    /// is automatically rolled back.
    ///
    /// # Parameters
    /// - `executor`: A mutable reference to the savepoint-capable executor.
    /// - `name`: The savepoint name (alphanumeric and underscores only).
    ///
    /// # Returns
    /// A `SavepointGuard` that manages the savepoint lifecycle.
    pub fn new(executor: &'a mut E, name: &str) -> Result<Self> {
        executor.create_savepoint(name)?;
        Ok(Self {
            executor,
            name: name.to_string(),
            released: false,
        })
    }

    /// Release (commit) the savepoint.
    ///
    /// Issues a `RELEASE SAVEPOINT <name>` statement and marks the guard as
    /// released so that the `Drop` impl will not roll back.
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if the release statement fails.
    pub fn release(mut self) -> Result<()> {
        self.executor.release_savepoint(&self.name)?;
        self.released = true;
        Ok(())
    }

    /// Rollback to the savepoint.
    ///
    /// Issues a `ROLLBACK TO SAVEPOINT <name>` statement and marks the guard
    /// as released so that the `Drop` impl will not roll back again.
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if the rollback statement fails.
    pub fn rollback(mut self) -> Result<()> {
        self.executor.rollback_to_savepoint(&self.name)?;
        self.released = true;
        Ok(())
    }

    /// Get the savepoint name.
    ///
    /// # Returns
    /// A string slice containing the savepoint name used in SQL statements.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<'a, E: SavepointExecutor> Drop for SavepointGuard<'a, E> {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.executor.rollback_to_savepoint(&self.name);
        }
    }
}

/// Trait for types that support savepoint operations.
///
/// This trait is implemented by both `DbDriver` (blocking) and `AsyncDbDriver` (non-blocking).
pub trait SavepointExecutor {
    /// Create a new savepoint with the given name.
    ///
    /// # Parameters
    /// - `name`: The savepoint name (should be sanitized before use).
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if the savepoint could not be created.
    fn create_savepoint(&mut self, name: &str) -> Result<()>;

    /// Release (commit) a savepoint, discarding the ability to roll back to it.
    ///
    /// # Parameters
    /// - `name`: The savepoint name to release.
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if the release fails.
    fn release_savepoint(&mut self, name: &str) -> Result<()>;

    /// Roll back to a savepoint, undoing all changes made after it was created.
    ///
    /// # Parameters
    /// - `name`: The savepoint name to roll back to.
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if the rollback fails.
    fn rollback_to_savepoint(&mut self, name: &str) -> Result<()>;
}

/// Generate savepoint SQL statements.
///
/// This module provides pure functions for building the SQL text needed for
/// savepoint operations. All names are sanitized to prevent SQL injection.
pub mod sql {
    /// Generate a `SAVEPOINT` SQL statement.
    ///
    /// # Parameters
    /// - `name`: The savepoint name (will be sanitized).
    ///
    /// # Returns
    /// A `String` containing the `SAVEPOINT <name>` SQL statement.
    pub fn create_savepoint(name: &str) -> String {
        format!("SAVEPOINT {}", sanitize_name(name))
    }

    /// Generate a `RELEASE SAVEPOINT` SQL statement.
    ///
    /// # Parameters
    /// - `name`: The savepoint name (will be sanitized).
    ///
    /// # Returns
    /// A `String` containing the `RELEASE SAVEPOINT <name>` SQL statement.
    pub fn release_savepoint(name: &str) -> String {
        format!("RELEASE SAVEPOINT {}", sanitize_name(name))
    }

    /// Generate a `ROLLBACK TO SAVEPOINT` SQL statement.
    ///
    /// # Parameters
    /// - `name`: The savepoint name (will be sanitized).
    ///
    /// # Returns
    /// A `String` containing the `ROLLBACK TO SAVEPOINT <name>` SQL statement.
    pub fn rollback_to_savepoint(name: &str) -> String {
        format!("ROLLBACK TO SAVEPOINT {}", sanitize_name(name))
    }

    /// Sanitize savepoint name to prevent SQL injection.
    fn sanitize_name(name: &str) -> String {
        // Only allow alphanumeric characters and underscores
        let sanitized: String = name
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .collect();

        if sanitized.is_empty() {
            "sp_default".to_string()
        } else {
            sanitized
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_create_savepoint_sql() {
            assert_eq!(create_savepoint("sp1"), "SAVEPOINT sp1");
            assert_eq!(create_savepoint("my_savepoint"), "SAVEPOINT my_savepoint");
        }

        #[test]
        fn test_release_savepoint_sql() {
            assert_eq!(release_savepoint("sp1"), "RELEASE SAVEPOINT sp1");
        }

        #[test]
        fn test_rollback_to_savepoint_sql() {
            assert_eq!(rollback_to_savepoint("sp1"), "ROLLBACK TO SAVEPOINT sp1");
        }

        #[test]
        fn test_sanitize_name() {
            assert_eq!(sanitize_name("sp1"), "sp1");
            assert_eq!(sanitize_name("my_savepoint"), "my_savepoint");
            assert_eq!(sanitize_name("sp; DROP TABLE"), "spDROPTABLE");
            assert_eq!(sanitize_name(""), "sp_default");
        }
    }
}
