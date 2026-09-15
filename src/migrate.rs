//! One-shot migration: import legacy `~/.local/share/claude-run/state/*.json`
//! files (written by the old nu implementation) into SQLite.
//!
//! Idempotent: re-running re-imports the same data into the same rows via the
//! `INSERT ... ON CONFLICT` upserts. Safe to call against a partially-populated
//! DB.

use std::collections::HashMap;
use std::path::Path;

use crate::db::DbPool;
use serde::Deserialize;
use tracing::info;

use crate::error::Result;

#[derive(Debug, Deserialize)]
struct LegacyState {
    #[serde(default)]
    attempts: HashMap<String, LegacyAttempt>,
    #[serde(default)]
    daily_count: HashMap<String, i64>,
    #[serde(default)]
    daily_cost_usd: HashMap<String, f64>,
}

#[derive(Debug, Deserialize)]
struct LegacyAttempt {
    count: i64,
    last_at: String,
}

pub async fn import_legacy_state(pool: &DbPool, legacy_dir: &Path) -> Result<MigrationReport> {
    let mut report = MigrationReport::default();
    if !legacy_dir.is_dir() {
        return Ok(report);
    }
    for entry in std::fs::read_dir(legacy_dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let action_name = stem.to_string();
        let raw = std::fs::read_to_string(&path)?;
        let parsed: LegacyState = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(e) => {
                info!(path = %path.display(), error = %e, "legacy state file unparseable, skipping");
                continue;
            }
        };
        info!(
            action = %action_name,
            attempts = parsed.attempts.len(),
            days = parsed.daily_count.len(),
            "importing legacy state"
        );

        for (job_key, att) in &parsed.attempts {
            sqlx::query(&crate::db::q(
                pool,
                r#"
                INSERT INTO attempt_counters (action_name, job_key, count, last_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(action_name, job_key) DO UPDATE SET
                    count = excluded.count,
                    last_at = excluded.last_at
                "#,
            ))
            .bind(&action_name)
            .bind(job_key)
            .bind(att.count)
            .bind(&att.last_at)
            .execute(pool)
            .await?;
            report.attempts += 1;
        }

        let dates: std::collections::BTreeSet<_> = parsed
            .daily_count
            .keys()
            .chain(parsed.daily_cost_usd.keys())
            .collect();
        for date in dates {
            let count = parsed.daily_count.get(date).copied().unwrap_or(0);
            let cost = parsed.daily_cost_usd.get(date).copied().unwrap_or(0.0);
            sqlx::query(&crate::db::q(
                pool,
                r#"
                INSERT INTO daily_ledger (action_name, date, run_count, cost_usd)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(action_name, date) DO UPDATE SET
                    run_count = excluded.run_count,
                    cost_usd = excluded.cost_usd
                "#,
            ))
            .bind(&action_name)
            .bind(date)
            .bind(count)
            .bind(cost)
            .execute(pool)
            .await?;
            report.daily_rows += 1;
        }
        report.actions += 1;
    }
    Ok(report)
}

#[derive(Debug, Default)]
pub struct MigrationReport {
    pub actions: usize,
    pub attempts: usize,
    pub daily_rows: usize,
}
