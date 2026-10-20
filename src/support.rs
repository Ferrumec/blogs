//! Shared helpers: identity, slugs, validation, the repository and a
//! UUID-column list query that viewset's string-only `?field=value` filters
//! cannot express.

use actix_web::{HttpMessage, HttpRequest};
use ferrumec::cache::{CacheFactory, Store};
use ferrumec::Authority;
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use viewset::{ApiError, Entity, Filters, Page, PaginationParams, Repository};

pub type ApiResult<T> = Result<T, ApiError>;

/// Role bit (`1 << ADMIN_BIT`) that lets a user moderate other people's
/// content. Route-level bits live in `permissions.json`.
pub const ADMIN_BIT: u32 = 20;

/// The caller, as put into the request extensions by ferrumec's `Permissions`
/// middleware. That middleware only runs the lookup for routes listed in
/// `permissions.json`, so every handler that calls this must be listed there.
pub fn authority(req: &HttpRequest) -> ApiResult<Authority> {
    req.extensions()
        .get::<Authority>()
        .cloned()
        .ok_or(ApiError::Forbidden)
}

pub fn is_admin(a: &Authority) -> bool {
    a.role & (1u128 << ADMIN_BIT) != 0
}

/// Owner-or-admin check used by update/delete/publish handlers.
pub fn ensure_owner_or_admin(a: &Authority, owner: Uuid) -> ApiResult<()> {
    if a.user == owner || is_admin(a) {
        Ok(())
    } else {
        Err(ApiError::Forbidden)
    }
}

pub fn parse_uuid(raw: &str) -> ApiResult<Uuid> {
    Uuid::parse_str(raw).map_err(|_| ApiError::Validation(format!("invalid id: {raw}")))
}

pub fn internal<E: std::fmt::Display>(e: E) -> ApiError {
    ApiError::Internal(e.to_string())
}

// ---- slugs & validation ----------------------------------------------------

pub fn slugify(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut last_dash = true;
    for c in input.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    out.truncate(120);
    while out.ends_with('-') {
        out.pop();
    }
    out
}

pub fn valid_slug(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 120
        && !s.starts_with('-')
        && !s.ends_with('-')
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

pub fn require_text(field: &str, value: &str, max: usize) -> ApiResult<()> {
    if value.trim().is_empty() {
        return Err(ApiError::Validation(format!("`{field}` must not be empty")));
    }
    if value.chars().count() > max {
        return Err(ApiError::Validation(format!(
            "`{field}` must be at most {max} characters"
        )));
    }
    Ok(())
}

// ---- repository ------------------------------------------------------------

/// Like viewset's `DefaultRepo`, but only caches single items.
///
/// `DefaultRepo` also caches list pages, and its `create_in_tx` /
/// `update_in_tx` / `delete_in_tx` (which `Service` always uses) never clear
/// that list cache, so lists would stay stale for the 30 minute TTL after any
/// write made through the service layer. Lists here always hit Postgres.
#[derive(Clone)]
pub struct BlogRepo<E: Entity> {
    db: PgPool,
    cache: Arc<dyn Store<E::Id, E>>,
}

impl<E: Entity + DeserializeOwned> BlogRepo<E> {
    pub fn new(db: PgPool, cache_factory: impl CacheFactory) -> Self {
        Self {
            db,
            cache: cache_factory.new_cache(&format!("{}_items", E::TABLE), Duration::from_secs(300)),
        }
    }
}

impl<E: Entity> Repository for BlogRepo<E> {
    type Entity = E;

    fn database(&self) -> &PgPool {
        &self.db
    }

    fn cache(&self) -> Arc<dyn Store<E::Id, E> + Send + Sync> {
        self.cache.clone()
    }
}

/// Paginated list of `E` rows where a UUID `column` equals `value`.
///
/// `extra` is an optional static SQL predicate (e.g. `"status = 'published'"`)
/// and `default_order` a static `ORDER BY` body; `?sort=`, `?page=` and
/// `?page_size=` are honoured the same way viewset does. `column`, `extra`
/// and `default_order` must never come from user input.
pub async fn list_where_uuid<E: Entity>(
    db: &PgPool,
    column: &'static str,
    value: Uuid,
    extra: Option<&'static str>,
    default_order: &'static str,
    q: &Filters,
) -> Result<Page<E>, sqlx::Error> {
    let mut where_sql = format!(" WHERE {column} = $1");
    if let Some(col) = E::SOFT_DELETE_COLUMN {
        where_sql.push_str(&format!(" AND {col} IS NULL"));
    }
    if let Some(extra) = extra {
        where_sql.push_str(&format!(" AND {extra}"));
    }

    let pagination = PaginationParams::from_query(q);

    let clauses: Vec<String> = q
        .get("sort")
        .map(|s| PaginationParams::parse_sort(s))
        .unwrap_or_default()
        .into_iter()
        .filter(|(field, _)| E::SORTABLE.contains(&field.as_str()))
        .map(|(field, dir)| {
            let dir = match dir {
                viewset::SortDirection::Asc => "ASC",
                viewset::SortDirection::Desc => "DESC",
            };
            format!("{field} {dir}")
        })
        .collect();
    let order = if clauses.is_empty() {
        default_order.to_string()
    } else {
        clauses.join(", ")
    };

    let select = format!(
        "SELECT {} FROM {}{where_sql} ORDER BY {order} LIMIT $2 OFFSET $3",
        E::COLUMNS.join(", "),
        E::TABLE
    );
    let items = sqlx::query_as::<_, E>(sqlx::AssertSqlSafe(select))
        .bind(value)
        .bind(pagination.limit as i64)
        .bind(pagination.offset as i64)
        .fetch_all(db)
        .await?;

    let total: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM {}{where_sql}",
        E::TABLE
    )))
    .bind(value)
    .fetch_one(db)
    .await?;

    Ok(Page::new(items, &pagination, total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugify_basic() {
        assert_eq!(slugify("Hello, World!"), "hello-world");
        assert_eq!(slugify("  --Rust & Actix--  "), "rust-actix");
        assert_eq!(slugify("***"), "");
    }

    #[test]
    fn slug_validation() {
        assert!(valid_slug("my-first-post-2"));
        assert!(!valid_slug("-bad"));
        assert!(!valid_slug("Bad"));
        assert!(!valid_slug(""));
    }

    #[test]
    fn text_validation() {
        assert!(require_text("title", "ok", 5).is_ok());
        assert!(require_text("title", "   ", 5).is_err());
        assert!(require_text("title", "toolong", 5).is_err());
    }

    #[test]
    fn admin_bit() {
        let a = Authority { user: Uuid::nil(), namespace: Uuid::nil(), role: 1u128 << ADMIN_BIT };
        assert!(is_admin(&a));
        let b = Authority { role: 0b1111, ..a.clone() };
        assert!(!is_admin(&b));
    }
}
