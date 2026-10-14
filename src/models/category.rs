use crate::support::{ApiResult, BlogRepo, require_text, slugify, valid_slug};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgExecutor;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;
use viewset::{ApiError, DefaultViewSet, Entity, Service};

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Category {
    pub id: Uuid,
    pub name: String,
    pub slug: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCategory {
    pub name: String,
    /// Derived from `name` when omitted.
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateCategory {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl Entity for Category {
    type Id = Uuid;
    type CreateDto = CreateCategory;
    type UpdateDto = UpdateCategory;
    type ResponseDto = Category;

    const TABLE: &'static str = "blog_categories";
    const COLUMNS: &'static [&'static str] = &["id", "name", "slug", "description", "created_at"];
    const SEARCHABLE: &'static [&'static str] = &["name", "description"];
    const SORTABLE: &'static [&'static str] = &["name", "created_at"];
    const FILTERABLE: &'static [&'static str] = &["slug"];

    fn id(&self) -> Uuid {
        self.id
    }

    async fn insert<'e, E>(dto: CreateCategory, exec: E) -> Result<Self, sqlx::Error>
    where
        E: PgExecutor<'e> + Send,
    {
        let slug = dto.slug.unwrap_or_else(|| slugify(&dto.name));
        sqlx::query_as::<_, Category>(
            "INSERT INTO blog_categories (name, slug, description) VALUES ($1, $2, $3) \
             RETURNING id, name, slug, description, created_at",
        )
        .bind(dto.name)
        .bind(slug)
        .bind(dto.description)
        .fetch_one(exec)
        .await
    }

    async fn update<'e, E>(id: &Uuid, dto: UpdateCategory, exec: E) -> Result<Self, sqlx::Error>
    where
        E: PgExecutor<'e> + Send,
    {
        sqlx::query_as::<_, Category>(
            "UPDATE blog_categories SET \
                name = COALESCE($2, name), \
                slug = COALESCE($3, slug), \
                description = COALESCE($4, description) \
             WHERE id = $1 \
             RETURNING id, name, slug, description, created_at",
        )
        .bind(id)
        .bind(dto.name)
        .bind(dto.slug)
        .bind(dto.description)
        .fetch_one(exec)
        .await
    }
}

pub struct CategoryService {
    repo: BlogRepo<Category>,
}

impl CategoryService {
    pub fn new(repo: BlogRepo<Category>) -> Self {
        Self { repo }
    }
}

async fn slug_taken(
    tx: &mut Transaction<'_, Postgres>,
    slug: &str,
    except: Option<Uuid>,
) -> ApiResult<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM blog_categories WHERE slug = $1 AND id IS DISTINCT FROM $2)",
    )
    .bind(slug)
    .bind(except)
    .fetch_one(&mut **tx)
    .await?)
}

#[async_trait]
impl Service for CategoryService {
    type Repository = BlogRepo<Category>;

    fn repository(&self) -> &Self::Repository {
        &self.repo
    }

    async fn before_create(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        mut dto: CreateCategory,
    ) -> ApiResult<CreateCategory> {
        require_text("name", &dto.name, 80)?;
        let slug = dto.slug.take().unwrap_or_else(|| slugify(&dto.name));
        if !valid_slug(&slug) {
            return Err(ApiError::Validation("invalid `slug`".into()));
        }
        if slug_taken(tx, &slug, None).await? {
            return Err(ApiError::Conflict(format!("slug `{slug}` is already in use")));
        }
        dto.slug = Some(slug);
        Ok(dto)
    }

    async fn before_update(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        id: &Uuid,
        dto: UpdateCategory,
    ) -> ApiResult<UpdateCategory> {
        if let Some(name) = &dto.name {
            require_text("name", name, 80)?;
        }
        if let Some(slug) = &dto.slug {
            if !valid_slug(slug) {
                return Err(ApiError::Validation("invalid `slug`".into()));
            }
            if slug_taken(tx, slug, Some(*id)).await? {
                return Err(ApiError::Conflict(format!("slug `{slug}` is already in use")));
            }
        }
        Ok(dto)
    }
}

/// Plain CRUD: reads are public, writes are gated by `permissions.json`.
pub type CategoryViewSet = DefaultViewSet<CategoryService>;
