use crate::events::{PRODUCER, PostDeleted, PostPublished, PostUnpublished};
use crate::support::{
    ApiResult, BlogRepo, internal, list_where_uuid, parse_uuid,
    require_text, slugify, valid_slug,
};
use actix_web::{HttpResponse, web};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ferrumec::Event;
use ferrumec::event::Outbox;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgExecutor;
use sqlx::{FromRow, Postgres, Transaction};
use std::sync::Arc;
use uuid::Uuid;
use viewset::{ApiError, Entity, Filters, Repository, Service, ViewSet};

pub const DRAFT: &str = "draft";
pub const PUBLISHED: &str = "published";

const POST_COLUMNS: &str = "id, author_id, category_id, title, slug, excerpt, body, status, \
                            published_at, created_at, updated_at, deleted_at";

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Post {
    pub id: Uuid,
    pub author_id: Uuid,
    pub category_id: Option<Uuid>,
    pub title: String,
    pub slug: String,
    pub excerpt: Option<String>,
    pub body: String,
    pub status: String,
    pub published_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing)]
    pub deleted_at: Option<DateTime<Utc>>,
}

fn default_status() -> String {
    DRAFT.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePost {
    /// Always taken from the authenticated caller, never from the body.
    #[serde(skip_deserializing)]
    pub author_id: Uuid,
    #[serde(default)]
    pub category_id: Option<Uuid>,
    pub title: String,
    /// Derived from `title` when omitted.
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub excerpt: Option<String>,
    pub body: String,
    /// `draft` (default) or `published`. Use the publish endpoints afterwards.
    #[serde(default = "default_status")]
    pub status: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdatePost {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excerpt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// Never read from a request body: status only changes through the
    /// publish / unpublish endpoints, which also emit the matching events.
    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

impl Entity for Post {
    type Id = Uuid;
    type CreateDto = CreatePost;
    type UpdateDto = UpdatePost;
    type ResponseDto = Post;

    const TABLE: &'static str = "blog_posts";
    const COLUMNS: &'static [&'static str] = &[
        "id",
        "author_id",
        "category_id",
        "title",
        "slug",
        "excerpt",
        "body",
        "status",
        "published_at",
        "created_at",
        "updated_at",
        "deleted_at",
    ];
    const SEARCHABLE: &'static [&'static str] = &["title", "excerpt", "body"];
    const SORTABLE: &'static [&'static str] =
        &["created_at", "published_at", "updated_at", "title"];
    // Only text columns: viewset binds filter values as TEXT, which Postgres
    // will not compare to UUID columns (see the /me and /categories routes).
    const FILTERABLE: &'static [&'static str] = &["slug", "status"];
    const SOFT_DELETE_COLUMN: Option<&'static str> = Some("deleted_at");

    fn id(&self) -> Uuid {
        self.id
    }

    async fn insert<'e, E>(dto: CreatePost, exec: E) -> Result<Self, sqlx::Error>
    where
        E: PgExecutor<'e> + Send,
    {
        let slug = dto.slug.unwrap_or_else(|| slugify(&dto.title));
        sqlx::query_as::<_, Post>(sqlx::AssertSqlSafe(format!(
            "INSERT INTO blog_posts \
                (author_id, category_id, title, slug, excerpt, body, status, published_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, CASE WHEN $7 = 'published' THEN now() END) \
             RETURNING {POST_COLUMNS}"
        )))
        .bind(dto.author_id)
        .bind(dto.category_id)
        .bind(dto.title)
        .bind(slug)
        .bind(dto.excerpt)
        .bind(dto.body)
        .bind(dto.status)
        .fetch_one(exec)
        .await
    }

    async fn update<'e, E>(id: &Uuid, dto: UpdatePost, exec: E) -> Result<Self, sqlx::Error>
    where
        E: PgExecutor<'e> + Send,
    {
        sqlx::query_as::<_, Post>(sqlx::AssertSqlSafe(format!(
            "UPDATE blog_posts SET \
                category_id = COALESCE($2, category_id), \
                title       = COALESCE($3, title), \
                slug        = COALESCE($4, slug), \
                excerpt     = COALESCE($5, excerpt), \
                body        = COALESCE($6, body), \
                status      = COALESCE($7, status), \
                published_at = CASE WHEN COALESCE($7, status) = 'published' \
                                    THEN COALESCE(published_at, now()) \
                                    ELSE published_at END, \
                updated_at  = now() \
             WHERE id = $1 AND deleted_at IS NULL \
             RETURNING {POST_COLUMNS}"
        )))
        .bind(id)
        .bind(dto.category_id)
        .bind(dto.title)
        .bind(dto.slug)
        .bind(dto.excerpt)
        .bind(dto.body)
        .bind(dto.status)
        .fetch_one(exec)
        .await
    }
}

// ---- service ---------------------------------------------------------------

pub struct PostService {
    repo: BlogRepo<Post>,
    outbox: Arc<Outbox>,
}

impl PostService {
    pub fn new(repo: BlogRepo<Post>, outbox: Arc<Outbox>) -> Self {
        Self { repo, outbox }
    }

    async fn push<T: ferrumec::EventType>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        payload: T,
        post_author: Uuid,
    ) -> ApiResult<()> {
        let event = Event::new(payload)
            .with_producer(PRODUCER)
            .with_user_id(post_author);
        self.outbox.push(&event, tx).await.map_err(internal)
    }

    fn published_event(post: &Post) -> PostPublished {
        PostPublished {
            post_id: post.id,
            author_id: post.author_id,
            title: post.title.clone(),
            slug: post.slug.clone(),
            published_at: post.published_at,
        }
    }

    /// Moves a post to `status` and emits the matching event in the same
    /// transaction. Only `published` and `draft` (unpublish) are accepted.
    pub async fn set_status(&self, id: Uuid, status: &'static str) -> ApiResult<Post> {
        let mut tx = self.repo.transaction().await?;
        let dto = UpdatePost {
            status: Some(status.to_string()),
            ..Default::default()
        };
        let post = self.repo.update_in_tx(&mut tx, &id, dto).await?;
        if status == PUBLISHED {
            self.push(&mut tx, Self::published_event(&post), post.author_id)
                .await?;
        } else {
            let ev = PostUnpublished {
                post_id: post.id,
                author_id: post.author_id,
            };
            self.push(&mut tx, ev, post.author_id).await?;
        }
        tx.commit().await?;
        // update_in_tx dropped the cached row before commit; drop it again so a
        // concurrent read cannot leave the pre-commit row cached.
        let _ = self.repo.cache().delete(&id).await;
        Ok(post)
    }
}

async fn slug_taken(
    tx: &mut Transaction<'_, Postgres>,
    slug: &str,
    except: Option<Uuid>,
) -> ApiResult<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM blog_posts WHERE slug = $1 AND id IS DISTINCT FROM $2)",
    )
    .bind(slug)
    .bind(except)
    .fetch_one(&mut **tx)
    .await?)
}

async fn category_exists(tx: &mut Transaction<'_, Postgres>, id: Uuid) -> ApiResult<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM blog_categories WHERE id = $1)")
            .bind(id)
            .fetch_one(&mut **tx)
            .await?,
    )
}

#[async_trait]
impl Service for PostService {
    type Repository = BlogRepo<Post>;

    fn repository(&self) -> &Self::Repository {
        &self.repo
    }

    async fn before_create(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        mut dto: CreatePost,
    ) -> ApiResult<CreatePost> {
        require_text("title", &dto.title, 200)?;
        require_text("body", &dto.body, 200_000)?;
        if dto.status != DRAFT && dto.status != PUBLISHED {
            return Err(ApiError::Validation(
                "`status` must be `draft` or `published`".into(),
            ));
        }
        if let Some(cat) = dto.category_id {
            if !category_exists(tx, cat).await? {
                return Err(ApiError::Validation("unknown `category_id`".into()));
            }
        }
        match dto.slug.take() {
            Some(slug) => {
                if !valid_slug(&slug) {
                    return Err(ApiError::Validation("invalid `slug`".into()));
                }
                if slug_taken(tx, &slug, None).await? {
                    return Err(ApiError::Conflict(format!(
                        "slug `{slug}` is already in use"
                    )));
                }
                dto.slug = Some(slug);
            }
            None => {
                let mut slug = slugify(&dto.title);
                if slug.is_empty() {
                    slug = "post".into();
                }
                if slug_taken(tx, &slug, None).await? {
                    slug = format!("{slug}-{}", &Uuid::new_v4().simple().to_string()[..8]);
                }
                dto.slug = Some(slug);
            }
        }
        Ok(dto)
    }

    async fn after_create(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        post: Post,
    ) -> ApiResult<Post> {
        if post.status == PUBLISHED {
            self.push(tx, Self::published_event(&post), post.author_id)
                .await?;
        }
        Ok(post)
    }

    async fn before_update(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        id: &Uuid,
        dto: UpdatePost,
    ) -> ApiResult<UpdatePost> {
        if let Some(t) = &dto.title {
            require_text("title", t, 200)?;
        }
        if let Some(b) = &dto.body {
            require_text("body", b, 200_000)?;
        }
        if let Some(cat) = dto.category_id {
            if !category_exists(tx, cat).await? {
                return Err(ApiError::Validation("unknown `category_id`".into()));
            }
        }
        if let Some(slug) = &dto.slug {
            if !valid_slug(slug) {
                return Err(ApiError::Validation("invalid `slug`".into()));
            }
            if slug_taken(tx, slug, Some(*id)).await? {
                return Err(ApiError::Conflict(format!(
                    "slug `{slug}` is already in use"
                )));
            }
        }
        Ok(dto)
    }

    async fn after_delete(&self, tx: &mut Transaction<'_, Postgres>, id: &Uuid) -> ApiResult<()> {
        // The author is not known here; consumers key off `post_id`.
        let event = Event::new(PostDeleted { post_id: *id }).with_producer(PRODUCER);
        self.outbox.push(&event, tx).await.map_err(internal)
    }
}

// ---- viewset ---------------------------------------------------------------

pub struct PostViewSet {
    service: PostService,
    middleware: Arc<viewset::web::SessionMiddleware>,
}

impl PostViewSet {
    pub fn new(service: PostService,cache: impl ferrumec::CacheFactory) -> Self {
        let middleware = Arc::new(viewset::web::SessionMiddleware::new(cache));
        Self { service, middleware}
    }

    fn db(&self) -> &sqlx::PgPool {
        self.service.repository().database()
    }

    /// Public feed: published posts only, newest first.
    async fn list_public(
        self: Arc<Self>,
        q: web::Query<Filters>,
    ) -> actix_web::Result<HttpResponse> {
        let mut q = q.into_inner();
        q.insert("status".into(), PUBLISHED.into());
        let page = self.service.list(q).await?;
        Ok(HttpResponse::Ok().json(page))
    }

    async fn by_slug(self: Arc<Self>, slug: web::Path<String>) -> actix_web::Result<HttpResponse> {
        let mut q = Filters::new();
        q.insert("slug".into(), slug.into_inner());
        q.insert("status".into(), PUBLISHED.into());
        let post = self
            .service
            .repository()
            .list_one(&q)
            .await
            .map_err(ApiError::from)?;
        Ok(HttpResponse::Ok().json(post))
    }

    /// A single post by id; drafts and archived posts are not public.
    async fn retrieve_public(
        self: Arc<Self>,
        id: web::Path<String>,
    ) -> actix_web::Result<HttpResponse> {
        let post = self.service.retrieve(parse_uuid(&id)?).await?;
        if post.status != PUBLISHED {
            return Err(ApiError::NotFound.into());
        }
        Ok(HttpResponse::Ok().json(post))
    }

    async fn create(
        self: Arc<Self>,
        caller: viewset::web::Identity,
        body: web::Json<CreatePost>,
    ) -> actix_web::Result<HttpResponse> {
        let mut dto = body.into_inner();
        dto.author_id = caller.sub;
        let post = self.service.create(dto).await?;
        Ok(HttpResponse::Created().json(post))
    }

    async fn update(
        self: Arc<Self>,
        _caller: viewset::web::Identity,
        id: web::Path<String>,
        body: web::Json<UpdatePost>,
    ) -> actix_web::Result<HttpResponse> {
        let id = parse_uuid(&id)?;
        let post = self.service.update(id, body.into_inner()).await?;
        Ok(HttpResponse::Ok().json(post))
    }

    async fn delete(
        self: Arc<Self>,
        caller: viewset::web::Identity,
        id: web::Path<uuid::Uuid>,
    ) -> actix_web::Result<HttpResponse> {
        let author_id = caller.sub;
        let id = id.into_inner();
        if let Err(e) = self.service.repo.list_one(viewset::filters!(author_id,id)).await{
            match e {
                sqlx::Error::RowNotFound => return Err(actix_web::error::ErrorForbidden("Not Allowed")),
                _ => return Err(actix_web::error::ErrorInternalServerError("Server error")),
            }
        };
        self.service.delete(id).await?;
        Ok(HttpResponse::NoContent().finish())
    }

    async fn change_status(
        self: Arc<Self>,
        _caller: viewset::web::Identity,
        id: web::Path<String>,
        status: &'static str,
    ) -> actix_web::Result<HttpResponse> {
        let id = parse_uuid(&id)?;
        let existing = self.service.retrieve(id).await?;
        if existing.status == status {
            // Already there: no write and no duplicate event.
            return Ok(HttpResponse::Ok().json(existing));
        }
        let post = self.service.set_status(id, status).await?;
        Ok(HttpResponse::Ok().json(post))
    }

    /// The caller's own posts, drafts included.
    async fn list_mine(
        self: Arc<Self>,
        caller: viewset::web::Identity,
        q: web::Query<Filters>,
    ) -> actix_web::Result<HttpResponse> {
        let page = list_where_uuid::<Post>(
            self.db(),
            "author_id",
            caller.sub,
            None,
            "created_at DESC",
            &q,
        )
        .await
        .map_err(ApiError::from)?;
        Ok(HttpResponse::Ok().json(page))
    }

    async fn retrieve_mine(
        self: Arc<Self>,
        caller: viewset::web::Identity,
        id: web::Path<String>,
    ) -> actix_web::Result<HttpResponse> {
        let post = self.service.retrieve(parse_uuid(&id)?).await?;
        if post.author_id != caller.sub {
            return Err(ApiError::NotFound.into());
        }
        Ok(HttpResponse::Ok().json(post))
    }

    async fn list_by_category(
        self: Arc<Self>,
        category_id: web::Path<String>,
        q: web::Query<Filters>,
    ) -> actix_web::Result<HttpResponse> {
        let page = list_where_uuid::<Post>(
            self.db(),
            "category_id",
            parse_uuid(&category_id)?,
            Some("status = 'published'"),
            "published_at DESC",
            &q,
        )
        .await
        .map_err(ApiError::from)?;
        Ok(HttpResponse::Ok().json(page))
    }
}

impl ViewSet for PostViewSet {
    type Service = PostService;

    fn service(&self) -> &PostService {
        &self.service
    }

    fn configure(self: Arc<Self>, cfg: &mut web::ServiceConfig, path: &str) {
        // Registration order matters: literal segments before `{id}`.
        let vs = self.clone();

        // Public endpoints: no Identity extractor.
        cfg.service(web::resource(path).route(web::get().to({
            let vs = vs.clone();
            move |q: web::Query<Filters>| vs.clone().list_public(q)
        })));

        cfg.service(
            web::resource(format!("{path}/by-slug/{{slug}}")).route(web::get().to({
                let vs = vs.clone();
                move |slug: web::Path<String>| vs.clone().by_slug(slug)
            })),
        );

        cfg.service(
            web::resource(format!("{path}/{{id}}")).route(web::get().to({
                let vs = vs.clone();
                move |id: web::Path<String>| vs.clone().retrieve_public(id)
            })),
        );

        cfg.service(
            web::resource(format!("{path}/categories/{{category_id}}/posts")).route(web::get().to(
                {
                    let vs = vs.clone();
                    move |cid: web::Path<String>, q: web::Query<Filters>| {
                        vs.clone().list_by_category(cid, q)
                    }
                },
            )),
        );

        // Authenticated endpoints: grouped under one scope.
        let vs = self.clone();

        cfg.service(
            web::scope(path)
                .wrap(self.middleware.clone())
                .route(
                    "",
                    web::post().to({
                        let vs = vs.clone();
                        move |req: viewset::web::Identity, body: web::Json<CreatePost>| {
                            vs.clone().create(req, body)
                        }
                    }),
                )
                .route(
                    "/{id}",
                    web::put().to({
                        let vs = vs.clone();
                        move |req: viewset::web::Identity,
                              id: web::Path<String>,
                              body: web::Json<UpdatePost>| {
                            vs.clone().update(req, id, body)
                        }
                    }),
                )
                .route(
                    "/{id}",
                    web::patch().to({
                        let vs = vs.clone();
                        move |req: viewset::web::Identity,
                              id: web::Path<String>,
                              body: web::Json<UpdatePost>| {
                            vs.clone().update(req, id, body)
                        }
                    }),
                )
                .route(
                    "/{id}",
                    web::delete().to({
                        let vs = vs.clone();
                        move |req: viewset::web::Identity, id: web::Path<uuid::Uuid>| {
                            vs.clone().delete(req, id)
                        }
                    }),
                )
                .route(
                    "/{id}/publish",
                    web::post().to({
                        let vs = vs.clone();
                        move |req: viewset::web::Identity, id: web::Path<String>| {
                            vs.clone().change_status(req, id, PUBLISHED)
                        }
                    }),
                )
                .route(
                    "/{id}/unpublish",
                    web::post().to({
                        let vs = vs.clone();
                        move |req: viewset::web::Identity, id: web::Path<String>| {
                            vs.clone().change_status(req, id, DRAFT)
                        }
                    }),
                ),
        );

        // Authenticated endpoints with paths outside the main `path` prefix.
        cfg.service(
            web::scope("")
                .wrap(self.middleware.clone())
                .route(
                    "/me/posts",
                    web::get().to({
                        let vs = vs.clone();
                        move |req: viewset::web::Identity, q: web::Query<Filters>| {
                            vs.clone().list_mine(req, q)
                        }
                    }),
                )
                .route(
                    "/me/posts/{id}",
                    web::get().to({
                        let vs = vs.clone();
                        move |req: viewset::web::Identity, id: web::Path<String>| {
                            vs.clone().retrieve_mine(req, id)
                        }
                    }),
                ),
        );
    }
}

/// Lets `comment.rs` check that a post is visible without depending on
/// `PostService` internals.
pub async fn published_post_author(
    tx: &mut Transaction<'_, Postgres>,
    post_id: Uuid,
) -> ApiResult<Uuid> {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT author_id FROM blog_posts \
         WHERE id = $1 AND status = 'published' AND deleted_at IS NULL",
    )
    .bind(post_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::NotFound)
}
