use crate::events::{CommentCreated, PRODUCER};
use crate::models::post::published_post_author;
use crate::support::{ApiResult, BlogRepo, internal, list_where_uuid, require_text};
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

const COMMENT_COLUMNS: &str = "id, post_id, author_id, parent_id, body, created_at, deleted_at";

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Comment {
    pub id: Uuid,
    pub post_id: Uuid,
    pub author_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub body: String,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing)]
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateComment {
    /// Taken from the URL (`/posts/{post_id}/comments`), never from the body.
    #[serde(skip_deserializing)]
    pub post_id: Uuid,
    /// Taken from the authenticated caller, never from the body.
    #[serde(skip_deserializing)]
    pub author_id: Uuid,
    /// Reply to another comment on the same post.
    #[serde(default)]
    pub parent_id: Option<Uuid>,
    pub body: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateComment {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

impl Entity for Comment {
    type Id = Uuid;
    type CreateDto = CreateComment;
    type UpdateDto = UpdateComment;
    type ResponseDto = Comment;

    const TABLE: &'static str = "blog_comments";
    const COLUMNS: &'static [&'static str] = &[
        "id",
        "post_id",
        "author_id",
        "parent_id",
        "body",
        "created_at",
        "deleted_at",
    ];
    const SORTABLE: &'static [&'static str] = &["created_at"];
    const SOFT_DELETE_COLUMN: Option<&'static str> = Some("deleted_at");

    fn id(&self) -> Uuid {
        self.id
    }

    async fn insert<'e, E>(dto: CreateComment, exec: E) -> Result<Self, sqlx::Error>
    where
        E: PgExecutor<'e> + Send,
    {
        sqlx::query_as::<_, Comment>(sqlx::AssertSqlSafe(format!(
            "INSERT INTO blog_comments (post_id, author_id, parent_id, body) \
             VALUES ($1, $2, $3, $4) RETURNING {COMMENT_COLUMNS}"
        )))
        .bind(dto.post_id)
        .bind(dto.author_id)
        .bind(dto.parent_id)
        .bind(dto.body)
        .fetch_one(exec)
        .await
    }

    async fn update<'e, E>(id: &Uuid, dto: UpdateComment, exec: E) -> Result<Self, sqlx::Error>
    where
        E: PgExecutor<'e> + Send,
    {
        sqlx::query_as::<_, Comment>(sqlx::AssertSqlSafe(format!(
            "UPDATE blog_comments SET body = COALESCE($2, body) \
             WHERE id = $1 AND deleted_at IS NULL RETURNING {COMMENT_COLUMNS}"
        )))
        .bind(id)
        .bind(dto.body)
        .fetch_one(exec)
        .await
    }
}

pub struct CommentService {
    repo: BlogRepo<Comment>,
    outbox: Arc<Outbox>,
}

impl CommentService {
    pub fn new(repo: BlogRepo<Comment>, outbox: Arc<Outbox>) -> Self {
        Self { repo, outbox }
    }
}

#[async_trait]
impl Service for CommentService {
    type Repository = BlogRepo<Comment>;

    fn repository(&self) -> &Self::Repository {
        &self.repo
    }

    async fn before_create(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        dto: CreateComment,
    ) -> ApiResult<CreateComment> {
        require_text("body", &dto.body, 5_000)?;
        // Comments are only allowed on published, non-deleted posts.
        published_post_author(tx, dto.post_id).await?;
        if let Some(parent) = dto.parent_id {
            let same_post: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM blog_comments \
                 WHERE id = $1 AND post_id = $2 AND deleted_at IS NULL)",
            )
            .bind(parent)
            .bind(dto.post_id)
            .fetch_one(&mut **tx)
            .await?;
            if !same_post {
                return Err(ApiError::Validation(
                    "`parent_id` must be a comment on the same post".into(),
                ));
            }
        }
        Ok(dto)
    }

    async fn after_create(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        comment: Comment,
    ) -> ApiResult<Comment> {
        let post_author = published_post_author(tx, comment.post_id).await?;
        let event = Event::new(CommentCreated {
            comment_id: comment.id,
            post_id: comment.post_id,
            post_author_id: post_author,
            author_id: comment.author_id,
            parent_id: comment.parent_id,
        })
        .with_producer(PRODUCER)
        .with_user_id(comment.author_id)
        .add_audience(post_author);
        self.outbox.push(&event, tx).await.map_err(internal)?;
        Ok(comment)
    }

    async fn before_update(
        &self,
        _tx: &mut Transaction<'_, Postgres>,
        _id: &Uuid,
        dto: UpdateComment,
    ) -> ApiResult<UpdateComment> {
        if let Some(b) = &dto.body {
            require_text("body", b, 5_000)?;
        }
        Ok(dto)
    }
}

pub struct CommentViewSet {
    service: CommentService,
}

impl CommentViewSet {
    pub fn new(service: CommentService) -> Self {
        Self { service }
    }

    /// 404 if the comment does not exist, 403 if `caller` is not its author.
    async fn ensure_owner(&self, id: Uuid, caller: Uuid) -> ApiResult<()> {
        let comment = self.service.retrieve(id).await?;
        if comment.author_id != caller {
            return Err(ApiError::Forbidden);
        }
        Ok(())
    }

    async fn list_for_post(
        self: Arc<Self>,
        post_id: web::Path<Uuid>,
        q: web::Query<Filters>,
    ) -> actix_web::Result<HttpResponse> {
        let page = list_where_uuid::<Comment>(
            self.service.repository().database(),
            "post_id",
            post_id.into_inner(),
            None,
            "created_at ASC",
            &q,
        )
        .await
        .map_err(ApiError::from)?;
        Ok(HttpResponse::Ok().json(page))
    }

    async fn create(
        self: Arc<Self>,
        caller: viewset::web::Identity,
        post_id: web::Path<Uuid>,
        body: web::Json<CreateComment>,
    ) -> actix_web::Result<HttpResponse> {
        let mut dto = body.into_inner();
        dto.post_id = post_id.into_inner();
        dto.author_id = caller.sub;
        let comment = self.service.create(dto).await?;
        Ok(HttpResponse::Created().json(comment))
    }

    async fn retrieve(
        self: Arc<Self>,
        id: web::Path<(Uuid, Uuid)>,
    ) -> actix_web::Result<HttpResponse> {
        let (_, comment) = id.into_inner();
        let comment = self.service.retrieve(comment).await?;
        Ok(HttpResponse::Ok().json(comment))
    }

    async fn delete(
        self: Arc<Self>,
        caller: viewset::web::Identity,
        id: web::Path<(Uuid, Uuid)>,
    ) -> actix_web::Result<HttpResponse> {
        let (_, id) = id.into_inner();
        self.ensure_owner(id, caller.sub).await?;
        self.service.delete(id).await?;
        Ok(HttpResponse::NoContent().finish())
    }
}

impl ViewSet for CommentViewSet {
    type Service = CommentService;

    fn service(&self) -> &CommentService {
        &self.service
    }

    /// `path` is the base for single-comment routes (`/comments/{id}`);
    /// the per-post list/create routes are fixed at `/posts/{post_id}/comments`.
    fn configure(self: Arc<Self>, cfg: &mut web::ServiceConfig, path: &str) {
        let vs = self.clone();
        cfg.service(
            web::scope(path)
                .route(
                    "",
                    web::get().to({
                        let vs = vs.clone();
                        move |pid: web::Path<Uuid>, q: web::Query<Filters>| {
                            vs.clone().list_for_post(pid, q)
                        }
                    }),
                )
                .route(
                    "",
                    web::post().to({
                        let vs = vs.clone();
                        move |caller: viewset::web::Identity,
                              pid: web::Path<Uuid>,
                              body: web::Json<CreateComment>| {
                            vs.clone().create(caller, pid, body)
                        }
                    }),
                )
                .service(
                    web::resource("/{comment}")
                        .route(web::get().to({
                            let vs = vs.clone();
                            move |id: web::Path<(Uuid, Uuid)>| vs.clone().retrieve(id)
                        }))
                        .route(web::delete().to({
                            let vs = vs.clone();
                            move |caller: viewset::web::Identity, id: web::Path<(Uuid, Uuid)>| {
                                vs.clone().delete(caller, id)
                            }
                        })),
                ),
        );
    }
}
