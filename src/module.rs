use crate::models::category::{CategoryService, CategoryViewSet};
use crate::models::comment::{CommentService, CommentViewSet};
use crate::models::post::{PostService, PostViewSet};
use crate::support::BlogRepo;
use actix_web::web;
use ferrumec::event::Outbox;
use ferrumec::{Infra, Module};
use std::sync::Arc;
use viewset::{DefaultViewSet, ViewSet};

/// The blogs service: categories, posts and comments mounted under
/// `/{namespace}` (`/blogs` when registered as `("blogs", BlogsModule)`).
pub struct BlogsModule {
    categories: Arc<CategoryViewSet>,
    posts: Arc<PostViewSet>,
    comments: Arc<CommentViewSet>,
    // Dropping the outbox stops its publisher task, so the module owns it.
    _outbox: Arc<Outbox>,
}

impl Module for BlogsModule {
    async fn new(infra: impl Infra) -> Result<Self, Box<dyn std::error::Error>> {
        let db = infra.database();
        sqlx::raw_sql(include_str!("../migrations/001_blogs.sql"))
            .execute(&db)
            .await?;

        let outbox = Arc::new(Outbox::new(db.clone(), infra.event_stream()).await?);
        let cache = infra.cache_factory();

        let categories = DefaultViewSet(CategoryService::new(BlogRepo::new(db.clone(), cache.clone())));
        let posts = PostViewSet::new(PostService::new(
            BlogRepo::new(db.clone(), cache.clone()),
            outbox.clone(),
        ));
        let comments = CommentViewSet::new(CommentService::new(
            BlogRepo::new(db.clone(), cache),
            outbox.clone(),
        ));

        Ok(Self {
            categories: Arc::new(categories),
            posts: Arc::new(posts),
            comments: Arc::new(comments),
            _outbox: outbox,
        })
    }

    fn configure(self: Arc<Self>, service_conf: &mut web::ServiceConfig, namespace: &str) {
        let this = self.clone();
        service_conf.service(
            web::scope(&format!("/{namespace}")).configure(move |cfg| {
                this.categories.clone().configure(cfg, "/categories");
                // Posts before comments: `/posts/by-slug/{slug}` must be
                // registered ahead of `/posts/{post_id}/comments`.
                this.posts.clone().configure(cfg, "/posts");
                this.comments.clone().configure(cfg, "/comments");
            }),
        );
    }
}
