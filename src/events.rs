//! Events published by the blogs service (through the transactional outbox,
//! so they exist if and only if the database change committed).

use chrono::{DateTime, Utc};
use ferrumec::EventType;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PRODUCER: &str = "blogs";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostPublished {
    pub post_id: Uuid,
    pub author_id: Uuid,
    pub title: String,
    pub slug: String,
    pub published_at: Option<DateTime<Utc>>,
}
impl EventType for PostPublished {
    const SUBJECT: &'static str = "blogs.post.published";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostUnpublished {
    pub post_id: Uuid,
    pub author_id: Uuid,
}
impl EventType for PostUnpublished {
    const SUBJECT: &'static str = "blogs.post.unpublished";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostDeleted {
    pub post_id: Uuid,
}
impl EventType for PostDeleted {
    const SUBJECT: &'static str = "blogs.post.deleted";
}

/// The post author is also placed in the event audience so a notification
/// service can target them without a lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentCreated {
    pub comment_id: Uuid,
    pub post_id: Uuid,
    pub post_author_id: Uuid,
    pub author_id: Uuid,
    pub parent_id: Option<Uuid>,
}
impl EventType for CommentCreated {
    const SUBJECT: &'static str = "blogs.comment.created";
}
