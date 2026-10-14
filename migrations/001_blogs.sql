-- Idempotent: applied on every start by BlogsModule::new.

CREATE TABLE IF NOT EXISTS blog_categories (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT        NOT NULL,
    slug        TEXT        NOT NULL UNIQUE,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS blog_posts (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    author_id    UUID        NOT NULL,
    category_id  UUID        REFERENCES blog_categories (id) ON DELETE SET NULL,
    title        TEXT        NOT NULL,
    slug         TEXT        NOT NULL UNIQUE,
    excerpt      TEXT,
    body         TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'draft'
                 CHECK (status IN ('draft', 'published', 'archived')),
    published_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS blog_posts_author_idx   ON blog_posts (author_id)   WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS blog_posts_category_idx ON blog_posts (category_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS blog_posts_feed_idx     ON blog_posts (published_at DESC)
    WHERE status = 'published' AND deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS blog_comments (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    post_id    UUID        NOT NULL REFERENCES blog_posts (id) ON DELETE CASCADE,
    author_id  UUID        NOT NULL,
    parent_id  UUID        REFERENCES blog_comments (id) ON DELETE CASCADE,
    body       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS blog_comments_post_idx ON blog_comments (post_id, created_at) WHERE deleted_at IS NULL;
