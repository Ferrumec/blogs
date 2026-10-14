# blogs

Ferrumec blogs microservice built on the `ferrumec` and `viewset` crates.

- **Entities** (`src/models`): categories, posts, comments. `Entity` is implemented by hand
  (plain `sqlx` queries), `Service` hooks do validation and publish events, `ViewSet`s expose REST.
- **Module**: `BlogsModule` implements `ferrumec::Module`; it runs `migrations/001_blogs.sql`
  (idempotent) on start and is mounted under `/blogs`.
- **Events** (transactional outbox, same DB transaction as the write): `blogs.post.published`,
  `blogs.post.unpublished`, `blogs.post.deleted`, `blogs.comment.created` (post author in the audience).
- **Auth**: `permissions.json` + ferrumec's `Permissions` middleware (via `launch`) put an `Authority`
  in the request; the author id always comes from it. Owners or users with role bit 20 (`ADMIN_BIT`)
  can modify/delete others' content.

## Run

```
cp .env.example .env   # DATABASE_URL
cargo run              # listens on 127.0.0.1:8080, needs permissions.json in the cwd
cargo test
```

## API (all under `/blogs`)

| Method | Path | Notes | Perm bit |
|---|---|---|---|
| GET | `/posts` | published only; `?search=&sort=-published_at&page=&page_size=` | – |
| GET | `/posts/{id}`, `/posts/by-slug/{slug}` | published only | – |
| POST | `/posts` | `{title, body, category_id?, slug?, excerpt?, status?}` | 0 |
| PUT/PATCH | `/posts/{id}` | owner/admin | 1 |
| DELETE | `/posts/{id}` | soft delete, owner/admin | 2 |
| POST | `/posts/{id}/publish` · `/unpublish` | owner/admin, emits event | 3 |
| GET | `/me/posts`, `/me/posts/{id}` | caller's posts incl. drafts | 4 |
| GET | `/categories`, `/categories/{id}`, `/categories/{id}/posts` | public | – |
| POST/PUT/PATCH/DELETE | `/categories[/{id}]` | | 5 |
| GET | `/posts/{post_id}/comments` | oldest first | – |
| POST | `/posts/{post_id}/comments` | `{body, parent_id?}`, published posts only | 6 |
| GET | `/comments/{id}` | | – |
| DELETE | `/comments/{id}` | soft delete, owner/admin | 7 |

## Design notes

- `BlogRepo` caches single rows only. viewset's `DefaultRepo` also caches list pages, but
  `Service::create/update/delete` go through `*_in_tx`, which never clear that list cache.
- viewset binds `?field=value` filters as TEXT, so UUID columns can't be filtered that way; the
  `/me`, `/categories/{id}/posts` and `/posts/{id}/comments` routes use a typed query instead.
- Post `status` is never read from an update body; it only changes via publish/unpublish.
