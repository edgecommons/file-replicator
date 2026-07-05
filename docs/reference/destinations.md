# Reference — destinations

Every backend implements one `Destination` trait (deliver → verify → complete, with resume + progress +
bandwidth), so behaviour is uniform and backends are feature-gated. `type` in an `egress` item selects the
backend; the field tables below are validated against `src/config.rs` (the `*Egress` structs). Full design
in [`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md).

An instance's `egress` is an **ordered list** of `N >= 1` entries. Each entry fans out independently and a
file completes (source deleted/archived) only once **every** entry has delivered and verified.

## Backends at a glance

| `type` | Feature (default?) | Resume | Parallelism | Verify | Notes |
|---|---|---|---|---|---|
| `local` | built-in (always) | temp + atomic rename | single stream | re-hash | subtree auto-created |
| `s3` | `dest-s3` (**on**) | multipart parts persisted | parallel parts + prefix | S3 flexible/trailing checksum + ETag | acceleration, unsigned-PUT, size-adaptive multipart |
| `sftp` | `dest-sftp` (off) | `APPE`/offset | single | local re-hash | host-key pinning; SSH key or password |
| `ftps` | `dest-ftps` (off) | `APPE`/offset | single | local re-hash | explicit (AUTH TLS) or implicit TLS |
| `http` | `dest-http` (off) | ranged `PUT` / `Content-Range` | single | local re-hash (2xx) | webhook / presigned; `PUT` resumable, `POST` single-shot |
| `azure` | `dest-azure` (off) | staged blocks | parallel blocks | local re-hash | crate maturity → off default |
| `gcs` | `dest-gcs` (off) | resumable session URI | chunked | CRC32C vs object metadata | crate maturity → off default |

Off-by-default backends join the build only when their `dest-*` cargo feature is enabled (CI builds with
all of them on). All are pure-Rust (rustls) — no OpenSSL/C toolchain.

**`checksumAlgorithm`** (every backend except `local`) defaults to `CRC32C`; the alternative is `SHA256`
(local always re-hashes). **`credentials`** everywhere is an optional `{"$secret": "name"}` vault reference;
the shape it must resolve to is noted per backend. Ambient plaintext secret fields are supported but
discouraged (and are redacted in any log/`Debug`/config dump).

---

## `local`

| Field | Type | Default | Notes |
|---|---|---|---|
| `path` | string | *(required)* | Destination root; the source subtree is recreated under it. |
| `fsync` | bool | `false` | `fsync` the written file before completion (destination-fs durability). |

Also covers **NFS/SMB shares** — point `path` at the mount.

## `s3`

| Field | Type | Default | Notes |
|---|---|---|---|
| `bucket` | string | *(required)* | |
| `prefix` | string | `""` | Object key = `prefix` + source-relative path. |
| `region` | string | — | |
| `endpointUrl` | string | — | MinIO / S3-compatible / GovCloud. |
| `credentials` | `$secret` | — | Omit → ambient provider chain (see below). |
| `storageClass` | string | — | e.g. `INTELLIGENT_TIERING`, `STANDARD_IA`. |
| `sse` | string | — | `AES256` or `aws:kms`. |
| `kmsKeyId` | string | — | Key for `sse: "aws:kms"`. |
| `accelerate` | bool | `false` | S3 Transfer Acceleration endpoint. |
| `unsignedPayload` | bool | `false` | `UNSIGNED-PAYLOAD` on simple PUTs over TLS (skip payload signing). |
| `checksumAlgorithm` | string | `CRC32C` | Flexible/trailing checksum; or `SHA256`. |
| `multipart.thresholdBytes` | int | — | Files larger than this use multipart; smaller = single `PutObject`. |
| `multipart.partSizeBytes` | int | — | Multipart part size. |
| `multipart.maxConcurrentParts` | int | — | Parallel parts in flight. |

**Credentials — ambient by default.** Omit `credentials` to inherit the platform provider chain:
Greengrass TokenExchangeService device role, Kubernetes IRSA / env / node role, or HOST env / shared
profile / instance role. Provide `{"$secret": "name"}` to resolve explicit credentials from the vault
(never logged). Least-privilege IAM: scope to `bucket/prefix/*` with the `PutObject`/multipart actions (+
KMS if SSE-KMS). MinIO / S3-compatible endpoints work via `endpointUrl` with no new code.

## `sftp`

| Field | Type | Default | Notes |
|---|---|---|---|
| `host` | string | *(required)* | |
| `port` | int | `22` | |
| `baseDir` | string | `""` | Remote root; the remote path preserves the source subtree under it. |
| `username` | string | — | Ambient (prefer `credentials`). |
| `password` | string | — | Ambient password (redacted in `Debug`). |
| `privateKey` | string | — | Path to a private key **file** on local disk. |
| `passphrase` | string | — | For an encrypted `privateKey` (redacted). |
| `credentials` | `$secret` | — | Resolves to `{username,password}` or `{username,privateKey,passphrase}`. |
| `hostKey` | string | — | Pinned server host key (OpenSSH `algo base64[ comment]` line). |
| `insecureAcceptAnyHostKey` | bool | `false` | **Fail-open** escape hatch — see below. |
| `tempSuffix` | string | `.part` | Hidden upload-in-progress suffix before the publish rename. |
| `fsync` | bool | `false` | Best-effort `fsync@openssh.com` before rename, if the server supports it. |
| `checksumAlgorithm` | string | `CRC32C` | Local re-hash (SFTP has no content-checksum echo). |

**Host-key policy.** With no `hostKey` pinned and `insecureAcceptAnyHostKey` unset (the default),
the SSH handshake is **refused** (fail-closed). Set `insecureAcceptAnyHostKey: true` only as an explicit,
documented escape hatch — it disables man-in-the-middle protection (a server presenting its own key is
accepted; under password auth the credential could be captured).

## `ftps`

| Field | Type | Default | Notes |
|---|---|---|---|
| `host` | string | *(required)* | |
| `port` | int | `21` | Explicit AUTH TLS on the plain control port; implicit FTPS conventionally uses `990`. |
| `baseDir` | string | `""` | Remote root; source subtree preserved under it. |
| `username` | string | — | Ambient (prefer `credentials`). |
| `password` | string | — | Ambient (redacted). |
| `credentials` | `$secret` | — | Resolves to `{username,password}`. |
| `explicitTls` | bool | `true` | `true` = explicit AUTH TLS on the plain port; `false` = implicit TLS. |
| `passive` | bool | `true` | PASV vs active mode. |
| `tempSuffix` | string | — | Upload-in-progress suffix before the publish rename. |
| `checksumAlgorithm` | string | `CRC32C` | Local re-hash. |

## `http` (HTTP/HTTPS)

| Field | Type | Default | Notes |
|---|---|---|---|
| `url` | string | *(required)* | Base URL; the source-relative path is joined onto its path (`.../upload` + `a/b.csv` → `.../upload/a/b.csv`). |
| `method` | string | `PUT` | `PUT` (resumable) or `POST` (single-shot; resume forced off). |
| `headers` | object | `{}` | Static headers sent on every request (e.g. a fixed API-key header). |
| `bearerToken` | string | — | Ambient bearer token (redacted). |
| `username` / `password` | string | — | Ambient HTTP Basic (password redacted). |
| `credentials` | `$secret` | — | Resolves to `{"bearerToken":"…"}` or `{"username":"…","password":"…"}`. |
| `resumable` | bool | `true` | Ranged `PUT` in `chunkBytes` segments with a persisted checkpoint; forced `false` for `POST`. |
| `chunkBytes` | int | `8 MiB` | Ranged-`PUT` chunk size. |
| `checksumAlgorithm` | string | `CRC32C` | Local re-hash (generic HTTP has no universal content-checksum echo). |

## `azure` (Azure Blob)

| Field | Type | Default | Notes |
|---|---|---|---|
| `account` | string | *(required)* | Storage account name (also the default cloud location). |
| `container` | string | *(required)* | |
| `prefix` | string | `""` | Blob name prefix; source subtree preserved. |
| `endpointUrl` | string | — | Custom blob endpoint — Azurite (`http://127.0.0.1:10000/devstoreaccount1`) or a sovereign cloud. |
| `accountKey` | string | — | Ambient account key (redacted). |
| `connectionString` | string | — | Ambient; only `AccountKey` is extracted (`account`/`endpointUrl` stay authoritative). Redacted. |
| `credentials` | `$secret` | — | Resolves to `{"accountKey":"…"}` or `{"connectionString":"…"}`. |
| `blocks.thresholdBytes` | int | — | Files larger than this use staged blocks; smaller = single `Put Blob`. |
| `blocks.blockSizeBytes` | int | — | Staged-block size. |
| `checksumAlgorithm` | string | `CRC32C` | Local re-hash (Azure's per-block MD5/CRC64 is not a whole-object echo). |

## `gcs` (Google Cloud Storage)

| Field | Type | Default | Notes |
|---|---|---|---|
| `bucket` | string | *(required)* | |
| `prefix` | string | `""` | Object name prefix; source subtree preserved. |
| `endpointUrl` | string | — | Custom JSON/upload endpoint — fake-gcs-server (`http://127.0.0.1:4443`); absent = the public GCS JSON API. |
| `accessToken` | string | — | Ambient OAuth2 access token (redacted). |
| `anonymous` | bool | `false` | Send no `Authorization` header (fake-gcs-server, or an `allUsers`-public bucket). |
| `credentials` | `$secret` | — | Resolves to `{"accessToken":"…"}`. This backend authenticates via a bearer access token only (no service-account JWT / ADC discovery). |
| `chunkBytes` | int | `8 MiB` | Resumable-upload chunk size, rounded down to the nearest 256 KiB (GCS alignment). |
| `checksumAlgorithm` | string | `CRC32C` | Local re-hash; `CRC32C` is also compared directly against the object's `crc32c` metadata on verify (no re-download). |

---

## Also possible (no new code)
- **MinIO / S3-compatible** → the `s3` destination with `endpointUrl`.
- **NFS/SMB share** → the `local` destination pointing at a mount.
