# Reference — destinations

Every backend implements one `Destination` trait (deliver → verify → complete, with resume + progress +
bandwidth), so behaviour is uniform and backends are feature-gated. Full design in
[`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md) §10–§11.

| Type | Phase | Feature | Resume | Parallelism | Verify | Notes |
|---|---|---|---|---|---|---|
| `local` | P1 | (always) | temp + atomic rename | single stream | re-hash | `path`, `fsync`; subtree auto-created |
| `s3` | P2 | `dest-s3` (joins default) | multipart parts persisted | parallel parts + prefix | S3 flexible/trailing checksum + ETag | acceleration, unsigned-PUT, size-adaptive |
| `sftp` / `ftps` | P5 | `dest-sftp` | `APPE`/offset | single | size + hash | |
| `http`/`https` | P5 | `dest-http` | `Content-Range`/resumable PUT | single/presigned | 2xx + `Content-MD5` | webhook / presigned URL |
| `azure` | P5 | `dest-azure` (off) | staged blocks | parallel blocks | MD5/CRC64 | crate maturity → off default |
| `gcs` | P5 | `dest-gcs` (off) | resumable session URI | chunked | CRC32C/MD5 | crate maturity → off default |

## S3 (P2)

Fields: `bucket`, `prefix`, `region`, `endpointUrl` (MinIO/S3-compat/govcloud), `storageClass`, `sse`
(`AES256`/`aws:kms`) + `kmsKeyId`, `accelerate` (Transfer Acceleration), `unsignedPayload` (skip payload
signing on simple PUTs over TLS), `checksumAlgorithm` (`CRC32C` default / `SHA256`),
`multipart.{thresholdBytes,partSizeBytes,maxConcurrentParts}`.

**Credentials — ambient by default.** Omit `credentials` to inherit the platform's provider chain:
Greengrass TokenExchangeService device role, Kubernetes IRSA / env / node role, or HOST env / shared
profile / instance role. Provide `{"$secret": "name"}` to resolve explicit credentials from the vault
(never logged). Least-privilege IAM: scope to `bucket/prefix/*` with the `PutObject`/multipart actions (+
KMS if SSE-KMS). DESIGN §11.5.

## Also possible (no new code)
- **MinIO / S3-compatible** → the `s3` destination with `endpointUrl`.
- **NFS/SMB share** → the `local` destination pointing at a mount.
