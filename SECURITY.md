# Security

Single-node URL shortener for trusted link creators.

- Writes + `/api/stats` require `Authorization: Bearer <RUSHORT_API_KEY>` (≥32 random ASCII chars). Rotate via env + restart; no overlap window.
- Redirects are public. Codes are sequential base62 IDs, not secrets. Don't use them for access control.
- Run behind Caddy/TLS per `deploy/`. Keep port 8080 on loopback. Set request body limits at the proxy too.
- Report vulnerabilities via a private GitHub Security Advisory or issue. Don't post exploit PoCs publicly before a fix.
- No retention/deletion API by design. Capacity is bounded (`--max-urls`); plan archival before it fills.
