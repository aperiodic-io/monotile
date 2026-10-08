# Security

Please report vulnerabilities privately, to **security@aperiodic.io** or through GitHub's
[private vulnerability reporting](https://github.com/aperiodic-io/monotile/security/advisories/new),
not in a public issue. You will hear back within three working days.

## What to know when you run brrrrr

- `brrrrr serve` listens on `127.0.0.1` by default. On another address, give it a token
  (`--token` or `BRRRRR_TOKEN`): HTTP clients send `Authorization: Bearer <token>`, PostgreSQL
  clients use it as their password. Without one, anyone who reaches the ports may read every table
  and write to any.
- Put TLS in front of it (a reverse proxy, or a load balancer) when it is reached over a network
  you do not trust; the PostgreSQL protocol's token is a cleartext password.
- Queries read any file the process may read, and any object store its credentials reach. Run it
  as a user that may read only what its users should.
- brrrrr-core has no `unsafe` code (`unsafe_code = "forbid"`); the one `unsafe` block is the Arrow C
  stream handed to Python (`crates/brrrrr-py`).
