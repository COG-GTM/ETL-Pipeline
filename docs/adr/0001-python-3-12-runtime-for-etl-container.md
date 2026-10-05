# 0001. Move the ETL container runtime from Python 3.8 to Python 3.12

- **Status:** Proposed
- **Date:** 2026-10-05
- **ARB ticket:** TO BE CREATED
- **Authors:** Devin (daily vulnerability-remediation automation), on behalf of vanessa.salas
- **Owning team:** COG-GTM / ETL-Pipeline maintainers
- **Related ADRs:** none (first ADR in this repository)

## Context

The ETL pipeline image is built `FROM python:3.8`. Python 3.8 reached end-of-life in October 2024
and receives no security fixes. On that interpreter, `pip install -r requirements.txt` resolves
`boto3`/`botocore` to the last 3.8-compatible releases, which pin `urllib3<1.27`, so the image ships
`urllib3 1.26.x`. Snyk reports 6 High findings against that version (CVE-2025-66471, CVE-2025-66418,
CVE-2026-21441 and related SNYK-PYTHON-URLLIB3-* advisories). The fixed release, `urllib3 2.8.0`,
requires Python >= 3.9, so **no remediation exists without changing the runtime**.

The repository's own CI (`.github/workflows/sonarqube-scan.yml`) already runs on Python 3.11, and
`devin.rules` documents "Python 3.8+". Dependencies are unpinned (`pandas`, `boto3`,
`psycopg2-binary`, `python-dotenv`), all of which publish wheels for 3.12. A fresh install on
Python 3.12 was scanned with Snyk and reported 0 vulnerable paths.

ARB triggers: T7 (runtime version upgrade, Python 3.8 -> 3.12, single service). The heuristic
detector also flagged T1 (Dockerfile touched) and T3 (requirements.txt touched); both are false
positives — the Dockerfile and the `urllib3` dependency already existed, only the base image tag and
a version floor changed. No new service, data store, vendor, or network boundary is introduced.

## Decision

We will build the ETL container `FROM python:3.12-slim` and add `urllib3>=2.8.0` to
`requirements.txt` as a security floor so the fix cannot silently regress.

## Alternatives considered

| Alternative | Pros | Cons | Why rejected |
| --- | --- | --- | --- |
| Do nothing | No change risk | Ships EOL interpreter + 6 High CVEs in the HTTP stack used for S3 uploads | Unacceptable security posture; no upstream fix on 3.8 |
| Pin `urllib3==1.26.x` latest | Minimal diff | 1.26 line is EOL; CVEs are only fixed in 2.8.0 | Does not remediate |
| Bump to `python:3.9` (smallest step that allows urllib3 2.8) | Smaller jump | 3.9 is EOL as of Oct 2025; would need another ADR within months | Short-lived |
| Bump to `python:3.11` (matches CI) | Matches SonarQube CI | 3.11 security-only since 2024; 3.12 is the current long-support line | 3.12 preferred; 3.11 is acceptable fallback if 3.12 breaks |

## Architecture

```mermaid
C4Container
    title ETL Pipeline (unchanged topology; runtime image only)
    Person(op, "Operator / scheduler")
    System_Boundary(b, "ETL-Pipeline") {
        Container(etl, "etl container", "Python 3.12-slim (was 3.8), pandas/boto3", "Extract from PostgreSQL, transform, load CSV to S3")
        ContainerDb(pg, "PostgreSQL", "postgres:15 (docker-compose)", "Vehicle sales & service data")
    }
    System_Ext(s3, "Amazon S3", "Destination bucket for cleaned CSVs")
    Rel(op, etl, "docker run / python main.py")
    Rel(etl, pg, "TCP 5432 / password from .env")
    Rel(etl, s3, "HTTPS (boto3 + urllib3 2.8) / AWS credentials from env")
```

## Non-functional requirements

| NFR | Target | How met |
| --- | --- | --- |
| Availability SLO | N/A — batch job, no serving SLO | unchanged |
| p95 latency | N/A — batch job | unchanged |
| RPO / RTO | TBD — owner to confirm before ARB | unchanged by this ADR |
| Peak load | unchanged (same tables, same code) | unchanged |
| Scaling model | single container run | unchanged |
| Data retention | unchanged (S3 bucket policy) | unchanged |

## Security & compliance

- **Data classification:** unchanged (vehicle sales/service records; owner to confirm whether PII).
- **Encryption at rest:** unchanged (S3 / PostgreSQL volume settings not touched).
- **Encryption in transit:** HTTPS to S3 via boto3; urllib3 2.8.0 removes the vulnerable 1.26 code paths.
- **AuthN / AuthZ:** unchanged (AWS credentials and DB password via environment / `.env`).
- **Secrets:** unchanged.
- **Audit logging:** unchanged.
- **Data residency / regions:** unchanged.
- **Policy sections satisfied:** supported-runtime requirement (EOL interpreter removed).
- **Threats considered:** urllib3 request smuggling / decompression / redirect issues listed above; EOL interpreter with no CVE fixes.

## Cost

| Item | Assumption | Monthly estimate |
| --- | --- | --- |
| Container image | `3.12-slim` is ~150 MB smaller than `python:3.8` full image | $0 (slight registry/storage saving) |
| **Total** | | $0 |

## Operations

- **On-call rotation:** TBD — owner to confirm before ARB
- **Runbook:** `README.md` / `devin.rules` (local run instructions)
- **Dashboards / alarms:** none specific; unchanged
- **Rollback plan:** revert the two-line change; previous image tag is still pullable.
- **Migration / cut-over plan:** rebuild image on merge; no data migration.

## Policy exceptions requested

| Rule | Resource | Justification | Compensating control | Expiry |
| --- | --- | --- | --- | --- |
| none | | | | |

## Consequences

- Positive: removes 6 High CVEs; supported interpreter; smaller image.
- Negative / risks: `python:3.12-slim` lacks build toolchain — all four dependencies ship manylinux wheels so no compilation is needed; pandas major version may float (unpinned, pre-existing risk).
- Follow-ups: consider pinning dependency versions / adding a lockfile so scans are reproducible.

## Open questions

- Owning team / on-call rotation and RPO/RTO for the batch job (not documented in the repo).
