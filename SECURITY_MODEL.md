# Threat model — COG-GTM/ETL-Pipeline

_Status: drafted from the code by Devin for the Devin security scan. Maintainers have not yet confirmed the attacker profiles or accepted risks; items marked unconfirmed are assumptions. Edit this file to correct them, and later scans and finding triage will use it._

## System overview
A Python batch ETL job plus a "Zero-Touch ETL" demo framework.
- `main.py` runs at import time: reads DB + AWS creds from `.env` (`python-dotenv`), extracts a fixed SQL join from Postgres/Redshift (`src/extract.py`, `pd.read_sql`, no user input in the query), dedupes (`src/transform.py`), and uploads a CSV to the hard-coded `s3://cognition-devin/auto_oem/etl/vehicle_sales_deduped.csv` (`src/load_data_to_s3.py`, boto3, `us-west-2`).
- `src/` zero-touch modules (profiler, consolidator, quality, model_designer, pipeline_generator, orchestrator, domain_learner) operate on local CSV/JSON/XML files and in-memory DataFrames; they generate DDL text (`src/profiler/schema_detector.py`, `src/model_designer/target_model_designer.py`) and Python pipeline source (`PipelineBuilder.generate_pipeline_code` in `src/pipeline_generator/pipeline_builder.py`, which interpolates source paths/names into code) plus reports. Nothing in the repo executes the generated DDL/code itself; whoever runs it downstream inherits any injected identifiers or code.
- `demo/run_demo.py` drives those modules over `demo/sample_data/` (interactive `input()` prompts unless `DEMO_AUTO_RUN=1`), writing `demo/domain_knowledge/domain_knowledge.json`.
- CI: `.github/workflows/sonarqube-scan.yml` runs SonarCloud on PRs and, on Quality Gate failure, calls `https://api.devin.ai/v1/sessions` with `DEVIN_API_KEY`, embedding SonarCloud issue messages, changed file names and the branch name into the prompt, and posts a PR comment with `GITHUB_TOKEN`.
- Runtime: `Dockerfile` (python:3.8, `COPY . /app`, runs as root); `docker-compose.yml` local Postgres 15 (`postgres`/`mysecretpassword`, port 5432 published on all interfaces).
- No infrastructure-as-code repository was provided; S3 bucket policy, IAM and Redshift networking are unknown.

## Components
| Name | Code | Notes |
|---|---|---|
| **etl-core** | `main.py`, `src/extract.py`, `src/transform.py`, `src/load_data_to_s3.py` | Production data path: DB → S3 |
| **zero-touch-framework** | `src/profiler/`, `src/consolidator/`, `src/quality/`, `src/model_designer/`, `src/pipeline_generator/`, `src/orchestrator/`, `src/domain_learner/` | File parsing (CSV/JSON/XML via `xml.etree.ElementTree`), DDL/report generation |
| **demo** | `demo/` (run_demo, playbooks, sample_data, zero_touch_etl.html) | Demo driver and sample data |
| **ci-remediation** | `.github/workflows/sonarqube-scan.yml`, `sonar-project.properties` | Holds `DEVIN_API_KEY`, `SONAR_TOKEN`, `GITHUB_TOKEN` (pull-requests: write) |
| **runtime-config** | `Dockerfile`, `docker-compose.yml`, `.env.example`, `init/init.sql`, `requirements.txt` | Container, local DB, unpinned deps |

## Trust boundaries and entry points
1. **Operator → job**: whoever runs `main.py`/container controls `.env`; assumed trusted.
2. **Source DB → job**: rows from Postgres/Redshift flow into a CSV; DB content assumed semi-trusted (could carry CSV-formula payloads into downstream spreadsheets).
3. **Job → S3**: write with static access keys; bucket ACL/policy unknown.
4. **Source files → zero-touch framework**: CSV/JSON/XML paths passed by the caller; whether these ever come from untrusted parties is unknown (demo only uses bundled samples).
5. **GitHub PR authors → CI workflow**: reachable by anyone who can open a PR or trigger `workflow_dispatch` (repo write). PR-controlled text (branch name, file names, code that produces Sonar messages) reaches a shell step and an LLM prompt that runs with push rights to the PR branch.
6. **Local Postgres container**: port 5432 published; default password.

## Assets
- AWS access keys and DB credentials (`.env`, env vars).
- `DEVIN_API_KEY`, `SONAR_TOKEN`, `GITHUB_TOKEN` in Actions.
- Vehicle sales data incl. `buyer_name` (PII) and prices; written to S3 as CSV.
- Repository integrity (branches Devin pushes to).

## Attacker profiles (draft — unconfirmed)
- External contributor opening a PR (fork or same-repo) to abuse CI / the Devin trigger.
- Supplier of a malicious source file to the profiler/consolidator.
- Someone with read access to S3, logs or the container image.
- Network-adjacent attacker reaching a published local Postgres.

## Existing controls
- `.env` is git-ignored; `.env.example` holds no values.
- Extraction SQL is static (no string-built queries).
- Workflow uses `env:` indirection for most inputs and `jq --arg` for JSON; loop guard via `[devin-fix]`/author check; `permissions: contents: read, pull-requests: write`.
- `pull_request` (not `pull_request_target`) trigger, so fork PRs do not get secrets.

## Out of scope / accepted risk
_None confirmed by the user yet._

## Open questions
- Is there an infra repo (S3/IAM/Redshift)?
- Which attackers matter; do source files for the framework ever come from untrusted parties?
- Is `demo/` and the local docker-compose password accepted risk?
- Who consumes the S3 CSV (spreadsheets → CSV injection relevance)?
