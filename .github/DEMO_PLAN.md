# SonarQube + Devin Auto-Remediation Demo Plan

## Overview

This demo showcases an automated security remediation pipeline:

1. A developer opens a PR with code that contains a **security vulnerability**
2. **SonarCloud** scans the PR and detects the vulnerability
3. The Quality Gate **fails**, triggering a GitHub Action
4. The workflow automatically creates a **Devin AI session** to remediate the issue
5. Devin fixes the vulnerability, pushes to the PR branch, and posts a comment
6. The workflow re-runs, SonarCloud re-scans, and the Quality Gate **passes**

---

## Setup Requirements

### GitHub Secrets (must be configured in repo Settings > Secrets)

| Secret | Description |
|--------|-------------|
| `SONAR_TOKEN` | SonarCloud token for the `cog-gtm` organization |
| `DEVIN_API_KEY` | Devin API key for creating remediation sessions. Use a dedicated service user whose Devin organization has **no secrets** and whose GitHub access is limited to this repository. |

### GitHub Variables (Settings > Secrets and variables > Actions > Variables)

| Variable | Description |
|----------|-------------|
| `DEVIN_TRUSTED_AUTHORS` | JSON array of GitHub logins allowed to trigger Devin, e.g. `["alice","bob"]`. A session is only created when the PR author, the user who triggered or dispatched the run, and the user who pushed the scanned commit are all listed. Unset means Devin is never triggered. |

### Trust model

The remediation session runs with Devin's credentials, so the workflow treats all PR content as untrusted:

- Only same-repository PRs from allowlisted authors can start a session; fork PRs and `workflow_dispatch` inputs that do not match the PR's real head branch fail the job.
- The prompt contains no PR-derived text (no branch name, file names, or SonarCloud messages) — only the PR number, the scanned commit SHA, and SonarCloud rule / issue keys. Devin looks the issues up itself.
- Devin is told not to execute anything from the branch (tests, linters, hooks, build scripts) and the session is created with `secret_ids: []`.

### SonarCloud

The project `COG-GTM_ETL-Pipeline` is already registered on SonarCloud under the `cog-gtm` organization. The `sonar-project.properties` file in the repo root configures the scan.

---

## Running the Demo

### Step 1: Create a branch with a vulnerability

Create a new branch off `main` and introduce an **SQL injection vulnerability** in `src/extract.py`. This is a classic, universally-understood security flaw that SonarCloud reliably detects.

**Example change** — replace the safe hardcoded query with one that uses string formatting with an untrusted parameter:

```python
# In src/extract.py — replace extract_vehicle_sales_data with this version:

def extract_vehicle_sales_data(dbname, host, port, user, password, region_filter=None):
    """
    Extract and transform vehicle sales and service data.
    - Joins vehicles, dealerships, sales_transactions, and service_records
    - Optionally filters by dealership region
    """
    conn = connect_to_postgres(dbname, host, port, user, password)

    # VULNERABLE: region_filter is concatenated directly into the SQL query
    query = """
    SELECT
        v.vin,
        v.model,
        v.year,
        d.name AS dealership_name,
        d.region,
        s.sale_date,
        s.sale_price,
        s.buyer_name,
        COALESCE(sr.service_date, NULL) AS service_date,
        COALESCE(sr.service_type, 'Unknown') AS service_type,
        COALESCE(sr.service_cost, 0) AS service_cost
    FROM vehicles v
    JOIN dealerships d ON v.dealership_id = d.id
    LEFT JOIN sales_transactions s ON v.vin = s.vin
    LEFT JOIN service_records sr ON v.vin = sr.vin
    WHERE d.region = '%s'
    """ % region_filter

    df = pd.read_sql(query, conn)

    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')

    print("Extracted rows:", df.shape[0])
    return df
```

### Step 2: Open a PR

The presenter's GitHub login must be listed in the `DEVIN_TRUSTED_AUTHORS` variable, otherwise the quality gate still fails but no Devin session is created.

```bash
git checkout -b demo/add-region-filter
# Make the change above to src/extract.py
git add src/extract.py
git commit -m "feat: add region filter to vehicle sales extraction"
git push origin demo/add-region-filter
# Open a PR from demo/add-region-filter → main
```

### Step 3: Watch the automation

1. The `SonarQube Security Scan` workflow triggers on the PR
2. SonarCloud scans the code and detects the SQL injection (rule `python:S3649`)
3. The Quality Gate fails
4. The workflow creates a Devin session and posts a comment on the PR
5. Click the Devin session link in the PR comment to watch Devin work

### Step 4: Devin remediates

Devin will:
- Clone the repo, check out the PR branch and confirm it is still at the scanned commit
- Look up the SonarCloud findings by issue key
- Replace the string-formatted SQL with a parameterized query
- Commit with `[devin-fix]` tag and push
- Post a comment on the PR explaining the fix

### Step 5: Quality Gate passes

The push triggers the workflow again. This time:
- SonarCloud scans and finds no new vulnerabilities
- The Quality Gate passes
- The `[devin-fix]` tag prevents another Devin session from being created

---

## Why This Vulnerability Always Triggers

**SQL Injection via string formatting** (`python:S3649`) is one of SonarQube's highest-confidence rules. It fires reliably whenever:
- A SQL query is built using `%` string formatting, f-strings, or `.format()` with external input
- The input flows into `cursor.execute()`, `pd.read_sql()`, or similar database query functions

This makes it a perfect demo candidate — it is:
- **Deterministic**: Always detected, never a false negative
- **High severity**: Rated as a VULNERABILITY with CRITICAL impact
- **Easy to understand**: SQL injection is universally recognized
- **Easy to fix**: The remediation (parameterized queries) is straightforward

---

## Alternative Vulnerabilities for Variety

If you want to rotate the demo vulnerability, here are other reliable options:

### Option 2: Hardcoded Credentials
```python
# In any new file, e.g. src/config.py
DB_PASSWORD = "admin123"
AWS_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```
SonarCloud rule: `python:S2068` (Credentials should not be hard-coded)

### Option 3: Insecure XML Parsing (XXE)
```python
# In src/profiler/source_profiler.py — use vulnerable XML parser
import xml.etree.ElementTree as ET
tree = ET.parse(user_provided_path)  # No XXE protection
```
SonarCloud rule: `python:S2755` (XML parsers should not be vulnerable to XXE attacks)

### Option 4: Command Injection
```python
import os
def run_export(filename):
    os.system("pg_dump -f " + filename)  # Unsanitized input in shell command
```
SonarCloud rule: `python:S4721` (OS commands should not be vulnerable to injection attacks)

---

## Workflow Architecture

```
Developer opens PR
        │
        ▼
GitHub Action triggers
        │
        ▼
SonarCloud scans PR ──────────────────┐
        │                              │
        ▼                              │
Quality Gate check                     │
   │          │                        │
   PASS       FAIL                     │
   │          │                        │
   ▼          ▼                        │
  Done    Is latest commit             │
          from Devin?                  │
           │       │                   │
           YES     NO                  │
           │       │                   │
           ▼       ▼                   │
      Fail CI   Extract issues         │
      (human    from SonarCloud        │
       review)        │                │
                      ▼                │
               Create Devin session    │
               via API                 │
                      │                │
                      ▼                │
               Devin fixes code        │
               and pushes              │
                      │                │
                      ▼                │
               PR updated ─────────────┘
               (re-triggers scan)
```
