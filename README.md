# Recon File Validation Pipeline

A pre-load tool that identifies and removes closed accounts from DCA 
reconciliation files before they cause downstream processing failures.

## The Problem

DCAs (Debt Collection Agencies) send weekly reconciliation files listing 
all active accounts they are currently working on. Occasionally these files 
contain accounts that have since been closed or returned to client. When 
loaded, the process fails with a vague, unreadable error message — requiring 
manual investigation by an analytics team to identify the failing accounts, 
followed by back-and-forth with the DCA to amend and resend.

As the intermediary between DCAs and clients, validation must occur at the 
point of receipt. Prevention at source is the only available control, as the 
upstream DCA process cannot be directly governed.

This tool runs pre-load validation checks, automatically identifies closed 
accounts, removes them, and produces a clean file ready to load — alongside 
a full audit trail of what was removed and why.

## Business Decisions

- Closed accounts are removed automatically and a clean file produced, but 
loading remains a manual operator step. This is a deliberate soft automation 
— preparation is automated, but a human gate remains before the file enters 
the system.

- Three distinct operational scenarios are handled separately because each 
requires different operator action. A single PASS/FAIL would obscure 
information the operator needs to respond correctly.

- In the all-rows-removed scenario, no amended file is produced. An empty 
file loading to the system would cause its own downstream failures.

- In production, the closed accounts reference data would be queried directly 
from the accounts database using SQL. A static CSV is used here to simulate 
that query result. The comparison logic is identical regardless of data source.

## How It Works

- Reconciliation file received from DCA
- Reference dataset of closed accounts loaded into memory as a set
- Each account checked against closed accounts reference
- Three scenarios handled:
  - Some accounts removed → amended file and removal report produced
  - All accounts removed → removal report only, do not load
  - No accounts removed → original file is clean, safe to load
- Every run logged to persistent audit trail

## How To Run

```bash
python main.py
```

```
main.py                      # main validation script
recon_file.csv               # sample incoming recon file
closed_accounts.csv          # reference dataset
recon_file_amended.csv       # generated on run - clean file
closed_accounts_reports.csv  # generated on run - audit
run_log.csv                  # persistent run history
```

## Future Improvements

- Live database connection replacing static CSV reference file
- Command line arguments for dynamic file input
- Airflow DAG integration for scheduled Monday morning runs
- Email or Slack notification to DCA on removal with account list
- Column validation checks before processing begins
