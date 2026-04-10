# SECTION 1: IMPORTS
# what do i need?
# csv to make the closed_accounts.csv to compare to recon_file.csv, closed_accounts_report.csv, recon_file_amended.csv, run_log.csv
# os, time, datetime, path fro, pathlib

import csv
import os 
import time
from datetime import datetime
from pathlib import Path

# SECTION 2: CONFIGURATION
# what files am I working with?: closed_accounts.csv to compare to recon_file.csv

RECON_FILE = "recon_file.csv"
CLOSED_ACCOUNTS = "closed_accounts.csv"
CLOSED_ACCOUNT_REPORT = "closed_accounts_reports.csv"
RECON_FILE_AMENDED = "recon_file_amended.csv" 
LOG_FILE = "run_log.csv"

# SECTION 3: SAFETY CHECKS
# what could go wrong before i start?
#column missing for 'active' status in recon_file.csv or closed_accounts.csv or missing account numbers in both, no config.json to get the rules from
if not os.path.exists(RECON_FILE):
    print(f"ERROR: Reconciliation file '{RECON_FILE}' not found. ")
    print(f"Validation could not run. Autoload not triggered. ")
    exit(1)
if not os.path.exists(CLOSED_ACCOUNTS):
     print(f"ERROR: Closed accounts file '{CLOSED_ACCOUNTS}' not found.")
     print(f"Validation could not run. Autoload not triggered. ")
     exit(1)

#SECTION 4: INITIALISATION
#need to introduce time & counters
run_start = time.time()
run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

removed_rows = [] #need for amended recon file
clean_rows = [] #need for amended recon file
total_rows = 0

# SECTION 5: LOAD REFERENCE DATA & loop through it
# what do i need to read in? > the recon_file.csv and closed_accounts.csv
closed_account_ids = set()
                         
with open(CLOSED_ACCOUNTS, newline="", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        closed_account_ids.add(row["account_id"])
# SECTION 6: COMPARISON & SORTING
# how do i find the closed accounts?
# need to read both the recon_file.csv then compare each line / account to whats in closed_accounts.csv, "r" go to 'status' column and read if active or not, then remove that line
with open(RECON_FILE, newline="", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    recon_fieldnames = reader.fieldnames
    for row in reader:
        total_rows += 1
        if row["account_id"] in closed_account_ids:
            removed_rows.append(row)
        else:
            clean_rows.append(row)

file_empty = len(clean_rows) == 0 # all rows removed, need closed account report? not sure, technically the recon file contains all of the closed accounts but creating a closed account report and calling it such may be clearer to operator
rows_removed = len(removed_rows) > 0 # some rows removed, need amended recon file and the closed account report 
file_clean = len(removed_rows) == 0 # nothing removed, no amended recon file needed nor closed account report

# SECTION 7
# what do i do with them once i find them?
# lines will need to be removed -- googled how and it's recommended Pandas? but could also use csv import? do they need to be stored somewhere?" like []? 


if rows_removed:
    with open (RECON_FILE_AMENDED, "w", newline="", encoding="utf-8") as file:
         writer = csv.DictWriter(file, fieldnames=recon_fieldnames)
         writer.writeheader()
         writer.writerows(clean_rows)

    with open (CLOSED_ACCOUNT_REPORT, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter (file, fieldnames=recon_fieldnames)
            writer.writeheader()
            writer.writerows(removed_rows)
    print(f"The {RECON_FILE} contained closed accounts. An amended file has been produced, ready to load: {RECON_FILE_AMENDED}. ")
    print(f"A list of closed accounts has been created: {CLOSED_ACCOUNT_REPORT}.")

elif file_empty:
    with open (CLOSED_ACCOUNT_REPORT, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter (file, fieldnames=recon_fieldnames)
        writer.writeheader()
        writer.writerows(removed_rows)
    print(f"WARNING: All rows were removed. {RECON_FILE_AMENDED} has not been created.")
    print(f"All accounts in the recon file were closed. See: {CLOSED_ACCOUNT_REPORT}")
    print("Do not trigger autoload.")
     
else:
    print(f"The {RECON_FILE} has been sent to autoload. ")

# SECTION 8: SUMMARY
# what files do i produce?
# need to produce a file validation summary and run log
run_duration = round(time.time() - run_start, 2)

print("----- FILE  SUMMARY -----")
print(f"Total rows in file: {total_rows}")
print(f"Total rows removed: {len(removed_rows)}")
print(f"Total rows in the {RECON_FILE_AMENDED}: {len(clean_rows)}")
print(f"Accounts removed: {len(removed_rows)}")
print(f"Removal report: {CLOSED_ACCOUNT_REPORT}")
# SECTION 9: RUN LOG
# what does the operator need to know?
# what accounts were removed and why, auto-load still triggered
log_exists = os.path.exists(LOG_FILE)

with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
    fieldnames = ["timestamp", "filename", "total_rows", 
              "removed_rows", "clean_rows", "status", "duration_seconds"]
    writer = csv.DictWriter(f, fieldnames=fieldnames)

    if not log_exists:
        writer.writeheader()

    writer.writerow({
        "timestamp": run_timestamp,
        "filename": RECON_FILE,
        "total_rows": total_rows,
        "removed_rows": len(removed_rows),
        "clean_rows": len(clean_rows),
        "status": "FILE_EMPTY" if file_empty else "ROWS_REMOVED" if rows_removed else "FILE_CLEAN",
        "duration_seconds": run_duration
        })
