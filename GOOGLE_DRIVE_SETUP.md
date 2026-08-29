# Google Drive Setup Guide — Customer Data Pipeline

This document covers **everything** needed to connect Airflow to Google Drive
so it can automatically read `customers_raw.csv` from your Drive account.
Follow it top to bottom, in order — nothing skipped.

---

## Why this is needed at all

Your Airflow DAG runs on a schedule, unattended, with no human clicking
"log in." It cannot use your personal Gmail login (that requires a browser
and 2FA prompts). Instead, Google lets you create a **service account** — a
robot identity that has its own email address and its own password (a JSON
key file), which code can use to authenticate automatically, forever, with
no human involved.

So the setup has two halves:
1. **Create the robot account** (Google Cloud Console)
2. **Give the robot account permission to see your file** (Google Drive)

---

## Part 1 — Create a Google Cloud project

1. Go to **https://console.cloud.google.com/**
2. If you've never used it before, Google will prompt you to create a
   project. Name it anything — we used **`airflow-pipeline`**.
3. Make sure the new project is selected (check the project switcher at
   the top of the page, next to the Google Cloud logo).

---

## Part 2 — Enable the Google Drive API

By default, a new Google Cloud project cannot talk to Drive at all — the
API has to be turned on explicitly.

1. In the top search bar, type **"Google Drive API"**.
2. Click on it in the results.
3. Click the blue **Enable** button.
4. Wait a few seconds for it to activate.

---

## Part 3 — Create the service account (the "robot")

1. Go to **IAM & Admin → Service Accounts** (left sidebar).
2. Click **+ Create Service Account** (top of the page).
3. **Step 1 — Create service account**:
   - Name: anything descriptive. We used **`airflow-drive-reader`**.
   - Click **Create and Continue**.
4. **Step 2 — Permissions (optional)**: **Leave this completely empty.**
   Do not select a role. This service account only needs to read one
   Drive file — that permission is granted separately, directly in Google
   Drive's Share dialog (Part 5 below), not here.
   - Click **Continue**.
5. **Step 3 — Principals with access (optional)**: **Also leave this
   empty.**
   - Click **Done**.
6. You'll land back on the Service Accounts list and see a green
   confirmation toast: **"Service account created."**

> **Common mistake we hit**: it's tempting to fill in Step 2/3 since they're
> right there on screen. Don't — they're for granting access to *other*
> Google Cloud resources (like Cloud Storage buckets), which this project
> doesn't use. Leaving them blank is correct.

---

## Part 4 — Generate the JSON key file

This is the actual "password" the robot account will use.

1. Click on the service account you just created (in the list),
   e.g. `airflow-drive-reader@airflow-pipeline-XXXXXX.iam.gserviceaccount.com`.
2. Click the **Keys** tab near the top.
3. Click **Add Key → Create new key**.
4. Choose **JSON** (should be selected by default).
5. Click **Create**.
6. A `.json` file downloads automatically to your computer (usually to
   `~/Downloads`). **This file is a secret / password — never share it,
   never commit it to git, never post it anywhere.**

### Rename and place the file correctly

This step tripped us up twice during setup, so follow it exactly:

1. Locate the downloaded file (something like
   `airflow-pipeline-505504-22e498a0fb90.json`).
2. **Rename it to exactly**: `gdrive_service_account.json`
   - ⚠️ Watch out for double extensions. If your file manager already
     shows `.json` as the extension and you type `gdrive_service_account.json`
     into a rename box that keeps the old extension, you can end up with
     `gdrive_service_account.json..json` by accident — this happened to us.
     After renaming, check the filename has **exactly one** `.json` at the
     end.
3. Move the renamed file into your project's `config/` folder, so it sits
   at:
   ```
   airflow_customer_pipeline/config/gdrive_service_account.json
   ```
4. Confirm in your file explorer / VS Code that `config/` contains exactly
   one file named `gdrive_service_account.json` (plus the `.gitkeep`
   placeholder that was already there).

### Find the robot's email address (you'll need this next)

1. Open the `.json` file in any text editor (VS Code, TextEdit, Notes).
2. Find the field `"client_email"`.
3. Copy the value — it looks like:
   `airflow-drive-reader@airflow-pipeline-505504.iam.gserviceaccount.com`
4. This is **not a real inbox** — it's an identifier, not something you can
   email. Keep it handy for the next part.

---

## Part 5 — Share the actual file with the robot account

Google Cloud now has a robot account, but Google Drive doesn't know
anything about it yet. This step connects the two.

1. Open **Google Drive** in your browser (drive.google.com).
2. Find `customers_raw.csv` in your Drive.
3. Right-click it → **Share** (or click the file, then the Share icon).
4. In the **"Add people, groups, and calendar events"** box, paste the
   `client_email` address you copied above.
5. **Set the permission level to `Viewer`** — not Editor. The pipeline only
   ever reads this file; it never needs to modify it. Click the dropdown
   next to the email and choose Viewer if it defaults to Editor.
6. You can **uncheck "Notify people"** — since this is a robot account, not
   a real inbox, the notification email would just bounce. This is
   optional and harmless either way.
7. Click **Send** (or **Share**).

> **Why this step is required even if your file is "Anyone with the link"**:
> general link-sharing settings don't automatically include service
> accounts. The robot needs to be explicitly listed as a person with
> access, exactly like you'd add a human collaborator.

---

## Part 6 — Get the file's ID

Your DAG identifies the file by its unique Drive ID, not by filename.

1. In Google Drive, right-click `customers_raw.csv` → **Share** (or **Get link**).
2. Copy the link. It looks like:
   ```
   https://drive.google.com/file/d/1kALm-0ALQGv59jYSBksRw-LD_TOdrofh/view?usp=drive_link
   ```
3. The **file ID** is the part between `/d/` and `/view`:
   ```
   1kALm-0ALQGv59jYSBksRw-LD_TOdrofh
   ```
4. The `?usp=...` part at the end is just how Google tags the link source —
   it's not part of the ID and doesn't matter.

---

## Part 7 — Put these values into Airflow

Once Airflow is running (see the companion Airflow guide), go to
**Admin → Variables** in the Airflow UI and set:

| Variable key | Value |
|---|---|
| `gdrive_customer_file_id` | `1kALm-0ALQGv59jYSBksRw-LD_TOdrofh` (your file ID from Part 6) |
| `gdrive_service_account_file` | `/opt/airflow/config/gdrive_service_account.json` |

**Do not put these values directly into `config.py`.** The code already has
placeholder fallbacks (like `"REPLACE_ME_FILE_ID"`) that only get used if
you forget to set the real Variable — the actual values always belong in
the Airflow UI, never hard-coded into the Python files.

---

## Full checklist (tick these off in order)

- [ ] Google Cloud project created
- [ ] Google Drive API enabled
- [ ] Service account created (no roles/permissions added)
- [ ] JSON key downloaded
- [ ] JSON key renamed to exactly `gdrive_service_account.json`
- [ ] JSON key placed in `config/gdrive_service_account.json`
- [ ] `client_email` copied from the JSON file
- [ ] `customers_raw.csv` shared with that `client_email`, set to **Viewer**
- [ ] File ID copied from the Drive share link
- [ ] `gdrive_customer_file_id` Variable set in Airflow
- [ ] `gdrive_service_account_file` Variable set in Airflow

---

## Troubleshooting

**Task `check_file_in_drive` fails with "File not found"**
→ Almost always means the file wasn't shared with the service account's
`client_email`, or the file ID is wrong/mistyped. Re-check Part 5 and 6.

**Task fails with "Service account file not found"**
→ The JSON key isn't at `config/gdrive_service_account.json` inside the
container. Check the filename for a double extension (`.json..json`) and
confirm the `config/` folder is correctly mounted as a Docker volume
(see the Airflow guide's `docker-compose.yaml` section).

**You accidentally shared the file as Editor instead of Viewer**
→ Not a security emergency, but fix it: open Share settings on the file
again, click the permission dropdown next to the service account's email,
change it to Viewer.
