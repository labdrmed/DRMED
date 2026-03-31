# DRMed Backend (Portal + Staff Upload API)

This backend keeps the website design/layout unchanged and adds a safer staff workflow:
- Portal patient login uses `Patient User ID + Secure PIN`.
- Staff can upload/report results via protected API.
- Backend writes Drive `fileId` to the sheet (no manual PDF link pasting).

## Sheet format (single sheet)

Use one tab (default: `Sheet1`) with these columns:

- Column A: `Patient User ID` (permanent account ID; same across visits)
- Column B: `Secure PIN`
- Column C: `Patient Name`
- Column D: `Control Number`
- Column E: `Test Name`
- Column F: `Test Date`
- Column G: `Status` (`Pending` or `Released`)
- Column H: `Released At` (timestamp)
- Column I: `Drive File ID` (auto-populated by backend)

Only these 9 columns are needed.

## One account, many control numbers (past results)

Use this rule:
- Control number changes per visit.
- Patient User ID stays the same forever.

Example:
- Visit 1: `PatientID=DRM-0001`, `Control=2026-0001`, test `CBC`
- Visit 2: `PatientID=DRM-0001`, `Control=2026-0007`, test `CHEMISTRY`

Patient logs in once using:
- `Patient User ID` + `Secure PIN`

Portal then returns all rows under that `Patient User ID` (all past visits/results).

## Google Cloud setup

1. Enable APIs in your GCP project:
   - Google Sheets API
   - Google Drive API
2. Create a service account and JSON key.
3. Share your Google Sheet with the service account email as `Editor`.
4. Share your private Drive results folder with the same service account email as `Editor`.

## Environment

Copy `.env.example` to `.env` and set:

- `SHEET_ID`
- `SHEET_NAME`
- `RESULT_STORAGE` (`drive` or `local`)
- `DRIVE_FOLDER_ID` (required only for `RESULT_STORAGE=drive`)
- `DRIVE_AUTH_MODE` (`service_account` or `oauth_user`) when using `RESULT_STORAGE=drive`
- `LOCAL_RESULTS_DIR` (used for `RESULT_STORAGE=local`)
- `ALLOWED_ORIGIN`
- `PORTAL_ACCESS_TOKEN_SECRET`
- `STAFF_GATE_USERS_JSON` (required for hidden staff login gate; supports password or scrypt hash)
- `STAFF_SESSION_TOKEN_SECRET` (staff session signing secret)
- `STAFF_SESSION_TTL_SECONDS` (default `43200` = 12 hours)
- `MAX_STAFF_BULK_ITEMS` (optional; max files per bulk request)
- `STAFF_LOCKOUT_MAX_ATTEMPTS`, `STAFF_LOCKOUT_WINDOW_MS`, `STAFF_LOCKOUT_DURATION_MS`, `STAFF_LOCKOUT_SCOPE`
- `STAFF_MFA_REQUIRED`, `STAFF_MFA_SECRETS_JSON` (optional TOTP MFA)
- `AUDIT_LOG_MAX_BYTES`, `AUDIT_LOG_MAX_DAYS` (log retention)
- `GOOGLE_APPLICATION_CREDENTIALS` (or `GOOGLE_SERVICE_ACCOUNT_JSON`)
- `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `GOOGLE_OAUTH_REFRESH_TOKEN` (required when `DRIVE_AUTH_MODE=oauth_user`)
- `PATIENT_MASTER_SYNC_WEBHOOK_TOKEN` (required to enable centralized sync webhook)
- `SYNC_SOURCE_SPREADSHEET_ID`, `SYNC_SOURCE_SHEET_NAME` (source = `CUSTOMER LIST`)
- `SYNC_TARGET_SPREADSHEET_ID`, `SYNC_TARGET_SHEET_NAME` (target = `PATIENT MASTER REVIEWED`)

Production baseline files:
- `.env.production.example` (strict production env template)
- `COMPLIANCE-RA10173-CHECKLIST.md` (security + data privacy go-live checklist)

Strict mode:
- Set `NODE_ENV=production` and `STRICT_SECURITY_MODE=true` in production.
- Backend will fail startup if critical controls (captcha, consent, audit logs, strict CORS, secrets) are misconfigured.

Security flags:
- `REQUIRE_PORTAL_CONSENT=true`
- `REQUIRE_PORTAL_CAPTCHA=true` (for production)
- `TURNSTILE_SECRET_KEY=<your-secret>`

## Storage mode

- `RESULT_STORAGE=drive`:
  - PDFs are uploaded to Google Drive.
  - `DRIVE_FOLDER_ID` must be configured and writable by service account.
  - `DRIVE_AUTH_MODE=service_account` uses service-account auth for Drive.
  - `DRIVE_AUTH_MODE=oauth_user` uses OAuth refresh token for Drive uploads/downloads (recommended when service account upload quota is blocked).

- `RESULT_STORAGE=local`:
  - PDFs are uploaded directly to your backend server folder (`LOCAL_RESULTS_DIR`).
  - No Google Drive upload needed.
  - Keep this folder on persistent disk in production.

## Run

```bash
cd "/Users/coleen/Desktop/DRMED Website/backend-node"
npm install
npm start
```

## OAuth Drive token helper

If using `DRIVE_AUTH_MODE=oauth_user`, generate refresh token:

```bash
cd "/Users/coleen/Desktop/DRMED Website/backend-node"
GOOGLE_OAUTH_CLIENT_ID="..." \
GOOGLE_OAUTH_CLIENT_SECRET="..." \
GOOGLE_OAUTH_REDIRECT_URI="http://localhost:8085/oauth2/callback" \
npm run oauth:drive-token
```

Then set returned `GOOGLE_OAUTH_REFRESH_TOKEN` in backend env vars.

## Patient endpoints

- `GET /healthz`
- `POST /api/portal/verify`
- `GET|POST /api/portal/report`

Portal verify returns a short-lived `accessToken`.  
Portal report requires that `accessToken` and optional `fileId`.

## Staff endpoints (Option #2)

Auth header (required):
- `X-Staff-Session: <session-token-from-login>`
- `X-Staff-User: <staff-id-or-name>`

### Hidden staff gate login
`POST /api/staff/auth/login`

Recommended: store hashed passwords (scrypt) in `STAFF_GATE_USERS_JSON`.

Generate a scrypt hash:
```bash
node -e "const crypto=require('crypto');const salt=crypto.randomBytes(16);const key=crypto.scryptSync('YOUR_PASSWORD', salt, 64, {N:16384,r:8,p:1});console.log(`scrypt$16384$8$1$${salt.toString('base64')}$${key.toString('base64')}`)"
```

Body (MFA optional):
```json
{
  "staffUser": "frontdesk-1",
  "password": "your_staff_password",
  "mfa": "123456"
}
```

Success response includes:
- `staffSessionToken`
- `expiresAt`
- `expiresInSec`

Use `X-Staff-Session` + `X-Staff-User` for all staff API calls after login.

### List reports for one control number
`GET /api/staff/reports?control=2026-0001`

### List reports for one patient account (all controls/visits)
`GET /api/staff/reports?patientId=DRM-0001`

### Create/update a report row and optionally upload PDF
`POST /api/staff/reports`

Body:
```json
{
  "patientId": "DRM-0001",
  "controlNumber": "2026-0001",
  "patientName": "Verlyn Devosora",
  "securePin": "A1B2C3",
  "testName": "CBC",
  "testDate": "2026-02-24",
  "status": "Released",
  "pdfBase64": "data:application/pdf;base64,JVBERi0xLjcK..."
}
```

Behavior:
- If `pdfBase64` is present:
  - backend uploads PDF to private Drive folder,
  - sets `Status=Released`,
  - writes `Released At` timestamp,
  - writes `Drive File ID` into column `I`.
- If `pdfBase64` is absent:
  - backend stores a pending row (`Status=Pending`) even if frontend requested `Released`.

### Bulk release upload
`POST /api/staff/reports/bulk`

Body:
```json
{
  "defaults": {
    "patientId": "DRM-0001",
    "patientName": "Verlyn Devosora",
    "securePin": "A1B2C3",
    "testDate": "2026-03-06"
  },
  "items": [
    {
      "originalFileName": "2026-0001-CBC.pdf",
      "pdfBase64": "data:application/pdf;base64,JVBERi0xLjcK..."
    },
    {
      "originalFileName": "2026-0001-CHEMISTRY.pdf",
      "pdfBase64": "data:application/pdf;base64,JVBERi0xLjcK..."
    }
  ]
}
```

Bulk behavior:
- Each file is processed independently and returns `ok=true/false`.
- Filename parser supports `CONTROL-TEST.pdf` and `CONTROL_TEST.pdf`.
- For repeated tests under same control, include date in filename: `CONTROL-TEST-YYYYMMDD.pdf`.
- Response can be `200` (all success) or `207` (partial success).

## Centralized patient-master sync webhook (Render)

Use this if you want backend-controlled sync from `CUSTOMER LIST` to `PATIENT MASTER REVIEWED`.

### Required env vars

- `PATIENT_MASTER_SYNC_WEBHOOK_TOKEN` (long random secret)
- `SYNC_SOURCE_SPREADSHEET_ID`
- `SYNC_SOURCE_SHEET_NAME` (usually `CUSTOMER LIST`)
- `SYNC_TARGET_SPREADSHEET_ID`
- `SYNC_TARGET_SHEET_NAME` (usually `PATIENT MASTER REVIEWED`)

### Endpoints

- `GET /api/internal/patient-master/sync` (status; requires webhook token)
- `POST /api/internal/patient-master/sync` (run sync; requires webhook token)

Auth options:
- Header `X-Sync-Token: <token>`
- Header `Authorization: Bearer <token>`

Optional body/query overrides:
- `sourceSpreadsheetId`
- `targetSpreadsheetId`
- `sourceSheet`
- `targetSheet`
- `dryRun=true`

### Trigger from Google Apps Script

```javascript
function triggerPatientMasterSync() {
  const url = 'https://api.staff.drmed.ph/api/internal/patient-master/sync';
  const token = 'REPLACE_WITH_PATIENT_MASTER_SYNC_WEBHOOK_TOKEN';
  const payload = {
    dryRun: false
  };
  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    headers: { 'X-Sync-Token': token },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  Logger.log(res.getResponseCode() + ' ' + res.getContentText());
}
```

### Same control + same test on different dates

This is supported. To prevent accidental overwrite, backend now requires `testDate`
when that control number already has the same test name in the sheet.

## cURL examples

Login and capture session token:
```bash
STAFF_SESSION=$(curl -s -X POST "http://localhost:8080/api/staff/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"staffUser":"frontdesk-1","password":"YOUR_STAFF_PASSWORD","mfa":"123456"}' \
  | node -e "const fs=require('fs');const d=JSON.parse(fs.readFileSync(0,'utf8'));process.stdout.write(d.staffSessionToken||'')")
```

List:
```bash
curl -s "http://localhost:8080/api/staff/reports?control=2026-0001" \
  -H "X-Staff-Session: $STAFF_SESSION" \
  -H "X-Staff-User: frontdesk-1"
```

Create pending:
```bash
curl -s -X POST "http://localhost:8080/api/staff/reports" \
  -H "X-Staff-Session: $STAFF_SESSION" \
  -H "X-Staff-User: frontdesk-1" \
  -H "Content-Type: application/json" \
  -d '{"controlNumber":"2026-0001","patientName":"Verlyn Devosora","securePin":"A1B2C3","testName":"CBC","testDate":"2026-02-24","status":"Pending"}'
```

Bulk release:
```bash
curl -s -X POST "http://localhost:8080/api/staff/reports/bulk" \
  -H "X-Staff-Session: $STAFF_SESSION" \
  -H "X-Staff-User: frontdesk-1" \
  -H "Content-Type: application/json" \
  -d '{"defaults":{"securePin":"A1B2C3"},"items":[{"originalFileName":"2026-0001-CBC.pdf","pdfBase64":"data:application/pdf;base64,JVBERi0xLjcK..."}]}'
```

## Staff internal page

Use the internal page:
- `/Users/coleen/Desktop/DRMED Website/staff-console.html`

Open via local server (not `file://`):
```bash
cd "/Users/coleen/Desktop/DRMED Website"
python3 -m http.server 5500
```
Then open:
- `http://localhost:5500/staff-console.html`

The console now has a hidden gate overlay. It needs:
- backend API URL,
- staff user ID,
- staff password from `STAFF_GATE_USERS_JSON`.

## Security notes

- Never expose service-account JSON in frontend code.
- Keep strict CORS allowlist in production.
- Keep CAPTCHA enabled in production.
- Keep consent logs enabled (`CONSENT_LOG_FILE` and optional `CONSENT_LOG_TO_SHEETS=true`).
- Rotate staff passwords in `STAFF_GATE_USERS_JSON` and `PORTAL_ACCESS_TOKEN_SECRET` periodically.
- Prefer hashed staff passwords and enable MFA for all staff accounts.
- Use HTTPS only in production.
