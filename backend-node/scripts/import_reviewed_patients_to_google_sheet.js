#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');
const { google } = require('googleapis');
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = true;
    } else {
      args[key] = next;
      i += 1;
    }
  }
  return args;
}

function parseCsv(content) {
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;

  for (let i = 0; i < content.length; i += 1) {
    const ch = content[i];
    const next = content[i + 1];

    if (inQuotes) {
      if (ch === '"' && next === '"') {
        field += '"';
        i += 1;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
    } else if (ch === ',') {
      row.push(field);
      field = '';
    } else if (ch === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
    } else if (ch === '\r') {
      // ignore CR
    } else {
      field += ch;
    }
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows;
}

function findLatestServiceAccountJson() {
  const downloadsDir = path.resolve('/Users/coleen/Downloads');
  if (!fs.existsSync(downloadsDir)) return '';
  const files = fs
    .readdirSync(downloadsDir)
    .filter((name) => /^drmed-.*\.json$/i.test(name))
    .map((name) => {
      const full = path.join(downloadsDir, name);
      const st = fs.statSync(full);
      return { full, mtime: st.mtimeMs };
    })
    .sort((a, b) => b.mtime - a.mtime);
  return files.length ? files[0].full : '';
}

function resolveCredentialsPath(argPath) {
  if (argPath && fs.existsSync(argPath)) return argPath;
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS && fs.existsSync(process.env.GOOGLE_APPLICATION_CREDENTIALS)) {
    return process.env.GOOGLE_APPLICATION_CREDENTIALS;
  }
  const latest = findLatestServiceAccountJson();
  if (latest && fs.existsSync(latest)) return latest;
  return '';
}

async function ensureSheet(sheets, spreadsheetId, sheetName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const current = (meta.data.sheets || []).find((s) => s.properties && s.properties.title === sheetName);
  if (current) {
    return current.properties.sheetId;
  }
  const addRes = await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [
        { addSheet: { properties: { title: sheetName } } },
      ],
    },
  });
  const reply = addRes.data.replies && addRes.data.replies[0] && addRes.data.replies[0].addSheet;
  if (!reply || !reply.properties) {
    throw new Error(`Failed to add sheet "${sheetName}".`);
  }
  return reply.properties.sheetId;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const csvPath = args.csv;
  const sheetName = args['sheet-name'];
  const spreadsheetId = args['spreadsheet-id'] || process.env.SHEET_ID;
  const credentialsPath = resolveCredentialsPath(args.credentials);

  if (!csvPath || !sheetName) {
    throw new Error('Required: --csv <path> --sheet-name <name>');
  }
  if (!spreadsheetId) {
    throw new Error('Missing spreadsheet id. Provide --spreadsheet-id or SHEET_ID in .env');
  }
  if (!credentialsPath) {
    throw new Error('No valid Google credentials file found.');
  }

  const csvRaw = fs.readFileSync(csvPath, 'utf8');
  const values = parseCsv(csvRaw).map((row) => row.map((v) => String(v || '')));
  if (!values.length) throw new Error('CSV has no rows.');

  process.env.GOOGLE_APPLICATION_CREDENTIALS = credentialsPath;
  const auth = new google.auth.GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });

  const sheetId = await ensureSheet(sheets, spreadsheetId, sheetName);

  await sheets.spreadsheets.values.clear({
    spreadsheetId,
    range: `'${sheetName}'!A:ZZ`,
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `'${sheetName}'!A1`,
    valueInputOption: 'RAW',
    requestBody: { values },
  });

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [
        {
          updateSheetProperties: {
            properties: {
              sheetId,
              gridProperties: { frozenRowCount: 1 },
            },
            fields: 'gridProperties.frozenRowCount',
          },
        },
      ],
    },
  });

  console.log(`Imported ${values.length - 1} patient rows into sheet "${sheetName}"`);
  console.log(`Spreadsheet ID: ${spreadsheetId}`);
}

main().catch((err) => {
  console.error('IMPORT_ERROR:', err.message);
  process.exit(1);
});

