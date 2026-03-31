#!/usr/bin/env node
/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');
const { google } = require('googleapis');
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });

const DEFAULT_SOURCE_SHEET = 'CUSTOMER LIST';
const DEFAULT_TARGET_SHEET = 'PATIENT MASTER REVIEWED';
const PATIENT_ID_PREFIX = 'DRM';
const PATIENT_ID_WIDTH = 6;

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      out[key] = true;
    } else {
      out[key] = next;
      i += 1;
    }
  }
  return out;
}

function text(v) {
  return String(v || '').trim();
}

function normalizeHeader(v) {
  return text(v).toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function normalizeToken(v) {
  return text(v).toLowerCase().replace(/[^a-z0-9]/g, '');
}

function normalizeName(v) {
  return text(v)
    .toUpperCase()
    .replace(/[^A-Z0-9 ]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeGender(v) {
  const n = normalizeName(v);
  if (n === 'M' || n === 'MALE') return 'MALE';
  if (n === 'F' || n === 'FEMALE') return 'FEMALE';
  return '';
}

function normalizeContact(v) {
  return text(v).replace(/[^0-9+]/g, '');
}

function normalizeEmail(v) {
  return text(v).toLowerCase();
}

function normalizeAddress(v) {
  return text(v)
    .toUpperCase()
    .replace(/[^A-Z0-9 ]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function toIsoDate(y, m, d) {
  const yy = Number(y);
  const mm = Number(m);
  const dd = Number(d);
  if (!Number.isFinite(yy) || !Number.isFinite(mm) || !Number.isFinite(dd)) return '';
  const dt = new Date(Date.UTC(yy, mm - 1, dd));
  if (dt.getUTCFullYear() !== yy || dt.getUTCMonth() + 1 !== mm || dt.getUTCDate() !== dd) return '';
  if (yy < 1900 || yy > 2100) return '';
  return `${String(yy).padStart(4, '0')}-${String(mm).padStart(2, '0')}-${String(dd).padStart(2, '0')}`;
}

function parseBirthday(rawValue) {
  const raw = text(rawValue);
  if (!raw) return '';

  if (/^\d+(?:\.\d+)?$/.test(raw)) {
    const num = Number(raw);
    if (Number.isFinite(num) && num > 1000) {
      const base = Date.UTC(1899, 11, 30);
      const asDate = new Date(base + Math.floor(num) * 24 * 60 * 60 * 1000);
      const y = asDate.getUTCFullYear();
      const m = asDate.getUTCMonth() + 1;
      const d = asDate.getUTCDate();
      return toIsoDate(y, m, d);
    }
  }

  let s = raw.replace(/[./]/g, '-').replace(/\s+/g, ' ').trim();

  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return toIsoDate(m[1], m[2], m[3]);

  m = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})$/);
  if (m) {
    const a = Number(m[1]);
    const b = Number(m[2]);
    const y = Number(m[3]);
    if (a > 12) return toIsoDate(y, b, a); // dd-mm-yyyy
    if (b > 12) return toIsoDate(y, a, b); // mm-dd-yyyy
    return toIsoDate(y, a, b); // default mm-dd-yyyy
  }

  const monthMap = {
    jan: 1, january: 1,
    feb: 2, february: 2,
    mar: 3, march: 3,
    apr: 4, april: 4,
    may: 5,
    jun: 6, june: 6,
    jul: 7, july: 7,
    aug: 8, august: 8,
    sep: 9, sept: 9, september: 9,
    oct: 10, october: 10,
    nov: 11, november: 11,
    dec: 12, december: 12
  };

  s = s.replace(/,/g, ' ');
  m = s.match(/^(\d{1,2})[- ]([A-Za-z]{3,9})[- ](\d{4})$/);
  if (m) {
    const mm = monthMap[text(m[2]).toLowerCase()];
    if (mm) return toIsoDate(m[3], mm, m[1]);
  }
  m = s.match(/^([A-Za-z]{3,9})[- ](\d{1,2})[- ](\d{4})$/);
  if (m) {
    const mm = monthMap[text(m[1]).toLowerCase()];
    if (mm) return toIsoDate(m[3], mm, m[2]);
  }

  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) {
    return toIsoDate(parsed.getUTCFullYear(), parsed.getUTCMonth() + 1, parsed.getUTCDate());
  }
  return '';
}

function splitName(rawName) {
  const raw = text(rawName);
  if (!raw) return { lastName: '', firstName: '', middleName: '' };
  if (raw.includes(',')) {
    const [left, right] = raw.split(',', 2);
    const lastName = normalizeName(left);
    const parts = normalizeName(right).split(' ').filter(Boolean);
    return {
      lastName,
      firstName: parts[0] || '',
      middleName: parts.slice(1).join(' ')
    };
  }
  const parts = normalizeName(raw).split(' ').filter(Boolean);
  if (!parts.length) return { lastName: '', firstName: '', middleName: '' };
  if (parts.length === 1) return { lastName: '', firstName: parts[0], middleName: '' };
  return {
    firstName: parts[0],
    lastName: parts[parts.length - 1],
    middleName: parts.slice(1, -1).join(' ')
  };
}

function findHeaderIndex(headers, aliases) {
  const wanted = new Set((aliases || []).map(normalizeHeader).filter(Boolean));
  if (!wanted.size) return -1;
  for (let i = 0; i < headers.length; i += 1) {
    if (wanted.has(normalizeHeader(headers[i]))) return i;
  }
  return -1;
}

function detectHeaderRow(values) {
  const maxScan = Math.min(values.length, 20);
  let bestIdx = -1;
  let bestScore = -1;
  for (let i = 0; i < maxScan; i += 1) {
    const row = values[i] || [];
    const tokens = row.map(normalizeHeader).filter(Boolean);
    if (!tokens.length) continue;
    let score = 0;
    if (tokens.includes('customername') || tokens.includes('patientname') || tokens.includes('fullname')) score += 5;
    if (tokens.includes('gender') || tokens.includes('sex')) score += 1;
    if (tokens.includes('birthday') || tokens.includes('birthdate') || tokens.includes('dob')) score += 1;
    if (tokens.includes('contact') || tokens.includes('contactnumber') || tokens.includes('contactno')) score += 1;
    if (tokens.includes('email') || tokens.includes('emailaddress')) score += 1;
    if (tokens.includes('city')) score += 1;
    if (score > bestScore) {
      bestScore = score;
      bestIdx = i;
    }
  }
  return bestScore >= 5 ? bestIdx : -1;
}

function buildSourceColMap(headers) {
  return {
    customerName: findHeaderIndex(headers, ['customer name', 'patient name', 'full name', 'name']),
    lastName: findHeaderIndex(headers, ['last name', 'surname']),
    firstName: findHeaderIndex(headers, ['first name', 'given name']),
    middleName: findHeaderIndex(headers, ['middle name', 'middle initial']),
    gender: findHeaderIndex(headers, ['gender', 'sex']),
    birthday: findHeaderIndex(headers, ['birthday', 'birthdate', 'date of birth', 'dob']),
    contactNumber: findHeaderIndex(headers, ['contact #', 'contact', 'contact number', 'contact no', 'mobile', 'phone']),
    email: findHeaderIndex(headers, ['email', 'email address']),
    address: findHeaderIndex(headers, ['address', 'full address']),
    addressStreet: findHeaderIndex(headers, ['st.', 'st', 'street', 'address st.', 'address street']),
    addressBarangay: findHeaderIndex(headers, ['barangay']),
    addressCity: findHeaderIndex(headers, ['city']),
    seniorFlag: findHeaderIndex(headers, ['senior id pwd id', 'senior/pwd id', 'senior or pwd id']),
    seniorId: findHeaderIndex(headers, ['senior / pwd id number', 'senior pwd id number', 'senior id number', 'pwd id number']),
    doctor: findHeaderIndex(headers, ['doctor', 'physician', 'doc', 'doc1'])
  };
}

function readCell(row, idx) {
  if (!Array.isArray(row) || !Number.isFinite(idx) || idx < 0) return '';
  return text(row[idx]);
}

function completenessScore(rec) {
  let score = 0;
  if (rec.birthdayIso) score += 3;
  if (rec.contactNorm) score += 2;
  if (rec.email) score += 2;
  if (rec.addressFull) score += 1;
  if (rec.gender) score += 1;
  if (rec.seniorOrPwdIdNumber) score += 1;
  return score;
}

function chooseMode(values) {
  const cleaned = values.map(text).filter(Boolean);
  if (!cleaned.length) return '';
  const counts = new Map();
  for (const v of cleaned) counts.set(v, (counts.get(v) || 0) + 1);
  return [...counts.entries()]
    .sort((a, b) => (b[1] - a[1]) || (b[0].length - a[0].length) || a[0].localeCompare(b[0]))[0][0];
}

function chooseLongest(values) {
  const cleaned = values.map(text).filter(Boolean);
  if (!cleaned.length) return '';
  return cleaned.sort((a, b) => (b.length - a.length) || a.localeCompare(b))[0];
}

class DSU {
  constructor(n) {
    this.parent = Array.from({ length: n }, (_, i) => i);
    this.rank = Array(n).fill(0);
  }
  find(x) {
    let p = x;
    while (this.parent[p] !== p) {
      this.parent[p] = this.parent[this.parent[p]];
      p = this.parent[p];
    }
    return p;
  }
  union(a, b) {
    let ra = this.find(a);
    let rb = this.find(b);
    if (ra === rb) return false;
    if (this.rank[ra] < this.rank[rb]) [ra, rb] = [rb, ra];
    this.parent[rb] = ra;
    if (this.rank[ra] === this.rank[rb]) this.rank[ra] += 1;
    return true;
  }
}

function shouldMerge(a, b) {
  if (!a.nameKey || !b.nameKey || a.nameKey !== b.nameKey) return { ok: false, reason: '' };

  if (a.birthdayIso && b.birthdayIso) {
    if (a.birthdayIso === b.birthdayIso) return { ok: true, reason: 'NAME_BDAY' };
    return { ok: false, reason: '' };
  }
  if (a.contactNorm && b.contactNorm && a.contactNorm === b.contactNorm) return { ok: true, reason: 'NAME_CONTACT' };
  if (a.email && b.email && a.email === b.email) return { ok: true, reason: 'NAME_EMAIL' };
  if (a.seniorOrPwdIdNumber && b.seniorOrPwdIdNumber && a.seniorOrPwdIdNumber === b.seniorOrPwdIdNumber) {
    return { ok: true, reason: 'NAME_SENIOR_PWD_ID' };
  }
  if (a.addressNorm && b.addressNorm && a.addressNorm === b.addressNorm) return { ok: true, reason: 'NAME_ADDRESS' };
  return { ok: false, reason: '' };
}

function aggregateCluster(members, reasons) {
  const sorted = members.slice().sort((a, b) => b.completeness - a.completeness || a.sourceRow - b.sourceRow);
  const primary = sorted[0];

  const birthdays = [...new Set(members.map((m) => m.birthdayIso).filter(Boolean))];
  const genders = [...new Set(members.map((m) => m.gender).filter(Boolean))];
  const birthdayConflict = birthdays.length > 1;
  const genderConflict = genders.length > 1;

  const addressStreet = chooseMode(members.map((m) => m.addressStreet));
  const addressBarangay = chooseMode(members.map((m) => m.addressBarangay));
  const addressCity = chooseMode(members.map((m) => m.addressCity));
  const addressFull =
    chooseLongest(members.map((m) => m.addressFull)) ||
    [addressStreet, addressBarangay, addressCity].filter(Boolean).join(', ');

  const dedupeBasis = reasons.size ? [...reasons].sort().join('|') : 'SINGLE_ROW';
  let needsReview = 'NO';
  let reviewReason = '';
  if (birthdayConflict) {
    needsReview = 'YES';
    reviewReason = 'Conflicting birthdays across merged rows.';
  } else if (genderConflict && members.length > 1) {
    needsReview = 'YES';
    reviewReason = 'Conflicting gender across merged rows.';
  }

  return {
    patientId: '',
    patientName: chooseLongest(members.map((m) => m.patientName)) || primary.patientName,
    lastName: chooseMode(members.map((m) => m.lastName)) || primary.lastName,
    firstName: chooseMode(members.map((m) => m.firstName)) || primary.firstName,
    middleName: chooseMode(members.map((m) => m.middleName)) || primary.middleName,
    gender: chooseMode(members.map((m) => m.gender)) || primary.gender,
    birthday: chooseMode(members.map((m) => m.birthdayIso)),
    contactNumber: chooseMode(members.map((m) => m.contactNumber)) || primary.contactNumber,
    email: chooseMode(members.map((m) => m.email)) || primary.email,
    address: addressFull,
    addressStreet,
    addressBarangay,
    addressCity,
    seniorOrPwdFlag: chooseMode(members.map((m) => m.seniorOrPwdFlag)) || primary.seniorOrPwdFlag,
    seniorOrPwdIdNumber: chooseMode(members.map((m) => m.seniorOrPwdIdNumber)) || primary.seniorOrPwdIdNumber,
    doctor: chooseMode(members.map((m) => m.doctor)) || primary.doctor,
    sourceRows: members.map((m) => String(m.sourceRow)).join(','),
    sourceCount: String(members.length),
    dedupeBasis,
    needsReview,
    reviewReason,
    members
  };
}

function buildPersonKeys(rec) {
  const nameKey = [normalizeName(rec.lastName), normalizeName(rec.firstName)].filter(Boolean).join('|');
  const keys = [];
  if (!nameKey) return keys;
  if (rec.birthday) keys.push(`NB:${nameKey}|${rec.birthday}`);
  if (rec.contactNumber) keys.push(`NC:${nameKey}|${normalizeContact(rec.contactNumber)}`);
  if (rec.email) keys.push(`NE:${nameKey}|${normalizeEmail(rec.email)}`);
  if (rec.address) keys.push(`NA:${nameKey}|${normalizeAddress(rec.address)}`);
  keys.push(`N:${nameKey}`);
  return keys;
}

function nextPatientId(counter) {
  const id = `${PATIENT_ID_PREFIX}-${String(counter.value).padStart(PATIENT_ID_WIDTH, '0')}`;
  counter.value += 1;
  return id;
}

function formatNowIso() {
  return new Date().toISOString();
}

function resolveCredentials(args) {
  if (args.credentials && fs.existsSync(args.credentials)) return args.credentials;
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS && fs.existsSync(process.env.GOOGLE_APPLICATION_CREDENTIALS)) {
    return process.env.GOOGLE_APPLICATION_CREDENTIALS;
  }
  const downloadsDir = path.resolve('/Users/coleen/Downloads');
  if (fs.existsSync(downloadsDir)) {
    const files = fs
      .readdirSync(downloadsDir)
      .filter((name) => /^drmed-.*\.json$/i.test(name))
      .map((name) => {
        const full = path.join(downloadsDir, name);
        const st = fs.statSync(full);
        return { full, mtime: st.mtimeMs };
      })
      .sort((a, b) => b.mtime - a.mtime);
    if (files.length && fs.existsSync(files[0].full)) return files[0].full;
  }
  return '';
}

async function buildGoogleClients(args) {
  if (process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
    const raw = String(process.env.GOOGLE_SERVICE_ACCOUNT_JSON).trim();
    if (raw) {
      const parsed = JSON.parse(raw);
      const auth = new google.auth.JWT({
        email: parsed.client_email,
        key: parsed.private_key,
        scopes: [
          'https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive.readonly'
        ]
      });
      return {
        sheets: google.sheets({ version: 'v4', auth })
      };
    }
  }

  const credentialsPath = resolveCredentials(args);
  if (credentialsPath) process.env.GOOGLE_APPLICATION_CREDENTIALS = credentialsPath;
  const auth = new google.auth.GoogleAuth({
    scopes: [
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive.readonly'
    ]
  });
  return {
    sheets: google.sheets({ version: 'v4', auth })
  };
}

async function ensureSheet(sheets, spreadsheetId, sheetName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const current = (meta.data.sheets || []).find((s) => s.properties && s.properties.title === sheetName);
  if (current) return current.properties.sheetId;

  const addRes = await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [{ addSheet: { properties: { title: sheetName } } }]
    }
  });
  const reply = addRes.data.replies && addRes.data.replies[0] && addRes.data.replies[0].addSheet;
  if (!reply || !reply.properties) throw new Error(`Failed to create sheet "${sheetName}"`);
  return reply.properties.sheetId;
}

function parseSourceRecords(values, headerRowIdx) {
  const headers = values[headerRowIdx] || [];
  const col = buildSourceColMap(headers);
  if (col.customerName < 0 && (col.lastName < 0 || col.firstName < 0)) {
    throw new Error('Could not map source columns. Ensure source tab has Customer Name or Last/First Name headers.');
  }

  const rows = values.slice(headerRowIdx + 1);
  const out = [];
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i] || [];
    const sourceRow = headerRowIdx + 2 + i;

    const explicitLast = normalizeName(readCell(row, col.lastName));
    const explicitFirst = normalizeName(readCell(row, col.firstName));
    const explicitMiddle = normalizeName(readCell(row, col.middleName));
    const customerNameRaw = readCell(row, col.customerName);
    const fromName = splitName(customerNameRaw);

    const lastName = explicitLast || fromName.lastName;
    const firstName = explicitFirst || fromName.firstName;
    const middleName = explicitMiddle || fromName.middleName;
    const patientName =
      customerNameRaw ||
      [lastName, firstName, middleName].filter(Boolean).join(', ');

    if (!text(patientName)) continue;

    const addressStreet = readCell(row, col.addressStreet);
    const addressBarangay = readCell(row, col.addressBarangay);
    const addressCity = readCell(row, col.addressCity);
    const addressFull =
      readCell(row, col.address) ||
      [addressStreet, addressBarangay, addressCity].filter(Boolean).join(', ');

    const rec = {
      sourceRow,
      patientName: text(patientName),
      lastName,
      firstName,
      middleName,
      nameKey: [normalizeName(lastName), normalizeName(firstName)].filter(Boolean).join('|'),
      gender: normalizeGender(readCell(row, col.gender)),
      birthdayIso: parseBirthday(readCell(row, col.birthday)),
      contactNumber: readCell(row, col.contactNumber),
      contactNorm: normalizeContact(readCell(row, col.contactNumber)),
      email: normalizeEmail(readCell(row, col.email)),
      addressStreet,
      addressBarangay,
      addressCity,
      addressFull,
      addressNorm: normalizeAddress(addressFull),
      seniorOrPwdFlag: readCell(row, col.seniorFlag),
      seniorOrPwdIdNumber: readCell(row, col.seniorId),
      doctor: readCell(row, col.doctor)
    };
    rec.completeness = completenessScore(rec);
    out.push(rec);
  }
  return out;
}

function mergeSourceRecords(records) {
  const groups = new Map();
  for (const rec of records) {
    const key = rec.nameKey || `ROW|${rec.sourceRow}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(rec);
  }

  const merged = [];
  for (const key of [...groups.keys()]) {
    const bucket = groups.get(key).slice().sort((a, b) => a.sourceRow - b.sourceRow);
    const dsu = new DSU(bucket.length);
    const pairReasons = [];

    for (let i = 0; i < bucket.length; i += 1) {
      for (let j = i + 1; j < bucket.length; j += 1) {
        const verdict = shouldMerge(bucket[i], bucket[j]);
        if (verdict.ok) {
          dsu.union(i, j);
          pairReasons.push({ i, j, reason: verdict.reason });
        }
      }
    }

    const components = new Map();
    for (let i = 0; i < bucket.length; i += 1) {
      const root = dsu.find(i);
      if (!components.has(root)) components.set(root, []);
      components.get(root).push(i);
    }

    for (const root of [...components.keys()]) {
      const idxs = components.get(root).sort((a, b) => bucket[a].sourceRow - bucket[b].sourceRow);
      const members = idxs.map((idx) => bucket[idx]);
      const reasons = new Set(
        pairReasons
          .filter((p) => idxs.includes(p.i) && idxs.includes(p.j))
          .map((p) => p.reason)
          .filter(Boolean)
      );
      merged.push(aggregateCluster(members, reasons));
    }
  }

  return merged.sort((a, b) => {
    const firstRowA = Number((a.sourceRows || '').split(',')[0] || 0);
    const firstRowB = Number((b.sourceRows || '').split(',')[0] || 0);
    return firstRowA - firstRowB;
  });
}

function parseExistingPatientId(raw) {
  const id = text(raw).toUpperCase();
  return /^DRM-\d+$/.test(id) ? id : '';
}

function parseMaxPatientNumber(patientId) {
  const m = String(patientId || '').match(/^DRM-(\d+)$/i);
  return m ? Number(m[1]) : 0;
}

function buildExistingIndex(existingRows) {
  if (!existingRows.length) {
    return { maxNum: 0, keyToId: new Map() };
  }

  const headers = existingRows[0] || [];
  const col = {
    patientId: findHeaderIndex(headers, ['patient id', 'patient user id', 'patient_id']),
    patientName: findHeaderIndex(headers, ['patient name', 'customer name', 'full name', 'name']),
    lastName: findHeaderIndex(headers, ['last name', 'surname']),
    firstName: findHeaderIndex(headers, ['first name', 'given name']),
    middleName: findHeaderIndex(headers, ['middle name']),
    birthday: findHeaderIndex(headers, ['birthday', 'birthdate', 'date of birth', 'dob']),
    contact: findHeaderIndex(headers, ['contact #', 'contact', 'contact number', 'contact no', 'mobile', 'phone']),
    email: findHeaderIndex(headers, ['email', 'email address']),
    address: findHeaderIndex(headers, ['address', 'full address'])
  };

  const keyToId = new Map();
  let maxNum = 0;
  const dataRows = existingRows.slice(1);
  for (const row of dataRows) {
    let patientId = parseExistingPatientId(readCell(row, col.patientId));
    if (!patientId) continue;
    maxNum = Math.max(maxNum, parseMaxPatientNumber(patientId));

    const patientName = readCell(row, col.patientName);
    const split = splitName(patientName);
    const lastName = normalizeName(readCell(row, col.lastName) || split.lastName);
    const firstName = normalizeName(readCell(row, col.firstName) || split.firstName);
    const birthday = parseBirthday(readCell(row, col.birthday));
    const contact = normalizeContact(readCell(row, col.contact));
    const email = normalizeEmail(readCell(row, col.email));
    const address = normalizeAddress(readCell(row, col.address));
    const baseName = [lastName, firstName].filter(Boolean).join('|');
    if (!baseName) continue;

    const keys = [
      birthday ? `NB:${baseName}|${birthday}` : '',
      contact ? `NC:${baseName}|${contact}` : '',
      email ? `NE:${baseName}|${email}` : '',
      address ? `NA:${baseName}|${address}` : '',
      `N:${baseName}`
    ].filter(Boolean);

    for (const key of keys) {
      if (!keyToId.has(key)) keyToId.set(key, patientId);
    }
  }

  return { maxNum, keyToId };
}

function assignPatientIds(mergedRows, existingIndex) {
  const used = new Set();
  const counter = { value: Math.max(1, existingIndex.maxNum + 1) };

  for (const row of mergedRows) {
    const candidateKeys = buildPersonKeys({
      lastName: row.lastName,
      firstName: row.firstName,
      birthday: row.birthday,
      contactNumber: row.contactNumber,
      email: row.email,
      address: row.address
    });

    let chosen = '';
    for (const key of candidateKeys) {
      const matched = existingIndex.keyToId.get(key);
      if (matched && !used.has(matched)) {
        chosen = matched;
        break;
      }
    }
    if (!chosen) {
      chosen = nextPatientId(counter);
    }
    row.patientId = chosen;
    used.add(chosen);
  }
}

function buildOutputRows(mergedRows) {
  const header = [
    'patient_id',
    'patient_name',
    'last_name',
    'first_name',
    'middle_name',
    'gender',
    'birthday',
    'contact_number',
    'email',
    'address',
    'address_street',
    'address_barangay',
    'address_city',
    'senior_or_pwd_flag',
    'senior_or_pwd_id_number',
    'doctor',
    'source_rows',
    'source_count',
    'dedupe_basis',
    'needs_review',
    'review_reason',
    'synced_at'
  ];
  const now = formatNowIso();
  const rows = mergedRows.map((r) => [
    r.patientId,
    r.patientName,
    r.lastName,
    r.firstName,
    r.middleName,
    r.gender,
    r.birthday,
    r.contactNumber,
    r.email,
    r.address,
    r.addressStreet,
    r.addressBarangay,
    r.addressCity,
    r.seniorOrPwdFlag,
    r.seniorOrPwdIdNumber,
    r.doctor,
    r.sourceRows,
    r.sourceCount,
    r.dedupeBasis,
    r.needsReview,
    r.reviewReason,
    now
  ]);
  return [header, ...rows];
}

async function runSync(options = {}) {
  const defaultSpreadsheetId =
    options.spreadsheetId ||
    process.env.PATIENT_MASTER_SHEET_ID ||
    process.env.SHEET_ID;
  const sourceSpreadsheetId = options.sourceSpreadsheetId || defaultSpreadsheetId;
  const targetSpreadsheetId = options.targetSpreadsheetId || defaultSpreadsheetId;
  const sourceSheet = text(options.sourceSheet || process.env.SYNC_SOURCE_SHEET || DEFAULT_SOURCE_SHEET);
  const targetSheet = text(options.targetSheet || process.env.PATIENT_MASTER_SHEET_NAME || DEFAULT_TARGET_SHEET);
  const dryRun = !!options.dryRun;

  if (!sourceSpreadsheetId || !targetSpreadsheetId) {
    throw new Error(
      'Missing spreadsheet id. Use spreadsheetId or set sourceSpreadsheetId and targetSpreadsheetId.'
    );
  }

  const sheets = options.sheets || (await buildGoogleClients(options)).sheets;
  const targetSheetId = await ensureSheet(sheets, targetSpreadsheetId, targetSheet);

  const sourceRes = await sheets.spreadsheets.values.get({
    spreadsheetId: sourceSpreadsheetId,
    range: `'${sourceSheet}'!A1:Z`,
    majorDimension: 'ROWS'
  });
  const sourceValues = sourceRes.data.values || [];
  if (!sourceValues.length) throw new Error(`Source sheet "${sourceSheet}" is empty.`);

  const headerRowIdx = detectHeaderRow(sourceValues);
  if (headerRowIdx < 0) {
    throw new Error(
      `Cannot detect header row in "${sourceSheet}". Make sure one row contains "Customer Name", "Gender", "Birthday", etc.`
    );
  }

  const sourceRecords = parseSourceRecords(sourceValues, headerRowIdx);
  if (!sourceRecords.length) throw new Error(`No source patient rows found in "${sourceSheet}".`);

  const mergedRows = mergeSourceRecords(sourceRecords);

  let existingValues = [];
  try {
    const existingRes = await sheets.spreadsheets.values.get({
      spreadsheetId: targetSpreadsheetId,
      range: `'${targetSheet}'!A1:Z`,
      majorDimension: 'ROWS'
    });
    existingValues = existingRes.data.values || [];
  } catch (_err) {
    existingValues = [];
  }

  const existingIndex = buildExistingIndex(existingValues);
  assignPatientIds(mergedRows, existingIndex);

  const output = buildOutputRows(mergedRows);
  const reviewCount = mergedRows.filter((r) => r.needsReview === 'YES').length;
  const multiSourceCount = mergedRows.filter((r) => Number(r.sourceCount) > 1).length;

  if (!dryRun) {
    await sheets.spreadsheets.values.clear({
      spreadsheetId: targetSpreadsheetId,
      range: `'${targetSheet}'!A:ZZ`
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId: targetSpreadsheetId,
      range: `'${targetSheet}'!A1`,
      valueInputOption: 'RAW',
      requestBody: { values: output }
    });
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: targetSpreadsheetId,
      requestBody: {
        requests: [
          {
            updateSheetProperties: {
              properties: {
                sheetId: targetSheetId,
                gridProperties: { frozenRowCount: 1 }
              },
              fields: 'gridProperties.frozenRowCount'
            }
          }
        ]
      }
    });
  }

  return {
    sourceSpreadsheetId,
    targetSpreadsheetId,
    sourceSheet,
    targetSheet,
    sourceRowsParsed: sourceRecords.length,
    uniqueMergedPatients: mergedRows.length,
    mergedClusters: multiSourceCount,
    needsReview: reviewCount,
    mode: dryRun ? 'DRY_RUN' : 'APPLIED'
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const summary = await runSync({
    credentials: args.credentials,
    spreadsheetId: args['spreadsheet-id'],
    sourceSpreadsheetId: args['source-spreadsheet-id'],
    targetSpreadsheetId: args['target-spreadsheet-id'],
    sourceSheet: args['source-sheet'],
    targetSheet: args['target-sheet'],
    dryRun: !!args['dry-run']
  });

  if (summary.mode === 'DRY_RUN') {
    console.log('DRY RUN: no sheet updates applied.');
  }
  console.log(`Source Spreadsheet ID: ${summary.sourceSpreadsheetId}`);
  console.log(`Target Spreadsheet ID: ${summary.targetSpreadsheetId}`);
  console.log(`Source sheet: ${summary.sourceSheet}`);
  console.log(`Target sheet: ${summary.targetSheet}`);
  console.log(`Source rows parsed: ${summary.sourceRowsParsed}`);
  console.log(`Unique merged patients: ${summary.uniqueMergedPatients}`);
  console.log(`Merged clusters (source_count > 1): ${summary.mergedClusters}`);
  console.log(`Needs review: ${summary.needsReview}`);
  console.log(`Mode: ${summary.mode}`);
}

module.exports = {
  runSync
};

if (require.main === module) {
  main().catch((err) => {
    console.error('SYNC_ERROR:', err.message);
    process.exit(1);
  });
}
