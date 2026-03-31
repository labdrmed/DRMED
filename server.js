'use strict';

require('dotenv').config();

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { google } = require('googleapis');
const fsNative = require('node:fs');
const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { Readable } = require('node:stream');

const NODE_ENV = String(process.env.NODE_ENV || 'development').trim().toLowerCase();
const STRICT_SECURITY_MODE = (() => {
  const raw = String(process.env.STRICT_SECURITY_MODE || '').trim().toLowerCase();
  if (!raw) return NODE_ENV === 'production';
  return ['1', 'true', 'yes', 'y', 'on'].includes(raw);
})();

const parsedPort = Number(process.env.PORT);
const PORT = Number.isFinite(parsedPort) && parsedPort > 0 ? Math.floor(parsedPort) : 8080;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || 'https://drmed.ph,https://www.drmed.ph,https://drmed.visionsbyiso.com,https://www.drmed.visionsbyiso.com';
const SHEET_ID = process.env.SHEET_ID || '1O09S6_hRv-c7irI_HJtbYraWSXQs08IET0-bqQIWQh4';
const SHEET_NAME = process.env.SHEET_NAME || 'Sheet1';
const PATIENT_MASTER_SHEET_ID = process.env.PATIENT_MASTER_SHEET_ID || SHEET_ID;
const PATIENT_MASTER_SHEET_NAME = process.env.PATIENT_MASTER_SHEET_NAME || 'PATIENT MASTER REVIEWED';
const DRIVE_FOLDER_ID = process.env.DRIVE_FOLDER_ID || '';
const RESULT_STORAGE = String(process.env.RESULT_STORAGE || 'drive').trim().toLowerCase();
const USE_LOCAL_STORAGE = RESULT_STORAGE === 'local' || RESULT_STORAGE === 'filesystem';
const RAW_DRIVE_AUTH_MODE = String(process.env.DRIVE_AUTH_MODE || 'service_account').trim().toLowerCase();
const DRIVE_AUTH_MODE = ['oauth', 'oauth_user', 'user'].includes(RAW_DRIVE_AUTH_MODE) ? 'oauth_user' : 'service_account';
const USE_DRIVE_OAUTH_USER = !USE_LOCAL_STORAGE && DRIVE_AUTH_MODE === 'oauth_user';
const LOCAL_RESULTS_DIR = process.env.LOCAL_RESULTS_DIR || path.join(__dirname, 'uploaded-results');
const GOOGLE_OAUTH_CLIENT_ID = process.env.GOOGLE_OAUTH_CLIENT_ID || '';
const GOOGLE_OAUTH_CLIENT_SECRET = process.env.GOOGLE_OAUTH_CLIENT_SECRET || '';
const GOOGLE_OAUTH_REDIRECT_URI = process.env.GOOGLE_OAUTH_REDIRECT_URI || '';
const GOOGLE_OAUTH_REFRESH_TOKEN = process.env.GOOGLE_OAUTH_REFRESH_TOKEN || '';
const JSON_BODY_LIMIT = process.env.JSON_BODY_LIMIT || '20mb';
const STAFF_GATE_USERS = parseStaffGateUsers(process.env.STAFF_GATE_USERS_JSON || '');
const ENABLE_STAFF_SESSION_GATE = STAFF_GATE_USERS.size > 0;
const ENABLE_STAFF_UPLOAD = ENABLE_STAFF_SESSION_GATE;
const STAFF_SESSION_TOKEN_SECRET = process.env.STAFF_SESSION_TOKEN_SECRET || '';
const STAFF_LOCKOUT_MAX_ATTEMPTS = Number(process.env.STAFF_LOCKOUT_MAX_ATTEMPTS || 5);
const STAFF_LOCKOUT_WINDOW_MS = Number(process.env.STAFF_LOCKOUT_WINDOW_MS || 15 * 60 * 1000);
const STAFF_LOCKOUT_DURATION_MS = Number(process.env.STAFF_LOCKOUT_DURATION_MS || 15 * 60 * 1000);
const STAFF_LOCKOUT_SCOPE = String(process.env.STAFF_LOCKOUT_SCOPE || 'user_ip').trim().toLowerCase();
const STAFF_MFA_REQUIRED = String(process.env.STAFF_MFA_REQUIRED || 'false').toLowerCase() === 'true';
const STAFF_MFA_SECRETS = parseMfaSecrets(process.env.STAFF_MFA_SECRETS_JSON || '');
const STAFF_SESSION_TTL_SECONDS = Number(process.env.STAFF_SESSION_TTL_SECONDS || 12 * 60 * 60);
const MAX_STAFF_PDF_BYTES = Number(process.env.MAX_STAFF_PDF_BYTES || 12 * 1024 * 1024);
const MAX_STAFF_BULK_ITEMS = Number(process.env.MAX_STAFF_BULK_ITEMS || 25);
const MAX_PATIENT_SEARCH_RESULTS = Number(process.env.MAX_PATIENT_SEARCH_RESULTS || 25);
const STAFF_AUDIT_LOG_FILE = process.env.STAFF_AUDIT_LOG_FILE || path.join(__dirname, 'logs', 'staff-audit.ndjson');
const ENABLE_STAFF_AUDIT_LOG = String(process.env.ENABLE_STAFF_AUDIT_LOG || 'true').toLowerCase() !== 'false';
const AUDIT_LOG_MAX_BYTES = Number(process.env.AUDIT_LOG_MAX_BYTES || 5 * 1024 * 1024);
const AUDIT_LOG_MAX_DAYS = Number(process.env.AUDIT_LOG_MAX_DAYS || 30);
const TURNSTILE_SECRET_KEY = process.env.TURNSTILE_SECRET_KEY || '';
const TURNSTILE_VERIFY_URL = process.env.TURNSTILE_VERIFY_URL || 'https://challenges.cloudflare.com/turnstile/v0/siteverify';
const REQUIRE_PORTAL_CONSENT = String(process.env.REQUIRE_PORTAL_CONSENT || 'true').toLowerCase() !== 'false';
const REQUIRE_PORTAL_CAPTCHA = String(process.env.REQUIRE_PORTAL_CAPTCHA || '').toLowerCase() === 'true' || !!TURNSTILE_SECRET_KEY;
const ENABLE_CONSENT_AUDIT_LOG = String(process.env.ENABLE_CONSENT_AUDIT_LOG || 'true').toLowerCase() !== 'false';
const CONSENT_NOTICE_VERSION = process.env.CONSENT_NOTICE_VERSION || '2026-02-24';
const CONSENT_LOG_FILE = process.env.CONSENT_LOG_FILE || path.join(__dirname, 'logs', 'portal-consent.ndjson');
const CONSENT_LOG_TO_SHEETS = String(process.env.CONSENT_LOG_TO_SHEETS || 'false').toLowerCase() === 'true';
const CONSENT_LOG_SHEET_NAME = process.env.CONSENT_LOG_SHEET_NAME || 'ConsentLogs';
const PORTAL_ACCESS_TOKEN_SECRET = process.env.PORTAL_ACCESS_TOKEN_SECRET || '';
const PORTAL_ACCESS_TOKEN_TTL_SECONDS = Number(process.env.PORTAL_ACCESS_TOKEN_TTL_SECONDS || 900);
const SAFE_PORTAL_ACCESS_TOKEN_TTL_SECONDS =
  Number.isFinite(PORTAL_ACCESS_TOKEN_TTL_SECONDS) && PORTAL_ACCESS_TOKEN_TTL_SECONDS > 0
    ? Math.floor(PORTAL_ACCESS_TOKEN_TTL_SECONDS)
    : 900;
const portalRuntimeTokenSecret = PORTAL_ACCESS_TOKEN_SECRET || crypto.randomBytes(32).toString('hex');
const SAFE_STAFF_SESSION_TTL_SECONDS =
  Number.isFinite(STAFF_SESSION_TTL_SECONDS) && STAFF_SESSION_TTL_SECONDS > 0
    ? Math.floor(STAFF_SESSION_TTL_SECONDS)
    : 12 * 60 * 60;
const SAFE_MAX_PATIENT_SEARCH_RESULTS =
  Number.isFinite(MAX_PATIENT_SEARCH_RESULTS) && MAX_PATIENT_SEARCH_RESULTS > 0
    ? Math.max(1, Math.min(100, Math.floor(MAX_PATIENT_SEARCH_RESULTS)))
    : 25;
const staffRuntimeTokenSecret = STAFF_SESSION_TOKEN_SECRET || portalRuntimeTokenSecret;
const ALLOWED_ORIGINS = String(ALLOWED_ORIGIN || '')
  .split(',')
  .map((item) => item.trim())
  .filter(Boolean);
const ALLOW_ALL_ORIGINS = ALLOWED_ORIGINS.includes('*');
const HAS_LOCAL_ORIGIN = ALLOWED_ORIGINS.some((origin) => /^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/i.test(origin));

if (!SHEET_ID) {
  throw new Error('Missing SHEET_ID');
}

if (!USE_LOCAL_STORAGE && !DRIVE_FOLDER_ID) {
  console.warn('[WARN] DRIVE_FOLDER_ID is empty. PDF lookup will fail until configured.');
}
if (USE_LOCAL_STORAGE) {
  console.warn('[INFO] RESULT_STORAGE=local. PDFs will be stored in LOCAL_RESULTS_DIR:', LOCAL_RESULTS_DIR);
} else {
  console.warn('[INFO] RESULT_STORAGE=drive. Drive auth mode:', DRIVE_AUTH_MODE);
}
if (!['', 'service_account', 'oauth', 'oauth_user', 'user'].includes(RAW_DRIVE_AUTH_MODE)) {
  console.warn('[WARN] Unknown DRIVE_AUTH_MODE value. Falling back to service_account mode.');
}
if (USE_DRIVE_OAUTH_USER) {
  if (!GOOGLE_OAUTH_CLIENT_ID || !GOOGLE_OAUTH_CLIENT_SECRET || !GOOGLE_OAUTH_REFRESH_TOKEN) {
    console.warn('[WARN] DRIVE_AUTH_MODE=oauth_user requires GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, and GOOGLE_OAUTH_REFRESH_TOKEN.');
  }
}

if (REQUIRE_PORTAL_CAPTCHA && !TURNSTILE_SECRET_KEY) {
  console.warn('[WARN] REQUIRE_PORTAL_CAPTCHA is enabled but TURNSTILE_SECRET_KEY is missing.');
}

if (ENABLE_CONSENT_AUDIT_LOG && !CONSENT_LOG_FILE) {
  console.warn('[WARN] ENABLE_CONSENT_AUDIT_LOG is enabled but CONSENT_LOG_FILE is empty.');
}

if (ENABLE_CONSENT_AUDIT_LOG && CONSENT_LOG_TO_SHEETS && !CONSENT_LOG_SHEET_NAME) {
  console.warn('[WARN] CONSENT_LOG_TO_SHEETS is enabled but CONSENT_LOG_SHEET_NAME is empty.');
}

if (!PORTAL_ACCESS_TOKEN_SECRET) {
  console.warn('[WARN] PORTAL_ACCESS_TOKEN_SECRET is not set. Session tokens reset on server restart.');
}

if (!ALLOWED_ORIGINS.length) {
  console.warn('[WARN] ALLOWED_ORIGIN is empty. Defaulting to deny all browser origins.');
}

if (!ENABLE_STAFF_UPLOAD) {
  console.warn('[WARN] Staff upload endpoints are disabled. Configure STAFF_GATE_USERS_JSON.');
}

if (ENABLE_STAFF_SESSION_GATE && !STAFF_SESSION_TOKEN_SECRET) {
  console.warn('[WARN] Staff session gate is enabled but STAFF_SESSION_TOKEN_SECRET is missing. Staff sessions reset on server restart.');
}

if (STRICT_SECURITY_MODE) {
  const strictErrors = [];
  if (!PORTAL_ACCESS_TOKEN_SECRET) strictErrors.push('PORTAL_ACCESS_TOKEN_SECRET must be set in strict security mode.');
  if (!STAFF_SESSION_TOKEN_SECRET) strictErrors.push('STAFF_SESSION_TOKEN_SECRET must be set in strict security mode.');
  if (!ENABLE_STAFF_SESSION_GATE) strictErrors.push('STAFF_GATE_USERS_JSON must be configured in strict security mode.');
  if (!REQUIRE_PORTAL_CAPTCHA) strictErrors.push('REQUIRE_PORTAL_CAPTCHA must be true in strict security mode.');
  if (!TURNSTILE_SECRET_KEY) strictErrors.push('TURNSTILE_SECRET_KEY must be set in strict security mode.');
  if (!REQUIRE_PORTAL_CONSENT) strictErrors.push('REQUIRE_PORTAL_CONSENT must be true in strict security mode.');
  if (!ENABLE_CONSENT_AUDIT_LOG) strictErrors.push('ENABLE_CONSENT_AUDIT_LOG must be true in strict security mode.');
  if (!CONSENT_LOG_TO_SHEETS) strictErrors.push('CONSENT_LOG_TO_SHEETS must be true in strict security mode.');
  if (!ALLOWED_ORIGINS.length || ALLOW_ALL_ORIGINS) strictErrors.push('ALLOWED_ORIGIN must be explicitly restricted in strict security mode.');
  if (HAS_LOCAL_ORIGIN) strictErrors.push('ALLOWED_ORIGIN must not include localhost/127.0.0.1 in strict security mode.');
  if (!USE_LOCAL_STORAGE && !DRIVE_FOLDER_ID) strictErrors.push('DRIVE_FOLDER_ID is required when RESULT_STORAGE=drive in strict security mode.');
  if (USE_DRIVE_OAUTH_USER && !GOOGLE_OAUTH_CLIENT_ID) strictErrors.push('GOOGLE_OAUTH_CLIENT_ID is required when DRIVE_AUTH_MODE=oauth_user.');
  if (USE_DRIVE_OAUTH_USER && !GOOGLE_OAUTH_CLIENT_SECRET) strictErrors.push('GOOGLE_OAUTH_CLIENT_SECRET is required when DRIVE_AUTH_MODE=oauth_user.');
  if (USE_DRIVE_OAUTH_USER && !GOOGLE_OAUTH_REFRESH_TOKEN) strictErrors.push('GOOGLE_OAUTH_REFRESH_TOKEN is required when DRIVE_AUTH_MODE=oauth_user.');
  if (USE_LOCAL_STORAGE && !path.isAbsolute(LOCAL_RESULTS_DIR)) strictErrors.push('LOCAL_RESULTS_DIR must be an absolute path in strict security mode.');

  if (strictErrors.length) {
    throw new Error(`Strict security mode validation failed:\n- ${strictErrors.join('\n- ')}`);
  }
}

const SHEETS_BASE_SCOPES = [
  'https://www.googleapis.com/auth/spreadsheets.readonly'
];

function getSheetsScopes() {
  const scopes = new Set(SHEETS_BASE_SCOPES);

  if (ENABLE_CONSENT_AUDIT_LOG && CONSENT_LOG_TO_SHEETS) {
    scopes.add('https://www.googleapis.com/auth/spreadsheets');
  }

  if (ENABLE_STAFF_UPLOAD) {
    scopes.add('https://www.googleapis.com/auth/spreadsheets');
  }

  return [...scopes];
}

function getDriveScopesForServiceAccount() {
  const scopes = new Set();
  if (ENABLE_STAFF_UPLOAD) scopes.add('https://www.googleapis.com/auth/drive');
  scopes.add('https://www.googleapis.com/auth/drive.readonly');

  return [...scopes];
}

let clientsPromise;

function getGoogleClients() {
  if (!clientsPromise) {
    clientsPromise = (async () => {
      const sheetAuth = new google.auth.GoogleAuth({
        credentials: getCredentialsFromEnv(),
        scopes: getSheetsScopes()
      });
      const sheetAuthClient = await sheetAuth.getClient();

      let driveClient = null;
      if (!USE_LOCAL_STORAGE) {
        if (USE_DRIVE_OAUTH_USER) {
          const oauthClient = getDriveOAuthClientFromEnv();
          driveClient = google.drive({ version: 'v3', auth: oauthClient });
        } else {
          const driveAuth = new google.auth.GoogleAuth({
            credentials: getCredentialsFromEnv(),
            scopes: getDriveScopesForServiceAccount()
          });
          const driveAuthClient = await driveAuth.getClient();
          driveClient = google.drive({ version: 'v3', auth: driveAuthClient });
        }
      }

      return {
        sheets: google.sheets({ version: 'v4', auth: sheetAuthClient }),
        drive: driveClient
      };
    })();
  }
  return clientsPromise;
}

function getCredentialsFromEnv() {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) {
    // On Cloud Run/GCE, prefer attached service account (ADC) and leave this undefined.
    return undefined;
  }

  try {
    const parsed = JSON.parse(raw);
    if (parsed.private_key) {
      parsed.private_key = String(parsed.private_key).replace(/\\n/g, '\n');
    }
    return parsed;
  } catch (err) {
    throw new Error('GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON');
  }
}

function getDriveOAuthClientFromEnv() {
  const clientId = String(GOOGLE_OAUTH_CLIENT_ID || '').trim();
  const clientSecret = String(GOOGLE_OAUTH_CLIENT_SECRET || '').trim();
  const refreshToken = String(GOOGLE_OAUTH_REFRESH_TOKEN || '').trim();
  const redirectUri = String(GOOGLE_OAUTH_REDIRECT_URI || '').trim();

  if (!clientId || !clientSecret || !refreshToken) {
    throw new Error('Missing OAuth env for Drive. Required: GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, GOOGLE_OAUTH_REFRESH_TOKEN');
  }

  const oauthClient = new google.auth.OAuth2(
    clientId,
    clientSecret,
    redirectUri || undefined
  );
  oauthClient.setCredentials({ refresh_token: refreshToken });
  return oauthClient;
}

function normalizeControl(value) {
  return String(value || '').trim().replace(/\s+/g, '').toUpperCase();
}

function normalizePin(value) {
  return String(value || '').trim().toUpperCase();
}

function normalizePatientId(value) {
  return String(value || '').trim().replace(/\s+/g, '').toUpperCase();
}

function normalizeStaffUser(value) {
  return String(value || '').trim().toLowerCase();
}

const SHEET_COL = Object.freeze({
  PATIENT_ID: 0,
  SECURE_PIN: 1,
  PATIENT_NAME: 2,
  CONTROL_NO: 3,
  TEST_NAME: 4,
  TEST_DATE: 5,
  STATUS: 6,
  RELEASED_AT: 7,
  DRIVE_FILE_ID: 8
});

const PATIENT_MASTER_FALLBACK_COL = Object.freeze({
  PATIENT_ID: -1,
  PATIENT_NAME: 0,
  LAST_NAME: 1,
  FIRST_NAME: 2,
  MIDDLE_NAME: 3,
  GENDER: 4,
  BIRTHDAY: 5,
  CONTACT: 6,
  EMAIL: 7,
  ADDRESS: 8,
  DOCTOR: 10,
  SECURE_PIN: -1
});

const PATIENT_MASTER_HEADER_ALIASES = Object.freeze({
  PATIENT_ID: ['patientid', 'patientuserid', 'userid', 'accountid', 'id'],
  PATIENT_NAME: ['patientname', 'customername', 'fullname', 'name'],
  LAST_NAME: ['lastname', 'surname', 'familyname', 'last'],
  FIRST_NAME: ['firstname', 'givenname', 'first'],
  MIDDLE_NAME: ['middlename', 'middleinitial', 'middle'],
  GENDER: ['gender', 'sex'],
  BIRTHDAY: ['birthday', 'birthdate', 'dateofbirth', 'dob'],
  CONTACT: ['contact', 'contactnumber', 'mobile', 'phone', 'contactno'],
  EMAIL: ['email', 'emailaddress'],
  ADDRESS: ['address', 'fulladdress', 'addressfull'],
  DOCTOR: ['doctor', 'physician'],
  SECURE_PIN: ['securepin', 'pin']
});

function parseStaffGateUsers(raw) {
  const input = String(raw || '').trim();
  const out = new Map();
  if (!input) return out;

  try {
    const parsed = JSON.parse(input);
    if (Array.isArray(parsed)) {
      for (const item of parsed) {
        if (typeof item === 'string') {
          const parts = item.split(':');
          if (parts.length >= 2) {
            const user = normalizeStaffUser(parts.shift());
            const password = parts.join(':').trim();
            if (user && password) out.set(user, buildStaffUserConfig({ user, password }));
          }
          continue;
        }
        const user = normalizeStaffUser(item && (item.user || item.staffUser || item.username));
        if (!user) continue;
        out.set(user, buildStaffUserConfig(Object.assign({ user }, item)));
      }
      return out;
    }
    if (parsed && typeof parsed === 'object') {
      for (const [key, value] of Object.entries(parsed)) {
        const user = normalizeStaffUser(key);
        if (!user) continue;
        if (value && typeof value === 'object') {
          out.set(user, buildStaffUserConfig(Object.assign({ user }, value)));
        } else {
          out.set(user, buildStaffUserConfig({ user, password: String(value || '').trim() }));
        }
      }
      return out;
    }
  } catch (err) {
    console.warn('[WARN] STAFF_GATE_USERS_JSON is invalid JSON. Staff user/password gate will be unavailable.');
    return out;
  }

  return out;
}

function parseMfaSecrets(raw) {
  const input = String(raw || '').trim();
  const out = new Map();
  if (!input) return out;
  try {
    const parsed = JSON.parse(input);
    if (Array.isArray(parsed)) {
      for (const item of parsed) {
        const user = normalizeStaffUser(item && (item.user || item.staffUser || item.username));
        const secret = String(item && (item.secret || item.mfaSecret || '') || '').trim();
        if (user && secret) out.set(user, secret);
      }
      return out;
    }
    if (parsed && typeof parsed === 'object') {
      for (const [key, value] of Object.entries(parsed)) {
        const user = normalizeStaffUser(key);
        const secret = String(value || '').trim();
        if (user && secret) out.set(user, secret);
      }
      return out;
    }
  } catch (err) {
    console.warn('[WARN] STAFF_MFA_SECRETS_JSON is invalid JSON. MFA secrets will be ignored.');
  }
  return out;
}

function buildStaffUserConfig(entry) {
  const config = {
    user: normalizeStaffUser(entry && (entry.user || entry.staffUser || entry.username)),
    password: '',
    passwordHash: '',
    mfaSecret: '',
    mfaRequired: false
  };

  if (!entry) return config;
  const rawPassword = String(entry.password || entry.passcode || '').trim();
  const rawHash = String(entry.passwordHash || entry.hash || '').trim();
  const secret = String(entry.mfaSecret || entry.mfa || entry.otpSecret || '').trim();
  const mfaRequired = parseBoolean(entry.mfaRequired);

  if (rawHash) {
    config.passwordHash = rawHash;
  } else if (rawPassword) {
    if (isScryptHash(rawPassword)) {
      config.passwordHash = rawPassword;
    } else {
      config.password = rawPassword;
    }
  }

  if (secret) config.mfaSecret = secret;
  if (mfaRequired) config.mfaRequired = true;
  return config;
}

function isScryptHash(value) {
  return String(value || '').trim().toLowerCase().startsWith('scrypt$');
}

function parseScryptHash(raw) {
  const value = String(raw || '').trim();
  if (!isScryptHash(value)) return null;
  const parts = value.split('$');
  if (parts.length < 6) return null;
  const N = Number(parts[1]);
  const r = Number(parts[2]);
  const p = Number(parts[3]);
  const salt = Buffer.from(parts[4], 'base64');
  const key = Buffer.from(parts[5], 'base64');
  if (!Number.isFinite(N) || !Number.isFinite(r) || !Number.isFinite(p)) return null;
  if (!salt.length || !key.length) return null;
  return { N, r, p, salt, key };
}

function verifyScryptPassword(password, rawHash) {
  const parsed = parseScryptHash(rawHash);
  if (!parsed) return false;
  try {
    const derived = crypto.scryptSync(String(password || ''), parsed.salt, parsed.key.length, {
      N: parsed.N,
      r: parsed.r,
      p: parsed.p
    });
    return timingSafeEquals(derived.toString('base64'), parsed.key.toString('base64'));
  } catch (_err) {
    return false;
  }
}

function resolveStaffUserConfig(staffUser) {
  const config = STAFF_GATE_USERS.get(staffUser) || null;
  if (!config) return null;
  const mfaSecret = pickFirst(config.mfaSecret, STAFF_MFA_SECRETS.get(staffUser));
  const mfaRequired = STAFF_MFA_REQUIRED || !!config.mfaRequired || !!mfaSecret;
  return Object.assign({}, config, { mfaSecret, mfaRequired });
}

function verifyStaffPassword(password, config) {
  if (!config || !password) return false;
  if (config.passwordHash) return verifyScryptPassword(password, config.passwordHash);
  return timingSafeEquals(password, config.password || '');
}

function normalizeMfaCode(value) {
  return String(value || '').replace(/\s+/g, '').trim();
}

function base32ToBuffer(input) {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  const normalized = String(input || '').toUpperCase().replace(/=+$/g, '').replace(/[^A-Z2-7]/g, '');
  let bits = 0;
  let value = 0;
  const bytes = [];
  for (const char of normalized) {
    const idx = alphabet.indexOf(char);
    if (idx < 0) continue;
    value = (value << 5) | idx;
    bits += 5;
    if (bits >= 8) {
      bytes.push((value >> (bits - 8)) & 0xff);
      bits -= 8;
    }
  }
  return Buffer.from(bytes);
}

function generateTotp(secret, timestampMs) {
  const key = base32ToBuffer(secret);
  if (!key.length) return '';
  const step = 30;
  const counter = Math.floor((timestampMs || Date.now()) / 1000 / step);
  const buf = Buffer.alloc(8);
  buf.writeUInt32BE(Math.floor(counter / 0x100000000), 0);
  buf.writeUInt32BE(counter % 0x100000000, 4);
  const hmac = crypto.createHmac('sha1', key).update(buf).digest();
  const offset = hmac[hmac.length - 1] & 0xf;
  const code = (hmac.readUInt32BE(offset) & 0x7fffffff) % 1000000;
  return String(code).padStart(6, '0');
}

function verifyTotp(secret, code) {
  const normalized = normalizeMfaCode(code);
  if (!secret || !normalized) return false;
  const now = Date.now();
  const windows = [-1, 0, 1];
  for (const offset of windows) {
    if (generateTotp(secret, now + offset * 30000) === normalized) return true;
  }
  return false;
}

const staffLockoutState = new Map();

function getStaffLockoutKey(req, staffUser) {
  const ip = getRequestIp(req) || 'unknown';
  const scope = STAFF_LOCKOUT_SCOPE;
  if (scope === 'ip') return `ip:${ip}`;
  if (scope === 'user') return `user:${staffUser || 'unknown'}`;
  return `user:${staffUser || 'unknown'}|ip:${ip}`;
}

function getStaffLockoutStatus(req, staffUser) {
  const key = getStaffLockoutKey(req, staffUser);
  const entry = staffLockoutState.get(key);
  if (!entry) return { locked: false };
  const now = Date.now();
  if (entry.lockedUntil && entry.lockedUntil > now) {
    return { locked: true, retryAfterMs: entry.lockedUntil - now, entry };
  }
  if (entry.lockedUntil && entry.lockedUntil <= now) {
    staffLockoutState.delete(key);
  }
  if (entry.firstAt && now - entry.firstAt > STAFF_LOCKOUT_WINDOW_MS) {
    staffLockoutState.delete(key);
  }
  return { locked: false };
}

async function recordStaffAuthFailure(req, staffUser, reason) {
  const key = getStaffLockoutKey(req, staffUser);
  const now = Date.now();
  const entry = staffLockoutState.get(key) || { count: 0, firstAt: 0, lockedUntil: 0 };

  if (!entry.firstAt || now - entry.firstAt > STAFF_LOCKOUT_WINDOW_MS) {
    entry.firstAt = now;
    entry.count = 1;
  } else {
    entry.count += 1;
  }

  if (entry.count >= STAFF_LOCKOUT_MAX_ATTEMPTS) {
    entry.lockedUntil = now + STAFF_LOCKOUT_DURATION_MS;
  }

  staffLockoutState.set(key, entry);
  await logStaffAuditEvent(req, 'staff_auth_login_failed', {
    staffUser,
    reason: reason || 'invalid_credentials',
    attempts: entry.count,
    lockedUntil: entry.lockedUntil ? new Date(entry.lockedUntil).toISOString() : ''
  });
  return entry;
}

function clearStaffAuthFailures(req, staffUser) {
  const key = getStaffLockoutKey(req, staffUser);
  staffLockoutState.delete(key);
}

function pickFirst(...vals) {
  for (const v of vals) {
    if (v !== undefined && v !== null && String(v).trim() !== '') return String(v).trim();
  }
  return '';
}

function parseBoolean(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return ['1', 'true', 'yes', 'y', 'on', 'accepted'].includes(normalized);
}

function getRequestIp(req) {
  const forwarded = pickFirst(req.headers && req.headers['x-forwarded-for']);
  if (forwarded) return String(forwarded).split(',')[0].trim();
  return pickFirst(req.ip, req.connection && req.connection.remoteAddress);
}

function hashString(value) {
  const data = String(value || '').trim();
  if (!data) return '';
  return crypto.createHash('sha256').update(data).digest('hex');
}

function base64UrlEncode(value) {
  return Buffer.from(String(value || ''), 'utf8')
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function base64UrlDecode(value) {
  const normalized = String(value || '').replace(/-/g, '+').replace(/_/g, '/');
  const padded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4);
  return Buffer.from(padded, 'base64').toString('utf8');
}

function timingSafeEquals(left, right) {
  const a = Buffer.from(String(left || ''), 'utf8');
  const b = Buffer.from(String(right || ''), 'utf8');
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function signPortalTokenPayload(encodedPayload) {
  return crypto.createHmac('sha256', portalRuntimeTokenSecret).update(encodedPayload).digest('hex');
}

function createPortalAccessToken({ patientId, securePin, consentVersion }) {
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    sub: normalizePatientId(patientId),
    ph: hashString(normalizePin(securePin)),
    cv: pickFirst(consentVersion, CONSENT_NOTICE_VERSION),
    iat: now,
    exp: now + SAFE_PORTAL_ACCESS_TOKEN_TTL_SECONDS
  };
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const signature = signPortalTokenPayload(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

function verifyPortalAccessToken(token) {
  const raw = String(token || '').trim();
  if (!raw) return { ok: false, reason: 'missing_token' };

  const parts = raw.split('.');
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    return { ok: false, reason: 'invalid_format' };
  }

  const [encodedPayload, providedSignature] = parts;
  const expectedSignature = signPortalTokenPayload(encodedPayload);
  if (!timingSafeEquals(providedSignature, expectedSignature)) {
    return { ok: false, reason: 'invalid_signature' };
  }

  try {
    const payload = JSON.parse(base64UrlDecode(encodedPayload));
    const now = Math.floor(Date.now() / 1000);
    if (!payload || !payload.sub || !payload.ph) {
      return { ok: false, reason: 'invalid_payload' };
    }
    if (!Number.isFinite(payload.exp) || payload.exp < now) {
      return { ok: false, reason: 'expired' };
    }

    return { ok: true, payload };
  } catch (_err) {
    return { ok: false, reason: 'invalid_payload' };
  }
}

function signStaffSessionPayload(encodedPayload) {
  return crypto.createHmac('sha256', staffRuntimeTokenSecret).update(encodedPayload).digest('hex');
}

function createStaffSessionToken({ actor }) {
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    sub: pickFirst(actor, 'staff'),
    iat: now,
    exp: now + SAFE_STAFF_SESSION_TTL_SECONDS
  };
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const signature = signStaffSessionPayload(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

function verifyStaffSessionToken(token) {
  const raw = String(token || '').trim();
  if (!raw) return { ok: false, reason: 'missing_token' };

  const parts = raw.split('.');
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    return { ok: false, reason: 'invalid_format' };
  }

  const [encodedPayload, providedSignature] = parts;
  const expectedSignature = signStaffSessionPayload(encodedPayload);
  if (!timingSafeEquals(providedSignature, expectedSignature)) {
    return { ok: false, reason: 'invalid_signature' };
  }

  try {
    const payload = JSON.parse(base64UrlDecode(encodedPayload));
    const now = Math.floor(Date.now() / 1000);
    if (!payload || !payload.sub) {
      return { ok: false, reason: 'invalid_payload' };
    }
    if (!Number.isFinite(payload.exp) || payload.exp < now) {
      return { ok: false, reason: 'expired' };
    }
    return { ok: true, payload };
  } catch (_err) {
    return { ok: false, reason: 'invalid_payload' };
  }
}

function extractPortalAccessToken(req) {
  const bearer = pickFirst(req.headers && req.headers.authorization);
  if (bearer.toLowerCase().startsWith('bearer ')) {
    return bearer.slice(7).trim();
  }
  return pickFirst(
    req.body && req.body.accessToken,
    req.query && req.query.accessToken
  );
}

function extractStaffSessionToken(req) {
  return pickFirst(
    req.headers && req.headers['x-staff-session'],
    req.body && req.body.staffSessionToken,
    req.query && req.query.staffSessionToken
  );
}

function extractStaffActor(req) {
  return pickFirst(
    req.headers && req.headers['x-staff-user'],
    req.headers && req.headers['x-staff-email'],
    req.body && req.body.staffUser,
    req.query && req.query.staffUser
  );
}

function requireStaffAuth(req, res) {
  if (!ENABLE_STAFF_UPLOAD) {
    res.status(503).json({ status: 'error', message: 'Staff upload API is disabled. Configure STAFF_GATE_USERS_JSON.' });
    return false;
  }

  const sessionToken = extractStaffSessionToken(req);
  if (sessionToken) {
    const verified = verifyStaffSessionToken(sessionToken);
    if (verified.ok) {
      req.staffSession = verified.payload;
      return true;
    }
  }

  res.status(401).json({ status: 'error', message: 'Staff session required. Please login with staff user and password.' });
  return false;
}

async function ensureLogRetention(filePath) {
  if (!filePath) return;
  try {
    const stat = await fs.stat(filePath);
    let rotate = false;
    if (Number.isFinite(AUDIT_LOG_MAX_BYTES) && AUDIT_LOG_MAX_BYTES > 0 && stat.size >= AUDIT_LOG_MAX_BYTES) {
      rotate = true;
    }
    if (Number.isFinite(AUDIT_LOG_MAX_DAYS) && AUDIT_LOG_MAX_DAYS > 0) {
      const maxAgeMs = AUDIT_LOG_MAX_DAYS * 24 * 60 * 60 * 1000;
      if (Date.now() - stat.mtimeMs >= maxAgeMs) rotate = true;
    }
    if (!rotate) return;
    const backup = `${filePath}.1`;
    await fs.rm(backup, { force: true });
    await fs.rename(filePath, backup);
  } catch (err) {
    if (err && err.code === 'ENOENT') return;
    console.error('Audit log retention error:', err && err.message ? err.message : String(err));
  }
}

async function logStaffAuditEvent(req, event, details) {
  if (!ENABLE_STAFF_AUDIT_LOG || !STAFF_AUDIT_LOG_FILE) return;
  const payload = {
    schemaVersion: 1,
    event: pickFirst(event, 'staff_event'),
    at: new Date().toISOString(),
    actor: pickFirst(req && req.staffSession && req.staffSession.sub, extractStaffActor(req)),
    ipHash: hashString(getRequestIp(req)),
    userAgentHash: hashString(pickFirst(req.headers && req.headers['user-agent'])),
    details: details || {}
  };
  try {
    await fs.mkdir(path.dirname(STAFF_AUDIT_LOG_FILE), { recursive: true });
    await ensureLogRetention(STAFF_AUDIT_LOG_FILE);
    await fs.appendFile(STAFF_AUDIT_LOG_FILE, `${JSON.stringify(payload)}\n`, 'utf8');
  } catch (err) {
    console.error('Staff audit log error:', err && err.message ? err.message : String(err));
  }
}

function resolveConsentNoticeVersion(req) {
  return pickFirst(
    req.body && req.body.consentVersion,
    req.body && req.body.noticeVersion,
    req.query && req.query.consentVersion,
    req.query && req.query.noticeVersion
  ) || CONSENT_NOTICE_VERSION;
}

async function logConsentAuditEvent(req, patient) {
  if (!ENABLE_CONSENT_AUDIT_LOG || !patient) return;

  const payload = {
    schemaVersion: 1,
    event: 'portal_consent_accepted',
    at: new Date().toISOString(),
    patientId: pickFirst(patient.userId),
    controlNumber: pickFirst(patient.controlNumber),
    consentVersion: resolveConsentNoticeVersion(req),
    ipHash: hashString(getRequestIp(req)),
    userAgentHash: hashString(pickFirst(req.headers && req.headers['user-agent'])),
    origin: pickFirst(req.headers && req.headers.origin)
  };

  const writeTasks = [];

  if (CONSENT_LOG_FILE) {
    writeTasks.push((async () => {
      await fs.mkdir(path.dirname(CONSENT_LOG_FILE), { recursive: true });
      await ensureLogRetention(CONSENT_LOG_FILE);
      await fs.appendFile(CONSENT_LOG_FILE, `${JSON.stringify(payload)}\n`, 'utf8');
    })());
  }

  if (CONSENT_LOG_TO_SHEETS && CONSENT_LOG_SHEET_NAME) {
    writeTasks.push((async () => {
      const { sheets } = await getGoogleClients();
      await sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: `${CONSENT_LOG_SHEET_NAME}!A:H`,
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        requestBody: {
          values: [[
            payload.at,
            payload.event,
            payload.patientId,
            payload.controlNumber,
            payload.consentVersion,
            payload.ipHash,
            payload.userAgentHash,
            payload.origin
          ]]
        }
      });
    })());
  }

  if (!writeTasks.length) return;

  const results = await Promise.allSettled(writeTasks);
  for (const result of results) {
    if (result.status === 'rejected') {
      const message = result.reason && result.reason.message ? result.reason.message : String(result.reason || 'Unknown error');
      console.error('Consent audit log error:', message);
    }
  }
}

async function verifyTurnstileToken({ token, remoteIp }) {
  if (!REQUIRE_PORTAL_CAPTCHA) return { ok: true };
  if (!TURNSTILE_SECRET_KEY) return { ok: false, message: 'CAPTCHA is not configured on the server.' };
  if (!token) return { ok: false, message: 'Please complete CAPTCHA verification.' };
  if (typeof fetch !== 'function') return { ok: false, message: 'CAPTCHA verification is unavailable on the server.' };

  try {
    const body = new URLSearchParams();
    body.set('secret', TURNSTILE_SECRET_KEY);
    body.set('response', token);
    if (remoteIp) body.set('remoteip', remoteIp);

    const response = await fetch(TURNSTILE_VERIFY_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: body.toString()
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok || !data.success) {
      return { ok: false, message: 'CAPTCHA check failed. Please try again.' };
    }
    return { ok: true };
  } catch (_err) {
    return { ok: false, message: 'Unable to verify CAPTCHA right now. Please try again.' };
  }
}

async function enforcePortalVerificationGuards(req, res) {
  const consentAccepted = pickFirst(
    req.body && req.body.consentAccepted,
    req.body && req.body.consent,
    req.query && req.query.consentAccepted,
    req.query && req.query.consent
  );

  if (REQUIRE_PORTAL_CONSENT && !parseBoolean(consentAccepted)) {
    res.status(400).json({
      status: 'error',
      message: 'You must accept the Terms of Use and Data Privacy Notice before accessing results.'
    });
    return false;
  }

  const captchaToken = pickFirst(
    req.body && req.body.captchaToken,
    req.body && req.body.turnstileToken,
    req.query && req.query.captchaToken,
    req.query && req.query.turnstileToken
  );

  const captchaResult = await verifyTurnstileToken({
    token: captchaToken,
    remoteIp: getRequestIp(req)
  });
  if (!captchaResult.ok) {
    res.status(400).json({ status: 'error', message: captchaResult.message });
    return false;
  }

  return true;
}

function escapeDriveQuery(value) {
  return String(value || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function safeFilename(name) {
  return String(name || 'report.pdf').replace(/[\r\n"\\]/g, '_');
}

function normalizeToken(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

function normalizeHumanName(value) {
  return String(value || '').trim().toLowerCase().replace(/\s+/g, ' ');
}

function normalizeHeaderToken(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

function normalizeSearchText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9@.\-+/ ]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function findHeaderIndex(headers, aliases) {
  const wanted = new Set((aliases || []).map((a) => normalizeHeaderToken(a)).filter(Boolean));
  if (!wanted.size) return -1;
  for (let i = 0; i < headers.length; i += 1) {
    const token = normalizeHeaderToken(headers[i]);
    if (wanted.has(token)) return i;
  }
  return -1;
}

function resolvePatientMasterColMap(headers) {
  const out = {};
  let recognized = 0;
  for (const key of Object.keys(PATIENT_MASTER_HEADER_ALIASES)) {
    const idx = findHeaderIndex(headers, PATIENT_MASTER_HEADER_ALIASES[key]);
    out[key] = idx;
    if (idx >= 0) recognized += 1;
  }
  const hasHeader = recognized >= 2;
  if (!hasHeader) {
    for (const key of Object.keys(PATIENT_MASTER_FALLBACK_COL)) {
      out[key] = PATIENT_MASTER_FALLBACK_COL[key];
    }
  }
  return { hasHeader, col: out };
}

function readCellValue(row, idx) {
  if (!Array.isArray(row)) return '';
  if (!Number.isFinite(idx) || idx < 0) return '';
  return String(row[idx] || '').trim();
}

function buildDerivedPatientId(entry, rowNumber) {
  const seed = [
    normalizeToken(entry.lastName),
    normalizeToken(entry.firstName),
    normalizeToken(entry.middleName),
    normalizeDateKey(entry.birthday),
    normalizeToken(entry.contactNumber),
    normalizeToken(entry.email),
    normalizeToken(entry.patientName)
  ]
    .filter(Boolean)
    .join('|');
  const raw = seed || `row:${rowNumber}`;
  const digest = crypto.createHash('sha1').update(raw).digest('hex').slice(0, 8).toUpperCase();
  return `DRM-${digest}`;
}

function fileNameMatchesControl(fileName, controlNumber) {
  const name = String(fileName || '').trim().toLowerCase();
  const control = String(controlNumber || '').trim().toLowerCase();
  if (!name || !control) return false;
  if (name === `${control}.pdf`) return true;
  if (name.startsWith(`${control}-`) || name.startsWith(`${control}_`)) return true;
  return name.includes(control);
}

function fileNameMatchesTest(fileName, testName) {
  const testToken = normalizeToken(testName);
  if (!testToken) return false;
  const nameToken = normalizeToken(String(fileName || '').replace(/\.pdf$/i, ''));
  return !!nameToken && nameToken.includes(testToken);
}

function normalizeReportStatus(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (['released', 'ready', 'done', 'complete', 'completed'].includes(normalized)) return 'Released';
  return 'Pending';
}

function toIsoDateString(dateObj) {
  const y = dateObj.getFullYear();
  const m = String(dateObj.getMonth() + 1).padStart(2, '0');
  const d = String(dateObj.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function normalizeDateKey(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  const compact = raw.replace(/[._]/g, '-').replace(/\//g, '-');
  const ymd = compact.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (ymd) return `${ymd[1]}-${ymd[2]}-${ymd[3]}`;

  const ymdCompact = raw.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (ymdCompact) return `${ymdCompact[1]}-${ymdCompact[2]}-${ymdCompact[3]}`;

  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) return toIsoDateString(parsed);

  return raw.toLowerCase().replace(/\s+/g, ' ');
}

function normalizeStaffDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return new Date().toISOString().slice(0, 10);
  const normalized = normalizeDateKey(raw);
  if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) return normalized;
  return raw;
}

function buildDriveReportFileName(controlNumber, testName) {
  const control = normalizeControl(controlNumber);
  const testSlug = normalizeToken(testName).slice(0, 30) || 'report';
  const stamp = new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14);
  const rand = crypto.randomBytes(3).toString('hex');
  return `${control}-${testSlug}-${stamp}-${rand}.pdf`;
}

function resolveLocalResultPath(fileKey) {
  const key = String(fileKey || '').trim();
  if (!/^[a-zA-Z0-9._-]+\.pdf$/i.test(key)) return '';
  const full = path.resolve(LOCAL_RESULTS_DIR, key);
  const root = path.resolve(LOCAL_RESULTS_DIR) + path.sep;
  if (!full.startsWith(root)) return '';
  return full;
}

async function readLocalResultMeta(fileKey) {
  const fullPath = resolveLocalResultPath(fileKey);
  if (!fullPath) return null;
  try {
    const stat = await fs.stat(fullPath);
    if (!stat.isFile()) return null;
    return {
      id: String(fileKey || '').trim(),
      name: path.basename(fullPath),
      size: String(stat.size || ''),
      modifiedTime: stat.mtime ? stat.mtime.toISOString() : '',
      fullPath
    };
  } catch (_err) {
    return null;
  }
}

async function uploadPdfToLocal({ controlNumber, testName, pdfBuffer }) {
  await fs.mkdir(LOCAL_RESULTS_DIR, { recursive: true });
  const fileKey = buildDriveReportFileName(controlNumber, testName);
  const fullPath = resolveLocalResultPath(fileKey);
  if (!fullPath) throw new Error('Unable to create local file path.');
  await fs.writeFile(fullPath, pdfBuffer);
  return readLocalResultMeta(fileKey);
}

function parseStaffFileName(fileName) {
  const input = String(fileName || '').trim();
  if (!input) return { controlNumber: '', testName: '' };
  const stem = input.replace(/\.pdf$/i, '').trim();
  if (!stem) return { controlNumber: '', testName: '' };

  const withDate = stem.match(/^([A-Za-z0-9]+(?:-\d+)?)(?:[-_ ]+(.+?))[-_ ]+((?:19|20)\d{2}[-_]?\d{2}[-_]?\d{2})$/);
  if (withDate) {
    const dateRaw = withDate[3].replace(/_/g, '-');
    return {
      controlNumber: normalizeControl(withDate[1]),
      testName: String(withDate[2] || '').replace(/[_-]+/g, ' ').trim().toUpperCase(),
      testDate: normalizeStaffDate(dateRaw)
    };
  }

  // Supports: "CONTROL SURNAME, FIRSTNAME TEST.pdf"
  const controlWithCommaName = stem.match(/^([A-Za-z0-9]+(?:-\d+)?)[-_ ]+[^,]+,\s*(.+)$/);
  if (controlWithCommaName) {
    const control = normalizeControl(controlWithCommaName[1]);
    const tail = String(controlWithCommaName[2] || '').replace(/[_-]+/g, ' ').trim();
    const parts = tail.split(/\s+/).filter(Boolean);
    if (parts.length >= 2) {
      // Uses trailing token as test label (e.g. "VERLYN CBC" -> "CBC")
      return { controlNumber: control, testName: parts[parts.length - 1].toUpperCase(), testDate: '' };
    }
    return { controlNumber: control, testName: '', testDate: '' };
  }

  const controlFirst = stem.match(/^([A-Za-z0-9]+(?:-\d+)?)(?:[-_ ]+(.+))?$/);
  if (controlFirst) {
    return {
      controlNumber: normalizeControl(controlFirst[1]),
      testName: String(controlFirst[2] || '').replace(/[_-]+/g, ' ').trim().toUpperCase(),
      testDate: ''
    };
  }

  const split = stem.split(/[_ ]+/).filter(Boolean);
  if (split.length >= 2) {
    return {
      controlNumber: normalizeControl(split[0]),
      testName: split.slice(1).join(' ').replace(/[_-]+/g, ' ').trim().toUpperCase(),
      testDate: ''
    };
  }

  return { controlNumber: '', testName: '', testDate: '' };
}

function decodePdfBase64(value) {
  const input = String(value || '').trim();
  if (!input) return null;
  const match = input.match(/^data:application\/pdf;base64,(.+)$/i);
  const base64 = (match ? match[1] : input).replace(/\s+/g, '');
  const buffer = Buffer.from(base64, 'base64');
  if (!buffer.length) throw new Error('Invalid PDF payload');
  if (buffer.length > MAX_STAFF_PDF_BYTES) throw new Error(`PDF exceeds max size of ${MAX_STAFF_PDF_BYTES} bytes`);
  if (buffer.slice(0, 4).toString('utf8') !== '%PDF') throw new Error('Uploaded file is not a valid PDF');
  return buffer;
}

async function uploadPdfToDrive({ drive, controlNumber, testName, pdfBuffer }) {
  if (!drive) throw new Error('Drive client is unavailable.');
  if (!DRIVE_FOLDER_ID) throw new Error('DRIVE_FOLDER_ID is required for staff uploads');
  const fileName = buildDriveReportFileName(controlNumber, testName);
  const mediaBody = Readable.from(pdfBuffer);
  const created = await drive.files.create({
    requestBody: {
      name: fileName,
      parents: [DRIVE_FOLDER_ID],
      mimeType: 'application/pdf'
    },
    media: {
      mimeType: 'application/pdf',
      body: mediaBody
    },
    fields: 'id,name,mimeType,size,modifiedTime,parents',
    supportsAllDrives: true
  });
  return created.data;
}

async function uploadPdfAsset({ drive, controlNumber, testName, pdfBuffer }) {
  if (USE_LOCAL_STORAGE) {
    return uploadPdfToLocal({ controlNumber, testName, pdfBuffer });
  }
  return uploadPdfToDrive({ drive, controlNumber, testName, pdfBuffer });
}

async function getSheetRows(sheets) {
  const range = `${SHEET_NAME}!A2:I`;
  const result = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range,
    majorDimension: 'ROWS'
  });
  return result.data.values || [];
}

async function getPatientMasterValues(sheets) {
  const range = `${PATIENT_MASTER_SHEET_NAME}!A1:Z`;
  const result = await sheets.spreadsheets.values.get({
    spreadsheetId: PATIENT_MASTER_SHEET_ID,
    range,
    majorDimension: 'ROWS'
  });
  return result.data.values || [];
}

function toPatientMasterEntry(row, rowNumber, col) {
  const patientName = pickFirst(
    readCellValue(row, col.PATIENT_NAME),
    [readCellValue(row, col.LAST_NAME), readCellValue(row, col.FIRST_NAME), readCellValue(row, col.MIDDLE_NAME)]
      .filter(Boolean)
      .join(', ')
  );
  const entry = {
    patientId: normalizePatientId(readCellValue(row, col.PATIENT_ID)),
    patientName,
    lastName: readCellValue(row, col.LAST_NAME),
    firstName: readCellValue(row, col.FIRST_NAME),
    middleName: readCellValue(row, col.MIDDLE_NAME),
    gender: readCellValue(row, col.GENDER),
    birthday: readCellValue(row, col.BIRTHDAY),
    contactNumber: readCellValue(row, col.CONTACT),
    email: readCellValue(row, col.EMAIL),
    address: readCellValue(row, col.ADDRESS),
    doctor: readCellValue(row, col.DOCTOR),
    securePin: readCellValue(row, col.SECURE_PIN),
    sourceRow: rowNumber,
    generatedPatientId: false
  };
  if (!entry.patientId) {
    entry.patientId = buildDerivedPatientId(entry, rowNumber);
    entry.generatedPatientId = true;
  }
  entry.searchText = normalizeSearchText(
    [
      entry.patientId,
      entry.patientName,
      entry.lastName,
      entry.firstName,
      entry.middleName,
      entry.gender,
      entry.birthday,
      entry.contactNumber,
      entry.email,
      entry.address,
      entry.doctor
    ].join(' ')
  );
  return entry;
}

function scorePatientSearchEntry(entry, query, tokens) {
  let score = 0;
  const qText = normalizeSearchText(query);
  const qId = normalizePatientId(query);
  const entryName = normalizeSearchText(
    pickFirst(
      entry.patientName,
      [entry.lastName, entry.firstName, entry.middleName].filter(Boolean).join(' ')
    )
  );
  if (qId && entry.patientId === qId) score += 500;
  if (entryName && entryName === qText) score += 220;
  if (entryName && qText && entryName.startsWith(qText)) score += 130;
  if (tokens.length) score += tokens.length * 25;
  if (!entry.generatedPatientId) score += 10;
  score += Math.max(0, 5 - Math.floor((entry.sourceRow || 0) / 1000));
  return score;
}

async function searchPatientMaster({ sheets, query, limit }) {
  const allValues = await getPatientMasterValues(sheets);
  if (!allValues.length) return [];

  const rawHeaders = allValues[0] || [];
  const { hasHeader, col } = resolvePatientMasterColMap(rawHeaders);
  const dataRows = hasHeader ? allValues.slice(1) : allValues;
  const firstDataRow = hasHeader ? 2 : 1;

  const entries = dataRows
    .map((row, idx) => toPatientMasterEntry(row, firstDataRow + idx, col))
    .filter((entry) => !!pickFirst(entry.patientName, entry.lastName, entry.firstName));

  const qText = normalizeSearchText(query);
  const tokens = qText.split(' ').filter(Boolean);
  const matches = entries.filter((entry) => {
    if (!tokens.length) return true;
    return tokens.every((token) => entry.searchText.includes(token));
  });

  matches.sort((a, b) => {
    const scoreDiff = scorePatientSearchEntry(b, query, tokens) - scorePatientSearchEntry(a, query, tokens);
    if (scoreDiff !== 0) return scoreDiff;
    const nameA = normalizeSearchText(pickFirst(a.patientName, `${a.lastName}, ${a.firstName}`));
    const nameB = normalizeSearchText(pickFirst(b.patientName, `${b.lastName}, ${b.firstName}`));
    return nameA.localeCompare(nameB);
  });

  return matches.slice(0, limit).map((entry) => ({
    patientId: entry.patientId,
    patientName: entry.patientName,
    lastName: entry.lastName,
    firstName: entry.firstName,
    middleName: entry.middleName,
    gender: entry.gender,
    birthday: entry.birthday,
    contactNumber: entry.contactNumber,
    email: entry.email,
    address: entry.address,
    doctor: entry.doctor,
    securePin: entry.securePin,
    sourceRow: entry.sourceRow,
    generatedPatientId: !!entry.generatedPatientId
  }));
}

async function findPatientEntries({ sheets, patientId, controlNumber, securePin }) {
  const rows = await getSheetRows(sheets);
  const wantedPatientId = normalizePatientId(patientId);
  const wantedControl = normalizeControl(controlNumber);
  const wantedPin = normalizePin(securePin);

  const matches = [];
  for (const row of rows) {
    const rowControl = normalizeControl(row[SHEET_COL.CONTROL_NO]); // Col D
    const rowPin = normalizePin(row[SHEET_COL.SECURE_PIN]); // Col B
    const rowPatientId = normalizePatientId(row[SHEET_COL.PATIENT_ID] || row[SHEET_COL.CONTROL_NO]); // Col A fallback to control #
    if (rowPin !== wantedPin) continue;

    if (wantedPatientId) {
      if (rowPatientId !== wantedPatientId) continue;
    } else if (wantedControl) {
      if (rowControl !== wantedControl) continue;
    } else {
      continue;
    }

    matches.push({
      date: row[SHEET_COL.TEST_DATE] || '',
      patientName: row[SHEET_COL.PATIENT_NAME] || 'Patient',
      controlNumber: row[SHEET_COL.CONTROL_NO] || controlNumber,
      userId: row[SHEET_COL.PATIENT_ID] || row[SHEET_COL.CONTROL_NO] || patientId || controlNumber,
      testName: row[SHEET_COL.TEST_NAME] || '',
      status: row[SHEET_COL.STATUS] || '',
      pdfFileName: '',
      releasedAt: row[SHEET_COL.RELEASED_AT] || '',
      fileId: row[SHEET_COL.DRIVE_FILE_ID] || ''
    });
  }

  return matches;
}

async function findPatientEntriesByPatientId({ sheets, patientId }) {
  const rows = await getSheetRows(sheets);
  const wantedPatientId = normalizePatientId(patientId);
  if (!wantedPatientId) return [];

  const matches = [];
  for (const row of rows) {
    const rowPatientId = normalizePatientId(row[SHEET_COL.PATIENT_ID] || row[SHEET_COL.CONTROL_NO]); // Col A fallback to control #
    if (rowPatientId !== wantedPatientId) continue;
    matches.push({
      date: row[SHEET_COL.TEST_DATE] || '',
      patientName: row[SHEET_COL.PATIENT_NAME] || 'Patient',
      controlNumber: row[SHEET_COL.CONTROL_NO] || '',
      userId: row[SHEET_COL.PATIENT_ID] || row[SHEET_COL.CONTROL_NO] || wantedPatientId,
      testName: row[SHEET_COL.TEST_NAME] || '',
      status: row[SHEET_COL.STATUS] || '',
      pdfFileName: '',
      releasedAt: row[SHEET_COL.RELEASED_AT] || '',
      fileId: row[SHEET_COL.DRIVE_FILE_ID] || ''
    });
  }

  return matches;
}

function summarizePatient(entries, fallbackPatientId) {
  if (!Array.isArray(entries) || !entries.length) return null;
  const named = entries.find((e) => String(e.patientName || '').trim());
  const account = entries.find((e) => String(e.userId || '').trim());
  const controlled = entries.find((e) => String(e.controlNumber || '').trim());
  return {
    patientName: named ? named.patientName : 'Patient',
    userId: account ? account.userId : fallbackPatientId,
    controlNumber: controlled ? controlled.controlNumber : ''
  };
}

function mapEntriesToReports(entries, files) {
  if (!Array.isArray(entries) || !entries.length) return [];
  const source = entries.filter(
    (e) => String(e.testName || '').trim() || String(e.date || '').trim() || String(e.releasedAt || '').trim()
  );
  if (!source.length) return [];

  const remaining = Array.isArray(files) ? files.slice() : [];
  const fileById = new Map(remaining.map((f) => [String(f.id || ''), f]));
  const used = new Set();
  const reports = [];

  for (const entry of source) {
    const testName = String(entry.testName || '').trim() || 'Laboratory Test';
    const testDate = String(entry.date || '').trim();
    const releasedAt = String(entry.releasedAt || '').trim();
    const isReleased = normalizeReportStatus(entry.status) === 'Released' || !!releasedAt;
    const preferredFileId = String(entry.fileId || '').trim();
    const preferredFileName = String(entry.pdfFileName || '').trim();
    let matchedFile = null;

    if (isReleased && remaining.length) {
      if (preferredFileId && fileById.has(preferredFileId)) {
        matchedFile = fileById.get(preferredFileId);
      }
      if (!matchedFile && preferredFileName) {
        const target = preferredFileName.toLowerCase();
        matchedFile = remaining.find((f) => !used.has(f.id) && String(f.name || '').toLowerCase() === target);
      }
      if (!matchedFile) matchedFile = remaining.find((f) => !used.has(f.id) && fileNameMatchesTest(f.name, testName));
      if (!matchedFile) matchedFile = remaining.find((f) => !used.has(f.id));
      if (matchedFile && matchedFile.id) used.add(matchedFile.id);
    }

    reports.push({
      testName,
      testDate,
      controlNumber: String(entry.controlNumber || '').trim(),
      releasedAt,
      testStatus: isReleased ? 'Released' : 'Pending',
      fileId: matchedFile && matchedFile.id ? matchedFile.id : preferredFileId,
      fileName: matchedFile && matchedFile.name ? matchedFile.name : preferredFileName
    });
  }

  return reports;
}

async function findPdfFile({ drive, controlNumber }) {
  if (!DRIVE_FOLDER_ID) return null;

  const exactFileName = `${String(controlNumber).trim()}.pdf`;
  const escapedExact = escapeDriveQuery(exactFileName);

  // Exact filename first.
  const exactQuery = [
    `'${DRIVE_FOLDER_ID}' in parents`,
    `name = '${escapedExact}'`,
    `mimeType = 'application/pdf'`,
    `trashed = false`
  ].join(' and ');

  const exact = await drive.files.list({
    q: exactQuery,
    fields: 'files(id,name,mimeType,size,modifiedTime)',
    pageSize: 1,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true
  });

  if (exact.data.files && exact.data.files.length) {
    return exact.data.files[0];
  }

  // Fallback: contains control number + .pdf somewhere.
  const controlToken = escapeDriveQuery(String(controlNumber).trim());
  const fallbackQuery = [
    `'${DRIVE_FOLDER_ID}' in parents`,
    `name contains '${controlToken}'`,
    `mimeType = 'application/pdf'`,
    `trashed = false`
  ].join(' and ');

  const fallback = await drive.files.list({
    q: fallbackQuery,
    fields: 'files(id,name,mimeType,size,modifiedTime)',
    pageSize: 20,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    orderBy: 'modifiedTime desc'
  });

  const files = fallback.data.files || [];
  if (!files.length) return null;

  const exactIgnoreCase = files.find(
    (f) => String(f.name || '').toLowerCase() === exactFileName.toLowerCase()
  );

  return exactIgnoreCase || files[0];
}

async function findPdfFiles({ drive, controlNumber }) {
  if (!DRIVE_FOLDER_ID) return [];

  const exactFileName = `${String(controlNumber).trim()}.pdf`;
  const escapedExact = escapeDriveQuery(exactFileName);
  const controlToken = escapeDriveQuery(String(controlNumber).trim());

  const exactQuery = [
    `'${DRIVE_FOLDER_ID}' in parents`,
    `name = '${escapedExact}'`,
    `mimeType = 'application/pdf'`,
    `trashed = false`
  ].join(' and ');

  const exact = await drive.files.list({
    q: exactQuery,
    fields: 'files(id,name,mimeType,size,modifiedTime)',
    pageSize: 1,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true
  });

  const fallbackQuery = [
    `'${DRIVE_FOLDER_ID}' in parents`,
    `name contains '${controlToken}'`,
    `mimeType = 'application/pdf'`,
    `trashed = false`
  ].join(' and ');

  const fallback = await drive.files.list({
    q: fallbackQuery,
    fields: 'files(id,name,mimeType,size,modifiedTime)',
    pageSize: 100,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    orderBy: 'modifiedTime desc'
  });

  const combined = [...(exact.data.files || []), ...(fallback.data.files || [])];
  const map = new Map();
  for (const file of combined) {
    if (file && file.id && !map.has(file.id) && fileNameMatchesControl(file.name, controlNumber)) {
      map.set(file.id, file);
    }
  }

  const files = [...map.values()];
  files.sort((a, b) => {
    const aExact = String(a.name || '').toLowerCase() === exactFileName.toLowerCase();
    const bExact = String(b.name || '').toLowerCase() === exactFileName.toLowerCase();
    if (aExact !== bExact) return aExact ? -1 : 1;
    const aTs = new Date(a.modifiedTime || 0).getTime();
    const bTs = new Date(b.modifiedTime || 0).getTime();
    return bTs - aTs;
  });

  return files;
}

async function findPdfFileById({ drive, fileId, controlNumber }) {
  if (!fileId) return null;
  if (!drive) return null;
  try {
    const res = await drive.files.get({
      fileId: String(fileId).trim(),
      fields: 'id,name,mimeType,size,modifiedTime,parents',
      supportsAllDrives: true
    });

    const file = res.data;
    if (!file || file.mimeType !== 'application/pdf') return null;
    if (!Array.isArray(file.parents) || !file.parents.includes(DRIVE_FOLDER_ID)) return null;
    if (controlNumber && !fileNameMatchesControl(file.name, controlNumber)) return null;
    return file;
  } catch (err) {
    if (err && err.code === 404) return null;
    throw err;
  }
}

async function findPdfFilesForEntries({ drive, entries }) {
  if (!drive) return [];
  const controls = [...new Set(
    (entries || [])
      .map((entry) => normalizeControl(entry && entry.controlNumber))
      .filter(Boolean)
  )];

  if (!controls.length) return [];

  const grouped = await Promise.all(controls.map((controlNumber) => findPdfFiles({ drive, controlNumber })));
  const map = new Map();
  for (const files of grouped) {
    for (const file of files || []) {
      if (file && file.id && !map.has(file.id)) map.set(file.id, file);
    }
  }

  const out = [...map.values()];
  out.sort((a, b) => {
    const aTs = new Date(a.modifiedTime || 0).getTime();
    const bTs = new Date(b.modifiedTime || 0).getTime();
    return (Number.isFinite(bTs) ? bTs : 0) - (Number.isFinite(aTs) ? aTs : 0);
  });
  return out;
}

async function findLocalFilesForEntries(entries) {
  const ids = [...new Set(
    (entries || [])
      .map((entry) => String(entry && entry.fileId || '').trim())
      .filter(Boolean)
  )];
  const out = [];
  for (const id of ids) {
    const file = await readLocalResultMeta(id);
    if (file) out.push(file);
  }
  out.sort((a, b) => {
    const aTs = new Date(a.modifiedTime || 0).getTime();
    const bTs = new Date(b.modifiedTime || 0).getTime();
    return (Number.isFinite(bTs) ? bTs : 0) - (Number.isFinite(aTs) ? aTs : 0);
  });
  return out;
}

async function findStoredFilesForEntries({ drive, entries }) {
  if (USE_LOCAL_STORAGE) return findLocalFilesForEntries(entries);
  return findPdfFilesForEntries({ drive, entries });
}

async function findStoredFileById({ drive, fileId, controlNumber }) {
  if (USE_LOCAL_STORAGE) return readLocalResultMeta(fileId);
  return findPdfFileById({ drive, fileId, controlNumber });
}

function findPatientIdByPinAndName(rows, securePin, patientName) {
  const wantedPin = normalizePin(securePin);
  const wantedName = normalizeHumanName(patientName);
  if (!wantedPin || !wantedName) return '';

  for (let i = (rows || []).length - 1; i >= 0; i -= 1) {
    const row = rows[i] || [];
    const rowPin = normalizePin(row[SHEET_COL.SECURE_PIN]);
    const rowName = normalizeHumanName(row[SHEET_COL.PATIENT_NAME]);
    if (rowPin !== wantedPin || rowName !== wantedName) continue;
    const inferred = normalizePatientId(row[SHEET_COL.PATIENT_ID] || row[SHEET_COL.CONTROL_NO]);
    if (inferred) return inferred;
  }

  return '';
}

function findControlDefaults(rows, controlNumber) {
  const wanted = normalizeControl(controlNumber);
  for (const row of rows) {
    if (normalizeControl(row[SHEET_COL.CONTROL_NO]) !== wanted) continue;
    return {
      patientName: pickFirst(row[SHEET_COL.PATIENT_NAME]),
      securePin: pickFirst(row[SHEET_COL.SECURE_PIN]),
      patientId: pickFirst(row[SHEET_COL.PATIENT_ID], row[SHEET_COL.CONTROL_NO])
    };
  }
  return { patientName: '', securePin: '', patientId: '' };
}

function findUpdatableReportRowIndex(rows, controlNumber, testName, testDate) {
  const wantedControl = normalizeControl(controlNumber);
  const wantedTest = normalizeToken(testName);
  const wantedDateKey = normalizeDateKey(testDate);
  let fallback = -1;

  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i] || [];
    if (normalizeControl(row[SHEET_COL.CONTROL_NO]) !== wantedControl) continue;
    if (normalizeToken(row[SHEET_COL.TEST_NAME]) !== wantedTest) continue;

    const rowDateKey = normalizeDateKey(row[SHEET_COL.TEST_DATE]);
    const rowStatus = normalizeReportStatus(row[SHEET_COL.STATUS]);
    const rowFileId = String(row[SHEET_COL.DRIVE_FILE_ID] || '').trim();

    if (wantedDateKey && rowDateKey === wantedDateKey && rowStatus === 'Pending') return i;
    if (wantedDateKey && rowDateKey === wantedDateKey && fallback < 0) fallback = i;
    if (!wantedDateKey && rowStatus === 'Pending' && !rowFileId && fallback < 0) fallback = i;
  }

  return fallback;
}

function countControlTestRows(rows, controlNumber, testName) {
  const wantedControl = normalizeControl(controlNumber);
  const wantedTest = normalizeToken(testName);
  let count = 0;
  for (const row of rows || []) {
    if (normalizeControl(row && row[SHEET_COL.CONTROL_NO]) !== wantedControl) continue;
    if (normalizeToken(row && row[SHEET_COL.TEST_NAME]) !== wantedTest) continue;
    count += 1;
  }
  return count;
}

function parseUpdatedRowNumber(updatedRange) {
  const match = String(updatedRange || '').match(/!A(\d+):I(\d+)$/);
  if (!match) return 0;
  return Number(match[1] || 0);
}

function buildSheetReportRow({
  existingRow,
  controlNumber,
  patientName,
  patientId,
  securePin,
  testName,
  testDate,
  status,
  releasedAt,
  fileName,
  fileId
}) {
  const base = Array.isArray(existingRow) ? existingRow.slice(0, 9) : [];
  while (base.length < 9) base.push('');

  base[SHEET_COL.PATIENT_ID] = normalizePatientId(
    pickFirst(patientId, base[SHEET_COL.PATIENT_ID], controlNumber, base[SHEET_COL.CONTROL_NO])
  );
  base[SHEET_COL.SECURE_PIN] = pickFirst(securePin, base[SHEET_COL.SECURE_PIN]);
  base[SHEET_COL.PATIENT_NAME] = pickFirst(patientName, base[SHEET_COL.PATIENT_NAME], 'Patient');
  base[SHEET_COL.CONTROL_NO] = normalizeControl(pickFirst(controlNumber, base[SHEET_COL.CONTROL_NO]));
  base[SHEET_COL.TEST_NAME] = pickFirst(testName, base[SHEET_COL.TEST_NAME], 'Laboratory Test');
  base[SHEET_COL.TEST_DATE] = pickFirst(testDate, base[SHEET_COL.TEST_DATE], new Date().toISOString().slice(0, 10));
  base[SHEET_COL.STATUS] = normalizeReportStatus(status);
  base[SHEET_COL.RELEASED_AT] =
    base[SHEET_COL.STATUS] === 'Released'
      ? pickFirst(releasedAt, base[SHEET_COL.RELEASED_AT], new Date().toISOString())
      : '';
  base[SHEET_COL.DRIVE_FILE_ID] = pickFirst(fileId, base[SHEET_COL.DRIVE_FILE_ID]);
  return base;
}

async function upsertStaffReportEntry({
  sheets,
  controlNumber,
  patientName,
  patientId,
  securePin,
  testName,
  testDate,
  status,
  releasedAt,
  fileName,
  fileId,
  rowsCache
}) {
  const rows = Array.isArray(rowsCache) ? rowsCache : await getSheetRows(sheets);
  const targetIndex = findUpdatableReportRowIndex(rows, controlNumber, testName, testDate);
  const defaults = findControlDefaults(rows, controlNumber);
  const existingRow = targetIndex >= 0 ? rows[targetIndex] : null;

  const mergedRow = buildSheetReportRow({
    existingRow,
    controlNumber,
    patientName: pickFirst(patientName, defaults.patientName),
    patientId: pickFirst(patientId, defaults.patientId),
    securePin: pickFirst(securePin, defaults.securePin),
    testName,
    testDate: normalizeStaffDate(testDate),
    status,
    releasedAt,
    fileName,
    fileId
  });

  if (!pickFirst(mergedRow[SHEET_COL.SECURE_PIN])) {
    throw new Error('Secure PIN is required for new control number entries.');
  }

  if (targetIndex >= 0) {
    const rowNumber = targetIndex + 2;
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A${rowNumber}:I${rowNumber}`,
      valueInputOption: 'RAW',
      requestBody: { values: [mergedRow] }
    });
    if (Array.isArray(rowsCache)) rowsCache[targetIndex] = mergedRow;
    return { rowNumber, rowValues: mergedRow, action: 'updated' };
  }

  const appendResult = await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:I`,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: [mergedRow] }
  });

  const rowNumber = parseUpdatedRowNumber(
    appendResult &&
      appendResult.data &&
      appendResult.data.updates &&
      appendResult.data.updates.updatedRange
  );

  if (Array.isArray(rowsCache)) rowsCache.push(mergedRow);
  return { rowNumber, rowValues: mergedRow, action: 'created' };
}

async function listStaffReports({ sheets, controlNumber, patientId }) {
  const rows = await getSheetRows(sheets);
  const wantedControl = normalizeControl(controlNumber);
  const wantedPatientId = normalizePatientId(patientId);
  const out = [];

  rows.forEach((row, idx) => {
    const rowPatientId = normalizePatientId(row[SHEET_COL.PATIENT_ID] || row[SHEET_COL.CONTROL_NO]);
    if (wantedPatientId) {
      if (rowPatientId !== wantedPatientId) return;
    } else if (wantedControl) {
      if (normalizeControl(row[SHEET_COL.CONTROL_NO]) !== wantedControl) return;
    } else {
      return;
    }
    out.push({
      rowNumber: idx + 2,
      date: row[SHEET_COL.TEST_DATE] || '',
      patientName: row[SHEET_COL.PATIENT_NAME] || 'Patient',
      patientId: row[SHEET_COL.PATIENT_ID] || row[SHEET_COL.CONTROL_NO] || '',
      controlNumber: row[SHEET_COL.CONTROL_NO] || wantedControl,
      securePinMasked: pickFirst(row[SHEET_COL.SECURE_PIN]) ? '********' : '',
      testName: row[SHEET_COL.TEST_NAME] || '',
      status: normalizeReportStatus(row[SHEET_COL.STATUS]),
      pdfFileName: '',
      releasedAt: row[SHEET_COL.RELEASED_AT] || '',
      fileId: row[SHEET_COL.DRIVE_FILE_ID] || ''
    });
  });

  out.sort((a, b) => {
    const aTs = new Date(a.releasedAt || a.date || 0).getTime();
    const bTs = new Date(b.releasedAt || b.date || 0).getTime();
    return (Number.isFinite(bTs) ? bTs : 0) - (Number.isFinite(aTs) ? aTs : 0);
  });

  return out;
}

async function processStaffReportSubmission({ sheets, drive, input, rowsCache }) {
  const source = input || {};
  const sourceFileName = pickFirst(source.originalFileName, source.fileName, source.filename);
  const inferred = parseStaffFileName(sourceFileName);

  const controlNumber = normalizeControl(
    pickFirst(source.controlNumber, source.control, inferred.controlNumber)
  );
  const testName = pickFirst(source.testName, inferred.testName);
  const testDate = pickFirst(source.testDate, source.date, inferred.testDate);
  const patientName = pickFirst(source.patientName, source.name);
  let patientId = normalizePatientId(pickFirst(source.patientId, source.userId, source.accountId));
  const securePin = pickFirst(source.securePin, source.pin);
  const status = normalizeReportStatus(source.status);
  const pdfBase64 = pickFirst(source.pdfBase64, source.pdfData, source.base64);

  if (!controlNumber || !testName) {
    throw new Error('Missing control number or test name. Use fields or file name format CONTROL-TEST.pdf.');
  }

  const lookupRows = Array.isArray(rowsCache) ? rowsCache : await getSheetRows(sheets);
  if (!patientId) {
    patientId = findPatientIdByPinAndName(lookupRows, securePin, patientName);
  }
  const sameTestCount = countControlTestRows(lookupRows, controlNumber, testName);
  if (sameTestCount > 0 && !normalizeDateKey(testDate)) {
    throw new Error('Test date is required when this control number already has the same test name.');
  }

  let uploadedFile = null;
  // No file upload means still pending, even if client requested "Released".
  const desiredStatus = pdfBase64 ? 'Released' : 'Pending';

  if (pdfBase64) {
    const pdfBuffer = decodePdfBase64(pdfBase64);
    uploadedFile = await uploadPdfAsset({
      drive,
      controlNumber,
      testName,
      pdfBuffer
    });
  }

  const releasedAt = desiredStatus === 'Released' ? new Date().toISOString() : '';
  const upsert = await upsertStaffReportEntry({
    sheets,
    controlNumber,
    patientName,
    patientId,
    securePin,
    testName,
    testDate,
    status: desiredStatus,
    releasedAt,
    fileName: uploadedFile && uploadedFile.name ? uploadedFile.name : '',
    fileId: uploadedFile && uploadedFile.id ? uploadedFile.id : '',
    rowsCache: lookupRows
  });

  return {
    action: upsert.action,
    rowNumber: upsert.rowNumber,
    patientId: normalizePatientId(upsert && upsert.rowValues && upsert.rowValues[SHEET_COL.PATIENT_ID]),
    controlNumber,
    testName,
    reportStatus: desiredStatus,
    releasedAt,
    file: uploadedFile
      ? {
          id: uploadedFile.id || '',
          name: uploadedFile.name || '',
          modifiedTime: uploadedFile.modifiedTime || '',
          size: uploadedFile.size || ''
        }
      : null
  };
}

const app = express();
app.disable('x-powered-by');
app.use(helmet({ crossOriginResourcePolicy: false }));
app.use(express.json({ limit: JSON_BODY_LIMIT }));
app.use(
  cors({
    origin(origin, callback) {
      if (!origin) return callback(null, true);
      if (ALLOW_ALL_ORIGINS) return callback(null, true);
      if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
      return callback(new Error('CORS origin denied'));
    },
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Staff-User', 'X-Staff-Session'],
    maxAge: 86400
  })
);

app.use((err, _req, res, next) => {
  if (err && String(err.message || '').includes('CORS')) {
    return res.status(403).json({ status: 'error', message: 'Origin is not allowed' });
  }
  return next(err);
});

app.use(
  rateLimit({
    windowMs: 60 * 1000,
    limit: 30,
    standardHeaders: true,
    legacyHeaders: false
  })
);

app.get('/healthz', (_req, res) => {
  res.json({
    status: 'ok',
    service: 'drmed-portal-backend',
    time: new Date().toISOString(),
    resultStorage: RESULT_STORAGE,
    driveAuthMode: USE_LOCAL_STORAGE ? 'none' : DRIVE_AUTH_MODE,
    driveOAuthConfigured: USE_DRIVE_OAUTH_USER
      ? !!(GOOGLE_OAUTH_CLIENT_ID && GOOGLE_OAUTH_CLIENT_SECRET && GOOGLE_OAUTH_REFRESH_TOKEN)
      : false
  });
});

app.post('/api/staff/auth/login', async (req, res) => {
  if (!ENABLE_STAFF_SESSION_GATE) {
    return res.status(503).json({
      status: 'error',
      message: 'Staff session gate is disabled. Configure STAFF_GATE_USERS_JSON.'
    });
  }

  const staffUserRaw = pickFirst(req.body && req.body.staffUser, req.body && req.body.user, req.query && req.query.staffUser);
  const staffUser = normalizeStaffUser(staffUserRaw);
  const password = pickFirst(
    req.body && req.body.password,
    req.body && req.body.staffPassword,
    req.query && req.query.password,
    req.query && req.query.staffPassword
  );
  const mfaCode = pickFirst(
    req.body && (req.body.mfa || req.body.otp || req.body.totp),
    req.query && (req.query.mfa || req.query.otp || req.query.totp)
  );

  if (!staffUser) {
    return res.status(400).json({ status: 'error', message: 'Staff user is required.' });
  }

  const lockout = getStaffLockoutStatus(req, staffUser);
  if (lockout.locked) {
    const retryAfterSec = Math.max(1, Math.ceil(lockout.retryAfterMs / 1000));
    res.set('Retry-After', String(retryAfterSec));
    await logStaffAuditEvent(req, 'staff_auth_login_locked', {
      staffUser,
      retryAfterSec
    });
    return res.status(429).json({
      status: 'error',
      message: `Too many failed attempts. Try again in ${Math.ceil(retryAfterSec / 60)} minutes.`,
      retryAfterSec
    });
  }

  const staffConfig = resolveStaffUserConfig(staffUser);
  if (!staffConfig) {
    await recordStaffAuthFailure(req, staffUser, 'unknown_user');
    return res.status(401).json({ status: 'error', message: 'Invalid staff user or password.' });
  }

  if (!verifyStaffPassword(password, staffConfig)) {
    await recordStaffAuthFailure(req, staffUser, 'invalid_password');
    return res.status(401).json({ status: 'error', message: 'Invalid staff user or password.' });
  }

  if (staffConfig.mfaRequired) {
    if (!staffConfig.mfaSecret) {
      await logStaffAuditEvent(req, 'staff_auth_mfa_missing_secret', { staffUser });
      return res.status(503).json({ status: 'error', message: 'MFA is required but not configured.' });
    }
    if (!normalizeMfaCode(mfaCode)) {
      await recordStaffAuthFailure(req, staffUser, 'mfa_required');
      return res.status(400).json({ status: 'error', message: 'MFA code required.' });
    }
    if (!verifyTotp(staffConfig.mfaSecret, mfaCode)) {
      await recordStaffAuthFailure(req, staffUser, 'invalid_mfa');
      return res.status(401).json({ status: 'error', message: 'Invalid staff user, password, or MFA code.' });
    }
  }

  clearStaffAuthFailures(req, staffUser);

  const staffSessionToken = createStaffSessionToken({ actor: staffUser });
  const verified = verifyStaffSessionToken(staffSessionToken);
  await logStaffAuditEvent(req, 'staff_auth_login_success', {
    staffUser,
    mfaRequired: staffConfig.mfaRequired || false
  });
  return res.json({
    status: 'success',
    staffUser,
    staffSessionToken,
    expiresInSec: SAFE_STAFF_SESSION_TTL_SECONDS,
    expiresAt: verified.ok && verified.payload && verified.payload.exp
      ? new Date(verified.payload.exp * 1000).toISOString()
      : ''
  });
});

/**
 * Staff report operations
 *
 * Auth:
 * - Header X-Staff-Session from /api/staff/auth/login
 */
app.get('/api/staff/patients/search', async (req, res) => {
  if (!requireStaffAuth(req, res)) return;
  const query = pickFirst(
    req.query && req.query.q,
    req.query && req.query.query,
    req.query && req.query.search,
    req.query && req.query.name,
    req.query && req.query.patient
  );
  if (String(query || '').trim().length < 2) {
    return res.status(400).json({ status: 'error', message: 'Search query must be at least 2 characters.' });
  }
  const limitRaw = Number(req.query && req.query.limit);
  const limit =
    Number.isFinite(limitRaw) && limitRaw > 0
      ? Math.max(1, Math.min(100, Math.floor(limitRaw)))
      : SAFE_MAX_PATIENT_SEARCH_RESULTS;

  try {
    const { sheets } = await getGoogleClients();
    const patients = await searchPatientMaster({ sheets, query, limit });
    return res.json({
      status: 'success',
      query: String(query).trim(),
      count: patients.length,
      sheetName: PATIENT_MASTER_SHEET_NAME,
      patients
    });
  } catch (err) {
    console.error('Staff patient search error:', err.message);
    return res.status(500).json({ status: 'error', message: 'Unable to search patient master.' });
  }
});

app.get('/api/staff/reports', async (req, res) => {
  if (!requireStaffAuth(req, res)) return;
  const patientId = pickFirst(req.query && req.query.patientId, req.query && req.query.userId, req.query && req.query.accountId);
  const controlNumber = pickFirst(req.query && req.query.control, req.query && req.query.controlNumber);
  if (!patientId && !controlNumber) {
    return res.status(400).json({ status: 'error', message: 'Missing control number or patient user ID.' });
  }

  try {
    const { sheets } = await getGoogleClients();
    const reports = await listStaffReports({ sheets, controlNumber, patientId });
    return res.json({
      status: 'success',
      patientId: normalizePatientId(patientId),
      controlNumber: normalizeControl(controlNumber),
      count: reports.length,
      reports
    });
  } catch (err) {
    console.error('Staff list error:', err.message);
    return res.status(500).json({ status: 'error', message: 'Unable to list staff reports.' });
  }
});

app.post('/api/staff/reports', async (req, res) => {
  if (!requireStaffAuth(req, res)) return;

  try {
    const { sheets, drive } = await getGoogleClients();
    const result = await processStaffReportSubmission({
      sheets,
      drive,
      input: req.body || {}
    });

    await logStaffAuditEvent(req, 'staff_report_upsert', {
      action: result.action,
      rowNumber: result.rowNumber,
      controlNumber: result.controlNumber,
      testName: result.testName,
      status: result.reportStatus,
      fileId: result.file && result.file.id ? result.file.id : ''
    });

    return res.json({ status: 'success', ...result });
  } catch (err) {
    console.error('Staff upload error:', err.message);
    return res.status(400).json({ status: 'error', message: err.message || 'Unable to save staff report.' });
  }
});

app.post('/api/staff/reports/bulk', async (req, res) => {
  if (!requireStaffAuth(req, res)) return;

  const items = Array.isArray(req.body && req.body.items) ? req.body.items : [];
  const defaults = (req.body && req.body.defaults && typeof req.body.defaults === 'object')
    ? req.body.defaults
    : {};

  if (!items.length) {
    return res.status(400).json({ status: 'error', message: 'Missing bulk items array.' });
  }

  if (items.length > MAX_STAFF_BULK_ITEMS) {
    return res.status(400).json({
      status: 'error',
      message: `Bulk upload limit exceeded. Max ${MAX_STAFF_BULK_ITEMS} items per request.`
    });
  }

  const output = [];
  try {
    const { sheets, drive } = await getGoogleClients();
    const rowsCache = await getSheetRows(sheets);
    for (let i = 0; i < items.length; i += 1) {
      const item = items[i] || {};
      const merged = {
        controlNumber: pickFirst(item.controlNumber, item.control, defaults.controlNumber, defaults.control),
        patientId: pickFirst(item.patientId, item.userId, item.accountId, defaults.patientId, defaults.userId, defaults.accountId),
        testName: pickFirst(item.testName, defaults.testName),
        testDate: pickFirst(item.testDate, item.date, defaults.testDate, defaults.date),
        patientName: pickFirst(item.patientName, item.name, defaults.patientName, defaults.name),
        securePin: pickFirst(item.securePin, item.pin, defaults.securePin, defaults.pin),
        status: pickFirst(item.status, defaults.status, 'Released'),
        pdfBase64: pickFirst(item.pdfBase64, item.pdfData, item.base64),
        originalFileName: pickFirst(item.originalFileName, item.fileName, item.filename)
      };

      try {
        const result = await processStaffReportSubmission({
          sheets,
          drive,
          input: merged,
          rowsCache
        });
        output.push({
          index: i,
          ok: true,
          ...result
        });
      } catch (err) {
        output.push({
          index: i,
          ok: false,
          controlNumber: normalizeControl(pickFirst(merged.controlNumber)),
          testName: pickFirst(merged.testName),
          fileName: pickFirst(merged.originalFileName),
          error: err && err.message ? err.message : 'Failed to process item.'
        });
      }
    }

    const successCount = output.filter((item) => item.ok).length;
    const errorCount = output.length - successCount;

    await logStaffAuditEvent(req, 'staff_report_bulk_upsert', {
      total: output.length,
      successCount,
      errorCount
    });

    return res.status(errorCount ? 207 : 200).json({
      status: errorCount ? 'partial' : 'success',
      total: output.length,
      successCount,
      errorCount,
      results: output
    });
  } catch (err) {
    console.error('Staff bulk upload error:', err.message);
    return res.status(500).json({ status: 'error', message: err.message || 'Unable to process bulk upload.' });
  }
});

/**
 * Streams a PDF using short-lived access token from /api/portal/verify.
 *
 * Request examples:
 * GET  /api/portal/report?accessToken=...&fileId=...
 * POST /api/portal/report {"accessToken":"...","fileId":"..."}
 */
app.all('/api/portal/report', async (req, res) => {
  const requestedFileId = pickFirst(
    req.body && req.body.fileId,
    req.query && req.query.fileId
  );
  const forceDownload = parseBoolean(
    pickFirst(
      req.body && req.body.download,
      req.query && req.query.download
    )
  );
  const portalAccessToken = extractPortalAccessToken(req);

  if (!portalAccessToken) {
    return res.status(401).json({ status: 'error', message: 'Session token required. Please verify credentials again.' });
  }

  try {
    const trustedSession = verifyPortalAccessToken(portalAccessToken);
    if (!trustedSession.ok || !trustedSession.payload || !trustedSession.payload.sub) {
      return res.status(401).json({ status: 'error', message: 'Invalid or expired session token. Please verify again.' });
    }
    const tokenPatientId = normalizePatientId(trustedSession.payload.sub);
    const { drive, sheets } = await getGoogleClients();
    const entries = await findPatientEntriesByPatientId({ sheets, patientId: tokenPatientId });

    if (!entries.length) {
      return res.status(401).json({ status: 'error', message: 'Session is no longer valid. Please verify again.' });
    }

    let selectedFileId = '';
    if (requestedFileId) {
      const wanted = String(requestedFileId).trim();
      const allowed = new Set(
        entries
          .filter((entry) => normalizeReportStatus(entry.status) === 'Released')
          .map((entry) => String(entry.fileId || '').trim())
          .filter(Boolean)
      );
      if (!allowed.has(wanted)) {
        return res.status(404).json({ status: 'error', message: 'Result PDF not found' });
      }
      selectedFileId = wanted;
    } else {
      const released = entries
        .filter((entry) => normalizeReportStatus(entry.status) === 'Released' && String(entry.fileId || '').trim())
        .sort((a, b) => new Date(b.releasedAt || b.date || 0).getTime() - new Date(a.releasedAt || a.date || 0).getTime());
      if (released.length) selectedFileId = String(released[0].fileId || '').trim();
    }

    if (!selectedFileId) {
      return res.status(404).json({ status: 'error', message: 'Result PDF not found' });
    }

    const file = await findStoredFileById({ drive, fileId: selectedFileId, controlNumber: '' });

    if (!file || !file.id) {
      return res.status(404).json({ status: 'error', message: 'Result PDF not found' });
    }

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader(
      'Content-Disposition',
      `${forceDownload ? 'attachment' : 'inline'}; filename="${safeFilename(file.name || `${tokenPatientId}.pdf`)}"`
    );
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Pragma', 'no-cache');

    if (USE_LOCAL_STORAGE) {
      if (!file.fullPath) {
        return res.status(404).json({ status: 'error', message: 'Result PDF not found' });
      }
      const stream = fsNative.createReadStream(file.fullPath);
      stream.on('error', (err) => {
        console.error('Local file stream error:', err.message);
        if (!res.headersSent) {
          res.status(500).json({ status: 'error', message: 'Failed to stream PDF' });
        } else {
          res.destroy(err);
        }
      });
      stream.pipe(res);
    } else {
      const response = await drive.files.get(
        {
          fileId: file.id,
          alt: 'media',
          supportsAllDrives: true
        },
        { responseType: 'stream' }
      );

      response.data.on('error', (err) => {
        console.error('Drive stream error:', err.message);
        if (!res.headersSent) {
          res.status(500).json({ status: 'error', message: 'Failed to stream PDF' });
        } else {
          res.destroy(err);
        }
      });

      response.data.pipe(res);
    }
  } catch (err) {
    console.error('Backend error:', err.message);
    return res.status(500).json({ status: 'error', message: 'Internal server error' });
  }
});

// Optional JSON-only check endpoint (no PDF stream)
app.all('/api/portal/verify', async (req, res) => {
  const patientId = pickFirst(
    req.body && req.body.userId,
    req.body && req.body.patientId,
    req.body && req.body.accountId,
    req.query && req.query.userId,
    req.query && req.query.patientId,
    req.query && req.query.accountId
  );

  const controlNumber = pickFirst(
    req.body && req.body.control,
    req.body && req.body.controlNumber,
    req.query && req.query.control,
    req.query && req.query.controlNumber
  );

  const securePin = pickFirst(
    req.body && req.body.pin,
    req.body && req.body.securePin,
    req.body && req.body.bday,
    req.query && req.query.pin,
    req.query && req.query.securePin,
    req.query && req.query.bday
  );

  const hasAccount = !!normalizePatientId(patientId);
  const hasControl = !!normalizeControl(controlNumber);
  if ((!hasAccount && !hasControl) || !securePin) {
    return res.status(400).json({ status: 'error', message: 'Missing Patient User ID (or control number) or secure PIN' });
  }

  try {
    const guardsPassed = await enforcePortalVerificationGuards(req, res);
    if (!guardsPassed) return;

    const { sheets, drive } = await getGoogleClients();
    const entries = await findPatientEntries({
      sheets,
      patientId,
      controlNumber,
      securePin
    });
    const fallbackAccountId = hasAccount ? normalizePatientId(patientId) : normalizeControl(controlNumber);
    const patient = summarizePatient(entries, fallbackAccountId);
    if (!patient) return res.status(401).json({ status: 'error' });

    await logConsentAuditEvent(req, patient);
    const consentVersion = resolveConsentNoticeVersion(req);
    const accessToken = createPortalAccessToken({
      patientId: patient.userId,
      securePin,
      consentVersion
    });

    const files = await findStoredFilesForEntries({ drive, entries });
    const reports = mapEntriesToReports(entries, files);
    const first = files[0] || null;
    return res.json({
      status: 'success',
      name: patient.patientName,
      userId: patient.userId,
      controlNumber: patient.controlNumber,
      accessToken,
      accessTokenExpiresInSec: SAFE_PORTAL_ACCESS_TOKEN_TTL_SECONDS,
      hasPdf: !!(first && first.id),
      pdfFileName: first ? first.name : '',
      reports,
      files: files.map((f) => ({
        id: f.id,
        name: f.name,
        modifiedTime: f.modifiedTime || '',
        size: f.size || ''
      }))
    });
  } catch (err) {
    console.error('Verify error:', err.message);
    return res.status(500).json({ status: 'error' });
  }
});

app.listen(PORT, () => {
  console.log(`DRMed backend listening on :${PORT}`);
});
