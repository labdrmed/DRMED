/**
 * Example: doctor queue backed by Google Sheets.
 * Merge into your real Express app; protect routes with your staff session middleware.
 *
 * npm i express googleapis
 *
 * Env:
 *   GOOGLE_SERVICE_ACCOUNT_JSON (full JSON string; needs spreadsheets scope)
 *   SCHEDULE_SPREADSHEET_ID
 *   SCHEDULE_SHEET_TAB             (default: DoctorQueue)
 *
 * Sheet row 1 headers:
 *   date | doctor | patient_name | contact | status | remarks | created_at | id
 */

const express = require("express");
const { google } = require("googleapis");
const crypto = require("crypto");

const router = express.Router();

const TAB = () => process.env.SCHEDULE_SHEET_TAB || "DoctorQueue";
const SPREADSHEET_ID = () => process.env.SCHEDULE_SPREADSHEET_ID;

function getSheets() {
  if (!process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
    throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON is required");
  }
  const creds = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  return google.sheets({ version: "v4", auth });
}

/** Parse data rows (skip header). Each row: 8 columns. */
async function readAllDataRows() {
  const sheets = getSheets();
  const id = SPREADSHEET_ID();
  if (!id) throw new Error("SCHEDULE_SPREADSHEET_ID is required");

  const range = `${TAB()}!A2:H`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: id, range });
  const values = res.data.values || [];
  return values.map((row, index) => ({
    rowNumber: index + 2,
    date: row[0] || "",
    doctor: row[1] || "",
    patient_name: row[2] || "",
    contact: row[3] || "",
    status: row[4] || "",
    remarks: row[5] || "",
    created_at: row[6] || "",
    id: row[7] || "",
  }));
}

/**
 * GET /api/staff/schedule?date=2026-04-16&doctor=Dr%20Smith
 * TODO: mount under your app as router.use("/api/staff", staffSessionMiddleware, scheduleRouter)
 * and define route as router.get("/schedule", ...)
 */
router.get("/schedule", async (req, res) => {
  try {
    const { date, doctor } = req.query;
    const rows = await readAllDataRows();
    let items = rows;
    if (date) items = items.filter((r) => r.date === date);
    if (doctor) items = items.filter((r) => r.doctor === doctor);
    res.json({ items });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "schedule_read_failed" });
  }
});

/**
 * POST /api/staff/schedule
 * body: { date, doctor, patient_name, contact, status, remarks }
 */
router.post("/schedule", express.json(), async (req, res) => {
  try {
    const id = SPREADSHEET_ID();
    const { date, doctor, patient_name, contact, status, remarks } = req.body || {};
    if (!date || !doctor || !patient_name) {
      return res.status(400).json({ error: "date_doctor_patient_name_required" });
    }
    const rowId = crypto.randomUUID();
    const created_at = new Date().toISOString();
    const row = [date, doctor, patient_name, contact || "", status || "Pending", remarks || "", created_at, rowId];

    const sheets = getSheets();
    await sheets.spreadsheets.values.append({
      spreadsheetId: id,
      range: `${TAB()}!A:H`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: [row] },
    });

    res.status(201).json({
      item: {
        date,
        doctor,
        patient_name,
        contact,
        status: status || "Pending",
        remarks,
        created_at,
        id: rowId,
      },
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "schedule_append_failed" });
  }
});

/**
 * PATCH /api/staff/schedule
 * body: { id, patient_name?, contact?, status?, remarks?, doctor?, date? }
 */
router.patch("/schedule", express.json(), async (req, res) => {
  try {
    const id = SPREADSHEET_ID();
    const body = req.body || {};
    const rowId = body.id;
    if (!rowId) return res.status(400).json({ error: "id_required" });

    const rows = await readAllDataRows();
    const found = rows.find((r) => r.id === rowId);
    if (!found) return res.status(404).json({ error: "not_found" });

    const updated = {
      date: body.date !== undefined ? body.date : found.date,
      doctor: body.doctor !== undefined ? body.doctor : found.doctor,
      patient_name: body.patient_name !== undefined ? body.patient_name : found.patient_name,
      contact: body.contact !== undefined ? body.contact : found.contact,
      status: body.status !== undefined ? body.status : found.status,
      remarks: body.remarks !== undefined ? body.remarks : found.remarks,
      created_at: found.created_at,
      id: found.id,
    };

    const line = [
      updated.date,
      updated.doctor,
      updated.patient_name,
      updated.contact,
      updated.status,
      updated.remarks,
      updated.created_at,
      updated.id,
    ];

    const sheets = getSheets();
    const range = `${TAB()}!A${found.rowNumber}:H${found.rowNumber}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId: id,
      range,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: [line] },
    });

    res.json({ item: { ...updated, rowNumber: found.rowNumber } });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "schedule_update_failed" });
  }
});

module.exports = { scheduleSheetRouter: router };
