#!/usr/bin/env python3
"""
Prepare import-ready patient records and duplicate-review outputs from:
  CUSTOMER LIST.xlsx -> sheet "CUSTOMER LIST"

No third-party dependencies required.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


XLSX_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
R_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS = {"x": XLSX_NS, "r": RELS_NS}


@dataclass
class RowRecord:
    source_row: int
    customer_name: str
    last_name: str
    first_name: str
    middle_name: str
    name_norm: str
    gender: str
    birthday_raw: str
    birthday_iso: str
    age_raw: str
    address_street: str
    address_barangay: str
    address_city: str
    address_full: str
    contact_number: str
    contact_norm: str
    email: str
    senior_or_pwd_flag: str
    senior_or_pwd_id_number: str
    doctor: str


def clean_text(value: str) -> str:
    out = value.replace("\n", " ").replace("\r", " ")
    out = re.sub(r"\s+", " ", out)
    return out.strip()


def normalize_token(value: str) -> str:
    out = clean_text(value).upper()
    out = re.sub(r"[^A-Z0-9/ ]+", "", out)
    out = re.sub(r"\s+", " ", out)
    return out.strip()


def normalize_gender(value: str) -> str:
    v = normalize_token(value)
    if v in {"MALE", "M"}:
        return "MALE"
    if v in {"FEMALE", "F"}:
        return "FEMALE"
    return "UNKNOWN"


def normalize_contact(value: str) -> str:
    digits = re.sub(r"[^0-9+]", "", clean_text(value))
    return digits


def parse_birthday(value: str) -> str:
    raw = clean_text(value)
    if not raw:
        return ""

    numeric = re.fullmatch(r"\d+(?:\.\d+)?", raw)
    if numeric:
        n = float(raw)
        if n > 1000:
            base = dt.date(1899, 12, 30)
            try:
                date_value = base + dt.timedelta(days=int(n))
                if 1900 <= date_value.year <= 2100:
                    return date_value.isoformat()
            except OverflowError:
                pass
        return ""

    normalized = raw.replace(".", "-").replace("/", "-")
    normalized = re.sub(r"\s+", "-", normalized)
    candidates = [normalized, raw]
    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%d-%b-%Y",
        "%d-%B-%Y",
        "%b-%d-%Y",
        "%B-%d-%Y",
    ]

    for candidate in candidates:
        for fmt in formats:
            try:
                parsed = dt.datetime.strptime(candidate, fmt).date()
                if 1900 <= parsed.year <= 2100:
                    return parsed.isoformat()
            except ValueError:
                continue

    return ""


def split_name(raw_name: str) -> Tuple[str, str, str, str]:
    raw = clean_text(raw_name)
    if not raw:
        return "", "", "", ""

    if "," in raw:
        left, right = raw.split(",", 1)
        last_name = normalize_token(left)
        right_tokens = normalize_token(right).split()
        first_name = right_tokens[0] if right_tokens else ""
        middle_name = " ".join(right_tokens[1:]) if len(right_tokens) > 1 else ""
    else:
        tokens = normalize_token(raw).split()
        if not tokens:
            return "", "", "", ""
        if len(tokens) == 1:
            first_name, last_name, middle_name = tokens[0], "", ""
        else:
            first_name = tokens[0]
            last_name = tokens[-1]
            middle_name = " ".join(tokens[1:-1]) if len(tokens) > 2 else ""

    name_norm = " ".join([x for x in [last_name, first_name, middle_name] if x]).strip()
    return last_name, first_name, middle_name, name_norm


def cell_ref_parts(ref: str) -> Tuple[str, int]:
    m = re.match(r"([A-Z]+)(\d+)$", ref or "")
    if not m:
        return "", 0
    return m.group(1), int(m.group(2))


def decode_cell(c: ET.Element, shared: List[str]) -> str:
    cell_type = c.attrib.get("t")
    if cell_type == "s":
        v = c.find("x:v", {"x": XLSX_NS})
        if v is None or v.text is None:
            return ""
        if not v.text.isdigit():
            return ""
        idx = int(v.text)
        return shared[idx] if 0 <= idx < len(shared) else ""
    if cell_type == "inlineStr":
        values = [t.text or "" for t in c.findall(".//x:t", {"x": XLSX_NS})]
        return "".join(values)
    v = c.find("x:v", {"x": XLSX_NS})
    return (v.text or "") if v is not None and v.text is not None else ""


def load_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    result = []
    for si in root.findall("x:si", {"x": XLSX_NS}):
        values = [t.text or "" for t in si.findall(".//x:t", {"x": XLSX_NS})]
        result.append("".join(values))
    return result


def resolve_sheet_path(zf: zipfile.ZipFile, sheet_name: str) -> str:
    wb = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_map: Dict[str, str] = {}
    for r in rels.findall("r:Relationship", NS):
        rel_map[r.attrib["Id"]] = r.attrib["Target"]

    for sheet in wb.findall("x:sheets/x:sheet", {"x": XLSX_NS}):
        if sheet.attrib.get("name") != sheet_name:
            continue
        rel_id = sheet.attrib.get(f"{{{R_NS}}}id", "")
        target = rel_map.get(rel_id, "")
        if not target:
            break
        return f"xl/{target}"

    raise ValueError(f'Sheet "{sheet_name}" not found in workbook.')


def build_records(xlsx_path: str, sheet_name: str) -> List[RowRecord]:
    with zipfile.ZipFile(xlsx_path) as zf:
        shared = load_shared_strings(zf)
        sheet_path = resolve_sheet_path(zf, sheet_name)
        sheet = ET.fromstring(zf.read(sheet_path))

    records: List[RowRecord] = []
    for row in sheet.findall(".//x:sheetData/x:row", {"x": XLSX_NS}):
        row_num = int(row.attrib.get("r", "0"))
        cells: Dict[str, str] = {}
        for c in row.findall("x:c", {"x": XLSX_NS}):
            ref = c.attrib.get("r", "")
            col, _ = cell_ref_parts(ref)
            if not col:
                continue
            cells[col] = clean_text(decode_cell(c, shared))

        customer_name = clean_text(cells.get("B", ""))
        if row_num <= 2 or not customer_name:
            continue

        last_name, first_name, middle_name, name_norm = split_name(customer_name)
        birthday_raw = clean_text(cells.get("D", ""))
        birthday_iso = parse_birthday(birthday_raw)

        street = clean_text(cells.get("F", ""))
        barangay = clean_text(cells.get("G", ""))
        city = clean_text(cells.get("H", ""))
        address_full = ", ".join([x for x in [street, barangay, city] if x])
        contact = clean_text(cells.get("I", ""))
        contact_norm = normalize_contact(contact)
        email = clean_text(cells.get("J", "")).lower()

        records.append(
            RowRecord(
                source_row=row_num,
                customer_name=customer_name,
                last_name=last_name,
                first_name=first_name,
                middle_name=middle_name,
                name_norm=name_norm,
                gender=normalize_gender(cells.get("C", "")),
                birthday_raw=birthday_raw,
                birthday_iso=birthday_iso,
                age_raw=clean_text(cells.get("E", "")),
                address_street=street,
                address_barangay=barangay,
                address_city=city,
                address_full=address_full,
                contact_number=contact,
                contact_norm=contact_norm,
                email=email,
                senior_or_pwd_flag=clean_text(cells.get("K", "")),
                senior_or_pwd_id_number=clean_text(cells.get("L", "")),
                doctor=clean_text(cells.get("M", "")),
            )
        )

    return records


def record_score(r: RowRecord) -> int:
    score = 0
    if r.birthday_iso:
        score += 3
    if r.contact_norm:
        score += 2
    if r.email:
        score += 1
    if r.address_full:
        score += 1
    if r.gender != "UNKNOWN":
        score += 1
    return score


def pick_canonical(records: List[RowRecord]) -> RowRecord:
    return sorted(records, key=lambda r: (-record_score(r), r.source_row))[0]


def make_group_key(r: RowRecord) -> Tuple[str, str]:
    strict = ""
    if r.last_name and r.first_name and r.birthday_iso:
        strict = f"{r.last_name}|{r.first_name}|{r.birthday_iso}"
    if strict:
        return "STRICT_NAME_BDAY", strict

    contact = ""
    if r.last_name and r.first_name and len(r.contact_norm) >= 7:
        contact = f"{r.last_name}|{r.first_name}|{r.contact_norm}"
    if contact:
        return "NAME_CONTACT", contact

    return "ROW_ONLY", str(r.source_row)


def likely_name_key(r: RowRecord) -> str:
    if r.last_name and r.first_name:
        return f"{r.last_name}|{r.first_name}"
    return r.name_norm


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_csv(path: str, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare patient import from CUSTOMER LIST sheet.")
    parser.add_argument(
        "--input",
        default="/Users/coleen/Desktop/DRMED Website/CUSTOMER LIST.xlsx",
        help="Path to CUSTOMER LIST.xlsx",
    )
    parser.add_argument(
        "--sheet",
        default="CUSTOMER LIST",
        help='Sheet name to read (default: "CUSTOMER LIST")',
    )
    parser.add_argument(
        "--output-dir",
        default="/Users/coleen/Desktop/DRMED Website/data-migration",
        help="Directory for generated CSV outputs",
    )
    parser.add_argument(
        "--id-prefix",
        default="DRM",
        help="Patient ID prefix",
    )
    parser.add_argument(
        "--id-width",
        default=6,
        type=int,
        help="Patient ID numeric width",
    )
    parser.add_argument(
        "--id-start",
        default=1,
        type=int,
        help="Starting numeric value for generated patient IDs",
    )
    args = parser.parse_args()

    records = build_records(args.input, args.sheet)
    if not records:
        raise SystemExit("No records found in the target sheet.")

    groups: Dict[Tuple[str, str], List[RowRecord]] = defaultdict(list)
    for r in records:
        basis, key = make_group_key(r)
        groups[(basis, key)].append(r)

    import_rows: List[Dict[str, str]] = []
    duplicates_rows: List[Dict[str, str]] = []
    raw_rows: List[Dict[str, str]] = []

    for r in records:
        raw_rows.append(
            {
                "source_row": str(r.source_row),
                "customer_name": r.customer_name,
                "last_name": r.last_name,
                "first_name": r.first_name,
                "middle_name": r.middle_name,
                "gender": r.gender,
                "birthday_raw": r.birthday_raw,
                "birthday_iso": r.birthday_iso,
                "age_raw": r.age_raw,
                "contact_number": r.contact_number,
                "contact_norm": r.contact_norm,
                "email": r.email,
                "address_street": r.address_street,
                "address_barangay": r.address_barangay,
                "address_city": r.address_city,
                "address_full": r.address_full,
                "senior_or_pwd_flag": r.senior_or_pwd_flag,
                "senior_or_pwd_id_number": r.senior_or_pwd_id_number,
                "doctor": r.doctor,
            }
        )

    sorted_groups = sorted(groups.items(), key=lambda x: min(r.source_row for r in x[1]))
    for idx, ((basis, key), members) in enumerate(sorted_groups, start=args.id_start):
        canonical = pick_canonical(members)
        source_rows = ",".join(str(m.source_row) for m in sorted(members, key=lambda m: m.source_row))
        patient_id = f"{args.id_prefix}-{idx:0{args.id_width}d}"

        needs_review = "NO"
        review_reason = ""
        if basis != "STRICT_NAME_BDAY" and len(members) > 1:
            needs_review = "YES"
            review_reason = f"Merged by {basis}; verify duplicates."
        elif basis == "ROW_ONLY":
            needs_review = "YES"
            review_reason = "No birthday/contact key; verify uniqueness."

        import_rows.append(
            {
                "patient_id": patient_id,
                "dedupe_basis": basis,
                "needs_review": needs_review,
                "review_reason": review_reason,
                "source_rows": source_rows,
                "customer_name": canonical.customer_name,
                "last_name": canonical.last_name,
                "first_name": canonical.first_name,
                "middle_name": canonical.middle_name,
                "gender": canonical.gender,
                "birthday_iso": canonical.birthday_iso,
                "contact_number": canonical.contact_number,
                "email": canonical.email,
                "address_full": canonical.address_full,
                "senior_or_pwd_flag": canonical.senior_or_pwd_flag,
                "senior_or_pwd_id_number": canonical.senior_or_pwd_id_number,
                "doctor": canonical.doctor,
            }
        )

        if len(members) > 1:
            for m in sorted(members, key=lambda x: x.source_row):
                duplicates_rows.append(
                    {
                        "group_type": "MERGED_GROUP",
                        "group_basis": basis,
                        "group_key": key,
                        "proposed_patient_id": patient_id,
                        "source_row": str(m.source_row),
                        "customer_name": m.customer_name,
                        "birthday_iso": m.birthday_iso,
                        "contact_number": m.contact_number,
                        "email": m.email,
                        "address_full": m.address_full,
                    }
                )

    # Name-only duplicate review (does not auto-merge, just flags)
    name_groups: Dict[str, List[RowRecord]] = defaultdict(list)
    for r in records:
        k = likely_name_key(r)
        if k:
            name_groups[k].append(r)
    for key, members in sorted(name_groups.items(), key=lambda x: min(r.source_row for r in x[1])):
        if len(members) <= 1:
            continue
        unique_birthdays = {m.birthday_iso for m in members if m.birthday_iso}
        if len(unique_birthdays) > 1:
            label = "SAME_NAME_DIFFERENT_BDAY"
        elif not unique_birthdays:
            label = "SAME_NAME_NO_BDAY"
        else:
            # already likely captured by strict merge; still useful if multiple source rows remain
            label = "SAME_NAME"
        for m in sorted(members, key=lambda x: x.source_row):
            duplicates_rows.append(
                {
                    "group_type": label,
                    "group_basis": "NAME_ONLY",
                    "group_key": key,
                    "proposed_patient_id": "",
                    "source_row": str(m.source_row),
                    "customer_name": m.customer_name,
                    "birthday_iso": m.birthday_iso,
                    "contact_number": m.contact_number,
                    "email": m.email,
                    "address_full": m.address_full,
                }
            )

    ensure_dir(args.output_dir)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_prefix = f"customer_list_{stamp}"

    raw_path = os.path.join(args.output_dir, f"{out_prefix}_raw_extract.csv")
    import_path = os.path.join(args.output_dir, f"{out_prefix}_patient_master_import_ready.csv")
    dupes_path = os.path.join(args.output_dir, f"{out_prefix}_duplicates_review.csv")
    summary_path = os.path.join(args.output_dir, f"{out_prefix}_summary.txt")

    write_csv(
        raw_path,
        raw_rows,
        [
            "source_row",
            "customer_name",
            "last_name",
            "first_name",
            "middle_name",
            "gender",
            "birthday_raw",
            "birthday_iso",
            "age_raw",
            "contact_number",
            "contact_norm",
            "email",
            "address_street",
            "address_barangay",
            "address_city",
            "address_full",
            "senior_or_pwd_flag",
            "senior_or_pwd_id_number",
            "doctor",
        ],
    )
    write_csv(
        import_path,
        import_rows,
        [
            "patient_id",
            "dedupe_basis",
            "needs_review",
            "review_reason",
            "source_rows",
            "customer_name",
            "last_name",
            "first_name",
            "middle_name",
            "gender",
            "birthday_iso",
            "contact_number",
            "email",
            "address_full",
            "senior_or_pwd_flag",
            "senior_or_pwd_id_number",
            "doctor",
        ],
    )
    write_csv(
        dupes_path,
        duplicates_rows,
        [
            "group_type",
            "group_basis",
            "group_key",
            "proposed_patient_id",
            "source_row",
            "customer_name",
            "birthday_iso",
            "contact_number",
            "email",
            "address_full",
        ],
    )

    total_rows = len(records)
    unique_patients = len(import_rows)
    merged_rows = sum(1 for r in import_rows if "," in r["source_rows"])
    needs_review_count = sum(1 for r in import_rows if r["needs_review"] == "YES")
    missing_bday = sum(1 for r in records if not r.birthday_iso)
    unknown_gender = sum(1 for r in records if r.gender == "UNKNOWN")

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("CUSTOMER LIST migration summary\n")
        f.write(f"input_file={args.input}\n")
        f.write(f"sheet={args.sheet}\n")
        f.write(f"total_source_rows={total_rows}\n")
        f.write(f"unique_patients_proposed={unique_patients}\n")
        f.write(f"merged_groups_count={merged_rows}\n")
        f.write(f"needs_review_patients={needs_review_count}\n")
        f.write(f"rows_missing_birthday={missing_bday}\n")
        f.write(f"rows_unknown_gender={unknown_gender}\n")
        f.write(f"raw_extract_csv={raw_path}\n")
        f.write(f"import_ready_csv={import_path}\n")
        f.write(f"duplicates_review_csv={dupes_path}\n")

    print("Generated files:")
    print(raw_path)
    print(import_path)
    print(dupes_path)
    print(summary_path)


if __name__ == "__main__":
    main()
