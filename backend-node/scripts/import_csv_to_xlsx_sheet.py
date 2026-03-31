#!/usr/bin/env python3
"""
Import a CSV file into a new worksheet tab inside an existing XLSX workbook.
No third-party dependencies.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from typing import List


NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
NS_DOCREL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS_CT = "http://schemas.openxmlformats.org/package/2006/content-types"

ET.register_namespace("", NS_MAIN)
ET.register_namespace("r", NS_DOCREL)


def col_name(idx: int) -> str:
    result = ""
    n = idx + 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def sanitize_sheet_name(name: str) -> str:
    name = re.sub(r"[\[\]\*\?/\\:]", " ", name).strip()
    name = re.sub(r"\s+", " ", name)
    if not name:
        name = "Imported Data"
    return name[:31]


def load_csv_rows(path: str) -> List[List[str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return [row for row in csv.reader(f)]


def ensure_unique_sheet_name(existing: List[str], requested: str) -> str:
    base = sanitize_sheet_name(requested)
    if base not in existing:
        return base
    i = 2
    while True:
        suffix = f" ({i})"
        candidate = (base[: 31 - len(suffix)] + suffix).strip()
        if candidate not in existing:
            return candidate
        i += 1


def build_worksheet_xml(rows: List[List[str]]) -> bytes:
    worksheet = ET.Element(f"{{{NS_MAIN}}}worksheet")

    max_cols = max((len(r) for r in rows), default=1)
    max_rows = max(len(rows), 1)
    dim_ref = f"A1:{col_name(max_cols - 1)}{max_rows}"
    ET.SubElement(worksheet, f"{{{NS_MAIN}}}dimension", {"ref": dim_ref})

    sheet_views = ET.SubElement(worksheet, f"{{{NS_MAIN}}}sheetViews")
    ET.SubElement(sheet_views, f"{{{NS_MAIN}}}sheetView", {"workbookViewId": "0"})

    ET.SubElement(worksheet, f"{{{NS_MAIN}}}sheetFormatPr", {"defaultRowHeight": "15"})
    sheet_data = ET.SubElement(worksheet, f"{{{NS_MAIN}}}sheetData")

    if not rows:
        rows = [[]]

    for r_idx, row_vals in enumerate(rows, start=1):
        row_el = ET.SubElement(sheet_data, f"{{{NS_MAIN}}}row", {"r": str(r_idx)})
        for c_idx, val in enumerate(row_vals):
            text = str(val or "")
            if text == "":
                continue
            ref = f"{col_name(c_idx)}{r_idx}"
            c_el = ET.SubElement(row_el, f"{{{NS_MAIN}}}c", {"r": ref, "t": "inlineStr"})
            is_el = ET.SubElement(c_el, f"{{{NS_MAIN}}}is")
            t_attrs = {}
            if text != text.strip() or "  " in text:
                t_attrs["{http://www.w3.org/XML/1998/namespace}space"] = "preserve"
            t_el = ET.SubElement(is_el, f"{{{NS_MAIN}}}t", t_attrs)
            t_el.text = text

    ET.SubElement(
        worksheet,
        f"{{{NS_MAIN}}}pageMargins",
        {
            "left": "0.7",
            "right": "0.7",
            "top": "0.75",
            "bottom": "0.75",
            "header": "0.3",
            "footer": "0.3",
        },
    )
    return ET.tostring(worksheet, encoding="utf-8", xml_declaration=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import CSV into a new sheet of an existing XLSX.")
    parser.add_argument("--xlsx", required=True, help="Input XLSX path")
    parser.add_argument("--csv", required=True, help="CSV path to import")
    parser.add_argument("--sheet-name", required=True, help="New worksheet tab name")
    parser.add_argument("--out", required=True, help="Output XLSX path")
    args = parser.parse_args()

    xlsx_path = args.xlsx
    csv_path = args.csv
    out_path = args.out
    requested_sheet_name = args.sheet_name

    rows = load_csv_rows(csv_path)

    with zipfile.ZipFile(xlsx_path, "r") as zin:
        workbook_xml = ET.fromstring(zin.read("xl/workbook.xml"))
        rels_xml = ET.fromstring(zin.read("xl/_rels/workbook.xml.rels"))
        ct_xml = ET.fromstring(zin.read("[Content_Types].xml"))
        names = zin.namelist()

        existing_sheet_names = []
        sheet_ids = []
        for s in workbook_xml.findall(f".//{{{NS_MAIN}}}sheets/{{{NS_MAIN}}}sheet"):
            existing_sheet_names.append(s.attrib.get("name", ""))
            sid = s.attrib.get("sheetId", "0")
            if str(sid).isdigit():
                sheet_ids.append(int(sid))

        existing_sheet_files = []
        for n in names:
            m = re.match(r"xl/worksheets/sheet(\d+)\.xml$", n)
            if m:
                existing_sheet_files.append(int(m.group(1)))

        existing_rids = []
        for rel in rels_xml.findall(f".//{{{NS_REL}}}Relationship"):
            rid = rel.attrib.get("Id", "")
            m = re.match(r"rId(\d+)$", rid)
            if m:
                existing_rids.append(int(m.group(1)))

        new_sheet_name = ensure_unique_sheet_name(existing_sheet_names, requested_sheet_name)
        new_sheet_id = (max(sheet_ids) + 1) if sheet_ids else 1
        new_sheet_num = (max(existing_sheet_files) + 1) if existing_sheet_files else 1
        new_rid = f"rId{(max(existing_rids) + 1) if existing_rids else 1}"
        new_sheet_rel_target = f"worksheets/sheet{new_sheet_num}.xml"
        new_sheet_part_name = f"/xl/{new_sheet_rel_target}"

        sheets_parent = workbook_xml.find(f".//{{{NS_MAIN}}}sheets")
        if sheets_parent is None:
            sheets_parent = ET.SubElement(workbook_xml, f"{{{NS_MAIN}}}sheets")

        ET.SubElement(
            sheets_parent,
            f"{{{NS_MAIN}}}sheet",
            {
                "name": new_sheet_name,
                "sheetId": str(new_sheet_id),
                f"{{{NS_DOCREL}}}id": new_rid,
            },
        )

        ET.SubElement(
            rels_xml,
            f"{{{NS_REL}}}Relationship",
            {
                "Id": new_rid,
                "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet",
                "Target": new_sheet_rel_target,
            },
        )

        existing_overrides = {
            o.attrib.get("PartName", "")
            for o in ct_xml.findall(f".//{{{NS_CT}}}Override")
        }
        if new_sheet_part_name not in existing_overrides:
            ET.SubElement(
                ct_xml,
                f"{{{NS_CT}}}Override",
                {
                    "PartName": new_sheet_part_name,
                    "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml",
                },
            )

        updated_workbook = ET.tostring(workbook_xml, encoding="utf-8", xml_declaration=True)
        updated_rels = ET.tostring(rels_xml, encoding="utf-8", xml_declaration=True)
        updated_ct = ET.tostring(ct_xml, encoding="utf-8", xml_declaration=True)
        new_sheet_xml = build_worksheet_xml(rows)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            temp_out = tmp.name

        try:
            with zipfile.ZipFile(temp_out, "w", compression=zipfile.ZIP_DEFLATED) as zout:
                for item in zin.infolist():
                    data = zin.read(item.filename)
                    if item.filename == "xl/workbook.xml":
                        data = updated_workbook
                    elif item.filename == "xl/_rels/workbook.xml.rels":
                        data = updated_rels
                    elif item.filename == "[Content_Types].xml":
                        data = updated_ct
                    zout.writestr(item, data)
                zout.writestr(f"xl/{new_sheet_rel_target}", new_sheet_xml)

            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            shutil.move(temp_out, out_path)
        finally:
            if os.path.exists(temp_out):
                os.unlink(temp_out)

    print(f"Imported CSV into new sheet '{new_sheet_name}'")
    print(f"Workbook: {out_path}")


if __name__ == "__main__":
    main()
