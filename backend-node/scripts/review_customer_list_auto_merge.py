#!/usr/bin/env python3
"""
Auto-review duplicate patients from raw CUSTOMER LIST extract.

Rules implemented:
1) Same normalized name + same birthday -> merge and fill missing details.
2) Name duplicate only -> merge if other details strongly support same person.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple


@dataclass
class Record:
    source_row: int
    customer_name: str
    last_name: str
    first_name: str
    middle_name: str
    gender: str
    birthday_iso: str
    contact_number: str
    contact_norm: str
    email: str
    address_street: str
    address_barangay: str
    address_city: str
    address_full: str
    senior_or_pwd_flag: str
    senior_or_pwd_id_number: str
    doctor: str


class DSU:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> bool:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return False
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1
        return True


def clean_text(value: str) -> str:
    out = (value or "").replace("\n", " ").replace("\r", " ").strip()
    out = re.sub(r"\s+", " ", out)
    return out


def norm_name(value: str) -> str:
    out = clean_text(value).upper()
    out = out.replace(".", " ")
    out = re.sub(r"[^A-Z0-9 ]+", "", out)
    out = re.sub(r"\s+", " ", out)
    return out.strip()


def norm_email(value: str) -> str:
    return clean_text(value).lower()


def norm_contact(value: str) -> str:
    return re.sub(r"[^0-9+]", "", clean_text(value))


def norm_address(value: str) -> str:
    out = clean_text(value).upper()
    out = re.sub(r"[^A-Z0-9 ]+", " ", out)
    out = re.sub(r"\s+", " ", out)
    return out.strip()


def norm_gender(value: str) -> str:
    v = norm_name(value)
    if v in {"MALE", "M"}:
        return "MALE"
    if v in {"FEMALE", "F"}:
        return "FEMALE"
    return "UNKNOWN"


def choose_mode_non_empty(values: List[str]) -> str:
    items = [clean_text(v) for v in values if clean_text(v)]
    if not items:
        return ""
    count = Counter(items)
    best = sorted(count.items(), key=lambda kv: (-kv[1], -len(kv[0]), kv[0]))[0][0]
    return best


def choose_longest_non_empty(values: List[str]) -> str:
    items = [clean_text(v) for v in values if clean_text(v)]
    if not items:
        return ""
    return sorted(items, key=lambda x: (-len(x), x))[0]


def gender_compatible(a: str, b: str) -> bool:
    if a == "UNKNOWN" or b == "UNKNOWN":
        return True
    return a == b


def load_records(raw_csv: str) -> List[Record]:
    rows = list(csv.DictReader(open(raw_csv, encoding="utf-8")))
    records: List[Record] = []
    for r in rows:
        source_row_raw = clean_text(r.get("source_row", "0"))
        source_row = int(source_row_raw) if source_row_raw.isdigit() else 0
        records.append(
            Record(
                source_row=source_row,
                customer_name=clean_text(r.get("customer_name", "")),
                last_name=norm_name(r.get("last_name", "")),
                first_name=norm_name(r.get("first_name", "")),
                middle_name=norm_name(r.get("middle_name", "")),
                gender=norm_gender(r.get("gender", "")),
                birthday_iso=clean_text(r.get("birthday_iso", "")),
                contact_number=clean_text(r.get("contact_number", "")),
                contact_norm=norm_contact(r.get("contact_norm", "")),
                email=norm_email(r.get("email", "")),
                address_street=clean_text(r.get("address_street", "")),
                address_barangay=clean_text(r.get("address_barangay", "")),
                address_city=clean_text(r.get("address_city", "")),
                address_full=clean_text(r.get("address_full", "")),
                senior_or_pwd_flag=clean_text(r.get("senior_or_pwd_flag", "")),
                senior_or_pwd_id_number=clean_text(r.get("senior_or_pwd_id_number", "")),
                doctor=clean_text(r.get("doctor", "")),
            )
        )
    return records


def base_name_key(r: Record) -> str:
    return f"{r.last_name}|{r.first_name}"


def should_merge(a: Record, b: Record) -> Tuple[bool, str]:
    if not a.last_name or not a.first_name or not b.last_name or not b.first_name:
        return False, ""
    if base_name_key(a) != base_name_key(b):
        return False, ""

    if a.birthday_iso and b.birthday_iso and a.birthday_iso == b.birthday_iso:
        return True, "NAME_BDAY"

    if len(a.contact_norm) >= 7 and a.contact_norm == b.contact_norm and len(b.contact_norm) >= 7:
        return True, "NAME_CONTACT"

    if a.email and b.email and a.email == b.email:
        return True, "NAME_EMAIL"

    if a.senior_or_pwd_id_number and b.senior_or_pwd_id_number and a.senior_or_pwd_id_number == b.senior_or_pwd_id_number:
        return True, "NAME_SENIOR_PWD_ID"

    if a.birthday_iso and b.birthday_iso and a.birthday_iso != b.birthday_iso:
        return False, ""

    if not gender_compatible(a.gender, b.gender):
        return False, ""

    city_a = norm_address(a.address_city)
    city_b = norm_address(b.address_city)
    addr_a = norm_address(a.address_full)
    addr_b = norm_address(b.address_full)
    doctor_a = norm_name(a.doctor)
    doctor_b = norm_name(b.doctor)

    if city_a and city_b and city_a == city_b and addr_a and addr_b and addr_a == addr_b:
        return True, "NAME_CITY_ADDRESS"

    if addr_a and addr_b and addr_a == addr_b:
        return True, "NAME_ADDRESS"

    if city_a and city_b and city_a == city_b and doctor_a and doctor_b and doctor_a == doctor_b:
        return True, "NAME_CITY_DOCTOR"

    return False, ""


def aggregate_cluster(rows: List[Record]) -> Dict[str, str]:
    birthdays = [r.birthday_iso for r in rows if r.birthday_iso]
    unique_birthdays = sorted(set(birthdays))
    birthday = choose_mode_non_empty(birthdays)
    birthday_conflict = "YES" if len(unique_birthdays) > 1 else "NO"

    contacts = [r.contact_number for r in rows if r.contact_number]
    emails = [r.email for r in rows if r.email]
    genders = [r.gender for r in rows if r.gender and r.gender != "UNKNOWN"]
    contact_norms = [r.contact_norm for r in rows if r.contact_norm]

    unique_contacts = sorted(set([c for c in contact_norms if c]))
    unique_emails = sorted(set([e for e in emails if e]))
    unique_genders = sorted(set([g for g in genders if g]))

    contact_conflict = "YES" if len(unique_contacts) > 1 else "NO"
    email_conflict = "YES" if len(unique_emails) > 1 else "NO"
    gender_conflict = "YES" if len(unique_genders) > 1 else "NO"

    gender = choose_mode_non_empty(genders) if genders else "UNKNOWN"
    customer_name = choose_longest_non_empty([r.customer_name for r in rows])
    last_name = choose_mode_non_empty([r.last_name for r in rows])
    first_name = choose_mode_non_empty([r.first_name for r in rows])
    middle_name = choose_mode_non_empty([r.middle_name for r in rows])
    contact = choose_mode_non_empty(contacts)
    email = choose_mode_non_empty(emails)
    address_full = choose_longest_non_empty([r.address_full for r in rows])
    senior_flag = choose_mode_non_empty([r.senior_or_pwd_flag for r in rows])
    senior_id = choose_mode_non_empty([r.senior_or_pwd_id_number for r in rows])
    doctor = choose_mode_non_empty([r.doctor for r in rows])

    return {
        "customer_name": customer_name,
        "last_name": last_name,
        "first_name": first_name,
        "middle_name": middle_name,
        "gender": gender or "UNKNOWN",
        "birthday_iso": birthday,
        "contact_number": contact,
        "email": email,
        "address_full": address_full,
        "senior_or_pwd_flag": senior_flag,
        "senior_or_pwd_id_number": senior_id,
        "doctor": doctor,
        "birthday_conflict": birthday_conflict,
        "contact_conflict": contact_conflict,
        "email_conflict": email_conflict,
        "gender_conflict": gender_conflict,
    }


def merge_records(records: List[Record], id_prefix: str, id_start: int, id_width: int) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    groups: Dict[str, List[Record]] = defaultdict(list)
    for r in records:
        key = base_name_key(r)
        if key == "|":
            key = f"ROW|{r.source_row}"
        groups[key].append(r)

    merged_rows: List[Dict[str, str]] = []
    review_rows: List[Dict[str, str]] = []

    current_id = id_start
    for key in sorted(groups.keys(), key=lambda k: min(r.source_row for r in groups[k])):
        bucket = sorted(groups[key], key=lambda r: r.source_row)
        n = len(bucket)
        dsu = DSU(n)
        pair_reasons: List[Tuple[int, int, str]] = []

        for i in range(n):
            for j in range(i + 1, n):
                ok, reason = should_merge(bucket[i], bucket[j])
                if ok:
                    dsu.union(i, j)
                    pair_reasons.append((i, j, reason))

        comp: Dict[int, List[int]] = defaultdict(list)
        for i in range(n):
            comp[dsu.find(i)].append(i)

        for root in sorted(comp.keys(), key=lambda idx: min(bucket[k].source_row for k in comp[idx])):
            idxs = sorted(comp[root], key=lambda k: bucket[k].source_row)
            members = [bucket[k] for k in idxs]
            agg = aggregate_cluster(members)

            reasons: Set[str] = set()
            for i, j, reason in pair_reasons:
                if i in idxs and j in idxs:
                    reasons.add(reason)
            if not reasons:
                reasons.add("SINGLE_ROW")

            birthday_values = sorted(set([m.birthday_iso for m in members if m.birthday_iso]))
            confidence = "HIGH"
            needs_review = "NO"
            review_reason = ""

            if agg["birthday_conflict"] == "YES":
                needs_review = "YES"
                confidence = "LOW"
                review_reason = "Conflicting birthdays across merged rows."
            elif agg["gender_conflict"] == "YES" and len(members) > 1:
                needs_review = "YES"
                confidence = "LOW"
                review_reason = "Conflicting gender across merged rows."
            elif len(members) == 1:
                confidence = "HIGH" if agg["birthday_iso"] else "MEDIUM"
            elif "NAME_BDAY" in reasons:
                confidence = "HIGH"
            elif "NAME_CONTACT" in reasons or "NAME_EMAIL" in reasons or "NAME_SENIOR_PWD_ID" in reasons:
                confidence = "MEDIUM"
            else:
                confidence = "LOW"
                needs_review = "YES"
                review_reason = "Merged by weaker name/address heuristics."

            patient_id = f"{id_prefix}-{current_id:0{id_width}d}"
            current_id += 1

            source_rows = ",".join(str(m.source_row) for m in members)
            merged_rows.append(
                {
                    "patient_id": patient_id,
                    "needs_review": needs_review,
                    "review_reason": review_reason,
                    "merge_confidence": confidence,
                    "merge_basis": "|".join(sorted(reasons)),
                    "source_rows": source_rows,
                    "source_count": str(len(members)),
                    "birthday_values_in_group": "|".join(birthday_values),
                    "customer_name": agg["customer_name"],
                    "last_name": agg["last_name"],
                    "first_name": agg["first_name"],
                    "middle_name": agg["middle_name"],
                    "gender": agg["gender"],
                    "birthday_iso": agg["birthday_iso"],
                    "contact_number": agg["contact_number"],
                    "email": agg["email"],
                    "address_full": agg["address_full"],
                    "senior_or_pwd_flag": agg["senior_or_pwd_flag"],
                    "senior_or_pwd_id_number": agg["senior_or_pwd_id_number"],
                    "doctor": agg["doctor"],
                }
            )

            if len(members) > 1 or needs_review == "YES":
                for m in members:
                    review_rows.append(
                        {
                            "patient_id": patient_id,
                            "needs_review": needs_review,
                            "merge_confidence": confidence,
                            "merge_basis": "|".join(sorted(reasons)),
                            "source_row": str(m.source_row),
                            "customer_name": m.customer_name,
                            "last_name": m.last_name,
                            "first_name": m.first_name,
                            "middle_name": m.middle_name,
                            "gender": m.gender,
                            "birthday_iso": m.birthday_iso,
                            "contact_number": m.contact_number,
                            "email": m.email,
                            "address_full": m.address_full,
                            "doctor": m.doctor,
                        }
                    )

    return merged_rows, review_rows


def write_csv(path: str, rows: List[Dict[str, str]], fields: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Auto-review merge for CUSTOMER LIST data.")
    parser.add_argument(
        "--raw-csv",
        default="/Users/coleen/Desktop/DRMED Website/data-migration/customer_list_20260328_162725_raw_extract.csv",
        help="Path to raw_extract CSV generated by prepare_customer_list_import.py",
    )
    parser.add_argument(
        "--output-dir",
        default="/Users/coleen/Desktop/DRMED Website/data-migration",
        help="Output directory",
    )
    parser.add_argument("--id-prefix", default="DRM")
    parser.add_argument("--id-start", type=int, default=1)
    parser.add_argument("--id-width", type=int, default=6)
    args = parser.parse_args()

    records = load_records(args.raw_csv)
    merged_rows, review_rows = merge_records(records, args.id_prefix, args.id_start, args.id_width)

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    merged_path = os.path.join(args.output_dir, f"customer_list_{stamp}_patient_master_reviewed.csv")
    safe_path = os.path.join(args.output_dir, f"customer_list_{stamp}_patient_master_reviewed_safe_first.csv")
    review_path = os.path.join(args.output_dir, f"customer_list_{stamp}_patient_master_review_queue.csv")
    audit_path = os.path.join(args.output_dir, f"customer_list_{stamp}_patient_master_review_audit.csv")
    summary_path = os.path.join(args.output_dir, f"customer_list_{stamp}_patient_master_review_summary.txt")

    main_fields = [
        "patient_id",
        "needs_review",
        "review_reason",
        "merge_confidence",
        "merge_basis",
        "source_rows",
        "source_count",
        "birthday_values_in_group",
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
    ]
    write_csv(merged_path, merged_rows, main_fields)
    write_csv(
        safe_path,
        [r for r in merged_rows if r["needs_review"] == "NO"],
        main_fields,
    )
    write_csv(
        review_path,
        [r for r in merged_rows if r["needs_review"] == "YES"],
        main_fields,
    )
    write_csv(
        audit_path,
        review_rows,
        [
            "patient_id",
            "needs_review",
            "merge_confidence",
            "merge_basis",
            "source_row",
            "customer_name",
            "last_name",
            "first_name",
            "middle_name",
            "gender",
            "birthday_iso",
            "contact_number",
            "email",
            "address_full",
            "doctor",
        ],
    )

    total_rows = len(records)
    unique_patients = len(merged_rows)
    safe_count = sum(1 for r in merged_rows if r["needs_review"] == "NO")
    review_count = sum(1 for r in merged_rows if r["needs_review"] == "YES")
    merged_count = sum(1 for r in merged_rows if int(r["source_count"]) > 1)
    high_conf = sum(1 for r in merged_rows if r["merge_confidence"] == "HIGH")
    med_conf = sum(1 for r in merged_rows if r["merge_confidence"] == "MEDIUM")
    low_conf = sum(1 for r in merged_rows if r["merge_confidence"] == "LOW")

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("CUSTOMER LIST auto-review merge summary\n")
        f.write(f"raw_csv={args.raw_csv}\n")
        f.write(f"total_source_rows={total_rows}\n")
        f.write(f"unique_patients_after_merge={unique_patients}\n")
        f.write(f"clusters_with_multiple_rows={merged_count}\n")
        f.write(f"safe_first_count={safe_count}\n")
        f.write(f"needs_review_count={review_count}\n")
        f.write(f"confidence_high={high_conf}\n")
        f.write(f"confidence_medium={med_conf}\n")
        f.write(f"confidence_low={low_conf}\n")
        f.write(f"reviewed_csv={merged_path}\n")
        f.write(f"safe_first_csv={safe_path}\n")
        f.write(f"review_queue_csv={review_path}\n")
        f.write(f"audit_csv={audit_path}\n")

    print("Generated files:")
    print(merged_path)
    print(safe_path)
    print(review_path)
    print(audit_path)
    print(summary_path)


if __name__ == "__main__":
    main()
