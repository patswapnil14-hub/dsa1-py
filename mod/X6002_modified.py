# Converted from x6002__bcch__to_api_uniflow.pm

import re
import dsa
import smtplib
from email.message import EmailMessage
import csv
import os
import pathlib


def cnf():
    return [
        {
            "src_job": "2002",
            "src_fld_name": "field_id",
            "tgt_fld_name": "field_id",
            "regex": "",
        },
        {
            "src_job": "2002",
            "src_fld_name": "field_usageend",
            "tgt_fld_name": "field_usageend",
            "regex": "datetime",
        },
        {
            "src_job": "2002",
            "src_fld_name": "field_jobname",
            "tgt_fld_name": "field_jobname",
            "regex": "",
        },
        {
            "src_job": "2002",
            "src_fld_name": "field_cardinality",
            "tgt_fld_name": "field_cardinality",
            "regex": "number",
        },
        {
            "src_job": "2002",
            "src_fld_name": "field_nonchargeable",
            "tgt_fld_name": "field_nonchargeable",
            "regex": "",
        },
        {
            "src_job": "2002",
            "src_fld_name": "field_was",
            "tgt_fld_name": "field_type",
            "regex": "lc",
        },
        {
            "src_job": "2002",
            "src_fld_name": "field_was",
            "tgt_fld_name": "field_size",
            "regex": "uc",
        },
        {
            "src_job": "2002",
            "src_fld_name": "field_was",
            "tgt_fld_name": "field_color",
            "regex": "",
        },
        {
            "src_job": "2002",
            "src_fld_name": "field_device",
            "tgt_fld_name": "field_device",
            "regex": "uc",
        },
        {
            "src_job": "2002",
            "src_fld_name": "field_auftragsnr",
            "tgt_fld_name": "field_projectno",
            "regex": "",
        },
        {
            "src_job": "2002",
            "src_fld_name": "field_kz",
            "tgt_fld_name": "field_resourceno",
            "regex": "uc",
        },
    ]


HEADERS = [
    "STATUS",
    "PKY",
    "PROJECTNO",
    "JOBNAME",
    "RESOURCENO",
    "DEVICE",
    "TYPE",
    "SIZE",
    "COLOR",
    "CARDINALITY",
    "NONCHARGEABLE",
    "ERRPREASON",
    "STARTDATE",
    "ENDDATE",
]


def create_report(success_records, error_records):
    file_name = "batch_report.csv"

    with open(file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)

        for record in success_records:
            writer.writerow(
                [
                    "SUCCESS",
                    record.get("_pky"),
                    record.get("field_projectno"),
                    record.get("field_jobname"),
                    record.get("field_resourceno"),
                    record.get("field_device"),
                    record.get("field_type"),
                    record.get("field_size"),
                    record.get("field_color"),
                    record.get("field_cardinality"),
                    record.get("field_nonchargeable"),
                    record.get("field_errpreason"),
                    record.get("field_usagestart"),
                    record.get("field_usageend"),
                ]
            )

        for record in error_records:
            writer.writerow(
                ["ERROR", record.get("_pky"), record.get("field_errpreason")]
            )

    return file_name


def send_mail(file_path, log, smtp_server=""):
    file_path = pathlib.Path(file_path)
    msg = EmailMessage()
    msg["Subject"] = "uniflow 6002 Batch Report"
    msg["From"] = "BATCH REPORT"
    msg["To"] = "BATCH REPORT"

    total = log.get("cnt_record", 0)
    success = log.get("cnt_success", 0)
    error = log.get("cnt_error", 0)

    msg.set_content(
        f"Total Records: {total}\nSuccess Records: {success}\nError Records: {error}"
    )

    # attach file
    with open(file_path, "rb") as f:
        file_data = f.read()
        msg.add_attachment(
            file_data,
            filename=file_path.name,
            maintype="application",
            subtype="csv",
            subtype="utf-8",
        )

    # send_file
    with smtplib.SMTP(smtp_server) as s:
        s.send_message(msg)


def run(conf):

    # 2 lines below are for creating the report - swapnil
    success_records = []
    error_records = []

    map_cfg = cnf()
    src = f"{conf['sys']}__stakeholder.uniflow__from_mssql_usage"
    log = {}
    dat = {}
    arr = []
    lov = {"druck": "print", "kopie": "copy"}
    res = {
        "copy_false_a4": 9100,
        "copy_false_a4_naba": 9101,
        "copy_false_a4_none": 9113,
        "copy_true_a4": 9102,
        "copy_true_a4_naba": 9103,
        "copy_true_a4_none": 9114,
        "copy_false_a3": 9104,
        "copy_false_a3_naba": 9105,
        "copy_false_a3_none": 9120,
        "copy_true_a3": 9106,
        "copy_true_a3_naba": 9107,
        "copy_true_a3_none": 9121,
        "print_false_a4": 9100,
        "print_false_a4_naba": 9101,
        "print_false_a4_none": 9113,
        "print_true_a4": 9102,
        "print_true_a4_naba": 9103,
        "print_true_a4_none": 9114,
        "print_false_a3": 9104,
        "print_false_a3_naba": 9105,
        "print_false_a3_none": 9120,
        "print_true_a3": 9106,
        "print_true_a3_naba": 9107,
        "print_true_a3_none": 9121,
        "scan_false_a4": 9116,
        "scan_false_a4_naba": 9117,
        "scan_false_a4_none": 9116,
        "scan_true_a4": 9118,
        "scan_true_a4_naba": 9119,
        "scan_true_a4_none": 9118,
        "scan_false_a3": 9116,
        "scan_false_a3_naba": 9117,
        "scan_false_a3_none": 9116,
        "scan_true_a3": 9118,
        "scan_true_a3_naba": 9119,
        "scan_true_a3_none": 9118,
    }

    # attributes
    dsa.field_loop_from_table(conf)

    # relation
    dsa.field_relation(conf, map_cfg)

    # project lookup
    for row in dsa.sql_array(
        conf["sql"], f"SELECT _pky, project_number FROM {conf['sys']}__data_nds.project"
    ):
        lov.setdefault("project", {})[row["project_number"].upper()] = row["_pky"]

    # loop source rows
    for row in dsa.sql_array(conf["sql"], f"SELECT * FROM {src} WHERE _nxt = 1"):
        p09 = (
            (row.get("field_auftragsnr") or "")[:5]
            + "."
            + (row.get("field_auftragsnr") or "")[5:8]
        )
        p10 = (row.get("field_auftrag") or "")[:10]
        p00 = ""
        if dsa.is_value(lov.get("project", {}).get(p09)):
            p00 = p09
        if dsa.is_value(lov.get("project", {}).get(p10)):
            p00 = p10

        if dsa.no_value(p00):
            dsa.sql_any(
                conf["sql"],
                f"UPDATE {src} SET _nxt = 9, _msg = 'Miss Project' WHERE _pky = '{row.get('_pky')}'",
            )
            log["cnt_error"] = log.get("cnt_error", 0) + 1
            log["cnt_record"] = log.get("cnt_record", 0) + 1

            # added by swapnil

            error_records.append(
                {
                    "_pky": h.get("_pky"),
                    "field_errpreason": "Miss Project",
                }
            )

        else:
            set_ = {
                "_pky": row.get("field_id"),
                "_ety": row.get("_ety"),
                "field_id": row.get("field_id"),
                "field_usageend": row.get("field_usageend"),
                "field_jobname": row.get("field_jobname"),
                "field_cardinality": row.get("field_cardinality"),
                "field_nonchargeable": "false",
                "field_type": "",
                "field_color": "false",
                "field_size": "",
                "field_device": row.get("field_device"),
                "field_projectno": p00,
                "field_resourceno": "9100",
            }

            # special parsing of field_was
            w = row.get("field_was") or ""
            w = " ".join(w.split())
            parts = w.split(" ", 2)
            one = parts[0] if len(parts) > 0 else ""
            two = parts[1] if len(parts) > 1 else ""
            tri = parts[2] if len(parts) > 2 else ""
            set_["field_type"] = lov.get(one.lower(), one.lower())
            set_["field_size"] = two.lower()
            if tri.lower() == "farbe":
                set_["field_color"] = "true"
            if row.get("field_nonchargeable") == "1":
                set_["field_nonchargeable"] = "true"

            # resource key
            key = f"{set_['field_type']}_{set_['field_color']}_{set_['field_size']}"
            if set_["field_nonchargeable"] == "true":
                key += "_none"
            else:
                # approximate Perl regex check: if first char of projectno formatted is letter
                first = (set_["field_projectno"] or "")[:1]
                fn = dsa.format_number(first) if first else ""
                if isinstance(fn, str) and fn.upper().isalpha():
                    key += "_naba"
            if dsa.is_value(res.get(key)):
                set_["field_resourceno"] = res[key]

            # format by cnf mapping
            set_ = dsa.format_by_cnf(set_, map_cfg)

            success_records.append(set_)

            dat[set_["_pky"]] = set_

    # collect and push to dsa
    for v in dat.values():
        arr.append(v)
    log = dsa.to_dsa(conf, arr, "_pky", log, "no_delete")

    # update source status
    for pky in dat.keys():
        dsa.sql_any(conf["sql"], f"UPDATE {src} SET _nxt = 2 WHERE _pky = '{pky}'")

    # functions get called here added by swapnil

    report_file = create_report(success_records, error_records)
    send_mail(report_file, log)

    return conf, log
