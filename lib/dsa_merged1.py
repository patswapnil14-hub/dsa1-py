import os
import re
import logging
import time
import calendar
import requests
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


# =============================================================================
# DATABASE CONNECTIVITY FUNCTIONS
# =============================================================================


def sql(db_name: str, prefix: str = "") -> Any:
    # Create SQLAlchemy engine for MySQL database.

    host = os.getenv(f"{prefix}MYSQL_DSA_HOST")
    port = os.getenv(f"{prefix}MYSQL_DSA_PORT", "3306")
    user = os.getenv(f"{prefix}MYSQL_DSA_USER")
    password = os.getenv(f"{prefix}MYSQL_DSA_PASS")

    db_url = (
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}?charset=utf8mb4"
    )

    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        future=True,
        echo=False,
    )
    return engine


def mss(cnf: Dict[str, Any], ali: str) -> Any:
    # Create MSSQL database connection via ODBC.

    # Connect to MySQL to fetch MSSQL credentials
    mysql_engine = cnf.get("sql")
    if isinstance(mysql_engine, str):
        mysql_engine = sql(cnf.get("db", "dsa"))

    # Read MSSQL credentials from _pwd table
    sql_query = f"SELECT * FROM {cnf.get('db', 'dsa')}._pwd WHERE _key = :ali"
    hsh = sql_hash(mysql_engine, sql_query, {"ali": ali})

    if not hsh:
        raise Exception(f"No entry found in _pwd for key: {ali}")

    user = hsh.get("pwd_user")
    password = hsh.get("pwd_pass")
    odbc_alias = hsh.get("pwd_odbc_alias")

    # Connect to MSSQL using ODBC
    mssql_url = f"mssql+pyodbc://{user}:{password}@{odbc_alias}"
    mssql_engine = create_engine(mssql_url)

    return mssql_engine


# =============================================================================
# SQL EXECUTION FUNCTIONS
# =============================================================================


def sql_array(
    sql_conn: Any, query: str, params: Optional[Dict] = None
) -> List[Dict[str, Any]]:
    # Execute query and return list of dict rows.

    if not sql_conn or not query:
        return []

    try:
        # SQLAlchemy engine
        if hasattr(sql_conn, "connect"):
            with sql_conn.connect() as conn:
                result = conn.execute(text(query), params or {})
                rows = result.mappings().all()
                return [dict(row) for row in rows]

        # Cursor-like object (DB-API)
        if hasattr(sql_conn, "execute") and hasattr(sql_conn, "fetchall"):
            cursor = sql_conn
            cursor.execute(query, params or {})
            rows = cursor.fetchall()
            return _rows_to_dicts(cursor, rows)

        # Connection-like object (DB-API)
        if hasattr(sql_conn, "cursor"):
            try:
                cur = sql_conn.cursor()
                cur.execute(query, params or {})
                rows = cur.fetchall()
                result = _rows_to_dicts(cur, rows)
                try:
                    cur.close()
                except:
                    pass
                return result
            except Exception:
                pass

        # Callable (test/mock)
        if callable(sql_conn):
            return sql_conn(query, params) or []

        # List/tuple (test data)
        if isinstance(sql_conn, (list, tuple)):
            return list(sql_conn)

    except Exception as e:
        logger.exception("sql_array failed: %s", e)

    return []


def _rows_to_dicts(cursor: Any, rows: List) -> List[Dict[str, Any]]:
    """Convert DB-API rows to list of dicts using column names."""
    cols = []
    if hasattr(cursor, "description") and cursor.description:
        cols = [d[0] if isinstance(d, tuple) else d for d in cursor.description]

    if not cols:
        return [dict(enumerate(r)) for r in rows]

    return [dict(zip(cols, r)) for r in rows]


def sql_any(sql_conn: Any, query: str, params: Optional[Dict] = None) -> bool:
    # Execute query (no return value), with commit if available.

    if not sql_conn or not query:
        return False

    try:
        # SQLAlchemy engine
        if hasattr(sql_conn, "connect"):
            with sql_conn.begin() as conn:
                conn.execute(text(query), params or {})
            return True

        # Cursor-like object
        if hasattr(sql_conn, "execute"):
            sql_conn.execute(query, params or {})
            # Try commit if available
            if hasattr(sql_conn, "connection") and hasattr(
                sql_conn.connection, "commit"
            ):
                sql_conn.connection.commit()
            return True

        # Connection-like object
        if hasattr(sql_conn, "cursor"):
            try:
                cur = sql_conn.cursor()
                cur.execute(query, params or {})
                if hasattr(sql_conn, "commit"):
                    sql_conn.commit()
                try:
                    cur.close()
                except:
                    pass
                return True
            except Exception:
                pass

        # Callable
        if callable(sql_conn):
            return bool(sql_conn(query, params))

    except Exception as e:
        logger.exception("sql_any failed: %s", e)

    return False


def sql_var(sql_conn: Any, query: str, params: Optional[Dict] = None) -> Any:
    # Execute query and return single scalar value from first row.

    if not sql_conn or not query:
        return ""

    try:
        rows = sql_array(sql_conn, query, params)
        if rows:
            for v in rows[0].values():
                if v is not None:
                    return v
    except Exception as e:
        logger.exception("sql_var failed: %s", e)

    return ""


def sql_hash(
    sql_conn: Any, query: str, params: Optional[Dict] = None
) -> Dict[str, Any]:
    "#Execute query and return first row as dict.

  
    if not sql_conn or not query:
        return {}

    try:
        rows = sql_array(sql_conn, query, params)
        return rows[0] if rows else {}
    except Exception as e:
        logger.exception("sql_hash failed: %s", e)

    return {}


def sql_create(sql_conn: Any, table: str, data: Dict[str, Any]) -> int:
    #INSERT IGNORE into table (new records only).

 
    if not sql_conn or not table or not data:
        return 0

    try:
        cols = list(data.keys())
        col_str = ", ".join([f"`{c}`" for c in cols])
        placeholders = ", ".join([f":{c}" for c in cols])

        query = f"INSERT IGNORE INTO {table} ({col_str}) VALUES ({placeholders})"
        values = {c: data.get(c) or "" for c in cols}

        if hasattr(sql_conn, "connect"):
            with sql_conn.begin() as conn:
                result = conn.execute(text(query), values)
            return 1

        if hasattr(sql_conn, "execute"):
            sql_conn.execute(query, values)
            if hasattr(sql_conn, "connection") and hasattr(
                sql_conn.connection, "commit"
            ):
                sql_conn.connection.commit()
            return 1

        if hasattr(sql_conn, "cursor"):
            try:
                cur = sql_conn.cursor()
                cur.execute(query, values)
                if hasattr(sql_conn, "commit"):
                    sql_conn.commit()
                try:
                    cur.close()
                except:
                    pass
                return 1
            except Exception:
                pass

    except Exception as e:
        logger.exception("sql_create failed: %s", e)

    return 0


def sql_replace(sql_conn: Any, table: str, data: Dict[str, Any]) -> bool:
    #REPLACE INTO table (insert or update).

 
    if not sql_conn or not table or not data:
        return False

    try:
        cols = list(data.keys())
        col_str = ", ".join([f"`{c}`" for c in cols])
        placeholders = ", ".join([f":{c}" for c in cols])

        query = f"REPLACE INTO {table} ({col_str}) VALUES ({placeholders})"
        values = {c: data.get(c) or "" for c in cols}

        if hasattr(sql_conn, "connect"):
            with sql_conn.begin() as conn:
                result = conn.execute(text(query), values)
            return True

        if hasattr(sql_conn, "execute"):
            sql_conn.execute(query, values)
            if hasattr(sql_conn, "connection") and hasattr(
                sql_conn.connection, "commit"
            ):
                sql_conn.connection.commit()
            return True

        if hasattr(sql_conn, "cursor"):
            try:
                cur = sql_conn.cursor()
                cur.execute(query, values)
                if hasattr(sql_conn, "commit"):
                    sql_conn.commit()
                try:
                    cur.close()
                except:
                    pass
                return True
            except Exception:
                pass

    except Exception as e:
        logger.exception("sql_replace failed: %s", e)

    return False


# =============================================================================
# VALUE CHECKING FUNCTIONS
# =============================================================================


def is_value(val: Any) -> bool:
    #Check if value is non-null/non-empty and valid.

 
    if val is None:
        return False

    val_str = str(val).strip()

    if val_str == "" or val_str == "0":
        return False

    # Invalid SQL date sentinels
    if val_str.startswith("1753-01-01 00:00:00"):
        return False
    if val_str.startswith("0001-01-01 00:00:00"):
        return False

    return True


def no_value(val: Any) -> bool:
    #Check if value is null/empty (inverse of is_value).

  
    return not is_value(val)


# =============================================================================
# FORMATTING FUNCTIONS
# =============================================================================


def format_number(s: Any) -> str:
    #Extract digits only from string.

    if s is None:
        return ""
    return re.sub(r"\D", "", str(s))


def format_uc(s: Any) -> str:
     #Convert to uppercase.

    
    return str(s or "").upper()


def format_lc(s: Any) -> str:
    #Convert to lowercase.

    
    return str(s or "").lower()


def format_by_cnf(
    data: Dict[str, Any], map_cfg: List[Dict[str, Any]]
) -> Dict[str, Any]:
    #Apply field mappings and format functions to data according to config.


    out = dict(data or {})

    for m in map_cfg:
        src = m.get("src_fld_name")
        tgt = m.get("tgt_fld_name")
        regex = m.get("regex", "").lower()

        if not src or not tgt or src not in out:
            continue

        val = out.get(src)

        if val is None:
            out[tgt] = val
            continue

        # Apply format functions from regex directive
        if regex:
            for fmt_name in regex.split():
                if fmt_name == "uc":
                    val = format_uc(val)
                elif fmt_name == "lc":
                    val = format_lc(val)
                elif fmt_name == "number":
                    try:
                        val = int(val)
                    except:
                        try:
                            val = float(val)
                        except:
                            pass

        out[tgt] = val

    return out


# =============================================================================
# HASH AND CLEANUP FUNCTIONS
# =============================================================================


def get_farmhash(s: str) -> str:
    #Generate hash from string using farmhash or SHA256 fallback.

   
    try:
        from farmhash import hash64

        return str(hash64(s))
    except ImportError:
        import hashlib

        return hashlib.sha256(str(s).encode()).hexdigest()[:16]


def get_farmhash_hsh(hsh: Dict[str, Any], exclude: str = "") -> str:
    #Generate hash from dict (excluding internal fields and specified keys).

  
    exclude_set = set(exclude.split()) if exclude else set()

    parts = []
    for k in sorted(hsh.keys()):
        if k.startswith("_"):
            continue
        if k in exclude_set:
            continue
        v = hsh[k]
        parts.append(f"{k}({v})")

    s = "".join(parts)
    return get_farmhash(s)


def clean(record: Dict[str, Any]) -> Dict[str, Any]:
    #Clean record by removing non-printable chars and collapsing whitespace.

 
    cleaned = {}
    for k, v in record.items():
        if v is None:
            cleaned[k] = v
        else:
            s = str(v)
            # Remove non-printable
            s = re.sub(r"[^\x20-\x7E\t\n\r]", " ", s)
            s = s.strip()
            # Collapse multiple spaces
            s = re.sub(r" +", " ", s)
            cleaned[k] = s

    return cleaned


# =============================================================================
# FIELD MANAGEMENT FUNCTIONS
# =============================================================================


def field_name(text: str) -> str:
    #Normalize field name: trim, replace special chars with _, add 
    if not text:
        return ""

    text = text.strip()
    text = re.sub(r"^\W+|\W+$", "", text)  # trim non-word start/end
    text = re.sub(r"\W+", "_", text)  # replace non-word with _
    text = re.sub(r"^_+|_+$", "", text)  # trim leading/trailing _
    text = re.sub(r"_+", "_", text)  # collapse multiple _

    if text:
        return f"field_{text.lower()}"
    return ""


def field_loop_from_table(conf: Dict[str, Any]) -> int:
    #Load field list from information_schema and register in _job_field.

    if not conf or "sql" not in conf or "_this" not in conf:
        return 0

    try:
        sql_conn = conf["sql"]
        sys = conf.get("sys", "")
        this = conf.get("_this", {})
        tab_pfix = this.get("tab_pfix", "")
        tab_name = this.get("tab_name", "")

        # Query information_schema for columns
        query = f"""
            SELECT COLUMN_NAME FROM `information_schema`.`COLUMNS`
            WHERE TABLE_SCHEMA='{sys}__{tab_pfix}'
            AND TABLE_NAME='{tab_name}'
            ORDER BY ORDINAL_POSITION
        """

        txt = ""
        for row in sql_array(sql_conn, query):
            col_name = row.get("COLUMN_NAME", "")
            if col_name and col_name[0] != "_":
                txt += f"\t{col_name}\n"

        if txt:
            field_loop(conf, txt)

        return 1
    except Exception as e:
        logger.exception("field_loop_from_table failed: %s", e)
        return 0


def field_loop(conf: Dict[str, Any], txt: str) -> str:
    #Process field list, normalize names, register in _job_field, build SELECT clause.


    if not conf or not txt:
        return ""

    try:
        sql_conn = conf["sql"]
        this = conf.get("_this", {})
        ety = this.get("_ety", "")
        ety_parent = this.get("ety_parent", "")
        if ety_parent:
            ety = ety_parent

        ali = this.get("ety_alias", "")
        ety_alias_parent = this.get("ety_alias_parent", "")
        if ety_alias_parent:
            ali = ety_alias_parent

        if not ety:
            logger.error("field_loop: missing ety")
            return ""

        db = conf.get("db", "")
        sql_parts = []

        for row in txt.strip().split("\n"):
            row = row.strip()
            if not row:
                continue

            fld = field_name(row)
            if not fld:
                continue

            # For ety starting with 2, keep field_ prefix
            if not ety.startswith("2"):
                fld = fld.replace("field_", "", 1)

            fld_hash = get_farmhash(f"{ali}.{fld}")

            try:
                sql_replace(
                    sql_conn,
                    f"{db}._job_field",
                    {
                        "_fld": fld_hash,
                        "_ety": ety,
                        "fld_alias": f"{ali}.{fld}",
                        "fld_name": fld,
                        "fld_name_origin": row,
                    },
                )
            except Exception as e:
                logger.warning("Could not register field %s: %s", fld, e)

            sql_parts.append(f" {row} AS {fld}")

        return ",".join(sql_parts).rstrip(",")

    except Exception as e:
        logger.exception("field_loop failed: %s", e)
        return ""


def field_relation(conf: Dict[str, Any], map_cfg: List[Dict[str, Any]]) -> int:
    #Create field relationships based on mapping config.

   
    if not conf or not map_cfg:
        return 0

    try:
        sql_conn = conf["sql"]
        db = conf.get("db", "")
        this = conf.get("_this", {})
        ety = this.get("_ety", "")
        ety_parent = this.get("ety_parent", "")
        if ety_parent:
            ety = ety_parent

        for mapping in map_cfg:
            src_job = mapping.get("src_job")
            src_fld_name = mapping.get("src_fld_name")
            tgt_fld_name = mapping.get("tgt_fld_name")

            # Skip if underscore prefix (internal fields)
            if not src_job or not src_fld_name or src_fld_name.startswith("_"):
                continue
            if not tgt_fld_name or not is_value(src_job):
                continue

            src_query = f"""
                SELECT _fld FROM {db}._job_field
                WHERE _ety = '{src_job}' AND fld_name = '{src_fld_name}'
            """
            src = sql_var(sql_conn, src_query)

            tgt_query = f"""
                SELECT _fld FROM {db}._job_field
                WHERE _ety = '{ety}' AND fld_name = '{tgt_fld_name}'
            """
            tgt = sql_var(sql_conn, tgt_query)

            if is_value(src) and is_value(tgt):
                try:
                    sql_create(sql_conn, f"{db}._con", {"tgt_ety": tgt, "src_ety": src})
                except Exception as e:
                    logger.warning("Could not create relation: %s", e)

        return 1

    except Exception as e:
        logger.exception("field_relation failed: %s", e)
        return 0


# =============================================================================
# CORE ETL FUNCTION
# =============================================================================


def to_dsa(
    conf: Dict[str, Any],
    arr: List[Dict[str, Any]],
    pky: str,
    log: Optional[Dict[str, Any]] = None,
    delete_mode: str = "",
) -> Dict[str, Any]:
    """Process input records: create new, update changed, delete missing records.

    Parameters
    ----------
    conf : Dict[str, Any]
        Configuration dict
    arr : List[Dict[str, Any]]
        Input records
    pky : str
        Primary key field name
    log : Dict, optional
        Log dict to update with counts (default: {})
    delete_mode : str, optional
        "delete" mode behavior (default: "" = allow deletes)

    Returns
    -------
    Dict[str, Any]
        Updated log dict with cnt_* counters
    """
    if not conf or not pky:
        return log or {}

    log = dict(log or {})
    sql_conn = conf.get("sql")
    this = conf.get("_this", {})
    ety = this.get("_ety", "")
    ety_parent = this.get("ety_parent", "")
    if ety_parent:
        ety = ety_parent

    db = conf.get("db", "")
    table_data = this.get("table_data", "")
    table_change = this.get("table_change", "")
    ety_alias = this.get("ety_alias", "")

    if not sql_conn or not ety or not table_data:
        return log

    try:
        # Load existing records and their hashes
        has = {}
        if delete_mode == "":
            grp_clause = ""
            if this.get("_grp"):
                grp_clause = f" WHERE _grp = '{this['_grp']}'"

            for h in sql_array(
                sql_conn, f"SELECT _pky, _fmh_object FROM {table_data}{grp_clause}"
            ):
                has[h.get("_pky")] = h.get("_fmh_object")

        log["cnt_loaded"] = len(has)

        # Load allowed field list
        att = {}
        for h in sql_array(
            sql_conn, f"SELECT fld_name FROM {db}._job_field WHERE _ety = '{ety}'"
        ):
            att[h.get("fld_name", "")] = "X"

        # Process input records
        for h in arr:
            if no_value(h.get(pky)):
                continue

            log["cnt_record"] = log.get("cnt_record", 0) + 1

            # Cleanup record
            h = clean(h)

            # Filter to allowed fields (keep _ prefixed)
            h_filtered = {}
            for k, v in h.items():
                if att.get(k) == "X" or k.startswith("_"):
                    h_filtered[k] = v

            h_filtered["_pky"] = h.get(pky)
            h_filtered["_ety"] = get_farmhash(f"{ety_alias}({h.get(pky)})")
            h_filtered["_fmh_object"] = get_farmhash_hsh(h_filtered, "")

            # Check if new or update
            old_fmh = has.get(h_filtered["_pky"])

            if no_value(old_fmh):
                # New record
                log["cnt_create"] = log.get("cnt_create", 0) + 1
                sql_create(sql_conn, table_data, h_filtered)

            elif old_fmh != h_filtered.get("_fmh_object"):
                # Update: save old to change table, replace current
                log["cnt_update"] = log.get("cnt_update", 0) + 1

                old_rec = sql_hash(
                    sql_conn,
                    f"SELECT * FROM {table_data} WHERE _pky = '{h_filtered['_pky']}'",
                )
                old_rec["i_reason"] = "change"
                sql_create(sql_conn, table_change, old_rec)
                sql_replace(sql_conn, table_data, h_filtered)

            else:
                # No change
                log["cnt_no_change"] = log.get("cnt_no_change", 0) + 1

            # Remove from has cache
            if h_filtered["_pky"] in has:
                del has[h_filtered["_pky"]]

        # Deletes: process remaining in has cache
        if delete_mode == "":
            for pky_val in has.keys():
                log["cnt_delete"] = log.get("cnt_delete", 0) + 1

                old_rec = sql_hash(
                    sql_conn, f"SELECT * FROM {table_data} WHERE _pky = '{pky_val}'"
                )
                old_rec["i_reason"] = "delete"
                sql_create(sql_conn, table_change, old_rec)
                sql_any(sql_conn, f"DELETE FROM {table_data} WHERE _pky = '{pky_val}'")

    except Exception as e:
        logger.exception("to_dsa failed: %s", e)

    return log


# =============================================================================
# DATETIME AND TIME MANAGEMENT FUNCTIONS
# =============================================================================


def dtm(cnf: Dict[str, Any], use: Optional[float] = None) -> Dict[str, Any]:
    #Populate config dict with comprehensive datetime components.

  
    if use is None:
        use = time.time()

    dt = datetime.fromtimestamp(use)

    sec = dt.second
    minute = dt.minute
    hour = dt.hour
    mday = dt.day
    mon = dt.month
    year = dt.year
    yday = dt.timetuple().tm_yday
    wday = dt.weekday()

    # Convert to Perl-compatible weekday (0=Sunday)
    wday = (wday + 1) % 7

    yweek = (yday - 1) // 7

    cnf["time_day"] = mday
    cnf["time_month"] = mon
    cnf["time_year"] = year
    cnf["time_hour"] = hour
    cnf["time_minute"] = minute
    cnf["time_second"] = sec
    cnf["time_day_of_year"] = yday
    cnf["time_day_of_week"] = wday
    cnf["time_week_of_year"] = yweek

    cnf["time_show"] = dt.strftime("%d.%m.%Y %H:%M:%S")
    cnf["time_show_date"] = dt.strftime("%d.%m.%Y")
    cnf["time_int_date"] = dt.strftime("%Y%m%d")
    cnf["time_sql_date"] = dt.strftime("%Y-%m-%d")
    cnf["time_sql_path"] = dt.strftime("%Y/%m/%d/")
    cnf["time_sql_time"] = dt.strftime("%H:%M:%S")
    cnf["time_sql"] = dt.strftime("%Y-%m-%d %H:%M:%S")
    cnf["time_pth"] = dt.strftime("%Y-%m-%d__%H_%M_%S")
    cnf["time_csv"] = dt.strftime("%Y_%m_%d_%H_%M_%S")
    cnf["time_ela"] = dt.strftime("%Y%m%d%H%M%S")
    cnf["time_hrm_date"] = dt.strftime("%Y-%m-01")

    # Last day of previous month
    prev_month = dt.replace(day=1) - timedelta(days=1)
    last_prev_day = calendar.monthrange(prev_month.year, prev_month.month)[1]
    last_prev = prev_month.replace(day=last_prev_day)

    cnf["time_last_ymd"] = last_prev.strftime("%Y-%m-%d")
    cnf["time_last_hrm"] = last_prev.strftime("%Y-%m-01")

    # Last day of current month
    last_day_current = calendar.monthrange(year, mon)[1]
    last_current = dt.replace(day=last_day_current)

    cnf["time_next_ymd"] = last_current.strftime("%Y-%m-%d")

    # Tomorrow
    tomorrow = dt + timedelta(days=1)
    cnf["time_sql_date_next"] = tomorrow.strftime("%Y-%m-%d")

    # UTC
    utc_now = datetime.now(timezone.utc)
    cnf["time_utc_date"] = utc_now.strftime("%Y-%m-%d %H:%M:%S")

    # Yesterday
    yesterday = dt - timedelta(days=1)
    cnf["time_sql_date_yesd"] = yesterday.strftime("%Y-%m-%d")

    return cnf


# =============================================================================
# PATH AND LOGGING FUNCTIONS
# =============================================================================


def get_paths(cnf: Dict[str, Any]) -> Dict[str, Any]:
    #Create log/data directory structure and file handles.

   
    mod = "x" + str(cnf["_this"]["_ety"])
    pth = cnf["path_logs"]

    base_path = os.path.join(pth, mod)

    os.makedirs(base_path, exist_ok=True)
    os.makedirs(os.path.join(base_path, "new"), exist_ok=True)
    os.makedirs(os.path.join(base_path, "old"), exist_ok=True)
    os.makedirs(os.path.join(base_path, "tmp"), exist_ok=True)
    os.makedirs(os.path.join(base_path, "log"), exist_ok=True)

    year = f"{int(cnf['time_year']):04d}"
    month = f"{int(cnf['time_month']):02d}"
    day = f"{int(cnf['time_day']):02d}"

    add_path = os.path.join(year, month, day)

    log_path = os.path.join(base_path, "log", add_path)
    old_path = os.path.join(base_path, "old", add_path)

    os.makedirs(log_path, exist_ok=True)
    os.makedirs(old_path, exist_ok=True)

    time_sql = cnf["time_sql"]
    time_csv = cnf.get("time_csv", time_sql)

    path_log_file = os.path.join(log_path, f"{time_sql}.log")
    path_dat_file = os.path.join(log_path, f"{time_sql}.dat")
    path_error_file = os.path.join(log_path, f"{time_sql}.error")

    path_last_log = os.path.join(base_path, "_last.log")
    path_last_dat = os.path.join(base_path, "_last.dat")
    path_last_error = os.path.join(base_path, "_last_error.log")

    # Open file handles
    LOG_OUT = open(path_log_file, "w", encoding="utf-8")
    DAT_OUT = open(path_dat_file, "w", encoding="utf-8")
    LOG_ERR = open(path_error_file, "w", encoding="utf-8")

    LST_OUT = open(path_last_log, "w", encoding="utf-8")
    LST_DAT = open(path_last_dat, "w", encoding="utf-8")
    LST_ERR = open(path_last_error, "w", encoding="utf-8")

    LST_OUT.write(f"{time_sql}\n")
    LST_DAT.write(f"{time_sql}\n")
    LST_ERR.write(f"{time_sql}\n")

    cnf["_this_path"] = {
        "handle_log": LOG_OUT,
        "handle_dat": DAT_OUT,
        "handle_error": LOG_ERR,
        "path_handle_log": path_log_file,
        "path_handle_dat": path_dat_file,
        "path_handle_any": os.path.join(log_path, f"{time_csv}."),
        "path_handle_error": path_error_file,
        "handle_last_log": LST_OUT,
        "handle_last_dat": LST_DAT,
        "handle_last_error": LST_ERR,
        "path_handle_last_json": os.path.join(base_path, "_last.jso"),
        "path_handle_last_call": os.path.join(base_path, "_last.api"),
        "path_handle_last_log": path_last_log,
        "path_handle_last_dat": path_last_dat,
        "path_handle_last_error": path_last_error,
        "path_log": log_path + os.sep,
        "path_new": os.path.join(base_path, "new") + os.sep,
        "path_old": old_path + os.sep,
        "path_tmp": os.path.join(base_path, "tmp") + os.sep,
        "path_lst": base_path + os.sep,
    }

    return cnf


def prints(cnf: Dict[str, Any], key: str, label: str, msg: str) -> bool:
    #Write formatted message to log/error file handles.

   
    if "log" not in key and "_this_path" not in cnf:
        return False

    try:
        paths = cnf.get("_this_path", {})
        chr_msg = f"\t-{label:<20}: {msg}\n"

        if key == "log":
            if "handle_log" in paths:
                paths["handle_log"].write(chr_msg)
            if "handle_last_log" in paths:
                paths["handle_last_log"].write(chr_msg)
            if cnf.get("log") == "true":
                print(chr_msg, end="")

        elif key == "error":
            if "handle_error" in paths:
                paths["handle_error"].write(chr_msg)
            if "handle_last_error" in paths:
                paths["handle_last_error"].write(chr_msg)
            if cnf.get("log") == "true":
                print(chr_msg, end="")

        elif key == "json":
            if "handle_dat" in paths:
                paths["handle_dat"].write(f"\t-{'URL':<20}: {msg}\n")
            if "handle_last_dat" in paths:
                paths["handle_last_dat"].write(f"\t-{'URL':<20}: {msg}\n")

        elif key == "dump":
            if "handle_dat" in paths:
                paths["handle_dat"].write(str(msg) + "\n")
            if "handle_last_dat" in paths:
                paths["handle_last_dat"].write(str(msg) + "\n")

        return True

    except Exception as e:
        logger.exception("prints failed: %s", e)
        return False


# =============================================================================
# TABLE CONFIGURATION FUNCTIONS
# =============================================================================


def this_tab(cnf: Dict[str, Any]) -> Dict[str, Any]:
    #Populate table names and URLs by substituting __MANDANT__ and __SYSTEM__ placeholders.

 
    this = cnf.get("_this", {})

    if "__MANDANT__" in this.get("api_host", ""):
        mdt = sql_var(
            cnf["sql"],
            f"SELECT pwd_info FROM {cnf['sys']}__basic._pwd WHERE _key = '{cnf['sys']}__bc_mandant'",
        )
        this["api_host"] = re.sub(
            r"__MANDANT__", str(mdt), this.get("api_host", ""), flags=re.I
        )
        this["set_host"] = re.sub(
            r"__MANDANT__", str(mdt), this.get("set_host", ""), flags=re.I
        )

        mdt = sql_var(
            cnf["sql"],
            f"SELECT pwd_key FROM {cnf['sys']}__basic._pwd WHERE _key = '{cnf['sys']}__bc_mandant'",
        )
        this["api_host"] = re.sub(
            r"__SYSTEM__", str(mdt), this.get("api_host", ""), flags=re.I
        )
        this["set_host"] = re.sub(
            r"__SYSTEM__", str(mdt), this.get("set_host", ""), flags=re.I
        )

        this["table_data"] = (
            f"{cnf['sys']}__{this.get('tab_pfix')}.{this.get('tab_name')}"
        )
        this["table_change"] = f"{cnf['sys']}__history.x{this.get('_ety')}"

        if is_value(this.get("ety_parent")):
            this["table_change"] = f"{cnf['sys']}__history.x{this.get('ety_parent')}"

    cnf["_this"] = this
    return cnf


# =============================================================================
# API AUTHENTICATION AND COMMUNICATIONS FUNCTIONS
# =============================================================================


def get_token(cnf: Dict[str, Any], ali: str) -> str:
    #Retrieve and cache API token from endpoint using credentials from _pwd table.

 
    pwd = sql_hash(cnf["sql"], f"SELECT * FROM {cnf['db']}._pwd WHERE _key = '{ali}'")

    url = pwd.get("pwd_host")
    par = pwd.get("pwd_parameter")

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    res = requests.post(url, data=par, headers=headers)
    dat = res.json()

    token = dat.get(pwd.get("pwd_key"), "")
    if not token:
        raise Exception(f"No Token for {ali}")

    prints(cnf, "log", "TOKEN", token)
    return token


def api_bcch_post_json(
    cnf: Dict[str, Any], api: str, jso: str, url: Optional[str] = None
):
    #POST JSON to BCCH API with bearer token authentication.

 
    if url is None:
        url = cnf["_this"]["api_host"]

    headers = {
        "Authorization": f"Bearer {api}",
        "Content-Type": "application/json",
    }

    prints(cnf, "log", "SEND_JSON", f"{url}: {jso}")
    res = requests.post(url, data=jso.encode("utf-8"), headers=headers)
    prints(cnf, "log", "RESULT_CONTENT", f"{res.status_code}: {res.text}")
    return res


def json_res(
    cnf: Dict[str, Any],
    res: Any,
    log: Dict[str, Any],
    pky: str,
    tab: Optional[str] = None,
) -> Dict[str, Any]:
    #Process API response: update record status and log errors.

   
 
    if tab is None:
        tab = cnf["_this"]["table_data"]

    if str(res.status_code).startswith("2"):
        log["cnt_create"] = log.get("cnt_create", 0) + 1
        prints(cnf, "log", "SEND_OK", str(pky))
        sql_any(
            cnf["sql"], f"UPDATE {tab} SET _nxt = 2, _msg = '' WHERE _pky = '{pky}'"
        )
    else:
        log["cnt_error"] = log.get("cnt_error", 0) + 1
        msg = res.text.replace("'", "")
        prints(cnf, "log", "SEND_ERROR", str(pky))
        prints(cnf, "log", "SEND_ERROR_INFO", msg)
        sql_any(
            cnf["sql"],
            f"UPDATE {tab} SET _nxt = 9, _msg = '{msg}' WHERE _pky = '{pky}'",
        )
        sql_replace(
            cnf["sql"],
            f"{cnf['sys']}__basic._err",
            {
                "_pky": pky,
                "_ety": cnf["_this"]["_ety"],
                "_tsk": cnf["_this"]["_tsk"],
                "_msg": msg,
            },
        )

    return log
