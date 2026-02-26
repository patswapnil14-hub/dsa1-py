

import re
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# FIELD MANAGEMENT FUNCTIONS
# ============================================================================

def field_loop_from_table(conf: Dict[str, Any]) -> int:
 
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
                    }
                )
            except Exception as e:
                logger.warning("Could not register field %s: %s", fld, e)
            
            sql_parts.append(f" {row} AS {fld}")
        
        return ",".join(sql_parts).rstrip(",")
    
    except Exception as e:
        logger.exception("field_loop failed: %s", e)
        return ""


def field_name(text: str) -> str:
  
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


def field_relation(conf: Dict[str, Any], map_cfg: List[Dict[str, Any]]) -> int:

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
                    sql_create(
                        sql_conn,
                        f"{db}._con",
                        {"tgt_ety": tgt, "src_ety": src}
                    )
                except Exception as e:
                    logger.warning("Could not create relation: %s", e)
        
        return 1
    
    except Exception as e:
        logger.exception("field_relation failed: %s", e)
        return 0


# ============================================================================
# SQL EXECUTION FUNCTIONS
# ============================================================================

def sql_array(sql_conn: Any, query: str) -> List[Dict[str, Any]]:
 
    if not sql_conn or not query:
        return []
    
    try:
        # DB-API cursor-like object
        if hasattr(sql_conn, "execute") and hasattr(sql_conn, "fetchall"):
            cursor = sql_conn
            cursor.execute(query)
            rows = cursor.fetchall()
            return _rows_to_dicts(cursor, rows)
        
        # DB-API connection-like object
        if hasattr(sql_conn, "cursor"):
            try:
                cur = sql_conn.cursor()
                cur.execute(query)
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
            return sql_conn(query) or []
        
        # List of dicts
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


def sql_any(sql_conn: Any, query: str) -> bool:
   
    if not sql_conn or not query:
        return False
    
    try:
        # Cursor-like object
        if hasattr(sql_conn, "execute"):
            sql_conn.execute(query)
            # Try commit if available
            if hasattr(sql_conn, "connection") and hasattr(sql_conn.connection, "commit"):
                sql_conn.connection.commit()
            return True
        
        # Connection-like object
        if hasattr(sql_conn, "cursor"):
            try:
                cur = sql_conn.cursor()
                cur.execute(query)
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
            return bool(sql_conn(query))
    
    except Exception as e:
        logger.exception("sql_any failed: %s", e)
    
    return False


def sql_var(sql_conn: Any, query: str) -> Any:
  
    if not sql_conn or not query:
        return ""
    
    try:
        rows = sql_array(sql_conn, query)
        if rows:
            for v in rows[0].values():
                return v if v is not None else ""
    except Exception as e:
        logger.exception("sql_var failed: %s", e)
    
    return ""


def sql_hash(sql_conn: Any, query: str) -> Dict[str, Any]:
   
    if not sql_conn or not query:
        return {}
    
    try:
        rows = sql_array(sql_conn, query)
        return rows[0] if rows else {}
    except Exception as e:
        logger.exception("sql_hash failed: %s", e)
    
    return {}


def sql_create(sql_conn: Any, table: str, data: Dict[str, Any]) -> int:
   
    if not sql_conn or not table or not data:
        return 0
    
    try:
        cols = list(data.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_str = ", ".join([f"`{c}`" for c in cols])
        
        query = f"INSERT IGNORE INTO {table} ({col_str}) VALUES ({placeholders})"
        values = [data.get(c) or "" for c in cols]
        
        if hasattr(sql_conn, "execute"):
            sql_conn.execute(query, values)
            if hasattr(sql_conn, "connection") and hasattr(sql_conn.connection, "commit"):
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
 
    if not sql_conn or not table or not data:
        return False
    
    try:
        cols = list(data.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_str = ", ".join(cols)
        
        query = f"REPLACE INTO {table} ({col_str}) VALUES ({placeholders})"
        values = [data.get(c) or "" for c in cols]
        
        if hasattr(sql_conn, "execute"):
            sql_conn.execute(query, values)
            if hasattr(sql_conn, "connection") and hasattr(sql_conn.connection, "commit"):
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


# ============================================================================
# VALUE CHECKING FUNCTIONS
# ============================================================================

def is_value(val: Any) -> bool:
   
    if val is None:
        return False
    
    val_str = str(val)
    
    if val_str == "":
        return False
    if val_str == "0":
        return False
    
    # Invalid SQL date sentinels
    if val_str.startswith("1753-01-01 00:00:00"):
        return False
    if val_str.startswith("0001-01-01 00:00:00"):
        return False
    
    return True


def no_value(val: Any) -> bool:
   
    return not is_value(val)


# ============================================================================
# FORMATTING FUNCTIONS
# ============================================================================

def format_number(s: Any) -> str:

    if s is None:
        return ""
    
    s_str = str(s)
    return re.sub(r"\D", "", s_str)


def format_uc(s: Any) -> str:
    """Convert to uppercase."""
    return str(s or "").upper()


def format_lc(s: Any) -> str:
    """Convert to lowercase."""
    return str(s or "").lower()


def format_by_cnf(data: Dict[str, Any], map_cfg: List[Dict[str, Any]]) -> Dict[str, Any]:
   
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


# ============================================================================
# HASH AND CLEANUP FUNCTIONS
# ============================================================================

def get_farmhash(s: str) -> str:
  
    try:
        from farmhash import hash64
        return str(hash64(s))
    except ImportError:
        import hashlib
        return hashlib.sha256(str(s).encode()).hexdigest()[:16]


def get_farmhash_hsh(hsh: Dict[str, Any], exclude: str = "") -> str:
    
    
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
 
    cleaned = {}
    for k, v in record.items():
        if v is None:
            cleaned[k] = v
        else:
            s = str(v)
            # Remove non-printable and leading/trailing whitespace
            s = re.sub(r"[^\x20-\x7E\t\n\r]", " ", s)
            s = s.strip()
            # Collapse multiple spaces
            s = re.sub(r" +", " ", s)
            cleaned[k] = s
    
    return cleaned


# ============================================================================
# CORE ETL FUNCTION
# ============================================================================

def to_dsa(
    conf: Dict[str, Any],
    arr: List[Dict[str, Any]],
    pky: str,
    log: Optional[Dict[str, Any]] = None,
    delete_mode: str = ""
) -> Dict[str, Any]:

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
            
            for h in sql_array(sql_conn, f"SELECT _pky, _fmh_object FROM {table_data}{grp_clause}"):
                has[h.get("_pky")] = h.get("_fmh_object")
        
        log["cnt_loaded"] = len(has)
        
        # Load allowed field list
        att = {}
        for h in sql_array(sql_conn, f"SELECT fld_name FROM {db}._job_field WHERE _ety = '{ety}'"):
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
                
                old_rec = sql_hash(sql_conn, f"SELECT * FROM {table_data} WHERE _pky = '{h_filtered['_pky']}'")
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
                
                old_rec = sql_hash(sql_conn, f"SELECT * FROM {table_data} WHERE _pky = '{pky_val}'")
                old_rec["i_reason"] = "delete"
                sql_create(sql_conn, table_change, old_rec)
                sql_any(sql_conn, f"DELETE FROM {table_data} WHERE _pky = '{pky_val}'")
    
    except Exception as e:
        logger.exception("to_dsa failed: %s", e)
    
    return log
            set_dict["fld_alias"] = f"{ali}.{fld}"
            set_dict["fld_name"] = fld
            set_dict["fld_name_origin"] = row

            # store back to has_dict
            has_dict[att] = set_dict

            # upsert operation
            dsa.sql_replace(cnf["sql"], f"{cnf['db']}_job_field", set_dict)

            # prepare select statement mapping
            # original field with cleaned field
            sql += f" {row} as {fld},"
    sql = re.sub(r",$", "", sql)
    return sql


def field_loop_from_table(cnf):
    txt = ""

    columns = sql_array(
        cnf["sql"],
        f"""
         SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{cnf['sys']}__{cnf['_this']['tab_pfix']}' AND TABLE_NAME='{cnf['_this']['tab_name']}' ORDER BY ORDINAL_POSITION
        """,
    )

    for h in columns:
        # skip columns start with _
        if not h["COLUMN_NAME"].startswith("_"):
            txt += f"\t{h['COLUMN_NAME']}\n"

    field_loop(cnf, txt)

    return 1


from sqlalchemy import create_engine, text


def sql_hash(mysql_engine, sql, params=None):

    with mysql_engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        row = result.mappings().first()
        return dict(row) if row else {}


def mss(cnf, ali):

    # python equivalent of dsa:mss
    # cnf:mysql connection plus dbname
    # ali: key to lookup _pwd table ( mssql connection)

    # connect to mysql
    mysql_engine = create_engine(cnf["sql"])

    # read credentials from _pwd table
    sql = f"SELECT * FROM {cnf["db"]}._pwd where _key = :ali"
    hsh = sql_hash(mysql_engine, sql, {"ali": ali})

    if not hsh:
        raise Exception(f"No entry found in _pwd for key: {ali}")

    user = hsh["pwd_user"]
    password = hash["pwd_pass"]
    odbc_alias = hsh["pwd_odbc_alias"]

    # connect to mssql using ODBC

    mssql_url = f"mssql+pyodbc://{user}:{password}@{odbc_alias}"

    mssql_engine = create_engine(mssql_url)

    return mssql_engine
