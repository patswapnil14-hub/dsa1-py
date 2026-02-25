import re
#has- full database snapshot
#att-primry key
#set- one row from db
#ety- job identifier
def field_loop(cnf,txt):
    sql = ""
    has_dict = {}
    ety = cnf["_this"].get("_ety")
    if dsa.is_value (cnf["_this"].get("_ety_parent")):
        ety = cnf["_this"].get("ety_parent")
    
    if dsa.no_value (ety):
        raise Exception ("missing job ety")
    ali = cnf["_this"].get("ety_alias")
    if dsa.is_value (cnf["_this"].get("ety_alias_parent")):
        ali = cnf["_this"].get("ety_alias_parent")
    #current value
    query = f"select * from {cnf['db']} job_field where _ety='{ety};"
    rows = dsa.sql_array(cnf["sql"],query)
    
    #h is current value
    for h in rows:
        has_dict[h["_fld"]] =h
    #trim
    txt = txt.strip()
    sql = ""
    #loop lines
    for row in txt.split("\n"):
        row = row.strip()
        if not row:
            continue
        fld = dsa.field_name(row)
        
        #if substr does not start with 2, remove prefix field_
        if not str(cnf["_this"]["_ety"]).startswith("2"):
            fld = re.sub(r"^field_",fld,flags=re.IGNORECASE)
        
        
        #generate a hash_key only if fld has value
        if dsa.is_value(fld):
            att = dsa.get_farmhash(f"{ali}.{fld}")
            
            #get existing dbrow or start new
            set_dict =has_dict.get(att,{})
              
            #update set_dict with
            set_dict["_fld"]    		= att
            set_dict["_ety"]  			= ety
            set_dict["fld_alias"]  		= f"{ali}.{fld}"
            set_dict["fld_name"]  		= fld
            set_dict["fld_name_origin"] = row
            
            #store back to has_dict
            has_dict[att] =set_dict
            
            #upsert operation
            dsa.sql_replace (
                cnf["sql"],
                f"{cnf['db']}_job_field",
                set_dict
            )
            
            #prepare select statement mapping
            #original field with cleaned field
            sql += f" {row} as {fld},"
    sql = re.sub(r",$","",sql)
    return sql
    


def field_loop_from_table(cnf):
        txt = ""

        columns = sql_array (cnf ['sql'],
                            f"""
                            SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{cnf['sys']}__{cnf['_this']['tab_pfix']}' AND TABLE_NAME='{cnf['_this']['tab_name']}' ORDER BY ORDINAL_POSITION
                            """
        )

        for h in columns:
        #skip columns start with _
            if not h['COLUMN_NAME'].startswith('_'):
                txt += f"\t{h['COLUMN_NAME']}\n"
            
        field_loop(cnf,txt)

        return 1

from sqlalchemy import create_engine,text



def sql_hash (mysql_engine, sql , params = None):

    with mysql_engine.connect() as conn:
        result = conn.execute( text(sql),params or {})
        row = result.mappings().first()
        return dict(row) if row else {}



def mss(cnf,ali):

    #python equivalent of dsa:mss
    #cnf:mysql connection plus dbname
    #ali: key to lookup _pwd table ( mssql connection)


    #connect to mysql
    mysql_engine = create_engine(cnf["sql"])

    #read credentials from _pwd table
    sql = f"SELECT * FROM {cnf["db"]}._pwd where _key = :ali"
    hsh = sql_hash(mysql_engine, sql, {"ali":ali})


    if not hsh:
        raise Exception(f"No entry found in _pwd for key: {ali}")
    

    user = hsh["pwd_user"]
    password=hash["pwd_pass"]
    odbc_alias = hsh ["pwd_odbc_alias"]



    #connect to mssql using ODBC

    mssql_url = f"mssql+pyodbc://{user}:{password}@{odbc_alias}"

    mssql_engine = create_engine(mssql_url)

    return mssql_engine