import dsa

#same modular arch
#dsa abstraction



#sub att in perl replaced 
#att takes these fields and format them for SQL select statement


def att (arg):
    fields = [
	    "ID",
        "UsageEnd",
        "JobName",
        "Cardinality",
        "NonChargeable",
        "Was",
        "Device",
        "Auftrag",
        "Auftragsnr",
        "Username",
        "KZ" 
    ]

    return dsa.field_loop(arg,fields)

def ask (cnf):
    return f"""
    SELECT {att(cnf)}
    FROM [DsPcDb].[dbo].[buh_druck_kopie]
    WHERE CAST(UsageEnd AS date) = CAST(DATEADD(day,-1,GETDATE()) AS date)"""

def run (cnf):
    #get sql
    sql = ask (cnf)
    #convert dsa.mss in to python 
    conn = dsa.mss(cnf, f"{cnf['sys']}___mssql_uniflow")

    #fetch array of dict
    #convert dsa.msa in to python 
    data = dsa.mss_array (conn,sql)

    for row in data:
        if row.get("field_usageend"):
            row["field_usageend"] = str(row["field_usageend"]) [:19]


    #push to DSA

    log = dsa.to_dsa(cnf, data, "field_id",{},"no_delete")

    return cnf,log