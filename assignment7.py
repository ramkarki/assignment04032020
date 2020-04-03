import psycopg2
from datetime import datetime

# declare source connection object for PostgreSQL
source_conn = psycopg2.connect(user = "postgres",
                                  password = "test123",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")

source_table = "public.test_msr_source"

# cursor object from source connection
source_cursor = source_conn.cursor()
# execute the query from source table
source_cursor.execute('select * from %s' % source_table)
# store all the records from the table to list
source_data_row = source_cursor.fetchall()

# redshift connection
target_conn = psycopg2.connect(user="**********",
                               password="********",
                               host="*********.redshift.amazonaws.com",
                               port="5432",
                               database="********")

target_table = "public.test_msr_target"
target_cursor = target_conn.cursor()
target_rows = []
id = 0
for row in source_data_row:
    id = id+1
    test_msr_target_id = id
    rpt_grp_cd = row[0]
    lctn_typ_cd = row[1]
    clctn_prd_txt = row[2]
    msr_cd = row[3]
    clcltn_date = datetime.strptime(row[4], "%m/%d/%Y")
    grp_rate_nmrtr = row[5]
    grp_rate_dnmntr = row[6]
    finl_sw = ''
    file_name = row[7]
    creat_ts = datetime.fromtimestamp(datetime.now().timestamp()) if len(row[8]) == 0 else datetime.fromtimestamp(
        datetime.strptime(row[8], '%m/%d/%Y %H:%M').timestamp())
    creat_user_id = row[9]
    submsn_cmplt_cd = row[10]

    target_rows.append(
        {
            "test_msr_target_id": test_msr_target_id,
            "rpt_grp_cd": rpt_grp_cd,
            "lctn_typ_cd": lctn_typ_cd,
            "clctn_prd_txt": clctn_prd_txt,
            "msr_cd": msr_cd,
            "clcltn_date": clcltn_date,
            "grp_rate_nmrtr": int(grp_rate_nmrtr),
            "grp_rate_dnmntr": int(grp_rate_dnmntr),
            "finl_sw": finl_sw,
            "file_name": file_name,
            "creat_ts": creat_ts,
            "creat_user_id": creat_user_id,
            "submsn_cmplt_cd": submsn_cmplt_cd
        }
    )

target_cursor.executemany("""INSERT INTO public.test_msr_target(
                                test_msr_target_id,
                                rpt_grp_cd,
                                lctn_typ_cd,
                                clctn_prd_txt,
                                msr_cd,
                                clcltn_date,
                                grp_rate_nmrtr,
                                grp_rate_dnmntr,
                                finl_sw,
                                file_name,
                                creat_ts,
                                creat_user_id,
                                submsn_cmplt_cd)
                    VALUES (%(test_msr_target_id)s
                    , %(rpt_grp_cd)s
                    , %(lctn_typ_cd)s
                    , %(clctn_prd_txt)s
                    , %(msr_cd)s
                    , %(clcltn_date)s
                    , %(grp_rate_nmrtr)s
                    , %(grp_rate_dnmntr)s
                    , %(finl_sw)s
                    , %(file_name)s
                    , %(creat_ts)s
                    , %(creat_user_id)s
                    , %(submsn_cmplt_cd)s
                    )""", target_rows)
target_conn.commit()
source_conn.close()
target_conn.close()
