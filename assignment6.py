import psycopg2

# connection object to
conn = psycopg2.connect(user = "postgres",
                                  password = "test123",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")

# cursor object from the connection
cur = conn.cursor()

# read the csv file
with open('python_test.csv') as content:
    # skip the header
    next(content)
    # write into source table from content(csv content)
    cur.copy_from(content, "public.test_msr_source", columns=('rpt_grp_cd',
                                                                 'lctn_typ_cd',
                                                                 'clctn_prd_txt',
                                                                 'msr_cd',
                                                                 'clcltn_date',
                                                                 'grp_rate_nmrtr',
                                                                 'grp_rate_dnmntr',
                                                                 'file_name',
                                                                 'creat_ts',
                                                                 'creat_user_id',
                                                                 'submsn_cmplt_cd'),
                  sep=",")
conn.commit()
conn.close()
