import psycopg2
import pandas as pd

DATASET_PATH = \
    '/home/luan/projects/covid_dashboard/pipelines/vn_etl/3_load/total/dataset/vn_provinces.csv'
df = pd.read_csv(DATASET_PATH, usecols=['Region code', 'Province/State'])
df = df[['Region code', 'Province/State']]

print(df)

try:
    conn = psycopg2.connect(
    host='localhost', 
    port='5433', 
    database='luandb',
    user='luan', 
    password='luan_password'
    )
    cur = conn.cursor()
    cur.execute(
        f"""
            DROP TABLE IF EXISTS vn_provinces;
            CREATE TABLE vn_provinces (
                    code varchar(10) PRIMARY KEY,
                    province varchar(50)
            )
            ;
        """
    )
    for row in df.itertuples(index=False):
        row_tuple = tuple(row)
        print(row_tuple)
        cur.execute(
            f"""
                INSERT INTO vn_provinces(code, province)
                VALUES (%s, %s)
                ;
            """,
            row_tuple
        )
    cur.close()
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()