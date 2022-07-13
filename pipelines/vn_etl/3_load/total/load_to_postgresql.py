import psycopg2
import pandas as pd

CRAWLED_DATASET_PATH=\
    '/home/luan/projects/covid_dashboard/pipelines/vn_etl/2_transformation/output/total_by_provinces.csv'
df = pd.read_csv(CRAWLED_DATASET_PATH)
province_df = pd.DataFrame()
conn = None
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
            DROP TABLE IF EXISTS vn_total_by_provinces
            ;
            CREATE TABLE vn_total_by_provinces (
                    province varchar(50),
                    total_cases int null,
                    total_deaths int null
            )
            ;
        """
    )
    for row in df.itertuples(index=False):
        row_tuple = tuple(row)
        cur.execute(
            f"""
                INSERT INTO vn_total_by_provinces(province, total_cases, total_deaths)
                VALUES (%s, %s, %s);
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