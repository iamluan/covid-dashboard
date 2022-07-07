import psycopg2
from datetime import date, timedelta

def add_next_date_to_date_dim():
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
                SELECT *
                FROM date_dim
                WHERE _key = (
                    SELECT MAX(_key)
                    FROM date_dim
                ); 
            """
        )
        row = cur.fetchone()
        # key = row[0] + 1
        lasted_date = date(row[1], row[2], row[3]) + timedelta(days=1)
        cur.execute(
            f"""
                INSERT INTO date_dim (year, month, day)
                VALUES ({lasted_date.year}, {lasted_date.month}, {lasted_date.day})
            """
        )
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

add_next_date_to_date_dim()