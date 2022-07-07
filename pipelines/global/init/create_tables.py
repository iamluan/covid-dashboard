def drop_tables():
    import psycopg2
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
                DROP TABLE IF EXISTS covid;
                DROP TABLE IF EXISTS country_dim;
                DROP TABLE IF EXISTS date_dim;
                DROP TABLE IF EXISTS VNProvinceCurrentTotalCases;
            """
        )
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def create_tables():
    import psycopg2
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
                CREATE TABLE country_dim (
                    iso_code_3 varchar(10) PRIMARY KEY,
                    continent varchar(50),
                    country varchar(50)
                );
                CREATE TABLE date_dim(
                    _key SERIAL PRIMARY KEY,
                    year SMALLINT,
                    month SMALLINT,
                    day SMALLINT
                );
                CREATE TABLE VNProvinceCurrentTotalCases (
                    province_code varchar(10) PRIMARY KEY,
                    province varchar(50),
                    yesterday_cases int,
                    total_cases int,
                    total_deaths int
                );
                CREATE TABLE covid (
                    iso_code_3 varchar(10),
                    date_key int,
                    new_cases float NULL,
                    total_cases float NULL,
                    new_deaths float NULL,
                    total_deaths float NULL,
                    PRIMARY KEY (iso_code_3, date_key),
                    FOREIGN KEY (iso_code_3) REFERENCES country_dim(iso_code_3),
                    FOREIGN KEY (date_key) REFERENCES date_dim(_key)
                );
            """
        )
        cur.close()
        conn.commit()
        print("Tables are created")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

drop_tables()
create_tables()