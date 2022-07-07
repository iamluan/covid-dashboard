import pandas as pd
import psycopg2

def write_data(data: pd.DataFrame, table_name: str):
    n_cols = len(data.columns)
    # print(n_cols)
    conn = psycopg2.connect(
            host='localhost', 
            port='5433', 
            database='luandb',
            user='luan', 
            password='luan_password'
        )
    try:
        cur = conn.cursor()
        for row in data.itertuples(index=False):
            record = tuple(row)
            if table_name == 'date_dim':
                cur.execute(
                f"""
                    INSERT INTO date_dim(year, month, day)
                    VALUES ({','.join(n_cols*['%s'])}); 
                """, 
                record
                )
                continue    
            cur.execute(
                f"""
                    INSERT INTO {table_name}
                    VALUES ({','.join(n_cols*['%s'])}); 
                """, 
                record
            )
        conn.commit()
        print("Commit successfully")
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Postgresql Error: {error}")
    finally:
        if conn is not None:
            conn.close()

DATASET_PATH = \
    "/home/luan/projects/covid_dashboard/pipelines/global/init/dataset/owid-covid-data.csv"
RELATIVE_DATASET_PATH = "./dataset/owid-covid-data.csv"
COLUMN_NAME_LIST = [
    'iso_code', 
    'continent', 
    'location', 
    'date', 
    'total_cases', 
    'new_cases',
    'total_deaths', 
    'new_deaths'
]


df = pd.read_csv(
    DATASET_PATH, parse_dates=['date'], 
    na_values=''
)
necessary_df = df[COLUMN_NAME_LIST]

print(necessary_df)

# TRANSFORM and Load DATE
unique_sorted_date_series = necessary_df['date']\
    .drop_duplicates()\
    .sort_values(ascending=True)
separated_date_df = unique_sorted_date_series\
    .transform(lambda x: (x.year, x.month, x.day))\
    .to_frame()
# print(separated_date_df)
date_df = pd.DataFrame()
date_df[['year', 'month', 'days']] = pd.DataFrame(separated_date_df['date'].tolist())
write_data(date_df, 'date_dim')
print('Successfully - INSERT date_dim ')


# TRANSFORM and load COUNTRY
country_df = pd.DataFrame()
country_df[['iso_code_3', 'continent', 'country']] = necessary_df[[
    'iso_code', 'continent', 'location']]\
    .drop_duplicates(subset=['iso_code'])\
    .sort_values(by='location' ,ascending=True)
# print(country_df)
write_data(country_df, 'country_dim')
print('Successfully - INSERT country_dim ')

# TRANSFORM COVID fact
## Read date_dim table
date_dim_df = pd.DataFrame()
try:
    conn = psycopg2.connect(
    host='localhost', 
    port='5433', 
    database='luandb',
    user='luan', 
    password='luan_password'
    )
    cur = conn.cursor()
    cur.execute('SELECT * FROM date_dim;')
    data = cur.fetchall()
    # print(data)
    date_dim_df = pd.DataFrame(data, columns=['key', 'year', 'month', 'day'])
    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
# print(date_dim_df)
## Read covid table
covid_df = necessary_df[
    [
        'iso_code', 
        'new_cases', 'total_cases', 
        'new_deaths', 'total_deaths'
    ]
]
separated_date_series = necessary_df['date']\
    .transform(lambda x: [x.year, x.month, x.day])
# print(separated_date_series)
separated_date_covid_df = pd.DataFrame(
    separated_date_series.to_list(), 
    columns=['year', 'month', 'day']
)
final_covid_df = pd.concat(
    [covid_df, separated_date_covid_df], 
    axis=1,
)
print('Start joining')
join_df = date_dim_df.set_index(keys=['year', 'month', 'day'])\
    .join(
        final_covid_df.set_index(keys=['year', 'month', 'day']), 
        on=['year', 'month', 'day'], how='inner', 
    )
join_df = join_df[[ 'iso_code', 'key','new_cases'  , 'total_cases', 'new_deaths','total_deaths']]
print('End joining')

print('Start writing')
write_data(join_df, 'covid')
print('Successfully - INSERT covid')