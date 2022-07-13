import pandas as pd

CRAWLED_FILE_PATH = \
    '/home/luan/projects/covid_dashboard/pipelines/vn_etl/1_extract/covid_crawler/output/total_by_provinces.csv'

def to_int(value: str):
    if value == '-':
        return -1
    num_str = ''
    for char in value:
        if char != '.':
            num_str = num_str + char
    return int(num_str)


df = pd.read_csv(
    CRAWLED_FILE_PATH, 
    na_values='-',
    converters={'total_cases': to_int, 'total_deaths': to_int}
)

# print(df)
# print(df.dtypes)

df.to_csv(
    path_or_buf=
        '/home/luan/projects/covid_dashboard/pipelines/vn_etl/2_transformation/output/total_by_provinces.csv',
    index=False
)