from urllib.request import urlopen
import json
import pandas as pd
import plotly.express as px
import psycopg2
import re


def no_accent_vietnamese(s):
    s = re.sub(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
    s = re.sub(r'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', s)
    s = re.sub(r'[èéẹẻẽêềếệểễ]', 'e', s)
    s = re.sub(r'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)
    s = re.sub(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
    s = re.sub(r'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)
    s = re.sub(r'[ìíịỉĩ]', 'i', s)
    s = re.sub(r'[ÌÍỊỈĨ]', 'I', s)
    s = re.sub(r'[ùúụủũưừứựửữ]', 'u', s)
    s = re.sub(r'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', s)
    s = re.sub(r'[ỳýỵỷỹ]', 'y', s)
    s = re.sub(r'[ỲÝỴỶỸ]', 'Y', s)
    s = re.sub(r'[Đ]', 'D', s)
    s = re.sub(r'[đ]', 'd', s)

    marks_list = [u'\u0300', u'\u0301', u'\u0302', u'\u0303', u'\u0306',u'\u0309', u'\u0323']

    for mark in marks_list:
        s = s.replace(mark, '')

    return s

GEOJSON_URL = 'https://raw.githubusercontent.com/Vizzuality/growasia_calculator/master/public/vietnam.geojson'
with urlopen(GEOJSON_URL) as response:
    provinces = json.load(response)
    
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
        SELECT * FROM vn_total_by_provinces;
    """
)
df = pd.DataFrame(cur.fetchall(), columns=['province', 'total_cases', 'total_deaths'])
cur.close()
conn.close()
province_series = df['province'].transform(lambda x: no_accent_vietnamese(x).lower().replace(' ', ''))
# print(province_series)
df2 = pd.concat(
    [df.drop(labels=['province'], axis=1), province_series.to_frame(name='province')], 
    axis=1,
)
for province in provinces['features']:
    for city in df2['province']:
        if city in province['properties']['slug']:
            province['id'] = city
            break
    if province['properties']['slug'] == 'vietnam-hoabinh':
        province['id'] = 'hoabinh'
    elif province['properties']['slug'] == 'vietnam-dacnong':
        province['id'] = 'dacnong'
    elif province['properties']['slug'] == 'vietnam-khanhhoa':
        province['id'] = 'khanhhoa'
    elif province['properties']['slug'] == 'vietnam-thanhhoa':
        province['id'] = 'thanhhoa'
    elif province['properties']['slug'] == 'vietnam-hochiminhcityhochiminh':
        province['id'] = 'tphcm'
    elif province['properties']['slug'] == 'vietnam-bariavtaubariavungtau':
        province['id'] = 'baria-vungtau'

def create_cases_map():
    max_value = df2['total_cases'].max()
    return px.choropleth(
        df2, geojson=provinces, locations='province',
        color='total_cases',
        color_continuous_scale="Viridis",
        range_color=(0, max_value),
        labels={'total_cases':'cases'},
        basemap_visible=False,
        fitbounds='geojson',
        title='Total Cases'
    )
def create_deaths_map():
    max_deaths = df2['total_deaths'].max()
    return px.choropleth(
        df2, geojson=provinces, locations='province',
        color='total_deaths',
        range_color=(0, max_deaths),
        labels={'total_deaths':'deaths'},
        basemap_visible=False,
        fitbounds='geojson',
        title='Total Deaths'
    )
