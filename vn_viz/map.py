from urllib.request import urlopen
import json
import pandas as pd
import plotly.express as px
import pymongo

MONGO_URI = 'mongodb+srv://luan:M7m0FZV5cfC6j5sm@cluster0.avzsj.mongodb.net/?retryWrites=true&w=majority'
MONGO_DATABASE = 'Covid'

GEOJSON_URL = 'https://raw.githubusercontent.com/Vizzuality/growasia_calculator/master/public/vietnam.geojson'

with urlopen(GEOJSON_URL) as response:
    provinces = json.load(response)
    
client = pymongo.MongoClient(MONGO_URI)
db = client[MONGO_DATABASE]
items = db['aggregated_covid'].find()
cities = []
cases = []
for item in items:
    city = item['city'].lower().replace(' ', '')
    if city == 'tp.hcm':
        city = 'hochiminh' 
    cities.append(city)
    cases.append(item['case'])
df = pd.DataFrame({
    'city': cities,
    'case': cases
})
client.close()
max_case = max(cases)
for province in provinces['features']:
    for city in df['city']:
        if city in province['properties']['slug']:
            # print(f"City {city} and {province['properties']['slug']}")
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

# for province in provinces['features']:
#     try:
#         province['id']
#     except:
#         print(province['properties']['slug'])


fig = px.choropleth(df, geojson=provinces, locations='city', color='case',
                           color_continuous_scale="Viridis",
                           range_color=(0, max_case),
                           labels={'case':'cases'},
                           basemap_visible=False,
                           fitbounds='geojson'
                          )
fig.write_html('first_figure.html')

