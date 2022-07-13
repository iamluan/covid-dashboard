import pymongo
import pandas as pd
from urllib.request import urlopen
import json
import pandas as pd
import plotly.express as px
import pymongo


MONGO_URI = 'mongodb+srv://luan:M7m0FZV5cfC6j5sm@cluster0.avzsj.mongodb.net/?retryWrites=true&w=majority'
MONGO_DATABASE = 'Covid'

def create_line_chart():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    items = db['cleaned'].find()

    _date = []
    province = []
    new_case = []
    for item in items:
        for content in item['content']:
            _date.append(item['date'])
            province.append(content['province'])
            new_case.append(content['new_cases'])
    client.close()

    df = pd.DataFrame(
        {
            'date': _date,
            'province': province,
            'new_case': new_case
        }
    )
    df['date'] = df['date'].transform(
        func=lambda x: pd.to_datetime(x, dayfirst=True, 
        yearfirst=False)
    )
    display_provinces = ['hanoi', 'hochiminhcity', 'binhdinh']
    line_chart = px.line(
        df, 
        x="date", y="new_case", color='province', 
        markers=True, title='Daily new cases in last 30 days in all provinces',
        )
    return line_chart

