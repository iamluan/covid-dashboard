import psycopg2
import pandas as pd
from datetime import date
import plotly.express as px
import plotly.graph_objects as go

host='localhost'
port='5433'
database='luandb'
user='luan' 
password='luan_password'

def create_time_series(country_iso: str, y):
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    cur = conn.cursor()
    cur.execute(
        f"""
            SELECT 
                c.iso_code_3, cd.country, 
                d.year, d.month, d.day,
                c.new_cases, c.total_cases, 
                c.new_deaths, c.total_deaths
            FROM covid c
            LEFT JOIN date_dim d ON c.date_key = d._key
            LEFT JOIN country_dim cd ON c.iso_code_3 = cd.iso_code_3
            WHERE c.iso_code_3 = '{country_iso}';
        """
    )
    df = pd.DataFrame.from_records(
        data=cur.fetchall(), 
        columns=[
            'iso_code_3', 'country', 
            'year', 'month', 'day',
            'new_cases', 'total_cases', 
            'new_deaths', 'total_deaths'
        ],

    )
    cur.close()
    conn.close()
    
    date_series = df.apply(
        lambda x: date(x['year'], x['month'], x['day']),
        axis=1
    )
    date_df = date_series.to_frame(name='date')
    new_df = pd.concat([df, date_df], axis=1)

    fig = px.line(
        new_df, x='date', y=y, 
        title=f"{new_df[new_df['iso_code_3']==country_iso]['country'].iloc[0]}"
    )
    return fig

def create_global_map():
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    cur = conn.cursor()
    from datetime import date, timedelta
    cur.execute(
        f"""
            create or replace view latest_update as
                select iso_code_3, max(date_key) as max_date_key
                from covid
                where char_length(iso_code_3) = 3
                group by iso_code_3;
            SELECT 
                c.iso_code_3, cd.country, 
                d.year, d.month, d.day,
                c.new_cases, c.total_cases, 
                c.new_deaths, c.total_deaths
            FROM covid c
            LEFT JOIN date_dim d ON c.date_key = d._key
            LEFT JOIN country_dim cd ON c.iso_code_3 = cd.iso_code_3
            INNER JOIN latest_update l ON 
                c.iso_code_3 = l.iso_code_3 AND
                c.date_key = l.max_date_key
            ;
        """
    )
    df = pd.DataFrame.from_records(
        data=cur.fetchall(), 
        columns=[
            'iso_code_3', 'country', 
            'year', 'month', 'day',
            'new_cases', 'total_cases', 
            'new_deaths', 'total_deaths'
        ],
    )
    cur.close()
    conn.close()
    date_series = df.apply(
        lambda x: date(x['year'], x['month'], x['day']),
        axis=1
    )
    date_df = date_series.to_frame(name='date')
    new_df = pd.concat([df, date_df], axis=1)
    fig = go.Figure(
        data=go.Choropleth(
            locations = new_df['iso_code_3'],
            z = new_df['total_cases'],
            text = new_df['country'],
            colorscale = 'Blues',
            autocolorscale=True,
            marker_line_color='darkgray',
            marker_line_width=0.5,
            colorbar_title = 'Cases',
        )
    )

    fig.update_layout(
        title_text='Global total cases covdid-19',
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection_type='equirectangular'
        ),
        annotations = [dict(
            x=0.55,
            y=0.1,
            xref='paper',
            yref='paper',
            showarrow = False
        )]
    )
    # fig.write_html('/home/luan/projects/covid_dashboard/viz/global_viz/global_map.html')
    return fig

def create_timeline_map():
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    cur = conn.cursor()
    from datetime import date, timedelta
    cur.execute(
        f"""
            SELECT 
                c.iso_code_3, cd.country, 
                d.year, d.month, d.day,
                c.new_cases, c.total_cases, 
                c.new_deaths, c.total_deaths
            FROM covid c
            LEFT JOIN date_dim d ON c.date_key = d._key
            LEFT JOIN country_dim cd ON c.iso_code_3 = cd.iso_code_3
            WHERE CHAR_LENGTH(cd.iso_code_3) = 3
            ORDER BY c.date_key ASC
            ;
        """
    )
    df = pd.DataFrame.from_records(
        data=cur.fetchall(), 
        columns=[
            'iso_code_3', 'country', 
            'year', 'month', 'day',
            'new_cases', 'total_cases', 
            'new_deaths', 'total_deaths'
        ],
    )
    cur.close()
    conn.close()
    date_series = df.apply(
        lambda x: date(x['year'], x['month'], x['day']),
        axis=1
    )
    date_df = date_series.to_frame(name='date')
    new_df = pd.concat([df, date_df], axis=1)
    print(new_df)
    fig = px.choropleth(
        new_df, locations=new_df['iso_code_3'],
        color=new_df["total_cases"],
        hover_name="country",
        hover_data=['total_cases','total_deaths'],
        animation_frame='date',
    )
    return fig

# create_time_series(country_iso='IND', y='new_cases')
country_iso_list = ['USA', 'JPN', 'CHN', 'IND', 'VNM']

with open('/home/luan/projects/covid_dashboard/viz/global_viz/index.html', 'w') as f:
    f.write(
        """
            <!DOCTYPE html>
                <html>
                <head><h1>Global Covid-19 dashboard</h1></head>
                    <body>
                    <div style="display:flex">
                        <div style="width:800px;float:left;">
        """
    )
    global_map = create_timeline_map()
    f.write(global_map.to_html(full_html=False, include_plotlyjs='cdn'))
    f.write(
        f"""            
                        </div>

                        <div style="overflow-y:scroll; height:700px;float:right;">
        """
    )
    for iso in country_iso_list:
        fig = create_time_series(country_iso=iso, y=['new_cases', 'new_deaths'])
        f.write(fig.to_html(full_html=False, include_plotlyjs='cdn'))
    f.write(
        """ 
                        </div>
                    </div>
                    </body>
                </html>
        """
    )
