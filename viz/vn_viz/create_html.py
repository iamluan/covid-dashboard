from line import create_line_chart
from map import create_cases_map, create_deaths_map

line_chart = create_line_chart()
case_map = create_cases_map()
death_map = create_deaths_map()

with open('/home/luan/projects/covid_dashboard/viz/vn_viz/index.html', 'w') as f:
    f.write(
        """
            <html>
                <head>
                    <h1>
                        Vietnam Covid-19 dashboard
                    </h1>
                </head>
                <body>
                    <div class="container" style="display: flex; height: 600px;">
        """
    )
    f.write(case_map.to_html(full_html=False, include_plotlyjs='cdn'))
    f.write(death_map.to_html(full_html=False, include_plotlyjs='cdn'))
    f.write("</div>")
    f.write("<div>")
    f.write(line_chart.to_html(full_html=False, include_plotlyjs='cdn'))
    f.write(
        """
                    </div>
                </body>
            </html>
        """
    )
        
    
