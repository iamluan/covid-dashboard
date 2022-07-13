source /home/luan/projects/covid_dashboard/venv/bin/activate
python /home/luan/projects/covid_dashboard/viz/global_viz/create.py
python /home/luan/projects/covid_dashboard/viz/vn_viz/create_html.py

cp /home/luan/projects/covid_dashboard/viz/global_viz/index.html /home/luan/iamluan.github.io/covid_viz/global/index.html
cp /home/luan/projects/covid_dashboard/viz/vn_viz/index.html /home/luan/iamluan.github.io/covid_viz/vn/index.html