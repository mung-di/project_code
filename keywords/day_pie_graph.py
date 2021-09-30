import pymysql.cursors
from datetime import date
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

connection = pymysql.connect(host='192.168.0.37',
                             user='root',
                             password='12345',
                             db='testbase',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

fig = go.Figure()

try:
    with connection.cursor() as cursor:

        sql_circle = "select category.name, count(petition.no) from category inner join petition on category.code = petition.code where subdate = (select subdate from petition order by subdate desc limit 1) group by category.name"
        cursor.execute(sql_circle)
        circle=cursor.fetchall()


        df = pd.DataFrame(circle)
        df.rename(columns={'name' : '카테고리', 'count(petition.no)' : '글 수'}, inplace=True)
        fig = px.pie(df, values='글 수', names='카테고리')
        fig.update_traces(textposition='inside' , textinfo='percent+label')
        #상세페이지용 범례있는 그래프
        pie_json = fig.to_json()
        fig.show()


        fig.update(layout_showlegend=False)
        #메인용 범례없는 그래프
        main_pie = fig.to_json()
        fig.show()




finally:
    connection.close()