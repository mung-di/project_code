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
        fig = px.pie(df, values='count(petition.no)', names='name')
        fig.show()




finally:
    connection.close()