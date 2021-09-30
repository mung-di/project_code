from collections import Counter
import pymysql.cursors
from datetime import date
import plotly.graph_objects as go


connection = pymysql.connect(host='192.168.0.37',
                             user='root',
                             password='12345',
                             db='testbase',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
except_count = 0
count=0
last_date  = date.today()
word_list = ""


fig = go.Figure()

def draw_graph():
    word = word_list.split(',')
    my_dict = sorted(Counter(word).items(), key=lambda x: x[1], reverse=True)[:15]
    my_dict.reverse()  # 그래프 값이 큰 순서대로 넣기
    x, y = zip(*my_dict)

    fig = go.Figure(go.Bar(x=y, y=x, orientation='h'))
    graph = fig.to_json()
    return graph

try:
    with connection.cursor() as cursor:
        sql = "select subdate,data from daydata"
        cursor.execute(sql)
        rows = cursor.fetchall()

        sql1 = "select subdate,data from daydata order by subdate desc limit 1"
        cursor.execute(sql1)
        final_data = cursor.fetchone()

        for row in rows:
            words = row['data']
            subdate = row['subdate']

            if subdate.isocalendar()[1] >= final_data['subdate'].isocalendar()[1] - 4 : # 최근 4주치 그래프만 나타내기 위해

                if final_data == row: #이번주차
                    draw_graph()

                else: #이번주 제외 3주
                    if last_date.isocalendar()[1] == subdate.isocalendar()[1]:
                        word_list += words

                    elif last_date.isocalendar()[1] != subdate.isocalendar()[1]:
                        if word_list != "":
                            draw_graph()


                    word_list = ""
                    word_list += words

            last_date = subdate


finally:
    connection.close()