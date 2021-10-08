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
graph_list =[]
last_date  = date.today()
word_list = ""


fig = go.Figure()

#시계열 그래프 그리는 함수 정의, 기간내의 단어의 빈도수를 분석해 상위 15개의 단어만 막대형 그래프로 출력함
def draw_graph():
    word = word_list.split(',')
    my_dict = sorted(Counter(word).items(), key=lambda x: x[1], reverse=True)[:15]
    my_dict.reverse()  # 그래프 값이 큰 순서대로 넣기위해 reverse
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
            # 최근 4주치 그래프만 나타내기 위해 isocalendar(0)[1] 사용해 조건 줌 . isocalendar : 1년을 52주로 나타낸 데이터 0:연도 1:주차 2:요일
            if subdate.isocalendar()[1] >= final_data['subdate'].isocalendar()[1] - 4 :

                if final_data.isocalendar()[1] == row.isocalendar()[1]: #이번주차
                    graph_list.append(draw_graph())

                else: #이번주 제외 3주
                    if last_date.isocalendar()[1] == subdate.isocalendar()[1]:
                        word_list += words

                    elif last_date.isocalendar()[1] != subdate.isocalendar()[1]:
                        if word_list != "":
                            graph_list.append(draw_graph())



                    word_list = ""
                    word_list += words

            last_date = subdate
        print(graph_list)

finally:
    connection.close()