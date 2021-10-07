from collections import Counter
import pymysql.cursors
from datetime import date

connection = pymysql.connect(host='192.168.0.37',
                             user='root',
                             password='12345',
                             db='testbase',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
except_count = 0
last_date = date.today()
word_list = ""
this_top =[]
last_top =[]
try:
    with connection.cursor() as cursor:
        sql = "select subdate,data from daydata"
        cursor.execute(sql)
        rows = cursor.fetchall()

        # daydata의 마지막 data를 가져오기 위해 subdate역순 정렬 제한 1줘서 가져옴
        sql1 = "select subdate,data from daydata order by subdate desc limit 1"
        cursor.execute(sql1)
        final_data = cursor.fetchone()

        for row in rows:
            words = row['data']
            subdate = row['subdate']
            # 최근 2달치 그래프만 나타내기 위해 month형식 이용해 분리
            if subdate.month >= final_data['subdate'].month - 1:

                if final_data == row:
                    word = word_list.split(',')

                    # print(last_date.month, Counter(word))
                    my_dict = sorted(Counter(word).items(), key=lambda x: x[1], reverse=True)[:10]
                    # print(my_dict)
                    for k in range(10):
                        this_top.append(my_dict[k][0])
                    print(this_top)

                else:
                    if last_date.month == subdate.month:
                        word_list += words
                    elif last_date.month != subdate.month:
                        if word_list != "":
                            word = word_list.split(',')

                            # print(last_date.month, Counter(word))
                            my_dict = sorted(Counter(word).items(), key=lambda x: x[1], reverse=True)[:10]
                            for k in range(10) :
                                last_top.append(my_dict[k][0])
                            print(last_top)





                    word_list = ""
                    word_list += words

                    # print(subdate.month,word_list)


                last_date = subdate

finally:
    connection.close()