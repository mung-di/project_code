from collections import Counter
import pymysql.cursors
from datetime import date
import matplotlib.pyplot as plt

connection = pymysql.connect(host='192.168.0.37',
                             user='root',
                             password='12345',
                             db='testbase',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
except_count = 0
last_date  = date.today()
word_list = ""


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

            if final_data == row:
                word = word_list.split(',')
                print(word)
                print(last_date.isocalendar()[0],last_date.isocalendar()[1], Counter(word))

                # dt = subdate
                # print(dt)
                # iso_data = dt.isocalendar()
                # print(iso_data)
                # print(iso_data[0])  # year
                # print(iso_data[1])  # week number
                # print(iso_data[2])  # week day number

            else:
                if last_date.isocalendar()[1] == subdate.isocalendar()[1]:
                    word_list += words

                elif last_date.isocalendar()[1] != subdate.isocalendar()[1]:
                    if word_list != "":
                        word = word_list.split(',')
                        #print(word)
                        print(last_date.isocalendar()[0] ,last_date.isocalendar()[1], Counter(word))
                        my_dict = sorted(Counter(word).items(), key=lambda x: x[1], reverse=True)[:15]
                        x,y = zip(*my_dict)
                        plt.rcParams['font.family'] = 'Malgun Gothic'
                        plt.gca().invert_yaxis()
                        plt.barh(x, y, color='#0670D9')

                        plt.savefig('D:\Project\image'+ str(last_date.isocalendar()[0]) + "-" + str(last_date.isocalendar()[1]) + '.png')
                        plt.close()
                        # key = [k for k , v in Counter(word).items() if v >1]
                        # print(key)
                        # value = [ v for v , k in key ]


                        # plt.plot[Counter(word).keys(),Counter(word).values()]


                    word_list = ""
                    word_list += words

                    # print(subdate.month,word_list)


                last_date = subdate


finally:
    connection.close()