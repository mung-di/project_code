import datetime
import pymysql.cursors


connection = pymysql.connect(host='192.168.0.37',
                             user='root',
                             password='12345',
                             db='testbase',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


list_number = 598770  # list_number = 청원 글 시작 번호
except_count = 0  # except_count = 예외 실행 횟수

last_date=""
word_list=""
while except_count <= 10:  # 예외가 10번 실행될때까지 반복
    try:
        with connection.cursor() as cursor:
            no = list_number
            sql = "select no, subdate from petition where no = %s"
            cursor.execute(sql, (no))
            row = cursor.fetchone()
            #print(row)
            subdate = row['subdate']

            sql_2 = "select worddata from PETITIONDATA where no = %s"
            cursor.execute(sql_2, (no))
            row = cursor.fetchone()
            worddata = row['worddata']
        #print(subdate,worddata)



        if last_date == subdate:
            word_list += worddata

        elif last_date != subdate:
            if word_list !="":
                with connection.cursor() as cursor:
                    cursor.execute("insert into daydata values(%s, %s)", (last_date , word_list))
                    connection.commit()


            word_list = ""
            word_list += worddata
            #print(last_date == subdate)



        #print(word_list)
        list_number += 1
        except_count = 0

        last_date = subdate
    except Exception as e:
        except_count += 1
        print(list_number, '예외가 발생했습니다!!!!!', e, except_count)
        if except_count == 10 :
            if word_list != "":
                with connection.cursor() as cursor:
                    cursor.execute("insert into daydata values(%s, %s)", (last_date, word_list))
                    connection.commit()
        list_number += 1