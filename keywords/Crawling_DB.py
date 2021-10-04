import csv
import time
import pymysql

# DB에 연결하는 코드
connection = pymysql.connect(host='192.168.0.37',
                             user='root',
                             password='12345',
                             db='testbase',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

# Create a new record
sql = "INSERT INTO PETITION (NO, CODE, TITLE, SUBDATE) VALUES (%s, %s, %s, %s)"
sql_2 = "SELECT CODE FROM CATEGORY WHERE NAME = (%s)"
sql_3 = "SELECT MAX(CODE) AS CODE FROM CATEGORY"
sql_4 = "INSERT INTO CATEGORY VALUES (%s, %s)"
sql_5 = "INSERT INTO PETITIONRAW VALUES (%s, %s)"
sql_6 = "SELECT no FROM PETITION order by no desc limit 1"

list_number = 598770    #list_number = 청원 글번호
with connection.cursor() as cursor:
    aaa = cursor.execute(sql_6)
    _list_number = cursor.fetchone()
    list_number = _list_number["no"]
except_count = 0    #except_count = 예외 실행 횟수
#헤더리스트 밑으로 크롤링 내용 저장 반복
while except_count <= 10:   #예외가 10번 실행될때까지 반복
    try:
        with connection.cursor() as cursor:
            # 청원글 자료(csv 파일)를 읽어오는 코드
            f = open('D:\Python\Project/DB_crawling/'+ str(list_number) + '.csv', 'r', encoding='utf-8-sig', newline='')
            rdr = csv.reader(f)


            # 한 개의 csv 파일(1개의 청원글)의 데이터를 받아오는 코드
            for line in rdr:
                no = line[0]
                title = line[1]
                subdate = line[2]
                name = line[4]
                word_data = line[5]

            # 카테고리 이름이 존재하지 않는 경우 DB에 등록 후 카테고리 코드 생성
            if cursor.execute(sql_2, (name)) == 0:
                cursor.execute(sql_3)
                result = cursor.fetchone()
                code = result["CODE"]
                if code == None:
                    code = 1
                else:
                    code += 1
                cursor.execute(sql_4, (code, name))
                connection.commit()

            # 카테고리 코드를 받아오는 코드
            cursor.execute(sql_2, (name))
            row = cursor.fetchone()
            code = row['CODE']

            # DB에 청원글 데이터 저장
            cursor.execute(sql, (no, code, title, subdate))
            connection.commit()
            
            # DB에 원본 데이터 저장
            cursor.execute(sql_5, (no, word_data))
            connection.commit()

        print(list_number)
        list_number += 1
        except_count = 0    #예외가 발생하지 않을시 예외 횟수 초기화
    #예외가 발생하면 except_count를 늘리고 발생 예외를 리스트로 저장
    except Exception as e:
        time.sleep(1)
        except_count += 1
        print(list_number, '예외가 발생했습니다.', e, except_count)
        with open('exception_list.csv', 'a', newline='') as el:
            el = csv.writer(el)
            el.writerow([list_number, e, except_count])
        list_number += 1