import socket
import os
import time
import csv
import operator
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
import json
import pymysql
import pymysql.cursors
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
from konlpy.tag import Mecab
from urllib.request import urlopen
from bs4 import BeautifulSoup
from textrank import KeywordSummarizer
from collections import Counter
from sklearn.feature_extraction.text import CountVectorizer
from konlpy.tag import Okt
from tqdm import tqdm


def crawl():

    m = Mecab(dicpath=r"c:\mecab\mecab-ko-dic")

    list_number = 598770  # list_number = 청원 글번호
    file_list = os.listdir('C:\Python\Project/DB_crawling')  # file_list = 폴더 내 파일 리스트
    if len(file_list) >= 2:
        list_number = int(file_list[-2].strip('.csv'))

    except_count = 0  # except_count = 예외 실행 횟수
    # 헤더리스트 밑으로 크롤링 내용 저장 반복
    while except_count <= 10:  # 예외가 10번 실행될때까지 반복
        try:
            url = ('https://www1.president.go.kr/petitions/')  # url = 청원글 url
            file = urlopen(url + str(list_number))  # 글 불러오기

            bs = BeautifulSoup(file, 'html.parser')

            text = bs.findAll('div', {'class': 'View_write'})  # text = 글 내용
            title = bs.find('h3', {'class': 'petitionsView_title'}).text  # title = 글 제목
            start_date = bs.find('ul', {'class': 'petitionsView_info_list'}).findAll('li')[
                1].text  # start_date = 청원 시작일
            end_date = bs.find('ul', {'class': 'petitionsView_info_list'}).findAll('li')[2].text  # end_date = 청원 마감일
            cate = bs.find('ul', {'class': 'petitionsView_info_list'}).findAll('li')[0].text  # cate = 카테고리
            start_date = start_date[4:]
            end_date = end_date[4:]
            cate = cate[4:]

            # p = p에 청원 내용 주입
            p = ""
            for ele in text:
                p += ele.get_text()
            p.strip()

            # csv파일로 크롤링한 내용 저장
            with open('C:\Python\Project/DB_crawling/' + str(list_number) + '.csv', 'w', encoding='utf-8-sig', newline='') as f:
                w = csv.writer(f)
                # header_list = ['글번호', '제목', '청원시작일', '청원마감일', '카테고리', '내용']
                # w.writerow(header_list)
                w.writerow([list_number, title, start_date, end_date, cate, p])
                time.sleep(3)
            print(list_number)
            list_number += 1
            except_count = 0  # 예외가 발생하지 않을시 예외 횟수 초기화
        # 예외가 발생하면 except_count를 늘리고 발생 예외를 리스트로 저장
        except Exception as e:
            time.sleep(3)
            except_count += 1
            print(list_number, '예외가 발생했습니다.', e, except_count)
            with open('C:\Python\Project/DB_crawling/' + 'exception_list.csv', 'a', newline='') as el:
                el = csv.writer(el)
                el.writerow([list_number, e, except_count])
            list_number += 1

def saveDB():
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

    list_number = 598770  # list_number = 청원 글번호
    with connection.cursor() as cursor:
        aaa = cursor.execute(sql_6)
        _list_number = cursor.fetchone()
        list_number = _list_number["no"]
    except_count = 0  # except_count = 예외 실행 횟수
    # 헤더리스트 밑으로 크롤링 내용 저장 반복
    while except_count <= 10:  # 예외가 10번 실행될때까지 반복
        try:
            with connection.cursor() as cursor:
                # 청원글 자료(csv 파일)를 읽어오는 코드
                f = open('C:\Python\Project/DB_crawling/' + str(list_number) + '.csv', 'r', encoding='utf-8-sig',
                         newline='')
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
            except_count = 0  # 예외가 발생하지 않을시 예외 횟수 초기화
        # 예외가 발생하면 except_count를 늘리고 발생 예외를 리스트로 저장
        except Exception as e:
            time.sleep(1)
            except_count += 1
            print(list_number, '예외가 발생했습니다.', e, except_count)
            with open('C:\Python\Project/DB_crawling/' + 'exception_list.csv', 'a', newline='') as el:
                el = csv.writer(el)
                el.writerow([list_number, e, except_count])
            list_number += 1

def keyword_fi():
    connection = pymysql.connect(host='192.168.0.37',
                                 user='root',
                                 password='12345',
                                 db='testbase',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    list_number = 598770  # list_number = 청원 글 시작 번호

    sql_6 = "SELECT no FROM petitiondata order by no desc limit 1"
    with connection.cursor() as cursor:
        aaa = cursor.execute(sql_6)
        _list_number = cursor.fetchone()
        list_number = _list_number["no"]

    except_count = 0  # except_count = 예외 실행 횟수

    while except_count <= 10:  # 예외가 10번 실행될때까지 반복
        try:
            with connection.cursor() as cursor:
                no = list_number
                sql = "select no, subdate from petition where no = %s"
                cursor.execute(sql, (no))
                row = cursor.fetchone()
                # print(row)
                subdate = row['subdate']

                sql_2 = "select rawdata from petitionraw where no = %s"
                cursor.execute(sql_2, (no))
                row = cursor.fetchone()
                content = row['rawdata']

            # file = open('D:\Project\data/' + str(list_number) + '.csv', "r", encoding="utf-8-sig")
            # fi = open('D:\Project/test/dictest0914.csv', 'a', encoding="utf-8-sig")
            #         all_fi =open('D:\Project/test/all_words_test0910.csv', 'a', encoding="utf-8-sig")
            #         data = csv.reader(file)
            #         data = next(data)[2:]
            #         st_date = data[0]
            #         text = data[3]
            #         cate = data[2]

            mecab = Mecab(dicpath=r"C:\mecab\mecab-ko-dic")

            def mecab_tokenizer(sent):
                words = mecab.pos(sent, join=True)
                word_list = []
                for w in words:
                    # print(w, w.split('/')[0], len(w.split('/')[0]))
                    if len(w.split('/')[0]) > 1:  # 한글자 제거
                        if ('/NNP' in w or '/NNG' in w):
                            word_list.append(w)
                return word_list

            docs = content.replace("\r", "")
            docs = docs.replace("\t", "")
            docs = docs.replace("  ", "")
            sents = list(set(docs.split("\n")))

            # 키워드 뽑기
            summarizer = KeywordSummarizer(tokenize=mecab_tokenizer, min_count=2, min_cooccurrence=2)
            kws = summarizer.summarize(sents, topk=10)
            # 키워드저장
            kw = [kw for kw in kws]
            list_line = ""
            for k in range(len(kw)):
                list_line += kw[k][0].split('/')[0] + ","

            print(list_number, list_line)

            # fi.write(list_line + "\n")
            with connection.cursor() as cursor:
                cursor.execute("insert into PETITIONDATA values(%s, %s)", (list_number, list_line))
                connection.commit()

            list_number += 1
            except_count = 0

        except Exception as e:
            except_count += 1
            print(list_number, '예외가 발생했습니다.', e, except_count)
            # with open('exception_list.csv', 'a', newline='') as el:
            #     el = csv.writer(el)
            #     el.writerow([list_number, e, except_count])
            list_number += 1

def day_data():
    connection = pymysql.connect(host='192.168.0.37',
                                 user='root',
                                 password='12345',
                                 db='testbase',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    list_number = 598770  # list_number = 청원 글 시작 번호
    except_count = 0  # except_count = 예외 실행 횟수
    with connection.cursor() as cursor:
        sel_date = "SELECT subdate FROM daydata ORDER BY subdate DESC limit 1 "
        cursor.execute(sel_date)
        final_date = cursor.fetchone()
        final_date = str(final_date['subdate'])

        sql = "select no, subdate from petition where subdate = %s ORDER BY no DESC"
        cursor.execute(sql, (final_date))
        row = cursor.fetchone()
        list_number = row['no'] + 1

    last_date = ""
    word_list = ""
    while except_count <= 10:  # 예외가 10번 실행될때까지 반복
        try:
            with connection.cursor() as cursor:
                no = list_number
                sql = "select no, subdate from petition where no = %s"
                cursor.execute(sql, (no))
                row = cursor.fetchone()
                # print(row)
                subdate = row['subdate']

                sql_2 = "select worddata from PETITIONDATA where no = %s"
                cursor.execute(sql_2, (no))
                row = cursor.fetchone()
                worddata = row['worddata']
            # print(subdate,worddata)

            if last_date == subdate:
                word_list += worddata

            elif last_date != subdate:
                if word_list != "":
                    with connection.cursor() as cursor:
                        cursor.execute("insert into daydata values(%s, %s)", (last_date, word_list))
                        connection.commit()

                word_list = ""
                word_list += worddata
                # print(last_date == subdate)

            # print(word_list)
            list_number += 1
            except_count = 0

            last_date = subdate
        except Exception as e:
            except_count += 1
            print(list_number, '예외가 발생했습니다!!!!!', e, except_count)
            if except_count == 10:
                if word_list != "":
                    with connection.cursor() as cursor:
                        cursor.execute("insert into daydata values(%s, %s)", (last_date, word_list))
                        connection.commit()
            list_number += 1

def pie_graph():
    connection = pymysql.connect(host='192.168.0.37',
                                 user='root',
                                 password='12345',
                                 db='testbase',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    fig = go.Figure()
    name_list = []
    count_list = []
    pie_list = []

    try:
        with connection.cursor() as cursor:

            sql_circle = "select category.name, count(petition.no) from category inner join petition on category.code = petition.code where subdate = (select subdate from petition order by subdate desc limit 1) group by category.name"
            cursor.execute(sql_circle)
            circle = cursor.fetchall()
            for row in circle:
                row_list = list(row.values())
                print(row_list)
                name_list.append(row_list[0])
                count_list.append(row_list[1])
            pie_list.append(name_list)
            pie_list.append(count_list)

            df = pd.DataFrame(circle)
            df.rename(columns={'name': '카테고리', 'count(petition.no)': '글 수'}, inplace=True)
            fig = px.pie(df, values='글 수', names='카테고리')
            fig.update_traces(textposition='inside', textinfo='percent+label')

            # if req == 'pie':
            #     fig.update(layout_showlegend=True)
            #
            # elif req == 'main':
            #     fig.update(layout_showlegend=False)

            # pie_json = fig.to_json()

            # return pie_json
            return pie_list


    finally:
        connection.close()

def time_graph(req):
    connection = pymysql.connect(host='192.168.0.37',
                                 user='root',
                                 password='12345',
                                 db='testbase',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    except_count = 0
    graph_list = []
    last_date = date.today()
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

                if subdate.isocalendar()[1] >= final_data['subdate'].isocalendar()[1] - 4:  # 최근 4주치 그래프만 나타내기 위해

                    if final_data['subdate'].isocalendar()[1] == subdate.isocalendar()[1]:  # 이번주차 
                        graph_list.append(str(subdate.isocalendar()[1]) + '/time/' + draw_graph())

                    else:  # 이번주 제외 3주
                        if req == 'time_series':
                            if last_date.isocalendar()[1] == subdate.isocalendar()[1]:
                                word_list += words

                            elif last_date.isocalendar()[1] != subdate.isocalendar()[1]:
                                if word_list != "":
                                    graph_list.append(str(subdate.isocalendar()[1]) + '/time/' + draw_graph())

                        word_list = ""
                        word_list += words

                last_date = subdate
            print(graph_list)

            return graph_list

    finally:
        connection.close()

def top_10():
    connection = pymysql.connect(host='192.168.0.37',
                                 user='root',
                                 password='12345',
                                 db='testbase',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    except_count = 0
    last_date = date.today()
    word_list = ""
    this_top = []
    last_top = []
    tot_list = []
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

                if subdate.month >= final_data['subdate'].month - 1:  # 최근 2달치 그래프만 나타내기 위해

                    if final_data == row:
                        word = word_list.split(',')

                        # print(last_date.month, Counter(word))
                        my_dict = sorted(Counter(word).items(), key=lambda x: x[1], reverse=True)[:10]
                        # print(my_dict)
                        for k in range(10):
                            this_top.append(my_dict[k][0])
                        tot_list.append(this_top)
                    else:
                        if last_date.month == subdate.month:
                            word_list += words
                        elif last_date.month != subdate.month:
                            print(word_list)
                            if word_list != "":
                                word = word_list.split(',')
                                # print(last_date.month, Counter(word))
                                my_dict = sorted(Counter(word).items(), key=lambda x: x[1], reverse=True)[:10]
                                for k in range(10):
                                    last_top.append(my_dict[k][0])
                                print(last_top)
                                tot_list.append(last_top)

                word_list = ""
                word_list += words

                        # print(subdate.month,word_list)

                last_date = subdate

    finally:
        connection.close()
        return tot_list

def make_WPL():
    month = [6, 7, 8, 9]  # month: 월 구분을 위한 리스트
    category = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]

    # 디렉토리 생성 함수
    def createFolder(directory):
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print('Error: Creating directory. ' + directory)

    for m in tqdm(month, desc='월별 WPL 만들기'):
        # 데이터 중복을 막기 위한 초기화
        dataset = pd.DataFrame.empty
        # 단어쌍의 빈도를 체크하기위해 DTM을 불러온다.
        dataset = pd.read_csv('C:/Python/Project/networkx/CSV/2021-' + str(m) + '/DTM/DTM_2021-' + str(m) + '.csv')

        # 단어들의 목록을 가져온다.
        # 이때 0번째 인덱스에는 빈 칸이 들어오므로 인덱싱을 통해 없애준다.
        column_list = dataset.columns[1:]
        word_length = len(column_list)

        # 각 단어쌍의 빈도수를 저장할 dictionary 생성
        count_dict = {}

        for doc_number in tqdm(range(len(dataset)), desc='단어쌍 만들기 진행중'):
            tmp = dataset.loc[doc_number]  # 현재 문서의 단어 출현 빈도 데이터를 가져온다.
            for i, word1 in enumerate(column_list):
                if tmp[word1]:  # 현재 문서에 첫번째 단어가 존재할 경우
                    for j in range(i + 1, word_length):
                        if tmp[column_list[j]]:  # 현재 문서에 두번째 단어가 존재할 경우
                            count_dict[column_list[i], column_list[j]] = count_dict.get(
                                (column_list[i], column_list[j]), 0) + max(tmp[word1], tmp[column_list[j]])

        # count_list에 word1, word2, frequency 형태로 저장할 것이다.
        count_list = []

        for words in count_dict:
            count_list.append([words[0], words[1], count_dict[words]])

        # 단어쌍 동시 출현 빈도를 DataFrame 형식으로 만든다.
        df = pd.DataFrame(count_list, columns=["word1", "word2", "freq"])
        df = df.sort_values(by=['freq'], ascending=False)
        df = df.reset_index(drop=True)

        createFolder('C:/Python/Project/networkx/CSV/2021-' + str(m) + '/WPL')
        # 이 작업이 오래 걸리기 때문에 csv파일로 저장 후 사용
        df.to_csv('C:/Python/Project/networkx/CSV/2021-' + str(m) + '/WPL/WPL_2021-' + str(m) + '.csv',
                  encoding='utf-8-sig')

        for c in tqdm(category, desc='카테고리별 WPL 작업중'):
            # 각종 데이터 초기화
            dataset = pd.DataFrame.empty
            column_list = []

            # 단어쌍의 빈도를 체크하기위해 DTM을 불러온다.
            dataset = pd.read_csv(
                'C:/Python/Project/networkx/CSV/2021-' + str(m) + '/DTM/DTM_2021-' + str(m) + '_' + str(c) + '.csv')

            # 단어들의 목록을 가져온다.
            # 이때 0번째 인덱스에는 빈 칸이 들어오므로 인덱싱을 통해 없애준다.
            column_list = dataset.columns[1:]
            word_length = len(column_list)

            # 각 단어쌍의 빈도수를 저장할 dictionary 생성
            count_dict = {}

            for doc_number in tqdm(range(len(dataset)), desc='단어쌍 만들기 진행중'):
                tmp = dataset.loc[doc_number]  # 현재 문서의 단어 출현 빈도 데이터를 가져온다.
                for i, word1 in enumerate(column_list):
                    if tmp[word1]:  # 현재 문서에 첫번째 단어가 존재할 경우
                        for j in range(i + 1, word_length):
                            if tmp[column_list[j]]:  # 현재 문서에 두번째 단어가 존재할 경우
                                count_dict[column_list[i], column_list[j]] = count_dict.get(
                                    (column_list[i], column_list[j]), 0) + max(tmp[word1], tmp[column_list[j]])

            # count_list에 word1, word2, frequency 형태로 저장할 것이다.
            count_list = []

            for words in count_dict:
                count_list.append([words[0], words[1], count_dict[words]])

            # 단어쌍 동시 출현 빈도를 DataFrame 형식으로 만든다.
            df = pd.DataFrame(count_list, columns=["word1", "word2", "freq"])
            df = df.sort_values(by=['freq'], ascending=False)
            df = df.reset_index(drop=True)

            createFolder('C:/Python/Project/networkx/CSV/2021-' + str(m) + '/WPL')
            # 이 작업이 오래 걸리기 때문에 csv파일로 저장 후 사용
            df.to_csv(
                'C:/Python/Project/networkx/CSV/2021-' + str(m) + '/WPL/WPL_2021-' + str(m) + '_' + str(c) + '.csv',
                encoding='utf-8-sig')

def make_DTM():
    tagger = Okt()
    connection = pymysql.connect(host='192.168.0.37',
                                 user='root',
                                 password='12345',
                                 db='testbase',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    month = [6, 7, 8, 9]  # month: 월 구분을 위한 리스트
    category = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]

    # 디렉토리 생성 함수
    def createFolder(directory):
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print('Error: Creating directory. ' + directory)

    # 불용어 파일을 불러와서 stop_word_list 변수에 저장
    s_file_name = open('C:/Python/Project/networkx/StopWords.txt', 'r', encoding='utf-8')
    stop_words_list = []
    for line in s_file_name.readlines():
        # rstrip 함수를 사용하여 공백을 제거
        stop_words_list.append(line.rstrip())
    s_file_name.close()

    for i in tqdm(month, desc='월별 DTM 작업중'):
        createFolder('C:/Python/Project/networkx/CSV/2021-' + str(i))
        dataset = pd.DataFrame.empty  # petitiondata가 들어가는 데이터프레임 초기화
        # petitiondata 에서 csv파일로 저장
        with connection.cursor() as cursor:
            sql_1 = "SELECT no, worddata FROM petitiondata WHERE no IN (SELECT no FROM petition WHERE subdate < '2021-" + str(
                i + 1) + "-01' AND subdate >= '2021-" + str(i) + "-01');"
            cursor.execute(sql_1)
            df = pd.read_sql(sql_1, connection)
            df.to_csv('C:/Python/Project/networkx/CSV/2021-' + str(i) + '/petition_2021-' + str(i) + '.csv',
                      index=False, encoding='utf-8-sig')
            dataset = df
        target = ['worddata']
        cv = CountVectorizer()
        # 각 문서들의 말뭉치(corpus)를 저장할 리스트 선언
        corpus = []
        for doc_num in tqdm(range(len(dataset)), desc='문자 변환중'):
            noun_list = tagger.nouns(dataset[target[0]].loc[doc_num])
            corpus.append(' '.join(noun_list))
        DTM_Array = cv.fit_transform(corpus).toarray()
        header = cv.get_feature_names()

        dtm_set = []  # dtm_set: raw 데이터에서 단어 빈도 추출을 위한 리스트
        with connection.cursor() as cursor:
            sql_2 = "SELECT no, rawdata FROM petitionraw WHERE no IN (SELECT no FROM petition WHERE subdate < '2021-" + str(
                i + 1) + "-01' AND subdate >= '2021-" + str(i) + "-01');"
            cursor.execute(sql_2)
            df2 = pd.read_sql(sql_2, connection)
            rows = cursor.fetchall()
            for row in tqdm(rows, desc='문서 변환중'):
                no = row['no']
                raw_data = row['rawdata']
                _nouns = tagger.nouns(raw_data)
                nouns = []
                for n in _nouns:
                    if len(n) > 1:
                        nouns.append(n)
                my_dict = sorted(Counter(nouns).items(), key=lambda x: x[1], reverse=True)
                dtm_set.append(my_dict)

        # DTM 만들기 전 선언과 초기화
        dtm_list = []
        dtm_list = [[0 for col in range(len(header))] for row in range(len(dtm_set))]

        for row in tqdm(range(len(dtm_set)), desc='데이터 프레임 입력중'):
            for col in range(len(header)):
                for j in range(len(dtm_set[row])):
                    if dtm_set[row][j][0] == header[col]:  # dtm_set[k][i][0] : 단어 빈도의 단어 key 값   / header[j] : 헤더의 위치
                        dtm_list[row][col] = dtm_set[row][j][
                            1]  # dtm_list[k][j] : value 값을 입력할 위치 / dtm_set[k][i][1] : 입력할 value 값

        DTM_DataFrame = pd.DataFrame(dtm_list, columns=header)

        for h in tqdm(range(len(header)), desc='불용어 처리중'):
            for s in range(len(stop_words_list)):
                if header[h] == stop_words_list[s]:
                    DTM_DataFrame.drop(stop_words_list[s], axis='columns', inplace=True)
        createFolder('C:/Python/Project/networkx/CSV/2021-' + str(i) + '/DTM')
        DTM_DataFrame.to_csv('C:/Python/Project/networkx/CSV/2021-' + str(i) + '/DTM/DTM_2021-' + str(i) + '.csv',
                             encoding='utf-8-sig')

        # 다음 반복을 위한 초기화
        header = []
        my_dict = []

        ###################################################################################################################

        for c in tqdm(category, desc='카테고리별 DTM 작업중'):
            # 헤더 삽입
            header = cv.get_feature_names()
            dtm_set = []  # dtm_set 초기화
            with connection.cursor() as cursor:
                sql_3 = "SELECT no, rawdata FROM petitionraw WHERE no IN (SELECT no FROM petition WHERE (subdate < '2021-" + str(
                    i + 1) + "-01' AND subdate >= '2021-" + str(i) + "-01') AND code = " + str(c) + ");"
                cursor.execute(sql_3)
                df2 = pd.read_sql(sql_3, connection)
                rows = cursor.fetchall()
                for row in tqdm(rows, desc='문서 변환중'):
                    no = row['no']
                    raw_data = row['rawdata']
                    _nouns = tagger.nouns(raw_data)
                    nouns = []
                    for n in _nouns:
                        if len(n) > 1:
                            nouns.append(n)
                    my_dict = sorted(Counter(nouns).items(), key=lambda x: x[1], reverse=True)
                    dtm_set.append(my_dict)

            # DTM 만들기 전 선언과 초기화
            dtm_list = []
            dtm_list = [[0 for col in range(len(header))] for row in range(len(dtm_set))]

            for row in tqdm(range(len(dtm_set)), desc='데이터 프레임 입력중'):
                for col in range(len(header)):
                    for j in range(len(dtm_set[row])):
                        if dtm_set[row][j][0] == header[
                            col]:  # dtm_set[k][i][0] : 단어 빈도의 단어 key 값   / header[j] : 헤더의 위치
                            dtm_list[row][col] = dtm_set[row][j][
                                1]  # dtm_list[k][j] : value 값을 입력할 위치 / dtm_set[k][i][1] : 입력할 value 값

            DTM_DataFrame = pd.DataFrame(dtm_list, columns=header)

            # 불용어 처리, 데이터프레임 드랍
            for h in tqdm(range(len(header)), desc='불용어 처리중'):
                for s in range(len(stop_words_list)):
                    if header[h] == stop_words_list[s]:
                        DTM_DataFrame.drop(stop_words_list[s], axis='columns', inplace=True)
            DTM_DataFrame.to_csv(
                'C:/Python/Project/networkx/CSV/2021-' + str(i) + '/DTM/DTM_2021-' + str(i) + '_' + str(c) + '.csv',
                encoding='utf-8-sig')

            header = []
            dtm_set = pd.DataFrame.empty

def network(sel_month, sel_category):
    if __name__ == '__main__':

        # 단어쌍 동시출현 빈도수를 담았던 networkx.csv파일을 불러온다.
        if sel_category == '0':
            print('C:\python/project/networkx/CSV/2021-' + str(sel_month) + '/WPL/WPL_2021-' + str(sel_month) + '.csv')
            dataset = pd.read_csv(
                'C:\python/project/networkx/CSV/2021-' + str(sel_month) + '/WPL/WPL_2021-' + str(sel_month) + '.csv')
            setting = 500
        # else:
        #     print(
        #         'C:\python/project/networkx/CSV/2021-' + str(sel_month) + '/WPL/WPL_2021-' + str(sel_month) + '_' + str(
        #             sel_category) + '.csv')
        #     dataset = pd.read_csv(
        #         'C:\python/project/networkx/CSV/2021-' + str(sel_month) + '/WPL/WPL_2021-' + str(sel_month) + '_' + str(
        #             sel_category) + '.csv')
        #     setting = 80

        # 중심성 척도 계산을 위한 Graph를 만든다
        G_centrality = nx.Graph()

        # 빈도수가 20000 이상인 단어쌍에 대해서만 edge(간선)을 표현한다.
        for ind in range((len(np.where(dataset['freq'] >= setting)[0]))):
            G_centrality.add_edge(dataset['word1'][ind], dataset['word2'][ind], weight=int(dataset['freq'][ind]))

        dgr = nx.degree_centrality(G_centrality)  # 연결 중심성
        btw = nx.betweenness_centrality(G_centrality)  # 매개 중심성
        cls = nx.closeness_centrality(G_centrality)  # 근접 중심성
        egv = nx.eigenvector_centrality(G_centrality)  # 고유벡터 중심성
        pgr = nx.pagerank(G_centrality)  # 페이지 랭크

        # 중심성이 큰 순서대로 정렬한다.
        sorted_dgr = sorted(dgr.items(), key=operator.itemgetter(1), reverse=True)
        sorted_btw = sorted(btw.items(), key=operator.itemgetter(1), reverse=True)
        sorted_cls = sorted(cls.items(), key=operator.itemgetter(1), reverse=True)
        sorted_egv = sorted(egv.items(), key=operator.itemgetter(1), reverse=True)
        sorted_pgr = sorted(pgr.items(), key=operator.itemgetter(1), reverse=True)

        # 단어 네트워크를 그려줄 Graph 선언
        G = nx.Graph()

        # 페이지 랭크에 따라 두 노드 사이의 연관성을 결정한다. (단어쌍의 연관성)
        # 연결 중심성으로 계산한 척도에 따라 노드의 크기가 결정된다. (단어의 등장 빈도수)
        for i in range(len(sorted_pgr)):
            G.add_node(sorted_pgr[i][0], nodesize=sorted_dgr[i][1])

        for ind in range((len(np.where(dataset['freq'] > setting)[0]))):
            G.add_weighted_edges_from([(dataset['word1'][ind], dataset['word2'][ind], int(dataset['freq'][ind]))])

        # 노드 크기 조정
        _sizes = [G.nodes[node]['nodesize'] * setting for node in G]
        sizes = []
        for s in _sizes:
            sizes.append(np.log10(s) * 20)

        options = {
            'edge_color': '#FFDEA2',
            'width': 1,
            'with_labels': True,
            'font_weight': 'regular',
        }
        pos = nx.spring_layout(G, k=3.5, iterations=100)

        for node in G.nodes:
            G.nodes[node]['pos'] = list(pos[node])
            # nx.draw(G,node_size = sizes, pos =nx.spring_layout(G,k=3.5,iterations=100) )

        edge_x = []
        edge_y = []
        # print(G.edges)

        for edge in G.edges():
            x0, y0 = G.nodes[edge[0]]['pos']
            x1, y1 = G.nodes[edge[1]]['pos']
            edge_x.append(x0)
            edge_x.append(x1)
            edge_x.append(None)
            edge_y.append(y0)
            edge_y.append(y1)
            edge_y.append(None)

            edge_trace = go.Scatter(
                x=edge_x, y=edge_y,
                line=dict(width=0.5, color='#888'),
                hoverinfo='none',
                mode='lines')

            node_x = []
            node_y = []
            for node in G.nodes():
                x, y = G.nodes[node]['pos']
                node_x.append(x)
                node_y.append(y)

            node_trace = go.Scatter(
                x=node_x, y=node_y,
                mode='markers ,text',
                hoverinfo='text',
                marker=dict(
                    showscale=True,
                    # colorscale options
                    # 'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
                    # 'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
                    # 'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
                    colorscale='Blues',
                    reversescale=True,
                    color=[],
                    size=sizes,
                    colorbar=dict(
                        thickness=15,
                        title='Node Connections',
                        xanchor='left',
                        titleside='right'
                    ),
                    line_width=2))

            node_adjacencies = []
            node_text = []
            for node, adjacencies in enumerate(G.adjacency()):
                node_adjacencies.append(len(adjacencies[1]))

                # 노드 호버 할 때 나타나는 텍스트
                node_text.append(sorted_pgr[node][0])
            node_trace.marker.color = node_adjacencies
            node_trace.text = node_text

            fig = go.Figure(data=[edge_trace, node_trace],
                            layout=go.Layout(
                                showlegend=False,
                                hovermode='closest',
                                margin=dict(b=20, l=5, r=5, t=40),
                                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                            )
            # print(fig.to_json())

        fig.for_each_trace(lambda t: t.update(textfont_color='forestgreen', textposition='top center'))
        return fig.to_json()


HOST = '192.168.0.37'
PORT = 9999

print('Socket Server Activated')

while True:

    ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    ss.bind((HOST, PORT))

    ss.listen(3)

    cs, addr = ss.accept()

    print('Connected by', addr)
    print('Response for the Client Request')


    data = cs.recv(1024)
    request = data.decode()
    print(request)
    if request == 'data':
        print('청원 데이터 수집')
        crawl()
        print('수집 완료')

        print('DB 저장')
        saveDB()
        print('저장 완료')

        print('키워드 생성')
        keyword_fi()
        print('생성 완료')

        print('일일 데이터 생성')
        day_data()
        print('생성 완료')

        print('')
        make_DTM()
        print('')

        print('')
        make_WPL()
        print('')


    elif request == 'main':
        print(request + ' 데이터 생성')
        time_series = time_graph(request)[0]
        time_series += '/asdf/'
        pie_list = pie_graph()
        top_list = top_10()
        this_top = top_list[0]
        last_top = top_list[1]
        tt = ','.join(this_top)
        tt += '/asdf/'
        lt = ','.join(last_top)
        lt += '/asdf/'
        _net = network('9', '0')
        net = str(_net)
        net += '/asdf/'
        print(tt)
        print(net)
        print('생성 완료')
        cs.send(('pie:' + str(pie_list[0]) + '/asdf/' + 'pie:' + str(pie_list[1]) + '/asdf/').encode())
        cs.send(('time:' + time_series).encode())
        cs.send(('top:' + tt).encode())
        cs.send(('top:' + lt).encode())
        cs.send(('net:' + net).encode())

    elif request == 'time_series':
        print(request + ' 데이터 생성')
        json_array = time_graph(request)
        cs.send((json_array[0] + '/asdf/').encode())
        cs.send((json_array[1] + '/asdf/').encode())
        cs.send((json_array[2] + '/asdf/').encode())
        cs.send((json_array[3] + '/asdf/').encode())
        print('생성 완료')
        print('데이터 전송')

    elif request.startswith('network'):
        str_array = request.split('-')
        sel_month = str(str_array[1])
        sel_category = str(str_array[2])
        _net = network(sel_month, sel_category)
        net = str(_net)
        cs.send(net.encode())

    cs.close()
    ss.close()

    print('Disconnected', addr)






