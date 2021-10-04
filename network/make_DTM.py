import pandas as pd
from collections import Counter
import pymysql.cursors
from sklearn.feature_extraction.text import CountVectorizer
from konlpy.tag import Okt
from tqdm import tqdm
import os
import csv

tagger = Okt()
connection = pymysql.connect(host='192.168.0.37',
                             user='root',
                             password='12345',
                             db='testbase',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

month = [6,7,8,9]    #month: 월 구분을 위한 리스트
category = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]


#디렉토리 생성 함수
def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)

#불용어 파일을 불러와서 stop_word_list 변수에 저장
s_file_name = open('D:/Python/Project/networkx/StopWords.txt', 'r', encoding='utf-8')
stop_words_list = []
for line in s_file_name.readlines():
    # rstrip 함수를 사용하여 공백을 제거
    stop_words_list.append(line.rstrip())
s_file_name.close()

for i in tqdm(month, desc='월별 DTM 작업중'):
    createFolder('D:/Python/Project/networkx/CSV/2021-'+str(i))
    dataset = pd.DataFrame.empty    #petitiondata가 들어가는 데이터프레임 초기화
    # petitiondata 에서 csv파일로 저장
    with connection.cursor() as cursor:
        sql_1 = "SELECT no, worddata FROM petitiondata WHERE no IN (SELECT no FROM petition WHERE subdate < '2021-"+str(i+1)+"-01' AND subdate >= '2021-"+str(i)+"-01');"
        cursor.execute(sql_1)
        df = pd.read_sql(sql_1, connection)
        df.to_csv('D:/Python/Project/networkx/CSV/2021-'+str(i)+'/petition_2021-'+str(i)+'.csv', index=False, encoding='utf-8-sig')
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
        sql_2 = "SELECT no, rawdata FROM petitionraw WHERE no IN (SELECT no FROM petition WHERE subdate < '2021-"+str(i+1)+"-01' AND subdate >= '2021-"+str(i)+"-01');"
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
    
    #DTM 만들기 전 선언과 초기화
    dtm_list = []
    dtm_list = [[0 for col in range(len(header))] for row in range(len(dtm_set))]

    for row in tqdm(range(len(dtm_set)), desc='데이터 프레임 입력중'):
        for col in range(len(header)):
            for j in range(len(dtm_set[row])):
                if dtm_set[row][j][0] == header[col]:  # dtm_set[k][i][0] : 단어 빈도의 단어 key 값   / header[j] : 헤더의 위치
                    dtm_list[row][col] = dtm_set[row][j][1]  # dtm_list[k][j] : value 값을 입력할 위치 / dtm_set[k][i][1] : 입력할 value 값

    DTM_DataFrame = pd.DataFrame(dtm_list, columns=header)

    for h in tqdm(range(len(header)), desc='불용어 처리중'):
        for s in range(len(stop_words_list)):
            if header[h] == stop_words_list[s]:
                DTM_DataFrame.drop(stop_words_list[s], axis='columns', inplace=True)
    createFolder('D:/Python/Project/networkx/CSV/2021-'+str(i)+'/DTM')
    DTM_DataFrame.to_csv('D:/Python/Project/networkx/CSV/2021-'+str(i)+'/DTM/DTM_2021-'+str(i)+'.csv', encoding='utf-8-sig')
    
    #다음 반복을 위한 초기화
    header = []
    my_dict = []
    

    ###################################################################################################################

    for c in tqdm(category, desc='카테고리별 DTM 작업중'):
        #헤더 삽입
        header = cv.get_feature_names()
        dtm_set = []    #dtm_set 초기화
        with connection.cursor() as cursor:
            sql_3 = "SELECT no, rawdata FROM petitionraw WHERE no IN (SELECT no FROM petition WHERE (subdate < '2021-" + str(
                i + 1) + "-01' AND subdate >= '2021-" + str(i) + "-01') AND code = "+str(c)+");"
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
                    if dtm_set[row][j][0] == header[col]:  # dtm_set[k][i][0] : 단어 빈도의 단어 key 값   / header[j] : 헤더의 위치
                        dtm_list[row][col] = dtm_set[row][j][1]  # dtm_list[k][j] : value 값을 입력할 위치 / dtm_set[k][i][1] : 입력할 value 값

        DTM_DataFrame = pd.DataFrame(dtm_list, columns=header)
        
        #불용어 처리, 데이터프레임 드랍
        for h in tqdm(range(len(header)), desc='불용어 처리중'):
            for s in range(len(stop_words_list)):
                if header[h] == stop_words_list[s]:
                    DTM_DataFrame.drop(stop_words_list[s], axis='columns', inplace=True)
        DTM_DataFrame.to_csv('D:/Python/Project/networkx/CSV/2021-' + str(i) + '/DTM/DTM_2021-' + str(i) + '_'+str(c)+'.csv',
                             encoding='utf-8-sig')

        header = []
        dtm_set = pd.DataFrame.empty
