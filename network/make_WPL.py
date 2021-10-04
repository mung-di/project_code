# -*- encoding: utf-8 -*-
import pandas as pd
from tqdm import tqdm
import os

month = [6,7,8,9]    #month: 월 구분을 위한 리스트
category = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]

#디렉토리 생성 함수
def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)

for m in tqdm(month, desc='월별 WPL 만들기'):
    #데이터 중복을 막기 위한 초기화
    dataset = pd.DataFrame.empty
    # 단어쌍의 빈도를 체크하기위해 DTM을 불러온다.
    dataset = pd.read_csv('D:/Python/Project/networkx/CSV/2021-'+str(m)+'/DTM/DTM_2021-'+str(m)+'.csv')

    # 단어들의 목록을 가져온다.
    # 이때 0번째 인덱스에는 빈 칸이 들어오므로 인덱싱을 통해 없애준다.
    column_list = dataset.columns[1:]
    word_length = len(column_list)

    # 각 단어쌍의 빈도수를 저장할 dictionary 생성
    count_dict = {}

    for doc_number in tqdm(range(len(dataset)), desc='단어쌍 만들기 진행중'):
        tmp = dataset.loc[doc_number]           # 현재 문서의 단어 출현 빈도 데이터를 가져온다.
        for i, word1 in enumerate(column_list):
            if tmp[word1]:              # 현재 문서에 첫번째 단어가 존재할 경우
                for j in range(i + 1, word_length):
                    if tmp[column_list[j]]:              # 현재 문서에 두번째 단어가 존재할 경우
                        count_dict[column_list[i], column_list[j]] = count_dict.get((column_list[i], column_list[j]), 0) + max(tmp[word1], tmp[column_list[j]])

    # count_list에 word1, word2, frequency 형태로 저장할 것이다.
    count_list = []

    for words in count_dict:
        count_list.append([words[0], words[1], count_dict[words]])

    # 단어쌍 동시 출현 빈도를 DataFrame 형식으로 만든다.
    df = pd.DataFrame(count_list, columns=["word1", "word2", "freq"])
    df = df.sort_values(by=['freq'], ascending=False)
    df = df.reset_index(drop=True)

    createFolder('D:/Python/Project/networkx/CSV/2021-'+str(m)+'/WPL')
    # 이 작업이 오래 걸리기 때문에 csv파일로 저장 후 사용
    df.to_csv('D:/Python/Project/networkx/CSV/2021-'+str(m)+'/WPL/WPL_2021-'+str(m)+'.csv', encoding='utf-8-sig')

    for c in tqdm(category, desc='카테고리별 WPL 작업중'):
        # 각종 데이터 초기화
        dataset = pd.DataFrame.empty
        column_list = []

        # 단어쌍의 빈도를 체크하기위해 DTM을 불러온다.
        dataset = pd.read_csv('D:/Python/Project/networkx/CSV/2021-'+str(m)+'/DTM/DTM_2021-'+str(m)+'_'+str(c)+'.csv')

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

        createFolder('D:/Python/Project/networkx/CSV/2021-'+str(m)+'/WPL')
        # 이 작업이 오래 걸리기 때문에 csv파일로 저장 후 사용
        df.to_csv('D:/Python/Project/networkx/CSV/2021-'+str(m)+'/WPL/WPL_2021-'+str(m)+'_'+str(c)+'.csv',
                  encoding='utf-8-sig')