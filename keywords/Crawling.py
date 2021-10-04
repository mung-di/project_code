from konlpy.tag import Mecab
from urllib.request import urlopen
from bs4 import BeautifulSoup
import csv
import time
import os

m = Mecab(dicpath=r"c:\mecab\mecab-ko-dic")

list_number = 598770    #list_number = 청원 글번호
file_list = os.listdir('D:\Python\Project/temp_crawling')   #file_list = 폴더 내 파일 리스트
if len(file_list) >=4:
    list_number = int(file_list[-4].strip('.csv'))


except_count = 0    #except_count = 예외 실행 횟수
#헤더리스트 밑으로 크롤링 내용 저장 반복
while except_count <= 10:   #예외가 10번 실행될때까지 반복
    try:
        url = ('https://www1.president.go.kr/petitions/')   #url = 청원글 url
        file = urlopen(url+str(list_number)) #글 불러오기

        bs = BeautifulSoup(file, 'html.parser')

        text = bs.findAll('div', {'class': 'View_write'})    #text = 글 내용
        title = bs.find('h3', {'class': 'petitionsView_title'}).text    #title = 글 제목
        start_date = bs.find('ul', {'class': 'petitionsView_info_list'}).findAll('li')[1].text    #start_date = 청원 시작일
        end_date = bs.find('ul', {'class': 'petitionsView_info_list'}).findAll('li')[2].text    #end_date = 청원 마감일
        cate = bs.find('ul', {'class': 'petitionsView_info_list'}).findAll('li')[0].text        #cate = 카테고리
        start_date = start_date[4:]
        end_date = end_date[4:]
        cate = cate[4:]

        #p = p에 청원 내용 주입
        p = ""
        for ele in text:
            p += ele.get_text()
        p.strip()

        #csv파일로 크롤링한 내용 저장
        with open(str(list_number)+'.csv', 'w', encoding='utf-8-sig', newline='') as f:
            w = csv.writer(f)
            # header_list = ['글번호', '제목', '청원시작일', '청원마감일', '카테고리', '내용']
            # w.writerow(header_list)
            w.writerow([list_number, title, start_date, end_date, cate, p])
            time.sleep(3)
        print(list_number)
        list_number += 1
        except_count = 0    #예외가 발생하지 않을시 예외 횟수 초기화
    #예외가 발생하면 except_count를 늘리고 발생 예외를 리스트로 저장
    except Exception as e:
        time.sleep(3)
        except_count += 1
        print(list_number, '예외가 발생했습니다.', e, except_count)
        with open('exception_list.csv', 'a', newline='') as el:
            el = csv.writer(el)
            el.writerow([list_number, e, except_count])
        list_number += 1