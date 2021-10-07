from konlpy.tag import Mecab
from textrank import KeywordSummarizer
import pymysql.cursors

connection = pymysql.connect(host='192.168.0.37',
                             user='root',
                             password='12345',
                             db='testbase',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


# list_number = 청원 글 시작 번호 , 기준 : 21.06.01 첫번째 글
list_number = 598770
# DB에 저장된 마지막 데이터 이후로 데이터 저장하기위함
fin_no = "SELECT no FROM PETITIONDATA order by no desc limit 1"
with connection.cursor() as cursor:
    aaa = cursor.execute(fin_no)
    _list_number = cursor.fetchone()
    list_number = _list_number["no"]

except_count = 0  # except_count = 예외 실행 횟수


while except_count <= 10:  # 예외가 10번 실행될때까지 반복
    try:
        with connection.cursor() as cursor:
            no = list_number
            #DB의 petition 테이블에서 no, subdate를 글번호에 따라 가져옴
            sql = "select no, subdate from petition where no = %s"
            cursor.execute(sql, (no))
            row = cursor.fetchone()
            #print(row)
            subdate = row['subdate']
            #DB의 petitionraw테이블(크롤링한 본문이 들어있는 테이블) 에서 rawdata(정제되지않은 본문)을 글번호에 따라 가져옴
            sql_2 = "select rawdata from petitionraw where no = %s"
            cursor.execute(sql_2, (no))
            row = cursor.fetchone()
            content = row['rawdata']


        mecab = Mecab(dicpath=r"C:\mecab\mecab-ko-dic")
        #textrank를 사용하기위해 mecab_tokenizer선언, 불필요한 한글자 제거 및 고유명사와 일반명사만 추출함
        def mecab_tokenizer(sent):
            words = mecab.pos(sent, join=True)
            word_list = []
            for w in words:
                if len(w.split('/')[0]) > 1:  # 한글자 제거
                    if ('/NNP' in w or '/NNG' in w):
                        word_list.append(w)
            return word_list

        #정제되지 않은 원문을 불필요한 특수문자 제거 및 줄바꿈(\n)을 기준으로 list에 넣어줌.
        docs = content.replace("\r", "")
        docs = docs.replace("\t", "")
        docs = docs.replace("  ", "")
        sents = list(set(docs.split("\n")))

        # 키워드 뽑기 textrank를 활용 min_count :최소 등장 횟수 ,min_cooccurrence = 최소 연관성) , 상위 10개만 추출함
        summarizer = KeywordSummarizer(tokenize=mecab_tokenizer, min_count=2, min_cooccurrence=2)
        kws = summarizer.summarize(sents, topk=10)
        # 키워드저장 , textrank 사용 시 ('단어/nnp')의 형식으로 출력되기에 /이후의 값 제거하고 후에 데이터 사용 시 구분을 위해 ',' 삽입
        kw = [kw for kw in kws]
        list_line = ""
        for k in range(len(kw)):
            list_line += kw[k][0].split('/')[0] + ","


        # DB의 petitiondata에 글번호와, 추출한 키워드를 넣음
        with connection.cursor() as cursor:
            cursor.execute("insert into PETITIONDATA values(%s, %s)", (list_number, list_line))
            connection.commit()

        list_number += 1
        except_count = 0

    except Exception as e:
        except_count += 1
        print(list_number, '예외가 발생했습니다.', e, except_count)
        list_number += 1