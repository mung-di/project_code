from konlpy.tag import Mecab
from textrank import KeywordSummarizer
import pymysql.cursors

connection = pymysql.connect(host='192.168.0.37',
                             user='root',
                             password='12345',
                             db='testbase',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)



list_number = 598770  # list_number = 청원 글 시작 번호

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

            sql = "select no, subdate from petition where no = %s"
            cursor.execute(sql, (no))
            row = cursor.fetchone()
            #print(row)
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
                #print(w, w.split('/')[0], len(w.split('/')[0]))
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