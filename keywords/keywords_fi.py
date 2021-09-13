import csv
from konlpy.tag import Mecab
from textrank import KeywordSummarizer
import os

list_number = 598770  # list_number = 청원 글 시작 번호
file_list = os.listdir('D:\Project\data/')

# list_number = int(file_list[-1].strip('.csv')) #저장된 마지막 글 불러오기 (마지막글부터 새 데이터 받아오기 위함)

except_count = 0  # except_count = 예외 실행 횟수

while except_count <= 30:  # 예외가 10번 실행될때까지 반복
    try:

        file = open('D:\Project\data/' + str(list_number) + '.csv', "r", encoding="utf-8-sig")
        fi = open('D:\Project/test/dictest0913.csv', 'a', encoding="utf-8-sig")
#         all_fi =open('D:\Project/test/all_words_test0910.csv', 'a', encoding="utf-8-sig")
        data = csv.reader(file)
        data = next(data)[2:]
        st_date = data[0]
        text = data[3]
        cate = data[2]

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


        docs = text.replace("\r", "")
        docs = docs.replace("\t", "")
        docs = docs.replace("  ", "")
        sents = list(set(docs.split("\n")))

        #형태소 (nnp,nng별 단어 모두 뽑아내기) ,30개만 뽑힘 쓸 필요 없을수도
        # all_words = KeywordSummarizer(tokenize=mecab_tokenizer, min_count=1, min_cooccurrence=-1)
        #
        # aws = all_words.summarize(sents)
        # aw = [aw for aw in aws]
        #
        # aw_line = str(list_number) + "," + st_date + "," + cate + ","  # 글번호, 날짜, 카테고리
        # for a in range(len(aw)):
        #     aw_line += aw[a][0].split('/')[0] + ","
        # all_fi.write(aw_line+"\n")

        # 키워드 뽑기
        summarizer = KeywordSummarizer(tokenize=mecab_tokenizer, min_count=1, min_cooccurrence=2)
        kws = summarizer.summarize(sents, topk=10)
        # 키워드저장
        kw = [kw for kw in kws]
        list_line = str(list_number) + "," + st_date + "," + cate      # 글번호, 날짜, 카테고리

        for k in range(len(kw)):
            list_line += "," + kw[k][0].split('/')[0]

        print(list_line)
        fi.write(list_line + "\n")

        list_number += 1

    except Exception as e:
        except_count += 1
        print(list_number, '예외가 발생했습니다.', e, except_count)
        # with open('exception_list.csv', 'a', newline='') as el:
        #     el = csv.writer(el)
        #     el.writerow([list_number, e, except_count])
        list_number += 1
