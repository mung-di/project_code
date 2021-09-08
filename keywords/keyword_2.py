import csv
from konlpy.tag import Mecab
from textrank import KeywordSummarizer
import os



list_number = 598770    #list_number = 청원 글번호
# file_list = os.listdir('D:\Project\project_code\keywords\data/')
# #print(file_list)
#
# #가장 최근에 저장 한 파일부터 시작하도록
# list_number = int(file_list[-1].strip('.csv'))
#

except_count = 0    #except_count = 예외 실행 횟수


while except_count <= 10:   #예외가 10번 실행될때까지 반복
    try:

        file = open('D:\Project\project_code\data/'+str(list_number)+'.csv', "r", encoding="utf-8-sig")
        data = csv.reader(file)
        text = next(data)[5]
        #print(text)

        mecab = Mecab(dicpath=r"C:\mecab\mecab-ko-dic")

        def mecab_tokenizer(sent):
            words = mecab.pos(sent, join=True)
            # words = [w
            #             for w in words
            #             if(len(w)>1)
            #                 if ('/NNP' in w or '/NNG' in w or '/SL' in w ) ]
            # return words
            word_list = []
            for w in words:
                if len(w) > 5:
                    if ('/NNP' in w or '/NNG' in w or '/SL' in w):
                        word_list.append(w)
            return word_list

        docs = text.replace("\r","")
        docs = docs.replace("\t","")
        docs = docs.replace("  ","")
        sents = list(set(docs.split("\n")))


        #키워드 뽑기
        summarizer = KeywordSummarizer(tokenize=mecab_tokenizer, min_count=2, min_cooccurrence=3)
        summarizer.summarize(sents, topk=10)

        print(summarizer.summarize(sents, topk=10))
        with open('D:\Project\project_code/test/keywords.csv', 'a', newline='') as kw:
            kw = csv.writer(kw)
            kw.writerow([list_number, summarizer.summarize(sents, topk=10)])

        list_number += 1

    except Exception as e:
        except_count += 1
        # print(list_number, '예외가 발생했습니다.', e, except_count)
        # with open('exception_list.csv', 'a', newline='') as el:
        #     el = csv.writer(el)
        #     el.writerow([list_number, e, except_count])
        list_number += 1
