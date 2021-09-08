import csv
from konlpy.tag import Mecab
from textrank import KeywordSummarizer

list_num =598770
for i in range(100):
    f = open('D:\Project\project_code\keywords\data/'+str(list_num)+'.csv', "r", encoding="utf-8-sig")
    data = csv.reader(f)
    text = next(data)[5]
    #print(text)

    mecab = Mecab(dicpath=r"C:\mecab\mecab-ko-dic")

    def mecab_tokenizer(sent):
        words = mecab.pos(sent, join=True)
        # words = [w
        #             for w in words
        #             if ('/NNP' in w or '/NNG' in w or '/SL' in w ) ]
        # return words
        word_list = []
        for w in words:
        #    print(w, w.split('/')[0], len(w.split('/')[0]))
            if len(w.split('/')[0]) > 1: #한글자 제거
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

    list_num +=1







