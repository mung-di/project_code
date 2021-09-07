from urllib.request import urlopen
from bs4 import BeautifulSoup
from konlpy.tag import Mecab
from textrank import KeywordSummarizer


mecab = Mecab(dicpath=r"C:\mecab\mecab-ko-dic")

def mecab_tokenizer(sent):
    words = mecab.pos(sent, join=True)
    words = [w
                for w in words
                if ('/NNP' in w or '/NNG' in w or '/SL' in w ) ]
    return words

file = urlopen('https://www1.president.go.kr/petitions/598772')
bs = BeautifulSoup(file, 'html.parser')

text = bs.findAll('div',{'class':'View_write'})



doc = ""

for ele in text:
    doc += ele.get_text()

#공백제거
docs = doc.replace("\r","")
docs = docs.replace("\t","")
docs = docs.replace("  ","")
sents = list(set(docs.split("\n")))


#키워드 뽑기
summarizer = KeywordSummarizer(tokenize=mecab_tokenizer, min_count=2, min_cooccurrence=2 )
#Co-occurrence 는 문장 내에서 두 단어의 간격이 window 인 횟수 , 2~8추천
summarizer.summarize(sents, topk=10)

print(summarizer.keywords())
print(summarizer.idx_to_vocab)






