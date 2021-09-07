from urllib.request import urlopen
from bs4 import BeautifulSoup
from krwordrank.word import KRWordRank

file = urlopen('https://www1.president.go.kr/petitions/598769')
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




wordrank_extractor = KRWordRank(min_count=5, max_length=10)

keywords, rank, graph = wordrank_extractor.extract(sents, num_keywords=10)


for word, r in sorted(keywords.items(), key=lambda x:x[1], reverse=True)[:30]:
        print('%8s:\t%.4f' % (word, r))



