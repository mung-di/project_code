import csv
from krwordrank.word import KRWordRank

list_num =598770
for i in range(100):
    f = open('D:\Project\project_code\keywords\data/'+str(list_num)+'.csv', "r", encoding="utf-8-sig")
    data = csv.reader(f)
    doc = next(data)[5]

    #공백제거
    docs = doc.replace("\r","")
    docs = docs.replace("\t","")
    docs = docs.replace("  ","")
    sents = list(set(docs.split("\n")))




    wordrank_extractor = KRWordRank(min_count=5, max_length=10)

    keywords, rank, graph = wordrank_extractor.extract(sents, num_keywords=10)

    print(keywords)
    list_num += 1


