import csv
from collections import Counter

all_fi =open('D:\Project/test/dictest0913.csv', 'r', encoding="utf-8")
data = csv.reader(all_fi)
# next(data)

# date= ch_data[1]
# word = ch_data[3:]
# date = date.split('-')
# print(date[1])
# print(word)

month = [[], [], [], [], [], [], [], [], [], [], [], []]

for row in data:
    date = row[1]
    word = row[3:]
    date = date.split('-')
    for mon in range(12):
        if int(date[1]) == mon:
            month[mon-1].extend(word)
        # print(month)
counts6 =Counter(month[5])
counts7 =Counter(month[6])
counts8 =Counter(month[7])
print(counts6)
print(counts7)
print(counts8)



