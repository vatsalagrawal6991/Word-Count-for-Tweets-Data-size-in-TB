from celery import chord
from celery import group
import tasks 
from sys import argv
from os import path, walk
divis = 100 



def para():
    divis = 10
    divis2 = 40
    divis3 = 180
    if (len(argv) != 2):
        print("Give File")
    pathabs=[path.join(x, fil) for x, y, z in walk(argv[1]) for fil in z]
    abc = len(pathabs)//divis3
    temp2a = []
    for abe in range(abc+1):
        if (abe==abc):
            temp2a.append(pathabs[(abe*divis3):])
        else:
            temp2a.append(pathabs[(abe*divis3):((abe+1)*divis3)])
    pathabs = temp2a
    abc = len(pathabs)//divis
    temp2 = []
    for abe in range(abc+1):
        if (abe == abc):
            temp1 = group(tasks.shar.s(filen) for filen in (pathabs[(abe*divis):]))()
            temp2.append(temp1.get())
        else:
            temp1 = group(tasks.shar.s(filen) for filen in (pathabs[(abe*divis):(divis*(abe+1))]))()
            temp2.append(temp1.get())

    finalf = {}
    while True:
        abc = len(temp2)//divis2
        temp3 = []
        if (len(temp2)==1):
            finalf = tasks.callb(temp2[0])
            #for abe in temp2[0]:
                #finalf = Counter(abe) + Counter(finalf)
            break
        else: 
            for abe in range(abc+1):
                if (abe==abc):
                    temp1 = group(tasks.callb.s(filen) for filen in temp2[(abe*divis2):])()
                    temp3.append(temp1.get())
                else:
                    temp1 = group(tasks.callb.s(filen) for filen in temp2[(abe*divis2):((abe+1)*divis2)])()
                    temp3.append(temp1.get())
        temp2 = temp3
    return finalf
#print(len(finalf))

def seri():
    if (len(argv) < 2):
        print("Use the command: python3 client.py <data_dir>")
    DIR=argv[1]
    #print(DIR)
    abs_files=[path.join(pth, f) for pth, dirs, files in walk(DIR) for f in files]
    #print(abs_files[0])
    wc = {}
    for filename in abs_files:
        with open(filename, mode='r', newline='\r') as f:
            for text in f:
                if text == '\n':
                    continue
                sp = text.split(',')[4:-2]
                tweet = " ".join(sp)
                for word in tweet.split(" "):
                    if word not in wc:
                        wc[word] = 0
                    wc[word] = wc[word] + 1

    return wc


a= para()
print("OK")
b= seri()
#a['sdhbchsbdhvw']=5
print(a==b)
#print(a)
