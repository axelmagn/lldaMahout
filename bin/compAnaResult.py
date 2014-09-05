import sys
def compareResult(fileName1,fileName2):
    sum1={}
    file1=file(fileName1,"r")
    while True:
        line=file1.readline()
        tokens=line.split(" ")
        print tokens[0],tokens[1]
        sum1[tokens[0]]=tokens[1]
    file1.close()
    sum2={}
    file2=file(fileName2,"r")
    while True:
        line=file2.readline()
        if len(line)==0:
            break
        tokens=line.split(" ")
        print tokens[0],tokens[1]
        sum2[tokens[0]]=tokens[1]
    file2.close()
    result={}
    for key in sum1:
        result[key]=sum2[key]-sum1[key]
    return sum2 , result

fileName1=sys.argv[1]
fileName2=sys.argv[2]
sum2,result=compareResult(fileName1,fileName2)
for key in sum2:
    print key,sum2[key],result[key]
