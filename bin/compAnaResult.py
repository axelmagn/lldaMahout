import sys
def compareResult(fileName1,fileName2):
    sum1={}
    file1=file(fileName1,"r")
    while True:
        line=file1.readline()
        if len(line)==0:
           break
        tokens=line.split("\t")
        print tokens[0],tokens[1]
        sum1[tokens[0]]=int(tokens[1])
    file1.close()
    sum2={}
    file2=file(fileName2,"r")
    while True:
        line=file2.readline()
        if len(line)==0:
            break
        tokens=line.split("\t")
        print tokens[0],tokens[1]
        sum2[tokens[0]]=int(tokens[1])
    file2.close()
    keys=set(sum1.keys()) | set(sum2.keys())
    result={}
    for key in keys:
        if (key in sum2) and (key in sum1):
            result[key]=sum2[key]-sum1[key]
        elif key in sum2 :
            result[key]=sum2[key]
        else:
            result[key]=0-sum1[key]
    return sum2 , keys,result

fileName1=sys.argv[1]
fileName2=sys.argv[2]
sum2,keys,result=compareResult(fileName1,fileName2)
for key in list(keys).sort():
    if key in sum2:
        print key,sum2[key],result[key]
    else:
        print key,0,result[key]