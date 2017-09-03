import sys
import pandas as pd
from sklearn.cluster import KMeans


from logging import getLogger,FileHandler,Formatter,StreamHandler,DEBUG,ERROR

logger = getLogger(__name__)
logger.setLevel(DEBUG)
formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh1 = FileHandler(filename="infolog.txt")
fh1.setLevel(DEBUG)
fh1.setFormatter(formatter)
logger.addHandler(fh1)
fh2 = FileHandler(filename="errorlog.txt")
fh2.setLevel(ERROR)
fh2.setFormatter(formatter)
logger.addHandler(fh2)
sh = StreamHandler()
sh.setLevel(ERROR)
sh.setFormatter(formatter)
logger.addHandler(sh)

def main():
    args = sys.argv
    try:
        if args[1]=="convert":
            filename=args[2]
            generateInput(filename)
        if args[1]=="clustering":
            day=int(args[2])
            clusterNum=int(args[3])
            clustering(day,clusterNum)
    except Exception,e:
        logger.error(e)
        raise

def generateInput(inputFileName):
    logger.info("Generate inputs")
    cust_df = pd.read_csv(inputFileName, header=None)
    del(cust_df[0])
    logger.info("Candle data loaded")
    for day in range(1,6):
        logger.info("Creating day data:"+unicode(day))
        output=open("day_"+unicode(day)+"_input.csv","w")
        output2=open("day_"+unicode(day)+"_result.csv","w")
        for startPoint in range(0,len(cust_df)-day*96-5*96):
            logger.info("Creating day data:"+unicode(day)+","+unicode(startPoint))
            writeStr=""
            for t in range(0,day*96):
                writeStr+=unicode(cust_df[1][startPoint+t]/cust_df[1][startPoint])+","
                writeStr+=unicode(cust_df[2][startPoint+t]/cust_df[1][startPoint])+","
                writeStr+=unicode(cust_df[3][startPoint+t]/cust_df[1][startPoint])+","
                writeStr+=unicode(cust_df[4][startPoint+t]/cust_df[1][startPoint])+","
            logger.info("Creating result data:"+unicode(day)+","+unicode(startPoint))
            nowPoint=startPoint+day*96
            futurePoint=nowPoint+4
            writeStr=writeStr[:-1]+"\n"
            output.write(writeStr)

            writeStr=unicode(cust_df[1][futurePoint]/cust_df[1][startPoint]-cust_df[1][nowPoint]/cust_df[1][startPoint])+","
            for t in range(1,6):
                futurePoint=nowPoint+t*96
                writeStr+=unicode(cust_df[1][futurePoint]/cust_df[1][startPoint]-cust_df[1][nowPoint]/cust_df[1][startPoint])+","
            writeStr=writeStr[:-1]+"\n"
            output2.write(writeStr)
        output.close()
        output2.close()

def clustering(day,clusterNum):
    logger.info("Generate clustering")
    input_df = pd.read_csv("day_"+unicode(day)+"_input.csv", header=None)
    input_array=input_df.as_matrix()
    logger.info("Input data created")
    pred = KMeans(n_clusters=clusterNum).fit_predict(input_array)
    logger.info("Cluster created")
    output_df = pd.read_csv("day_"+unicode(day)+"_result.csv", header=None)
    outputs=[]
    for i in range(clusterNum):
        tempDic={"gid":i}
        tempDic["counterUp"]=[0,0,0,0,0,0]
        tempDic["counterDown"]=[0,0,0,0,0,0]
        tempDic["return"]=[0,0,0,0,0,0]
        tempDic["avg"]=[]
        for t in range(0,day*96):
            tempDic["avg"].append(0.0)
        outputs.append(tempDic)

    f=open("day_"+unicode(day)+"_cluster.csv","w")
    for i in range(len(output_df)):
        for i2 in range(0,6):
            if output_df[i2][i]>0:
                outputs[pred[i]]["counterUp"][i2]+=1
            else:
                outputs[pred[i]]["counterDown"][i2]+=1
            outputs[pred[i]]["return"][i2]+=output_df[i2][i]
        for t in range(0,day*96):
            outputs[pred[i]]["avg"][t]+=input_df[t][i]
        f.write(unicode(pred[i])+"\n")
    f.close()
    logger.info("Result calced")
    f=open("day_"+unicode(day)+"_output.csv","w")
    f2=open("day_"+unicode(day)+"_avg.csv","w")
    f3=open("day_"+unicode(day)+"_return.csv","w")
    for i in range(clusterNum):
        totalNum=outputs[i]["counterUp"][0]+outputs[i]["counterDown"][0]
        tempStr=unicode(totalNum)+","
        tempStr2=unicode(totalNum)+","
        for i2 in range(0,6):
            tempStr+=unicode(float(outputs[i]["counterUp"][i2])/float(totalNum))+","
            tempStr2+=unicode(float(outputs[i]["return"][i2])/float(totalNum))+","
        tempStr=tempStr[:-1]+"\n"
        tempStr2=tempStr2[:-1]+"\n"
        f.write(tempStr)
        f3.write(tempStr2)
        tempStr=""
        for t in range(0,day*96):
            tempStr+=unicode(outputs[i]["avg"][t]/float(totalNum))+","
        tempStr=tempStr[:-1]+"\n"
        f2.write(tempStr)
    f.close()
    f2.close()
    f3.close()

if __name__ == '__main__':
    main()
