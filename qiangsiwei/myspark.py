# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

####################################################################################
############################         数据预处理           ############################
####################################################################################
# def extract(line):
# 	import time
# 	try:
# 		part = line.strip().split(",")
# 		uid, iid, beh, ict, time = part[0], part[1], part[2], part[4], "-".join(part[5].split(" ")[0].split("-")[1:])+"-"+part[5].split(" ")[1]
# 		return ((uid, iid, ict), time+","+beh)
# 	except:
# 		return ((""), "")

# global bss

# if __name__ == "__main__":
# 	conf = (SparkConf()
#     	.setMaster("spark://namenode.omnilab.sjtu.edu.cn:7077")
#     	.setAppName("Extract")
#     	.set("spark.cores.max", "32")
#     	.set("spark.driver.memory", "4g")
# 		.set("spark.executor.memory", "6g"))
# 	# .setMaster("yarn-client")
# 	sc = SparkContext(conf = conf)
# 	# sc = SparkContext('spark://namenode.omnilab.sjtu.edu.cn:7077',appName="Extract")
# 	lines = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei/tianchi/tianchi_mobile_recommend_train_user.txt', 1)
# 	counts = lines.map(lambda x : extract(x)) \
# 				  .filter(lambda x : x[0]!="") \
# 				  .groupByKey() \
# 				  .map(lambda x : (" ".join(x[0])+"\t"+" ".join([str(item["time"])+","+item["beh"] for item in sorted([{"time":content.split(",")[0],"beh":content.split(",")[1]} for content in x[1]],key=lambda x:x["time"])])))
# 	output = counts.saveAsTextFile("./tianchi/test")

####################################################################################
############################       用户-商品特征          ############################
####################################################################################
# def extract(line):
# 	import time
# 	import itertools
# 	(uid, iid, ict) = line.strip().split("\t")[0].split(" ")
# 	if subset.has_key(iid):
# 		items = filter(lambda x:x[0]>0, [(int(time.mktime(time.strptime('2014-'+etime,'%Y-%m-%d-%H'))-time.mktime(time.strptime('2014-'+i.split(",")[0],'%Y-%m-%d-%H')))/(24*3600)+1, int(i.split(",")[1])) for i in line.strip().split("\t")[1].split(" ")])
# 		f, inf = [0]*16, 100
# 		buy = filter(lambda x:x[1]==4, items)
# 		last = buy[-1][0] if len(buy)!=0 else inf
# 		a1 = filter(lambda x:x[0]<last and x[1]==1, items)
# 		a2 = filter(lambda x:x[0]<last and x[1]==2, items)
# 		a3 = filter(lambda x:x[0]<last and x[1]==3, items)
# 		f[0] = a2[-1][0] if len(a2)!=0 else inf # 购买后最后一次加入收藏夹距离天数
# 		f[1] = a3[-1][0] if len(a3)!=0 else inf # 购买后最后一次加入购物车距离天数
# 		f[2] = len(a1) # 购买后点击次数
# 		f[3] = len(a2) # 购买后加收次数
# 		f[4] = len(a3) # 购买后加购次数
# 		f[5] = len(filter(lambda x:x[0]<=1, items)) # 最后1天交互次数
# 		f[6] = len(filter(lambda x:x[0]<=3, items)) # 最后3天交互次数
# 		f[7] = len(filter(lambda x:x[0]<=7, items)) # 最后7天交互次数
# 		f[8] = len(buy) # 历史购买次数
# 		f[9] = last # 最后一次购买距离天数
# 		f[10] = len(set([item[0] for item in items if item[0]<=3])) # 最后3天内交互天数
# 		f[11] = len(set([item[0] for item in items if item[0]<=7])) # 最后1周内交互天数
# 		f[12] = len(set([item[0] for item in items if item[0]<=21])) # 最后3周内交互天数
# 		f[13] = items[-1][0] if len(items)!=0 else inf # 最后1次交互距离天数
# 		inter = [len(list(i)) for _,i in itertools.groupby(items, lambda x: x[0])]
# 		f[14] = len(inter) #交互天数
# 		f[15] = max(inter) if len(inter)!=0 else 0 #交互最多的一天交互次数
# 		return (uid+"\t"+iid+"\t"+ict+"\t"+"\t".join([str(i) for i in f]))
# 	else:
# 		return ("")

# global etime
# global subset

# if __name__ == "__main__":
# 	import fileinput
# 	conf = (SparkConf()
#     	.setMaster("spark://namenode.omnilab.sjtu.edu.cn:7077")
#     	.setAppName("Extract")
#     	.set("spark.cores.max", "32")
#     	.set("spark.driver.memory", "4g")
# 		.set("spark.executor.memory", "6g"))
# 	# .setMaster("yarn-client")
# 	sc = SparkContext(conf = conf)
# 	# sc = SparkContext('spark://namenode.omnilab.sjtu.edu.cn:7077',appName="Extract")
# 	lines = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei/tianchi/uid_iid', 1)
# 	# target, etime, subset = "12-18-0", "12-17-23", {}
# 	# target, etime, subset = "12-17-0", "12-16-23", {}
# 	# target, etime, subset = "12-16-0", "12-15-23", {}
# 	# target, etime, subset = "12-15-0", "12-14-23", {}
# 	# target, etime, subset = "12-14-0", "12-13-23", {}
# 	target, etime, subset = "12-13-0", "12-12-23", {}
# 	for line in fileinput.input("../tianchi_mobile_recommend_train_item.txt"):
# 		subset[line.strip().split(",")[0]] = True
# 	counts = lines.map(lambda x : extract(x))\
# 				  .filter(lambda x : x!="")
# 	output = counts.saveAsTextFile("./tianchi/feature/"+target+"/user_prod/")

####################################################################################
############################       用户-品牌特征          ############################
####################################################################################
# def extract1(line):
# 	import time
# 	(uid, iid, ict) = line.strip().split("\t")[0].split(" ")
# 	if subset.has_key(ict):
# 		items = filter(lambda x:x[0]>0, [(int(time.mktime(time.strptime('2014-'+etime,'%Y-%m-%d-%H'))-time.mktime(time.strptime('2014-'+i.split(",")[0],'%Y-%m-%d-%H')))/(24*3600)+1, int(i.split(",")[1])) for i in line.strip().split("\t")[1].split(" ")])
# 		return (uid+"\t"+ict,items)
# 	else:
# 		return ("","")

# def extract2(items_list):
# 	import itertools
# 	items = []
# 	for i in items_list:
# 		items.extend(i)
# 	items = sorted(items, key=lambda x:x[1], reverse=True) if len(items)!=0 else []
# 	f, inf = [0]*19, 100
# 	buy = filter(lambda x:x[1]==4, items)
# 	last = buy[-1][0] if len(buy)!=0 else inf
# 	a1 = filter(lambda x:x[0]<last and x[1]==1, items)
# 	a2 = filter(lambda x:x[0]<last and x[1]==2, items)
# 	a3 = filter(lambda x:x[0]<last and x[1]==3, items)
# 	f[0] = a2[-1][0] if len(a2)!=0 else inf # 购买后最后一次加入收藏夹距离天数
# 	f[1] = a3[-1][0] if len(a3)!=0 else inf # 购买后最后一次加入购物车距离天数
# 	f[2] = len(a1) # 购买后点击次数
# 	f[3] = len(a2) # 购买后加收次数
# 	f[4] = len(a3) # 购买后加购次数
# 	f[5] = len(filter(lambda x:x[0]<=1, items)) # 最后1天交互次数
# 	f[6] = len(filter(lambda x:x[0]<=3, items)) # 最后3天交互次数
# 	f[7] = len(filter(lambda x:x[0]<=7, items)) # 最后7天交互次数
# 	f[8] = len(buy) # 历史购买次数
# 	f[9] = last # 最后一次购买距离天数
# 	f[10] = len(set([item[0] for item in items if item[0]<=3])) # 最后3天内交互天数
# 	f[11] = len(set([item[0] for item in items if item[0]<=7])) # 最后1周内交互天数
# 	f[12] = len(set([item[0] for item in items if item[0]<=21])) # 最后3周内交互天数
# 	f[13] = items[-1][0] if len(items)!=0 else inf # 最后1次交互距离天数
# 	inter = [len(list(i)) for _,i in itertools.groupby(items, lambda x: x[0])]
# 	f[14] = len(inter) #交互天数
# 	f[15] = max(inter) if len(inter)!=0 else 0 #交互最多的一天交互次数
# 	f[16] = len(filter(lambda x:x[0]<=1 and x[1]==4, items)) # 最后1天购买次数
# 	f[17] = len(filter(lambda x:x[0]<=3 and x[1]==4, items)) # 最后3天购买次数
# 	f[18] = len(filter(lambda x:x[0]<=7 and x[1]==4, items)) # 最后7天购买次数
# 	return " ".join([str(i) for i in f])

# global etime
# global subset

# if __name__ == "__main__":
# 	import fileinput
# 	conf = (SparkConf()
#     	.setMaster("spark://namenode.omnilab.sjtu.edu.cn:7077")
#     	.setAppName("Extract")
#     	.set("spark.cores.max", "32")
#     	.set("spark.driver.memory", "4g")
# 		.set("spark.executor.memory", "6g"))
# 	# .setMaster("yarn-client")
# 	sc = SparkContext(conf = conf)
# 	# sc = SparkContext('spark://namenode.omnilab.sjtu.edu.cn:7077',appName="Extract")
# 	lines = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei/tianchi/uid_iid', 1)
# 	# target, etime, subset = "12-18-0", "12-17-23", {}
# 	# target, etime, subset = "12-17-0", "12-16-23", {}
# 	# target, etime, subset = "12-16-0", "12-15-23", {}
# 	# target, etime, subset = "12-15-0", "12-14-23", {}
# 	# target, etime, subset = "12-14-0", "12-13-23", {}
# 	target, etime, subset = "12-13-0", "12-12-23", {}
# 	for line in fileinput.input("../tianchi_mobile_recommend_train_item.txt"):
# 		subset[line.strip().split(",")[2]] = True
# 	counts = lines.map(lambda x : extract1(x))\
# 				  .filter(lambda x : x[0]!="")\
# 				  .groupByKey()\
# 				  .map(lambda x : x[0]+"\t"+extract2(x[1]))
# 	output = counts.saveAsTextFile("./tianchi/feature/"+target+"/user_ict/")

####################################################################################
############################           商品特征           ############################
####################################################################################
# def extract1(line):
# 	import time
# 	(uid, iid, ict) = line.strip().split("\t")[0].split(" ")
# 	if subset.has_key(iid):
# 		items = filter(lambda x:x[0]>0, [(int(time.mktime(time.strptime('2014-'+etime,'%Y-%m-%d-%H'))-time.mktime(time.strptime('2014-'+i.split(",")[0],'%Y-%m-%d-%H')))/(24*3600)+1, int(i.split(",")[1])) for i in line.strip().split("\t")[1].split(" ")])
# 		return (iid,items)
# 	else:
# 		return ("","")

# def extract2(items_list):
# 	items, f = [], [0]*14
# 	for i in items_list:
# 		items.extend(i)
# 	items = sorted(items, key=lambda x:x[1], reverse=True)
# 	f[0] = len(filter(lambda x:x[0]<=3 and x[1]==1, items)) # 最后3天点击次数
# 	f[1] = len(filter(lambda x:x[0]<=3 and x[1]==2, items)) # 最后3天加收次数
# 	f[2] = len(filter(lambda x:x[0]<=3 and x[1]==3, items)) # 最后3天加购次数
# 	f[3] = len(filter(lambda x:x[0]<=3 and x[1]==4, items)) # 最后3天购买次数
# 	f[4] = len(filter(lambda x:x[0]<=14 and x[1]==1, items)) # 最后2周点击次数
# 	f[5] = len(filter(lambda x:x[0]<=14 and x[1]==2, items)) # 最后2周加收次数
# 	f[6] = len(filter(lambda x:x[0]<=14 and x[1]==3, items)) # 最后2周加购次数
# 	f[7] = len(filter(lambda x:x[0]<=14 and x[1]==4, items)) # 最后2周购买次数
# 	f[8] = min(1.0,round(1.0*f[3]/f[0],4)) if f[0]!=0 else 0.0 # 最后3天点击转化率
# 	f[9] = min(1.0,round(1.0*f[3]/f[1],4)) if f[1]!=0 else 0.0 # 最后3天加收转化率
# 	f[10] = min(1.0,round(1.0*f[3]/f[2],4)) if f[2]!=0 else 0.0 # 最后3天加购转化率
# 	f[11] = min(1.0,round(1.0*f[7]/f[4],4)) if f[4]!=0 else 0.0 # 最后2周点击转化率
# 	f[12] = min(1.0,round(1.0*f[7]/f[5],4)) if f[5]!=0 else 0.0 # 最后2周加收转化率
# 	f[13] = min(1.0,round(1.0*f[7]/f[6],4)) if f[6]!=0 else 0.0 # 最后2周加购转化率
# 	return " ".join([str(i) for i in f])
	
# global etime
# global subset

# if __name__ == "__main__":
# 	import fileinput
# 	conf = (SparkConf()
#     	.setMaster("spark://namenode.omnilab.sjtu.edu.cn:7077")
#     	.setAppName("Extract")
#     	.set("spark.cores.max", "32")
#     	.set("spark.driver.memory", "4g")
# 		.set("spark.executor.memory", "6g"))
# 	# .setMaster("yarn-client")
# 	sc = SparkContext(conf = conf)
# 	# sc = SparkContext('spark://namenode.omnilab.sjtu.edu.cn:7077',appName="Extract")
# 	lines = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei/tianchi/uid_iid', 1)
# 	# target, etime, subset = "12-18-0", "12-17-23", {}
# 	# target, etime, subset = "12-17-0", "12-16-23", {}
# 	# target, etime, subset = "12-16-0", "12-15-23", {}
# 	# target, etime, subset = "12-15-0", "12-14-23", {}
# 	# target, etime, subset = "12-14-0", "12-13-23", {}
# 	target, etime, subset = "12-13-0", "12-12-23", {}
# 	for line in fileinput.input("../tianchi_mobile_recommend_train_item.txt"):
# 		subset[line.split(",")[0]] = True
# 	counts = lines.map(lambda x : extract1(x))\
# 				  .filter(lambda x : x[0]!="")\
# 				  .groupByKey()\
# 				  .map(lambda x : x[0]+"\t"+extract2(x[1]))
# 	output = counts.saveAsTextFile("./tianchi/feature/"+target+"/prod/")

####################################################################################
############################           用户特征           ############################
####################################################################################
def extract1(line):
	import time
	(uid, iid, ict) = line.strip().split("\t")[0].split(" ")
	items = filter(lambda x:x[0]>0, [(int(time.mktime(time.strptime('2014-'+etime,'%Y-%m-%d-%H'))-time.mktime(time.strptime('2014-'+i.split(",")[0],'%Y-%m-%d-%H')))/(24*3600)+1, int(i.split(",")[1])) for i in line.strip().split("\t")[1].split(" ")])
	return (uid,items)

def extract2(items_list):
	import itertools
	items, f, inf = [], [0]*24, 100
	for i in items_list:
		items.extend(i)
	items = sorted(items, key=lambda x:x[1], reverse=True)
	buy = filter(lambda x:x[1]==4, items)
	last = buy[-1][0] if len(buy)!=0 else inf
	f[0] = len(filter(lambda x:x[0]<=7 and x[1]==1, items)) # 最后1周点击次数
	f[1] = len(filter(lambda x:x[0]<=7 and x[1]==2, items)) # 最后1周加收次数
	f[2] = len(filter(lambda x:x[0]<=7 and x[1]==3, items)) # 最后1周加购次数
	f[3] = len(filter(lambda x:x[0]<=7 and x[1]==4, items)) # 最后1周购买次数
	f[4] = len(filter(lambda x:x[0]<=21 and x[1]==1, items)) # 最后3周点击次数
	f[5] = len(filter(lambda x:x[0]<=21 and x[1]==2, items)) # 最后3周加收次数
	f[6] = len(filter(lambda x:x[0]<=21 and x[1]==3, items)) # 最后3周加购次数
	f[7] = len(filter(lambda x:x[0]<=21 and x[1]==4, items)) # 最后3周购买次数
	f[8] = min(1.0,round(1.0*f[3]/f[0],4)) if f[0]!=0 else 0.0 # 最后1周点击转化率
	f[9] = min(1.0,round(1.0*f[3]/f[1],4)) if f[1]!=0 else 0.0 # 最后1周加收转化率
	f[10] = min(1.0,round(1.0*f[3]/f[2],4)) if f[2]!=0 else 0.0 # 最后1周加购转化率
	f[11] = min(1.0,round(1.0*f[7]/f[4],4)) if f[4]!=0 else 0.0 # 最后3周点击转化率
	f[12] = min(1.0,round(1.0*f[7]/f[5],4)) if f[5]!=0 else 0.0 # 最后3周加收转化率
	f[13] = min(1.0,round(1.0*f[7]/f[6],4)) if f[6]!=0 else 0.0 # 最后3周加购转化率
	f[14] = last # 最后一次购买距离天数
	f[15] = len(set([item[0] for item in items if item[0]<=3])) # 最后3天内交互天数
	f[16] = len(set([item[0] for item in items if item[0]<=7])) # 最后1周内交互天数
	f[17] = len(set([item[0] for item in items if item[0]<=21])) # 最后3周内交互天数
	f[18] = items[-1][0] if len(items)!=0 else inf # 最后1次交互距离天数
	inter = [len(list(i)) for _,i in itertools.groupby(items, lambda x: x[0])]
	f[19] = len(inter) #交互天数
	f[20] = max(inter) if len(inter)!=0 else 0 #交互最多的一天交互次数
	f[21] = len(filter(lambda x:x[0]<=1 and x[1]==4, items)) # 最后1天购买次数
	f[22] = len(filter(lambda x:x[0]<=3 and x[1]==4, items)) # 最后3天购买次数
	f[23] = len(filter(lambda x:x[0]<=7 and x[1]==4, items)) # 最后7天购买次数
	return " ".join([str(i) for i in f])
	
global etime
global subset

if __name__ == "__main__":
	import fileinput
	conf = (SparkConf()
    	.setMaster("spark://namenode.omnilab.sjtu.edu.cn:7077")
    	.setAppName("Extract")
    	.set("spark.cores.max", "32")
    	.set("spark.driver.memory", "4g")
		.set("spark.executor.memory", "6g"))
	# .setMaster("yarn-client")
	sc = SparkContext(conf = conf)
	# sc = SparkContext('spark://namenode.omnilab.sjtu.edu.cn:7077',appName="Extract")
	lines = sc.textFile('hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei/tianchi/uid_iid', 1)
	# target, etime, subset = "12-18-0", "12-17-23", {}
	# target, etime, subset = "12-17-0", "12-16-23", {}
	# target, etime, subset = "12-16-0", "12-15-23", {}
	# target, etime, subset = "12-15-0", "12-14-23", {}
	# target, etime, subset = "12-14-0", "12-13-23", {}
	target, etime, subset = "12-13-0", "12-12-23", {}
	for line in fileinput.input("../tianchi_mobile_recommend_train_item.txt"):
		subset[line.split(",")[0]] = True
	counts = lines.map(lambda x : extract1(x))\
				  .groupByKey()\
				  .map(lambda x : x[0]+"\t"+extract2(x[1]))
	output = counts.saveAsTextFile("./tianchi/feature/"+target+"/user/")
