# -*- coding: utf-8 -*- 

import time
import gzip
import fileinput
import random
# from sklearn.ensemble import RandomForestClassifier

def func0():
	imap = {}
	for line in gzip.open("tianchi_mobile_recommend_train_item.txt.gz"):
		imap[line.split(",")[0]] = True
	file = open("data_filter.txt","w")
	c = 0
	for line in gzip.open("uid_iid.txt.gz"):
		c += 1
		print c
		(uid, iid, ict) = line.strip().split("\t")[0].split(" ")
		if imap.has_key(iid):
			file.write(line)
	file.close()
	pass

def func1():
	pmap = {}
	c = 0
	for line in gzip.open("uid_iid_filter.txt.gz"):
		c += 1
		print c
		(uid, iid, ict), items = line.strip().split("\t")[0].split(" "), filter(lambda x:x[1]==4, [(i.split(",")[0], int(i.split(",")[1])) for i in line.strip().split("\t")[1].split(" ")])
		for item in items:
			day = "-".join(item[0].split("-")[:2])+"-0"
			if not pmap.has_key(uid+" "+iid+" "+day):
				f = open("feature/buy/"+day,'a')
				f.write(uid+" "+iid+"\n")
				pmap[uid+" "+iid+" "+day] = True
				f.close()
	pass

def func2():
	time_start = time.time()
	target = "12-19-0"
	imap = {}
	c = 0
	for line in gzip.open("uid_iid_filter.txt.gz"):
		c += 1
		print c
		(uid, iid, ict), items = line.strip().split("\t")[0].split(" "), [(int(time.mktime(time.strptime('2014-'+target,'%Y-%m-%d-%H'))-time.mktime(time.strptime('2014-'+i.split(",")[0],'%Y-%m-%d-%H')))/(24*3600)+1, int(i.split(",")[1])) for i in line.strip().split("\t")[1].split(" ")]
		if not imap.has_key(iid): imap[iid] = {"iid":iid,"b1_21":0.0,"b2_21":0.0,"b3_21":0.0,"b4_21":0.0,"r1_21":0.0,"r2_21":0.0,"r3_21":0.0,"t_21":0.0,"r_21":0.0,\
														 "b1_7":0.0,"b2_7":0.0,"b3_7":0.0,"b4_7":0.0,"r1_7":0.0,"r2_7":0.0,"r3_7":0.0,"t_7":0.0,"r_7":0.0,\
														 "b1_3":0.0,"b2_3":0.0,"b3_3":0.0,"b4_3":0.0,"r1_3":0.0,"r2_3":0.0,"r3_3":0.0,"t_3":0.0,"r_3":0.0}
		for d in [3,7,21]:
			b1, b2, b3, b4 = filter(lambda x:0<x[0]<=d and x[1]==1, items), filter(lambda x:0<x[0]<=d and x[1]==2, items), filter(lambda x:0<x[0]<=d and x[1]==3, items), filter(lambda x:0<x[0]<=d and x[1]==4, items)
			if len(b1) != 0: imap[iid]["b1_"+str(d)] += 1
			if len(b2) != 0: imap[iid]["b2_"+str(d)] += 1
			if len(b3) != 0: imap[iid]["b3_"+str(d)] += 1 
			if len(b4) != 0: imap[iid]["b4_"+str(d)] += 1
			if len(b4) >= 2: imap[iid]["t_"+str(d)] += 1
	fileinput.close()
	# ilist = []
	for key in imap:
		for d in [3,7,21]:
			imap[key]["r1_"+str(d)] = float(imap[key]["b4_"+str(d)])/imap[key]["b1_"+str(d)] if imap[key]["b1_"+str(d)]!=0 else 0
			imap[key]["r2_"+str(d)] = float(imap[key]["b4_"+str(d)])/imap[key]["b2_"+str(d)] if imap[key]["b2_"+str(d)]!=0 else 0
			imap[key]["r3_"+str(d)] = float(imap[key]["b4_"+str(d)])/imap[key]["b3_"+str(d)] if imap[key]["b3_"+str(d)]!=0 else 0
			imap[key]["r_"+str(d)] = float(imap[key]["t_"+str(d)])/imap[key]["b4_"+str(d)] if imap[key]["b4_"+str(d)]!=0 else 0
	# 	ilist.append(imap[key])
	# ilist = sorted(ilist, key=lambda x:x["b4_21"], reverse=True)[:100]
	fset = ["b1_21","b2_21","b3_21","b4_21","r1_21","r2_21","r3_21","t_21","r_21","b1_7","b2_7","b3_7","b4_7","r1_7","r2_7","r3_7","t_7","r_7","b1_3","b2_3","b3_3","b4_3","r1_3","r2_3","r3_3","t_3","r_3"]
	file = open("feature/iid/"+target+".txt","w")
	for key in imap:
		file.write(imap[key]["iid"])
		for f in fset:
			file.write(" "+str(round(imap[key][f],4)))
		file.write("\n")
	file.close()
	time_end = time.time()
	print time_end-time_start
	pass

def func3():
	target = "12-19-0"
	file = open("feature/uid_iid/"+target+".txt","w")
	c = 0
	for line in gzip.open("uid_iid_filter.txt.gz"):
		c += 1
		print c
		(uid, iid, ict), items = line.strip().split("\t")[0].split(" "), [(int(time.mktime(time.strptime('2014-'+target,'%Y-%m-%d-%H'))-time.mktime(time.strptime('2014-'+i.split(",")[0],'%Y-%m-%d-%H')))/(24*3600)+1, int(i.split(",")[1])) for i in line.strip().split("\t")[1].split(" ")]
		buy = filter(lambda x:x[0]>0 and x[1]==4, items)
		last = buy[-1][0] if len(buy)!=0 else -30
		feat = {"uid":uid,"iid":iid,"last":last,"b1_21":0.0,"b2_21":0.0,"b3_21":0.0,"b4_21":0.0,"r1_21":0.0,"r2_21":0.0,"r3_21":0.0,\
												"b1_7":0.0,"b2_7":0.0,"b3_7":0.0,"b4_7":0.0,"r1_7":0.0,"r2_7":0.0,"r3_7":0.0,\
												"b1_3":0.0,"b2_3":0.0,"b3_3":0.0,"b4_3":0.0,"r1_3":0.0,"r2_3":0.0,"r3_3":0.0,\
												"b1_1":0.0,"b2_1":0.0,"b3_1":0.0,"b4_1":0.0,"r1_1":0.0,"r2_1":0.0,"r3_1":0.0,\
												"b1_l":0.0,"b2_l":0.0,"b3_l":0.0,"b4_l":0.0,"r1_l":0.0,"r2_l":0.0,"r3_l":0.0}
		for d in [1,3,7,21]:
			b1, b2, b3, b4 = filter(lambda x:0<x[0]<=d and x[1]==1, items), filter(lambda x:0<x[0]<=d and x[1]==2, items), filter(lambda x:0<x[0]<=d and x[1]==3, items), filter(lambda x:0<x[0]<=d and x[1]==4, items)
			feat["b1_"+str(d)], feat["b2_"+str(d)], feat["b3_"+str(d)], feat["b4_"+str(d)] = len(b1), len(b2), len(b3), len(b4)
			feat["r1_"+str(d)], feat["r2_"+str(d)], feat["r3_"+str(d)] = len(b1)/len(b4) if len(b4)!=0 else 0, len(b2)/len(b4) if len(b4)!=0 else 0, len(b3)/len(b4) if len(b4)!=0 else 0 
		b1, b2, b3, b4 = filter(lambda x:0<x[0]<=last and x[1]==1, items), filter(lambda x:0<x[0]<=last and x[1]==2, items), filter(lambda x:0<x[0]<=last and x[1]==3, items), filter(lambda x:0<x[0]<=last and x[1]==4, items)
		feat["b1_l"], feat["b2_l"], feat["b3_l"], feat["b4_l"] = len(b1), len(b2), len(b3), len(b4)
		feat["r1_l"], feat["r2_l"], feat["r3_l"] = len(b1)/len(b4) if len(b4)!=0 else 0, len(b2)/len(b4) if len(b4)!=0 else 0, len(b3)/len(b4) if len(b4)!=0 else 0 
		fset = ["b1_21","b2_21","b3_21","b4_21","r1_21","r2_21","r3_21",\
				"b1_7","b2_7","b3_7","b4_7","r1_7","r2_7","r3_7",\
				"b1_3","b2_3","b3_3","b4_3","r1_3","r2_3","r3_3",\
				"b1_1","b2_1","b3_1","b4_1","r1_1","r2_1","r3_1",\
				"b1_l","b2_l","b3_l","b4_l","r1_l","r2_l","r3_l"]
		file.write(feat["uid"]+" "+feat["iid"]+" "+str(feat["last"]))
		for f in fset:
			file.write(" "+str(round(feat[f],4)))
		file.write("\n")
	pass

# 过滤待预测集
def func4():
	target = "12-18-0" #评估日期，分界日期
	pos = {}
	for line in fileinput.input("feature/buy/"+target):
		pos[line.strip()] = True
	fileinput.close()
	file = open("feature/act/"+target,"w")
	for line in gzip.open("uid_iid_filter.txt.gz"):
		(uid, iid, ict) = line.strip().split("\t")[0].split(" ")
		if pos.has_key(uid+" "+iid):
			file.write(line)
	file.close()
	pass

# 基于规则预测
def func5():
	# target, etime = "12-13-0", "12-12-23" #评估日期，分界日期
	# target, etime = "12-14-0", "12-13-23" #评估日期，分界日期
	# target, etime = "12-15-0", "12-14-23" #评估日期，分界日期
	# target, etime = "12-16-0", "12-15-23" #评估日期，分界日期
	# target, etime = "12-17-0", "12-16-23" #评估日期，分界日期
	target, etime = "12-18-0", "12-17-23" #评估日期，分界日期
	pos = {}
	for line in fileinput.input("feature/buy/"+target):
		pos[line.strip()] = True
	fileinput.close()
	cnt, tot, cor = 0, 0, 0
	# for line in fileinput.input("feature/act/"+target):
	for line in gzip.open("uid_iid_filter.txt.gz"):
		(uid, iid, ict), items = line.strip().split("\t")[0].split(" "), filter(lambda x:x[0]>0, [(int(time.mktime(time.strptime('2014-'+etime,'%Y-%m-%d-%H'))-time.mktime(time.strptime('2014-'+i.split(",")[0],'%Y-%m-%d-%H')))/(24*3600)+1, int(i.split(",")[1])) for i in line.strip().split("\t")[1].split(" ")])
		buy = filter(lambda x:x[0]>0 and x[1]==4, items)
		last = buy[-1][0] if len(buy)!=0 else 100
		# 加购物车
		# b1 = filter(lambda x:0<x[0]<=min(1,last-1) and x[1]==3, items)
		b1 = filter(lambda x:0<x[0]<=min(5,last-1) and x[1]==3, items)
		# 加搜藏夹
		b2, b3 = filter(lambda x:0<x[0]<=min(3,last-1) and x[1]==2, items), filter(lambda x:0<x[0]<=2 and x[1]==1, items)
		# 最后点击
		b4 = filter(lambda x:0<x[0]<=min(1,last-1) and x[1]==1, items)
		# 最近点击
		b5 = filter(lambda x:0<x[0]<=min(2,last-1) and x[1]==1, items)
		dm = {}
		for b in b5:
			dm[b] = dm[b]+1 if dm.has_key(b) else 1
		cnt += 1
		# 规则条件
		# if len(b1) >= 1:
		# if len(b2) >= 1 and len(b3) >= 2:
		# if len(b4) >= 4:
		# if any([dm[d]>=4 for d in dm]):
		if len(b1) >= 1 or len(b2) >= 1 and len(b3) >= 2 or any([dm[d]>=4 for d in dm]):
			tot += 1
			# print items
			if pos.has_key(uid+" "+iid):
				cor += 1
		# elif len(items)!=0:
		# 	print items
	print target, len(pos), cnt, tot, cor, 1.0*cor/tot, 1.0*cor/len(pos), 2.0*cor**2/tot/len(pos)/(1.0*cor/tot+1.0*cor/len(pos))
	pass

# 基于机器学习预测
def func6():
	# X = [[0, 0], [1, 1]]
	# Y = [0, 1]
	# clf = RandomForestClassifier(n_estimators=10)
	# clf = clf.fit(X, Y)
	# clf.predict([[2., 2.]])
	# clf.predict_proba([[2., 2.]])
	#### 训练 ####
	train, predict = ["12-15-0","12-16-0","12-17-0"], "12-18-0"
	# train, predict = ["12-16-0","12-17-0","12-18-0"], "12-19-0"
	X0, Y0, X1, Y1, X2, Y2 = [], [], [], [], [], []
	for date in train:
		print date
		pos, imap = {}, {}
		for line in fileinput.input("feature/buy/"+date):
			pos[line.strip()] = True
		fileinput.close()
		for line in gzip.open("feature/iid/"+date+".txt.gz"):
			imap[line.strip().split(" ")[0]] = [float(i) for i in line.strip().split(" ")[1:]]
		fileinput.close()
		for line in gzip.open("feature/uid_iid/"+date+".txt.gz"):
			uid, iid, feat = line.strip().split(" ")[0], line.strip().split(" ")[1], [float(i) for i in line.strip().split(" ")[2:]]
			feat.extend(imap[iid])
			if pos.has_key(uid+" "+iid):
				Y1.append(1)
				X1.append(feat)
			else:
				Y2.append(0)
				X2.append(feat)
	for i in random.sample([i for i in xrange(len(Y2))], 10*len(Y1)):
		Y0.append(Y2[i])
		X0.append(X2[i])
	X_train, Y_train = [], []
	X_train.extend(X0)
	X_train.extend(X1)
	Y_train.extend(Y0)
	Y_train.extend(Y1)
	clf = RandomForestClassifier(n_estimators=10)
	clf = clf.fit(X_train, Y_train)
	#### 预测 ####
	pos, imap = {}, {}
	for line in fileinput.input("feature/buy/"+predict):
		pos[line.strip()] = True
	fileinput.close()
	for line in gzip.open("feature/iid/"+predict+".txt.gz"):
		imap[line.strip().split(" ")[0]] = [float(i) for i in line.strip().split(" ")[1:]]
	fileinput.close()
	file = open("tianchi_mobile_recommendation_predict_4_16.txt","w")
	num, tot, cor = 0, 0, 0
	for line in gzip.open("feature/uid_iid/"+predict+".txt.gz"):
		num += 1
		uid, iid, feat = line.strip().split(" ")[0], line.strip().split(" ")[1], [float(i) for i in line.strip().split(" ")[2:]]
		feat.extend(imap[iid])
		# pred = clf.predict(feat)
		pred, proba = 0, clf.predict_proba(feat)
		print uid, iid, proba, num, tot, cor
		if clf.predict_proba(feat)[0][1] >= 0.6:
			file.write(uid+","+iid+"\n")
			pred = 1
		if pred == 1:
			tot += 1
		if pred == 1 and pos.has_key(uid+" "+iid):
			cor += 1
	print len(pos), tot, cor
	pass

def func7():
	predict, pos = "12-18-0", {}
	for line in fileinput.input("feature/buy/"+predict):
		pos[line.strip()] = True
	fileinput.close()
	tot, cor = 0, 0
	for line in fileinput.input("tianchi_mobile_recommendation_predict.txt"):
		tot += 1
		if pos.has_key(" ".join(line.strip().split(","))):
			cor += 1
	fileinput.close()
	print len(pos), tot, cor
	pass

# func0()
# func1()
# func2()
# func3()
# func4()
func5()
# func6()
# func7()

