# -*- coding: utf-8 -*- 

import time
import gzip
import fileinput

# 提取出所有user_id
def func_user():
	user = []
	a = 0
	for line in gzip.open("../uid_iid.txt.gz"):
		# print a
		a = 0
		c = line.split(' ')[0]
		for i in range(len(user)):
			if (user[i]==c):
				a = 1
				break
		if(a==0):
			user.append(line.split(' ')[0])

	print len(user)
	file = open("user_feature.txt","w")
	for i in range(len(user)):
		file.write(user[i])
		file.write('\n')

# 基本特征统计
def func_feature_1():
	endtime=time.mktime(time.strptime('2014-12-17-00','%Y-%m-%d-%H'))
	user_dict = {}
	user_action_day = {}
	user_action_day_5 = {}
	user_action_day_7 = {}
	user_action_day_15 = {}
	# user_item = {}
	# user_category = {}
	user = []
	f = open("user_feature.txt","r")
	for i in range(10000):
		user.append(f.readline().strip())
	f.close()

	for i in range(len(user)):
		# print user[i]
		user_dict[user[i]]=[0 for x in range(0,36)]
		user_action_day[user[i]]=[]
		user_action_day_5[user[i]]=[]
		user_action_day_7[user[i]]=[]
		user_action_day_15[user[i]]=[]
		# user_item[user[i]]=[[],[],[],[]] 
		# user_category[user[i]]=[[],[],[],[]] 

	for line in gzip.open("../uid_iid.txt.gz"):
		(uid, iid, ict) = line.strip().split("\t")[0].split(" ")
		action_category = []
		action_category_5 = []
		action_category_7 = []
		action_category_15 = []
		for action in line.strip().split("\t")[1].split(" "):
			if(endtime-3600*24*3<time.mktime(time.strptime('2014-'+action.split(",")[0],'%Y-%m-%d-%H'))<endtime):
				action_category.append(action.split(",")[1])
				if action.split(",")[0] in user_action_day[uid]:
					pass
				else:
					user_action_day[uid].append(action.split(",")[0])
			if(endtime-3600*24*5<time.mktime(time.strptime('2014-'+action.split(",")[0],'%Y-%m-%d-%H'))<endtime):
				action_category_5.append(action.split(",")[1])
				if action.split(",")[0] in user_action_day_5[uid]:
					pass
				else:
					user_action_day[uid].append(action.split(",")[0])
			if(endtime-3600*24*7<time.mktime(time.strptime('2014-'+action.split(",")[0],'%Y-%m-%d-%H'))<endtime):
				action_category_7.append(action.split(",")[1])
				if action.split(",")[0] in user_action_day_7[uid]:
					pass
				else:
					user_action_day[uid].append(action.split(",")[0])
			if(endtime-3600*24*15<time.mktime(time.strptime('2014-'+action.split(",")[0],'%Y-%m-%d-%H'))<endtime):
				action_category_15.append(action.split(",")[1])
				if action.split(",")[0] in user_action_day_15[uid]:
					pass
				else:
					user_action_day[uid].append(action.split(",")[0])
		# print action_category
		user_dict[uid][8] = len(user_action_day[uid])
		user_dict[uid][17] = len(user_action_day[uid])
		user_dict[uid][26] = len(user_action_day[uid])
		user_dict[uid][35] = len(user_action_day[uid])
		a = [0 for x in range(0,4)]
		for j in action_category:
			if j=="1":
				a[0] += 1
			elif j=="2":
				a[1] += 1
			elif j=="3":
				a[2] += 1
			elif j=="4":
				a[3] += 1
		user_dict[uid][0] += a[0]
		# print user_dict[uid][0]
		user_dict[uid][2] += a[1]
		# print user_dict[uid][3]
		user_dict[uid][4] += a[2]
		user_dict[uid][6] += a[3]
		if a[0] != 0 :
			user_dict[uid][1] += 1
			# print user_dict[uid][1]
		if a[1] != 0 :
			user_dict[uid][3] += 1
		if a[2] != 0 :
			user_dict[uid][5] += 1
		if a[3] != 0 :
			user_dict[uid][7] += 1 
		a = [0 for x in range(0,4)]
		for j in action_category_5:
			if j=="1":
				a[0] += 1
			elif j=="2":
				a[1] += 1
			elif j=="3":
				a[2] += 1
			elif j=="4":
				a[3] += 1
		user_dict[uid][9] += a[0]
		# print user_dict[uid][0]
		user_dict[uid][11] += a[1]
		# print user_dict[uid][3]
		user_dict[uid][13] += a[2]
		user_dict[uid][15] += a[3]
		if a[0] != 0 :
			user_dict[uid][10] += 1
			# print user_dict[uid][1]
		if a[1] != 0 :
			user_dict[uid][12] += 1
		if a[2] != 0 :
			user_dict[uid][14] += 1
		if a[3] != 0 :
			user_dict[uid][16] += 1 
		a = [0 for x in range(0,4)]
		for j in action_category_7:
			if j=="1":
				a[0] += 1
			elif j=="2":
				a[1] += 1
			elif j=="3":
				a[2] += 1
			elif j=="4":
				a[3] += 1
		user_dict[uid][18] += a[0]
		# print user_dict[uid][0]
		user_dict[uid][20] += a[1]
		# print user_dict[uid][3]
		user_dict[uid][22] += a[2]
		user_dict[uid][24] += a[3]
		if a[0] != 0 :
			user_dict[uid][19] += 1
			# print user_dict[uid][1]
		if a[1] != 0 :
			user_dict[uid][21] += 1
		if a[2] != 0 :
			user_dict[uid][23] += 1
		if a[3] != 0 :
			user_dict[uid][25] += 1 
		a = [0 for x in range(0,4)]
		for j in action_category_15:
			if j=="1":
				a[0] += 1
			elif j=="2":
				a[1] += 1
			elif j=="3":
				a[2] += 1
			elif j=="4":
				a[3] += 1
		user_dict[uid][27] += a[0]
		# print user_dict[uid][0]
		user_dict[uid][29] += a[1]
		# print user_dict[uid][3]
		user_dict[uid][31] += a[2]
		user_dict[uid][33] += a[3]
		if a[0] != 0 :
			user_dict[uid][28] += 1
			# print user_dict[uid][1]
		if a[1] != 0 :
			user_dict[uid][30] += 1
		if a[2] != 0 :
			user_dict[uid][32] += 1
		if a[3] != 0 :
			user_dict[uid][34] += 1 

	file = open("12-17-00.txt","w")
	for i in range(len(user)):
		file.write(user[i])
		for j in range(0,36):
			file.write('\t'+str(user_dict[user[i]][j]))
		file.write('\n')
	file.close()

# date = raw_input("date: ")
func_feature_1()

# func_user()