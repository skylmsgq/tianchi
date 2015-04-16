# -*- coding: utf-8 -*- 

import time
import gzip
import fileinput

def func1():
	imap = {}
	for line in gzip.open("tianchi_mobile_recommend_train_item.txt.gz"):
		imap[line.split(",")[0]] = True
	file = open("data_filter.txt","w")
	c = 0
	for line in gzip.open("data.txt.gz"):
		c += 1
		print c
		(uid, iid, ict) = line.strip().split("\t")[0].split(" ")
		if imap.has_key(iid):
			file.write(line)
	file.close()

def func2():
	time_start = time.time()
	target = "12-19-0"
	imap = {}
	c = 0
	for line in fileinput.input("uid_iid_filter.txt"):
		c += 1
		print c
		(uid, iid, ict), items = line.strip().split("\t")[0].split(" "), [(int((time.mktime(time.strptime('2014-'+target,'%Y-%m-%d-%H'))-time.mktime(time.strptime('2014-'+i.split(",")[0],'%Y-%m-%d-%H')))/(24*3600))+1, int(i.split(",")[1])) for i in line.strip().split("\t")[1].split(" ")]
		b0, b1, b2, b3 = filter(lambda x:x[0]<=7 and x[1]==1, items), filter(lambda x:x[0]<=7 and x[1]==2, items), filter(lambda x:x[0]<=7 and x[1]==3, items), filter(lambda x:x[0]<=7 and x[1]==4, items)
		if not imap.has_key(iid): imap[iid] = [0.0,0.0,0.0,0.0]
		if len(b0) != 0: imap[iid][0] += 1
		if len(b1) != 0: imap[iid][1] += 1
		if len(b2) != 0: imap[iid][2] += 1
		if len(b3) != 0: imap[iid][3] += 1
		# print (uid, iid, ict), items
	fileinput.close()
	for key in imap:
		print key, imap[key], [float(imap[key][3])/max(imap[key][3],b) if imap[key][3]!=0 else 0 for b in imap[key][:3]]
	time_end = time.time()
	print time_end-time_start
	pass

# func1()
func2()

