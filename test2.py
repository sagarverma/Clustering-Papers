from pymongo import MongoClient
from nltk import FreqDist
import time

tot_time = 0

client = MongoClient()

db = client.paper
doc_word_map = db.doc_word_map
word_doc_map = db.word_doc_map
doc_doc_map = db.doc_doc_map

count = 0
TOTAL = 22724405

for doc in doc_word_map.find():
	count += 1
	ts = time.clock()
	doc_id = doc['_id']
	doc_words = doc['word']
	all_related = []
	for word in doc_words:
		temp = word_doc_map.find_one({'_id':word})
		if temp != None:
				all_related += temp['docid']
	doc_doc_map.insert({'_id':doc_id,'rel': dict(FreqDist(all_related).most_common())})
	te = time.clock
	td += te-ts

	time_left = (tot_time/(count * 1.0))*(TOTAL - count)

	if td < .001:
		print str(time_left) + "us"
	elif td < 1:
		print str(time_left) + "ms"
	elif td < 60:
		print str(time_left) + "s"
	elif td < 3600:
		print str(time_left/60) + "m"
	elif td < 86400:
		print str(time_left/3600) + "h"
	
