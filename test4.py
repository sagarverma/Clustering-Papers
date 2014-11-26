from pymongo import MongoClient
from nltk import FreqDist

client = MongoClient()
db = client.paper
doc_word_map = db.doc_word_map
word_doc_map = db.word_doc_map
tclusters = db.tclusters
tmatched = db.tmatched

cluster_id = tclusters.count()

for doc in doc_word_map.find():
	#print matched.find_one({'_id':doc['_id']})
	if tmatched.find_one({'_id':doc['_id']}) == None:
		words = doc['word']
		all_matched_docs = []
		temp = word_doc_map.find({'_id':{'$in':words},'count':{'$gt':5,'$lt':100000}}).sort('count')

		for t  in temp:
			all_matched_docs.append(t['docid'])

		maximal_matches = set([])
		#print all_matched_docs
		for i in range(0,len(all_matched_docs),2):
			temp = []
			if len(all_matched_docs) == i + 1:
				temp = set(all_matched_docs[i])
			else:
				temp = (set(all_matched_docs[i])).intersection(set(all_matched_docs[i+1]))
			if len(temp) > 10:
				maximal_matches = maximal_matches.union(temp)

		all_matched_docs = list(maximal_matches)
		maximal_matches = None
		if len(all_matched_docs) > 0 and len(all_matched_docs) < 500000:
			cluster_id += 1
			for match in all_matched_docs:
				if tmatched.find_one({'_id':match}) == None:
					tmatched.insert({'_id':match})
			cluster_id = tclusters.insert({'_id':cluster_id,'docs':all_matched_docs})
			print cluster_id