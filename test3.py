from pymongo import MongoClient
from nltk import FreqDist

client = MongoClient()
db = client.paper
doc_word_map = db.doc_word_map
word_doc_map = db.word_doc_map
clusters = db.clusters
matched = db.matched

cluster_id = clusters.count()

for doc in doc_word_map.find():
	#print matched.find_one({'_id':doc['_id']})
	if matched.find_one({'_id':doc['_id']}) == None:
		cluster_id += 1
		words = doc['word']
		all_matched_docs = []
		for word in words:
			temp = word_doc_map.find_one({'_id':word})
			if temp != None:
				all_matched_docs += temp['docid']
		all_matched_docs += [doc['_id']]
		all_matched_docs = list(set(all_matched_docs))
		for match in all_matched_docs:
			if matched.find_one({'_id':match}) == None:
				matched.insert({'_id':match})
		cluster_id = clusters.insert({'_id':cluster_id,'docs':all_matched_docs})
		print cluster_id