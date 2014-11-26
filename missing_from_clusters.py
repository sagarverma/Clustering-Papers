from pymongo import MongoClient

client = MongoClient()
db = client.paper
matched = db.matched
doc_word_map = db.doc_word_map

missing = []
docs = doc_word_map.find()
for doc in docs:
	if matched.find_one({'_id':doc['_id']}) == None:
		missing.append(doc['_id'])
		print doc['_id']

fout = open('D:\paper\missing_from_cluster.lst','w')
fout.write(str(missing))
fout.close()