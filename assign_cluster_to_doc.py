from pymongo import MongoClient

client = MongoClient()
db = client.paper

doc_cluster_map = db.doc_cluster_map
clusters = db.clusters

curs = clusters.find()

for cur in curs:
	docs = cur['docs']
	for doc in docs:
		if doc_cluster_map.find_one({'_id':doc}) == None:
			doc_cluster_map.insert({'_id':doc,'this':[{'cid':cur['_id'],'cids':cur['cids'],'mid':cur['mid']}]})
		else:
			this = cur['this']
			cur['this'].append({'cid':cur['_id'],'cids':cur['cids'],'mid':cur['mid']})
			doc_cluster_map.update({'_id':doc,cur,True})
			