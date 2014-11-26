from pymongo import MongoClient

client = MongoClient()
db = client.paper
clusters = db.clusters
doc_word_map = db.doc_word_map
word_doc_map = db.word_doc_map

seen = {}
childrens = []
cluster_id = clusters.count()

to_cluster = []

fin = open("D:\paper\cluster_id_size.csv","r")
data = fin.read().split('\n')
for d in data:
	t = map(int, d.split())
	if t[1] >= 5000:
		to_cluster.append(t[0])

for tc in to_cluster:
	cur = clusters.find_one({'_id':tc})
	mid = cur['_id']
	nodes = cur['docs']
	for node in nodes:
		if node not in seen:
			words = doc_word_map.find_one({'_id':node})['word']
			temp = word_doc_map.find({'_id':{'$in':words},'count':{'$gt':50,'$lt':100000}}).sort('count')
			all_matched_docs = []
			for t  in temp:
				all_matched_docs.append(t['docid'])
			temp = all_matched_docs
			all_matched_docs = []
			maximal_matches = set([])
			for i in range(0,len(temp),2):
				t = []
				if len(temp) == i + 1:
					t = set(temp[i])
				else:
					t = (set(temp[i])).intersection(set(temp[i+1]))
				if len(t) >= 100:
					maximal_matches = maximal_matches.union(t)
			for match in maximal_matches:
				if match not in seen:
					seen[match] = 1
			cluster_id += 1

			cid = clusters.insert({'_id':cluster_id,'docs':list(maximal_matches),'mid':mid})
			print "new cluster added " + str(cid)
			childrens.append(cluster_id)
	cur['cids'] = childrens
	update_info = clusters.update({'_id':mid},cur, True)
	seen = {}
	childrens = []
	print "master updated " + str(update_info) + " " + str(mid)