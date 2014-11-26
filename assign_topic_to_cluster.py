from  pymongo import MongoClient
from nltk import FreqDist

client = MongoClient()
db = client.paper
clusters = db.clusters
doc_word_map = db.doc_word_map

cluster_topic = db.cluster_topic

fin = open('D:\paper\cluster_id_size.csv','r')
data = fin.read().split('\n')
cluster_ids = []
for d in data[:-1]:
	t = map(int, d.split())
	if t[1] < 1000:
		cluster_ids.append(t[0])

for cluster_id in cluster_ids:
	nodes = clusters.find_one({'_id':cluster_id})['docs']
	words = []
	for node in nodes:
		words += doc_word_map.find_one({'_id':node})['word']
	freqdist = FreqDist(words)
	tops = freqdist.most_common()[:30]
	topics = []
	for top in tops:
		topics.append(top[0])
	ctid = cluster_topic.insert({'_id':cluster_id,'topic':topics})
	print ctid