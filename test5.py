from pymongo import MongoClient
from ast import literal_eval

client = MongoClient()
db = client.paper
clusters = db.clusters
sub_clusters = db.sub_clusters

cluster_ids = literal_eval(open('D:/paper/cluster_ids.csv','r').read())

sub_cluster_id = 0

for i in range(0,len(cluster_ids),100):
	group = cluster_ids[i:i+100]
	for j in range(0,len(group)):
		a = set(clusters.find_one({'_id':cluster_ids[j]})['docs'])
		for k in range(j,len(group)):
			b = set(clusters.find_one({'_id':cluster_ids[k]})['docs'])
			temp = a.intersection(b)
			if len(temp) > 1000:
				sub_cluster_id += 1
				sid = sub_clusters.insert({'_id':sub_cluster_id,'cid':group[j],'docs':list(temp)})
				print sid
		print "done " + str(j)