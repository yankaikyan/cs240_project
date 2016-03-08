import sys
import traceback
from utils.test_utils import FakeRDD
Tresh = 0.6
#define documents class
class Doc:
	def __init__(self, name, one_norm, tf_idf, value):
		self.name = name
		self.one_norm = one_norm
		self.tf_idf = tf_idf
		self.value = value
	def __lt__(self, other):
		return self.one_norm < other.one_norm
		
#define group class
class Group:
	def __init__(self, file_list):
		self.file_list = file_list	
	def setSubgroup(self, subgroup):
		self.subgroup = subgroup
	def setMaxWeight(self, maxweight):
		self.maxweight = maxweight
        def setDissimilar(self, dissimilar):
		self.dissimilar = dissimilar

#store tf_idf
documents = open("110.txt", 'r')
line = documents.readline()
line_count = 0
while line !='':
	line_count = line_count + 1
	line = documents.readline()
documents.close()

documents = open("110.txt", 'r')
line = documents.readline()
t = 0
data = [[] for row in range(line_count)]
while line != '':
	a = line.split()
	for i in range(0, len(a)-1, 2):
		data[t].append((a[i+1], float(a[i+2])))
	line = documents.readline()
	t = t + 1


#get value list
value = [[] for row in range(line_count)]
for i in range(len(data)):
	for j in range(len(data[i])):
		value[i].append(float(data[i][j][1]))

#calculate one-norm value
one_norm = []
for i in range(len(data)):
	sum = 0
	for j in range(len(data[i])):
		sum = sum + float(data[i][j][1])
	one_norm.append(sum)

doc_sort = []
for i in range(len(data)):
	doc_sort.append(Doc(i, one_norm[i], data[i], value[i]))
doc_sort.sort()

#partitioning
partition = []
partition_used = []
for i in range(0, len(doc_sort), 3):
	partition.append(Group(doc_sort[i:i+3]))
	partition_used.append(Group(doc_sort[i:i+3]))
	
#subgroup	
for i in range(len(partition)):
	list_of_subgroup = []
	for j in range(i+1):
		subgroup = []
		for k in range((len(partition[i].file_list))):
			if j != i:
				if Tresh / max(partition[i].file_list[k].value) >= partition[j].file_list[2].one_norm \
				and Tresh / max(partition[i].file_list[k].value) < partition[j+1].file_list[2].one_norm :
					subgroup.append(partition[i].file_list[k])
					partition_used[i].file_list.remove(partition[i].file_list[k])					
		if j == i:
			for l in range(len(partition_used[i].file_list)):
				subgroup.append(partition_used[i].file_list[l])
		list_of_subgroup.append(subgroup)
	partition[i].setSubgroup(list_of_subgroup)
#print len(partition[2].subgroup[2])

#calculate d_max for each partition
for i in range(len(partition)):
	par = []
	for j in range(len(partition[i].file_list)):
		par = par + partition[i].file_list[j].tf_idf
	#print ([(x, max(y[1] for y in par if y[0]==x)) for x in set(y[0] for y in par)])
	max_weight = [(x, max(y[1] for y in par if y[0]==x)) for x in set(y[0] for y in par)]
	partition[i].setMaxWeight(max_weight)
	del par[:]


i_j = []
#compare different partitions
for i in range(len(partition)):
	dissimilar = []
	for j in range(i):
		i_j = partition[i].maxweight + partition[j].maxweight
	        input_rdd = FakeRDD(i_j)
                counts = input_rdd.reduceByKey(lambda x, y: x*y)
                sum = 0
                for k in range(len(counts.collect())):
			if counts.collect()[k] not in i_j:
				sum = sum + counts.collect()[k][1]
		print sum		
                if sum <= Tresh:
			dissimilar.append(j)        
		del i_j [:]
                del counts.collect()[:]
	partition[i].setDissimilar(dissimilar)

#documents similarity
a_b = []
for i in range(len(partition)):
	for j in range(len(partition[i].file_list)):
		for k in range(j):
			a_b = partition[i].file_list[j].tf_idf + partition[i].file_list[k].tf_idf
			doc_rdd = FakeRDD(a_b)
			doc_cnt = doc_rdd.reduceByKey(lambda x, y: x*y)
			sum = 0
			for l in range(len(doc_cnt.collect())):
				if doc_cnt.collect()[l] not in a_b :
						sum = sum + doc_cnt.collect()[k][1]
			print sum
			if sum > Tresh:
				print "document[%d] and document[%d] are similar", i, j
			del a_b [:]
			del doc_cnt.collect() [:]

	for n in range(i):
	 	if n not in partition[i].dissimilar:
			for o in range(len(partition[i].subgroup)):
				if o != len(partition[i].subgroup)-1:
					if o <= n:
						for p in range(len(partition[n].file_list)):
							rj = partition[n].file_list[p].one_norm
							score = [0]*len(partition[i].subgroup[o])
							store_key = []
							for r in range(len(partition[i].subgroup[o])):
								for s in range(len(partition[i].subgroup[o][r].tf_idf)):
									store_key.append(partition[i].subgroup[o][r].tf_idf[s][0])
								for q in range(len(partition[n].file_list[p].tf_idf)):
									wj = 0
									if partition[n].file_list[p].tf_idf[q][0] in store_key:
										if score[r] + max(partition[i].subgroup[o][r].value)*rj < Tresh:
											 print "%s is dissimilar to %s", partition[n].file_list[p].name, partition[i].subgroup[o][r].name
										else:
											for s in range(len(partition[i].subgroup[o][r].tf_idf)):
												if partition[n].file_list[p].tf_idf[q][0] == partition[i].subgroup[o][r].tf_idf[s][0]:
													score[r] = score[r] + partition[i].subgroup[o][r].tf_idf[s][1]*partition[n].file_list[p].tf_idf[q][1]
									 				wj = partition[n].file_list[p].tf_idf[q][1]
									rj = rj - wj
								for t in range(len(partition[i].subgroup[o].file_list)):
									if score[t] >= Tresh:
										print "%s is similar to %s", partition[i].subgroup[o].file_list[t].name, partition[n].file_list[p].name
									
									
										
