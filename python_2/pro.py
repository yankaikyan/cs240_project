import sys
import traceback
from par_sim import partition_similarity
from pyspark import SparkContext
from pro1 import Doc
from pro2 import Group
from sim_self import self_similarity
from datetime import datetime
print str(datetime.now())


Tresh = 0.6

nop = 1000

#store tf_idf
documents = open("./Data/part10000.txt", 'r')
line = documents.readline()
line_count = 0
while line !='':
	line_count = line_count + 1
	line = documents.readline()
documents.close()

documents = open("./Data/part10000.txt", 'r')
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
for i in range(0, len(doc_sort), nop):
	partition.append(Group(i/nop, doc_sort[i:i+nop]))
	partition_used.append(Group(i/nop, doc_sort[i:i+nop]))
	
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

sc = SparkContext()
i_j = []
similar = []
self = []
#get whole tuple list for partition similarity
for i in range(len(partition)):
    self.append((str(i),partition[i]))
    for j in range(i):
#        i_j = partition[i].maxweight + partition[j].maxweight
       
        sum  = 0
        for k in range(len(partition[i].maxweight)):
            for l in range(len(partition[j].maxweight)):
                if partition[i].maxweight[k][0] == partition[j].maxweight[l][0]:
                    sum = sum + partition[i].maxweight[k][1] * partition[j].maxweight[l][1]
        """
        input_rdd = sc.parallelize(i_j)
        counts = input_rdd.reduceByKey(lambda x, y: x*y)
        sum = 0
        for k in range(len(counts.collect())):
            if counts.collect()[k] not in i_j:
                sum = sum + counts.collect()[k][1]
        print sum
        """
        if sum >= Tresh:
            similar.append((str(i),partition[j]))
#        del i_j [:]
#        del counts.collect() [:]
    self_rdd = sc.parallelize(self)
    similar_rdd = sc.parallelize(similar)
#print similar_rdd.collect()
#print self_rdd.collect() 
b = similar_rdd.join(self_rdd)
a = partition_similarity(b).compare().collect()
c = self_similarity(self_rdd).compare().collect()
#for i in range(len(a.collect())):
#    print a.collect()[i]
#for i in range(len(c.collect())):
#    print c.collect()[i]
print "now print"

#print c

#print a

print str(datetime.now())







