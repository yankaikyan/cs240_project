from pyspark import SparkContext
Tresh = 0.6
class partition_similarity(object):

    def __init__(self, input_rdd):
        self.input_rdd = input_rdd

    def compare(self):
        out = self.par_sim(self.input_rdd)
        return out
    
    @staticmethod
    def par_sim(input_rdd):

        def similarity(line):
  	      	
#            print "now enter partitions comparison"	    
	    #documents similarity
       	    files = []	
                
            a_b = []
            partition_i = line[1][0]
            partition_j = line[1][1]

    	    #sc = SparkContext()							
    	    #for i in range(len(partition)):
            """ 
            for i in range(len(partition_i.file_list)):
            	disimilarPartition = []
                print ("******")
                print (partition_i.file_list[i].name)
                print ("&&&&&&")
            	for j in range(i):
            		inner_product = 0;
            		for k in range(len(partition_i.file_list[i].tf_idf)):
            			for l in range(len(partition_i.file_list[j].tf_idf)):
            				if partition_i.file_list[i].tf_idf[k][0] == partition_i.file_list[j].tf_idf[l][0]:
            					inner_product += float(partition_i.file_list[i].tf_idf[k][1])*float(partition_i.file_list[j].tf_idf[l][1])
            		#print(inner_product)
            		if inner_product <= Tresh:
            			files.append((partition_i.file_list[i].name, partition_i.file_list[j].name))            
	    """ 
            """
            for j in range(len(partition_i.file_list)):
                for k in range(j):
                    a_b = partition_i.file_list[j].tf_idf + partition_i.file_list[k].tf_idf
                    doc_rdd = sc.parallelize(a_b)
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
            sc.stop()
            """
            for o in range(len(partition_i.subgroup)):
                if o != len(partition_i.subgroup)-1:
                    if o <= int(partition_j.name):
                        for p in range(len(partition_j.file_list)):
                            rj = partition_j.file_list[p].one_norm
                            score = [0]*len(partition_i.subgroup[o])
                            for r in range(len(partition_i.subgroup[o])):
    	    		        store_key = []
                                for s in range(len(partition_i.subgroup[o][r].tf_idf)):
    	    			    store_key.append(partition_i.subgroup[o][r].tf_idf[s][0])
    	    		        for q in range(len(partition_j.file_list[p].tf_idf)):
    	    		            wj = 0
    	    		            if partition_j.file_list[p].tf_idf[q][0] in store_key:
    	    		                if score[r] + max(partition_i.subgroup[o][r].value)*rj < Tresh:
                                            break
    	    		                else:
    	    		                    for s in range(len(partition_i.subgroup[o][r].tf_idf)):
    	    			                if partition_j.file_list[p].tf_idf[q][0] == partition_i.subgroup[o][r].tf_idf[s][0]:
    	    				            score[r] = score[r] + partition_i.subgroup[o][r].tf_idf[s][1]*partition_j.file_list[p].tf_idf[q][1]
    	    				            wj = partition_j.file_list[p].tf_idf[q][1]
    	    		        rj = rj - wj
    	    		    for t in range(len(partition_i.subgroup[o])):
    	    		        if score[t] >= Tresh:
				    files.append((partition_i.subgroup[o][t].name, partition_j.file_list[p].name))
            
                else:
                    for p in range(len(partition_j.file_list)):
                            rj = partition_j.file_list[p].one_norm
                            score = [0]*len(partition_i.file_list)
                            for r in range(len(partition_i.subgroup[o])):
                                store_key = []
                                for s in range(len(partition_i.subgroup[o][r].tf_idf)):
                                    store_key.append(partition_i.subgroup[o][r].tf_idf[s][0])
                                for q in range(len(partition_j.file_list[p].tf_idf)):
                                    wj = 0
                                    if partition_j.file_list[p].tf_idf[q][0] in store_key:
                                        if score[r] + max(partition_i.subgroup[o][r].value)*rj < Tresh:
                                            break
                                        else:
                                            for s in range(len(partition_i.subgroup[o][r].tf_idf)):
                                                if partition_j.file_list[p].tf_idf[q][0] == partition_i.subgroup[o][r].tf_idf[s][0]:
                                                    score[r] = score[r] + partition_i.subgroup[o][r].tf_idf[s][1]*partition_j.file_list[p].tf_idf[q][1]
                                                    wj = partition_j.file_list[p].tf_idf[q][1]
                                rj = rj - wj
                            for t in range(len(partition_i.subgroup[o])):
                                if score[t] >= Tresh:
                                    files.append((partition_i.subgroup[o][t].name, partition_j.file_list[p].name)) 
        
            #files.append((1, 1))
            return files
        output = input_rdd.map(similarity) 
        return output    	    									
    	    									
       	    
	             
