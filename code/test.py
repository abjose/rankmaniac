#!/usr/bin/env python


import sys
import os
import operator


# some test code to see convergence over time
# can press enter to see list of NodeID, current q, diff in q

if __name__=='__main__':
    # assume you want lines with "q_tuple" in them in output file
    # could try getting r_tuple from edge? but don't think that's right
    outputs = ["../local_test_data/output1.txt",
               "../local_test_data/output2.txt"]
    old_ranks = {}

    initial_call = "python pagerank_map.py < ../local_test_data/GNPn100p05 | sort | python pagerank_reduce.py | python process_map.py | sort | python process_reduce.py > ../local_test_data/output1.txt"
    #initial_call = "python pagerank_map.py < ../local_test_data/EmailEnron | sort | python pagerank_reduce.py | python process_map.py | sort | python process_reduce.py > ../local_test_data/output1.txt"
    #initial_call = "python pagerank_map.py < ../local_test_data/temp_data.txt | sort | python pagerank_reduce.py | python process_map.py | sort | python process_reduce.py > ../local_test_data/output1.txt"

    update_calls = ["python pagerank_map.py < ../local_test_data/output1.txt | sort | python pagerank_reduce.py | python process_map.py | sort | python process_reduce.py > ../local_test_data/output2.txt",
"python pagerank_map.py < ../local_test_data/output2.txt | sort | python pagerank_reduce.py | python process_map.py | sort | python process_reduce.py > ../local_test_data/output1.txt"]

    # run initial call
    os.system(initial_call)
        
    # run subsequent calls
    i = 0
    while True:
        # call mapreduce code again
        os.system(update_calls[i%2])

        # read output file
        ranks = {}
        with open(outputs[1-i%2],'r') as f:
            for line in f:
                if "node" in line:
                    ID,data = line.split('\t')
                    data = data.split(',')
                    ranks[ID] = float(data[3])

                if "FinalRank" in line:
                    print "TERMINATED."
                    exit(0)

        # sort ranks
        sorted_ranks = sorted(ranks.iteritems(), 
                              key=operator.itemgetter(1),
                              reverse=True)

        # print out new ranks
        print "ITERATION",i
        for ID,q in sorted_ranks[:20]:
            if old_ranks.get(ID) != None:
                print ID,'\t',q,'\t',abs(q-old_ranks.get(ID))
            else:
                print ID,'\t',q,'\t',old_ranks.get(ID)

        # get ready for next step
        old_ranks = ranks.copy() # hmm, deep enough?
        i += 1
        #raw_input("Enter to continue...\n")
        
