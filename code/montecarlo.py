

import sys
import networkx as nx
from random import choice, random
import operator


def random_walk(graph, node, eps, count_dict):
    curr_node = node
    #for i in range(length):
    while True:
        #if random() < eps and graph.successors(curr_node): 
        if random() < eps: 
            if graph.successors(curr_node): 
                # if should transition and have successors, transition
                curr_node = choice(graph.successors(curr_node))
            count_dict[curr_node] = count_dict.get(curr_node,0.) + 1.
        # hmm, see what happens if transition to self if no successors...
        else:
            # otherwise, terminate (could count here if wanted)
            # do both? lol
            #count_dict[curr_node] = count_dict.get(curr_node,0.) + .75
            return
        # count visits
        #count_dict[curr_node] = count_dict.get(curr_node,0.) + 1.

# add some kind of convergence condition?

if __name__=='__main__':
    
    G = nx.DiGraph()

    for line in sys.stdin:
        # split data up
        line = line.strip()
        ID,data = line.split('\t')
        data = data.split(',')
        # extract actual NodeID...not super important...
        ID = int(ID.split(':')[1])
        # extract pageranks and neighbors from data
        currPR, prevPR = data[0], data[1]
        neighbors = map(int, data[2:])
        # update node dictionary
        #self.nodes[ID] = [currPR, prevPR, neighbors]
        G.add_edges_from([ID, n] for n in neighbors)

    #iterations = len(G)
    iterations = 50
    walks = 50
    eps = 0.85
    count_dict = {}
    # run actual algorithm
    for i in range(iterations):
        for node in G.nodes():
            for walk in range(walks):
                random_walk(G, node, eps, count_dict)

    # print out rankings
    sorted_ranks = sorted(count_dict.iteritems(), 
                          key=operator.itemgetter(1),
                          reverse=True)
    for ID,count in sorted_ranks[:20]:
        print ID,'\t',count
