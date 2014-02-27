#!/usr/bin/env python

import sys
import operator
from random import choice, random
    
# TODO: enough common functionality to justify superclass?
#       everything pretty similar except for inside loop of produce...
# TODO: consider replacing 'random choice' with something from numpy
# TODO: make sure to remove "smallT" stuff

def make_node(u, rank, time, visits, coupons, neighbors):
    # create a node description string
    return (u + "\tnode," 
            + str(rank) + "," 
            + str(time) + "," 
            + str(visits) + ","
            + str(coupons) + "," 
            + ",".join(neighbors)+"\n")

class PRMapProducer:
    # takes InitConsumer OR NodeConsumer
    # and emits tuples of the form(s)
    # <u, ('node', prev_rank, T, visits, coupons, v1,v2,...)>
    # <v, 'coupon'>

    def __init__(self, init_or_nodeconsumer):
        self.num_coupons = 10 # coupons to produce per node
        self.alpha = 0.9
        self.nc = init_or_nodeconsumer

    def produce(self):
        # initialize tuple arrays so can dump to stdout all at once
        node_tuples   = []
        coupon_tuples = []

        # iterate through inputs
        for (u, rnk, T, vst, cpn, neighbors) in self.nc.nodes:
            # first construct new node output - only need one per node
            node_tuples.append(make_node(u, rnk, T, vst, cpn, neighbors))

            # pass on old coupons
            for i in range(cpn):
                if random() < self.alpha:
                    coupon_tuples.append(choice(neighbors) + "\tcoupon\n")

            # generate new coupons
            coupon_tuples += [choice(neighbors) + "\tcoupon\n"
                              for i in range(self.num_coupons)]

        # write everything to stdout
        sys.stdout.write("".join(node_tuples)+"".join(coupon_tuples))


class PRReduceProducer:
    # takes NodeConsumer, CouponConsumer
    # and emits tuples of the form(s)
    # <u, ('node', prev_rank, T, new_visits, new_coupons, v1,v2,...)>

    def __init__(self, nodeconsumer, couponconsumer):
        self.nc = nodeconsumer
        self.cc = couponconsumer
        self.coupon_scale = 0.000001

    def produce(self):
        # init node tuples so can print all at once
        node_tuples = []

        # iterate over nodes
        for (u, rnk, T, vst, cpn, neighbors) in self.nc.nodes:
            # construct and append new node
            coupon_count = self.cc.coupon_counts.get(u,0)
            node_tuples.append(make_node(u, rnk, T, 
                                         vst + coupon_count*self.coupon_scale, 
                                         coupon_count, neighbors))

        # write everything to stdout
        sys.stdout.write("".join(node_tuples))


class ProcessMapProducer:
    # takes NodeConsumer
    # and emits tuples of the form(s)
    # <'rank', (u, prev_rank, T, visits, coupons, v1,v2,...)>

    def __init__(self, nodeconsumer):
        self.nc = nodeconsumer

    def produce(self):
        # init rank tuples so can print all at once
        rank_tuples = []

        # iterate over nodes
        for (u, rnk, T, vst, cpn, neighbors) in self.nc.nodes:
            # construct and append new rank tuple
            rank_tuples.append("rank\t" 
                               + u + "," 
                               + str(rnk) + ","
                               + str(T) + ","
                               + str(vst) + ","
                               + str(cpn) + ","
                               + ",".join(neighbors)+"\n")
        
        # write everything to stdout
        sys.stdout.write("".join(rank_tuples))


class ProcessReduceProducer:
    # takes RankConsumer
    # and emits tuples of the form(s)
    # <'FinalRank:'rank, u> (if converged)
    # <u, ('node', prev_rank, T, visits, coupons, v1,v2,...)> (otherwise)

    def __init__(self, rankconsumer):
        self.rc = rankconsumer
        self.target_reps = 5

    def produce(self):
        # make tuple list for end (for holding either kind of tuples)
        tuples = []

        # convert best_ranks to a dict for easy lookup - need to reverse tuples
        ranks = dict([(ID, rank) 
                      for rank, (visits,ID) in enumerate(self.rc.best_ranks)])

        # now update rank-repetition counts (only care if in top 20)
        for u, new_rank in ranks.items():
            (old_rank, T, vst, cpn, neighbors) = self.rc.nodes[u]
            if int(new_rank) == int(old_rank): T = int(T) + 1
            self.rc.nodes[u] = [new_rank, str(T), vst, cpn, neighbors]

        # set other repetition counts to 0 - TODO: sorta inefficient
        for u, (rnk, T, vst, cpn, neighbors) in self.rc.nodes.items():
            if u not in ranks:
                self.rc.nodes[u] = (-1, 0, vst, cpn, neighbors)
                
        # check to see if top 20 have been the same for long enough
        #sys.stderr.write(str(ranks)+'\n')
        #sys.stderr.write(str(self.rc.nodes)+'\n')
        #smallT = 999
        converged = True        
        for u in ranks:
            (rnk, T, vst, cpn, neighbors) = self.rc.nodes[u]
            #sys.stderr.write(u + ' ' + str(rnk) + ' ' + T + '\n')
            #if int(T) < smallT:
            #    smallT = int(T)
            if int(T) < self.target_reps:
                converged = False
                break
        #sys.stderr.write("Smallest T: " + str(smallT) + "\n")

        # if have 'converged', print out FinalRanks
        if converged:
            # need to sort ranks first, from high to low
            sorted_ranks = sorted(ranks.iteritems(), 
                                  key=operator.itemgetter(1), 
                                  reverse=True)
            # then create tuples
            for u, rank in sorted_ranks:
                (rnk, T, vst, cpn, neighbors) = self.rc.nodes[u]
                tuples.append("FinalRank:" + str(vst) + "\t" + u + "\n")

        # otherwise, print nodes as usual
        else:
            for u, (rnk, T, vst, cpn, neighbors) in self.rc.nodes.items():
                tuples.append(make_node(u, rnk, T, vst, cpn, neighbors))
            
        # write everything to stdout
        sys.stdout.write("".join(tuples))


class TemplateProducer:
    # takes ...
    # and emits tuples of the form(s)
    # <...>

    def __init__(self):
        pass

    def produce(self):
        pass
