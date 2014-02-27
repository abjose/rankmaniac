#!/usr/bin/env python

from bisect import insort_left

# TODO: could change can_consumes to check more specific part of string

# just a convenience function to make things look nicer
def try_consume(consumers, line):
    # given list of consumers and line, try to consume line
    for consumer in consumers:
        if consumer.can_consume(line):
            consumer.consume(line)
            return True
    return False

# TODO: enough common functionality to justify superclass?

class InitConsumer:
    # consume initial tuples of form
    # <u, (pr_curr, pr_prev, v1, v2, ...)>

    def __init__(self):
        self.nodes = []

    def can_consume(self, line):
        return "NodeId" in line

    def consume(self, line):
        # split data up
        ID,data = line.strip().split('\t')
        data = data.split(',')
        # extract actual NodeID...not super important...
        ID = ID.split(':')[1]

        # extract pageranks and neighbors from data
        currPR, prevPR = data[0], data[1]
        neighbors = data[2:]
        # test out adding self-edge...
        if not neighbors: neighbors = [ID]

        # update node dictionary
        rank    = -1
        time    = 0
        visits  = 0.
        coupons = 0
        self.nodes.append([ID, rank, time, visits, coupons, neighbors])


class NodeConsumer:
    # consume node tuples of form
    # <u, ('node', prev_rank, T, visits, coupons, v1,v2,...)>

    def __init__(self):
        self.nodes = []

    def can_consume(self, line):
        return "node" in line

    def consume(self, line):
        # split data up
        ID,data = line.strip().split('\t')
        data = data.split(',')
        # create node and update node list
        rank, time, visits = data[1], data[2], data[3]
        coupons, neighbors = data[4], data[5:]
        self.nodes.append([ID, int(rank), int(time), 
                           float(visits), int(coupons), 
                           neighbors])


class CouponConsumer:
    # consume coupon tuples of form
    # <u, 'coupon'>

    def __init__(self):
        # can't assume only one type of coupon will be visible
        self.coupon_counts = {}

    def can_consume(self, line):
        return "coupon" in line

    def consume(self, line):
        # split data up
        line = line.strip()
        ID = line.split('\t')[0]

        # increment appropriate count
        self.coupon_counts[ID] = self.coupon_counts.get(ID,0)+1


class RankConsumer:
    # consume rank tuples of form
    # <'rank', (u, prev_rank, T, visits, coupons, v1,v2,...)>

    def __init__(self):
        self.nodes = {} # dict to make it easier to check/update ranks
        self.best_ranks = []

    def can_consume(self, line):
        return "rank" in line

    def consume(self, line):
        # split data up
        line = line.strip()
        name, data = line.split('\t')
        data = data.split(',')

        # read out values
        ID, rank, time, visits = data[0], data[1], data[2], data[3]
        coupons, neighbors = data[4], data[5:]

        # append rank - only keeping if in largest 20 by clipping off smallest
        insort_left(self.best_ranks, [float(visits), ID])
        if len(self.best_ranks) > 20: self.best_ranks = self.best_ranks[1:21]

        # append to node list either way
        self.nodes[ID] = [rank, time, visits, coupons, neighbors]


class TemplateConsumer:
    # consume edge tuples of form
    # <...>

    def __init__(self):
        pass

    def can_consume(self, line):
        pass

    def consume(self, line):
        pass

