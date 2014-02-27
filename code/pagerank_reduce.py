#!/usr/bin/env python2.5

import sys

from line_consumers import try_consume, NodeConsumer, CouponConsumer
from line_producers import PRReduceProducer

if __name__=='__main__':

    # define list of line consumers for each type of input line we can receive
    nc, cc = NodeConsumer(),  CouponConsumer()
    consumers = [cc, nc]

    # iterate over input, consuming lines as we go
    for line in sys.stdin:
        if not try_consume(consumers, line):
            # raise an exception if no consumer wants the line
            raise Exception("Line wasn't consumed at all!!:\n"+line)
    
    # now that we've parsed the lines, emit output tuples
    PRReduceProducer(nc,cc).produce()
