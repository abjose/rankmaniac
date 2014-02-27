#!/usr/bin/env python2.5

import sys

from line_consumers import try_consume, RankConsumer
from line_producers import ProcessReduceProducer

if __name__=='__main__':

    # define list of line consumers for each type of input line we can receive
    rc = RankConsumer()
    consumers = [rc]

    # iterate over input, consuming lines as we go
    for line in sys.stdin:
        if not try_consume(consumers, line):
            # raise an exception if no consumer wants the line
            raise Exception("Line wasn't consumed at all!!:\n"+line)
    
    # now that we've parsed the lines, emit output tuples
    ProcessReduceProducer(rc).produce()
