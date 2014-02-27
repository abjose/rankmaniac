#!/usr/bin/env python

import sys

from line_consumers import try_consume, InitConsumer, NodeConsumer
from line_producers import PRMapProducer

if __name__=='__main__':

    # define list of line consumers for each type of input line we can receive
    # should think about ordering...
    ic, nc = InitConsumer(), NodeConsumer()
    consumers = [nc, ic]

    # iterate over input, consuming lines as we go
    for line in sys.stdin:
        if not try_consume(consumers, line):
            # raise an exception if no consumer wants the line
            raise Exception("Line wasn't consumed at all!!:\n"+line)

    # TODO: add ConverProducer -- or could just do in pre-existing producer?
    # now that we've parsed the lines, emit output tuples
    if ic.nodes: PRMapProducer(ic).produce()
    else:        PRMapProducer(nc).produce()

