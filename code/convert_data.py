
import sys

# converts data from (from, to) edge pair tuples to the desired format

if __name__=='__main__':

    edges = dict()

    for line in sys.stdin:
        if "#" in line: continue
        line = line.strip()
        line = line.split('\t')
        a,b  = line[0], line[1]
        edges[a] = edges.get(a,[]) + [b]

    sys.stdout.write(["NodeId:"+u + "\t1.0,0.0,"+",".join(neighbors) + "\n"
                      for u,neighbors in edges.items()])
