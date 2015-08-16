import MapReduce
import sys

"""
Inverted Index
"""

mr = MapReduce.MapReduce()


def mapper(docs):
    key = docs[0]
    value = docs[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, key)
        

def reducer(key, list_of_values):
    idlist = []
    for v in list_of_values:
        idlist.append(v)
    mr.emit((key,idlist))


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
    