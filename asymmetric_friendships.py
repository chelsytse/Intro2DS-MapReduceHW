import MapReduce
import sys

"""
Social Network: Find Asymmetric Friendships
Learn from https://github.com/calvdee/coursera-data-sci/blob/master/assignment3/problem4.py
"""

mr = MapReduce.MapReduce()

def mapper(record):
    person=record[0]
    friend=record[1]
    mr.emit_intermediate(person, [person,friend])
    mr.emit_intermediate(friend, [friend,person])

def reducer(key, list_of_values):
    pairs = [tuple(v) for v in list_of_values]
    allset = set(pairs)
    dupset = set([p for p in pairs if pairs.count(p)>1])
    asymset = allset.difference(dupset)
    map(lambda x: mr.emit(list(x)), asymset)
    
                

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
