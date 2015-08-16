import MapReduce
import sys

"""
Relational Join
"""

mr = MapReduce.MapReduce()


def mapper(record):
    key = record[1]
    mr.emit_intermediate(key, record)


def reducer(key, list_of_values):
    for v1 in list_of_values:
        for v2 in list_of_values:
            if v1[0] == "order" and v2[0] == "line_item":
                mr.emit(v1+v2)


if __name__ == '__main__':
   inputdata = open(sys.argv[1])
   mr.execute(inputdata, mapper, reducer)
    