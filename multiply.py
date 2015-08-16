import MapReduce
import sys

"""
Matrix Multiplication
Have to check inputdata to find out the size of A and B
"""

mr = MapReduce.MapReduce()
a_rownum = 5
b_colnum = 5

def mapper(record):
    matrix = record[0]
    i = record[1]
    j = record[2]
    value = record[3]
    if matrix == 'a':
        for k in range(b_colnum):
            mr.emit_intermediate((i,k), record)
    else:
        for k in range(a_rownum):
            mr.emit_intermediate((k,j), record)
        

def reducer(key, list_of_values):
    alist = [v for v in list_of_values if v[0]=='a']
    blist = [v for v in list_of_values if v[0]=='b']
    thissum = 0
    for va in alist:
        for vb in blist:
            if va[2] == vb[1]:
                thissum += va[3]*vb[3]
    output = list(key)
    output.append(thissum)
    mr.emit(tuple(output))
    

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
    