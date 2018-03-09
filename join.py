import MapReduce
import sys


mr = MapReduce.MapReduce()

def mapper(record):
    mr.emit_intermediate(record[1],record)

def reducer(key, list_of_values):
    for i in list_of_values:
        if i[0] == 'order':
            for j in list_of_values:
                if j[0] == 'line_item':
                    mr.emit(i + j) #merge two lists

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

