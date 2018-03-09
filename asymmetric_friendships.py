import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
	mr.emit_intermediate(record[0],(record[0],record[1]))
	mr.emit_intermediate(record[1],(record[0],record[1]))


def reducer(key, list_of_values):
	for i in list_of_values:
		if (i[1],i[0]) not in list_of_values:
			if i[0] == key: # remove duplicate
				mr.emit((i[1],i[0]))



inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
