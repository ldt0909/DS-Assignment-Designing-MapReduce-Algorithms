import MapReduce
import sys


# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
    '''
    Input:
    record: a 2 elements list,[doc_id,text]
    doc_id: string, document identifier 
    text: string, text of the document
    '''
    doc_id = record[0]
    doc_text = record[1]
    for word in doc_text.split():
        mr.emit_intermediate(word, doc_id)

# Part 3
def reducer(key, list_of_values):
    '''
    Output:
    tuple(word, doc_id list)
    word: string
    doc_id list: list of strings
    '''
    result = []
    for doc_id in list_of_values:
        if doc_id not in result:
            result.append(doc_id)
    mr.emit((key, result))

# Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

