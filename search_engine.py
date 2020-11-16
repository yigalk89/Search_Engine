from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
from os import listdir
from ziplnew import Zipf

from os.path import isfile, join
import  pandas as pd


def run_engine():
    """
    :return:
    """
    number_of_documents = 0

    config = ConfigClass(r"C:\Users\User\Desktop\project_reterive_info\Data")
    #onlyfiles = [f for f in listdir("{}\{}".format(config.get__corpusPath(),f)) if isfile(join(config.get__corpusPath(), f) )]
    onlyfiles = ["{}\{}".format(config.get__corpusPath(),f)  for f in listdir(config.get__corpusPath()) ]
    #print(onlyfiles)


    r = ReadFile(corpus_path=config.get__corpusPath())
    documents_list = []
    p = Parse()
    indexer = Indexer(config)
    cnt=0
    for file in onlyfiles:

        if(cnt<2):
         for f in listdir(file):
          documents_list = documents_list + (r.read_file(file_name="{}\{}".format(file,f)))
        else:
            break
    cnt = cnt + 1

    print(documents_list[0:2])
    '''''''''
    for i in (documents_list[0]):
       print(i[1][0])
       ##for j in  i[1] :
        ##print (j)
    '''''''''


    #pd.to_csv(documents_list['full_text'][0:1])
#   '''''''''
    # Iterate over every document in the file

    for idx, document in enumerate(documents_list):
        # parse the document
        #print(document)
        parsed_document = p.parse_doc(document)
        number_of_documents += 1
        # index the document data
        indexer.add_new_doc(parsed_document)

    print('Finished parsing and indexing. Starting to export files')

    zp = Zipf(indexer.inverted_idx)
    zp.parse_uplowchar()

    utils.save_obj(indexer.inverted_idx, "inverted_idx")
    utils.save_obj(indexer.postingDict, "posting")



def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")


    return inverted_index

newDict = { key:value for (key,value) in utils.load_obj("inverted_idx").items() if value> 1}



'''''''''
for  i in utils.load_obj("inverted_idx").keys():
    if utils.load_obj("inverted_idx")[i] >1:
        newdc[i] = utils.load_obj("inverted_idx")[i]

print(newdc)
'''''''''

def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)

 #   '''''''''
def main():
    run_engine()
    query = input("Please enter a query: ")
    k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    for doc_tuple in search_and_rank_query(query, inverted_index, k):
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
