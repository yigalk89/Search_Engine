from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import sys
import os

def run_engine(corpus_path='', output_path='', stemming=False):
    """

    :return:
    """
    number_of_documents = 0

    config = ConfigClass(corpus_path, stemming, output_path)
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse()
    indexer = Indexer(config)

    documents_list = r.read_next_file()
    while documents_list:

        # Iterate over every document in the file
        for idx, document in enumerate(documents_list):
            # parse the document
            parsed_document = p.parse_doc(document)
            number_of_documents += 1
            # index the document data
            indexer.add_new_doc(parsed_document)
        print('Finished parsing and indexing. Starting to export files')

        utils.save_obj(indexer.inverted_idx, "inverted_idx")
        utils.save_obj(indexer.postingDict, "posting")
        documents_list = r.read_next_file()


def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main(corpus_path='', output_path='', stemming=False, queries='', num_docs_to_retrive=0):
    run_engine(corpus_path, output_path, stemming)
    # query = input("Please enter a query: ")
    # k = int(input("Please enter number of docs to retrieve: "))
    k = num_docs_to_retrive
    inverted_index = load_index()
    for query in queries:

        for doc_tuple in search_and_rank_query(query, inverted_index, k):
            print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))


if __name__ == "__main__":
    main(*sys.argv[1:])
