from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import sys
import os
from orchestrate_parsing import parse_wrapper
import datetime as dt


def run_engine(corpus_path='', output_path='.', stemming=False):
    """

    :return:
    """

    config = ConfigClass(corpus_path, stemming, output_path)
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse()

    tweets_parsed = parse_wrapper(r, p, config)
    print("total parse {} tweets".format(tweets_parsed))


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


def main(corpus_path='', output_path='.', stemming=False, queries='', num_docs_to_retrive=0):
    start = dt.datetime.now()
    run_engine(corpus_path, output_path, stemming)
    # query = input("Please enter a query: ")
    # k = int(input("Please enter number of docs to retrieve: "))
    k = num_docs_to_retrive
    inverted_index = load_index()
    for query in queries:

        for doc_tuple in search_and_rank_query(query, inverted_index, k):
            print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
    end = dt.datetime.now()
    total_time = (end - start).total_seconds()
    print("Total runing time was {} minutes".format(total_time / 60.0))

if __name__ == "__main__":
    main(*sys.argv[1:])
