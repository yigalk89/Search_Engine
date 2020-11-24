from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import re
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


def load_index(config):
    print('Load inverted index')
    folder = config.get_save_files_dir()
    inverted_index = utils.load_obj(folder + "/inverted_index")
    return inverted_index


def search_and_rank_query(query, inverted_index, k, config, stemming=False):
    p = Parse()
    query_as_list = p.parse_query(query, stemming)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list, config)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, config, query_as_list)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def parse_queries_from_file(filename):
    file1 = open(filename, 'r', encoding="utf8")
    line = file1.readline()
    queries = []
    while line:
        if line == '\n':
            line = file1.readline()
            continue
        pattern = r"^(\d+\.\s*)(.+)"
        res = re.search(pattern, line)
        queries.append(res.group(2))
        line = file1.readline()
    file1.close()
    return queries


def main(corpus_path='', output_path='.', stemming=False, queries='', num_docs_to_retrive=0):
    start = dt.datetime.now()
    run_engine(corpus_path, output_path, stemming)
    k = num_docs_to_retrive
    config = ConfigClass(corpus_path, stemming, output_path)
    inverted_index = load_index(config)
    if type(queries) is list:
        queries_list = queries
    else:
        queries_list = parse_queries_from_file(queries)

    for query in queries_list:
        print(query)
        for doc_tuple in search_and_rank_query(query, inverted_index, k, config, False):
            print('tweet id: {}, score (TF-idf): {}'.format(doc_tuple[0], doc_tuple[1]))
    end = dt.datetime.now()
    total_time = (end - start).total_seconds()
    print("Total runing time was {} minutes".format(total_time / 60.0))




if __name__ == "__main__":
    main('Data', '.', False, 'queries.txt', 10)
    #main(*sys.argv[1:])
