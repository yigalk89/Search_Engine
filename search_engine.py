from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import re
from orchestrate_parsing import parse_wrapper
import datetime as dt
import pandas as pd


def run_engine(corpus_path='', output_path='.', stemming=False):
    """

    :return:
    """

    config = ConfigClass(corpus_path, stemming, output_path)
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse(stemming)

    tweets_parsed = parse_wrapper(r, p, config)
    print("total parse {} tweets".format(tweets_parsed))


def load_index(config):
    print('Load inverted index')
    folder = config.get_save_files_dir()
    inverted_index = utils.load_obj(folder + "/inverted_index")
    return inverted_index


def search_and_rank_query(query, inverted_index, k, config):
    start = dt.datetime.now()
    p = Parse(config.toStem)
    query_as_list = p.parse_query(query)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list, config)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, config, query_as_list)
    end = dt.datetime.now()
    tot_time = (end - start).total_seconds()/60.0
    print("Query \"{}\" took {} minutes to analayze".format(query, tot_time))
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
    end = dt.datetime.now()
    total_parse_and_ind_time = (end - start).total_seconds() / 60.0
    print("Total parsing and building index and posting time was: {}".format(total_parse_and_ind_time))
    start = dt.datetime.now()
    k = num_docs_to_retrive
    config = ConfigClass(corpus_path, stemming, output_path)
    inverted_index = load_index(config)
    if type(queries) is list:
        queries_list = queries
    else:
        queries_list = parse_queries_from_file(queries)
    output_set = []
    for i in range(len(queries_list)):
        query = queries_list[i]

        print(query)
        doc_tuples = search_and_rank_query(query, inverted_index, k, config)
        for j in range(len(doc_tuples)):
            doc_tuple = doc_tuples[j]
            output_set.append((i+1, doc_tuple[0], doc_tuple[1]))
            #print('tweet id: {}, score (TF-idf): {}'.format(doc_tuple[0], doc_tuple[1]))
    results_set = pd.DataFrame(output_set, columns=['query_num', 'tweet_id', 'tf_score'])
    results_set.to_csv(output_path + '/results.csv')
    end = dt.datetime.now()
    total_query_time = (end - start).total_seconds()
    print("Total Query time was {} minutes".format(total_query_time / 60.0))




if __name__ == "__main__":
    main('Data', '.', False, 'queries.txt', 2000)
    #main(*sys.argv[1:])
