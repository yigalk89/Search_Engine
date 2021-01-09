import search_engine_1
import configuration
import os
import logging
import pandas as pd
from searcher import Searcher


def test_file_exists(fn):
    if os.path.exists(fn):
        return True
    logging.error(f'{fn} does not exist.')
    return False

query_to_zoom = 24
config = configuration.ConfigClass()
engine = search_engine_1.SearchEngine(config=config)
bench_data_path = os.path.join('C:\\Users\\YigalK\\bgu\\ir\\project\\part3\\data', 'benchmark_data_train.snappy.parquet')
bench_lbls_path = os.path.join('C:\\Users\\YigalK\\bgu\\ir\\project\\part3\\data', 'benchmark_lbls_train.csv')
queries_path = os.path.join('C:\\Users\\YigalK\\bgu\\ir\\project\\part3\\data', 'queries_train.tsv')
model_dir = os.path.join('.', 'model')
engine.build_index_from_parquet(bench_data_path)
queries = None
if not test_file_exists(queries_path):
    logging.error("Queries data not found ~> skipping some tests.")
else:
    queries = pd.read_csv(os.path.join('C:\\Users\\YigalK\\bgu\\ir\\project\\part3\\data', 'queries_train.tsv'), sep='\t')
    logging.info("Successfully loaded queries data.")

for i, row in queries.iterrows():
    q_id = row['query_id']
    q_keywords = row['keywords']
    if q_id == query_to_zoom:
        searcher = Searcher(engine._parser, engine._indexer, model=engine._model)
        query_as_list = searcher._parser.parse_query(q_keywords)

        relevant_docs = searcher._relevant_docs_from_posting(query_as_list)
        n_relevant = len(relevant_docs)
        ranked_doc_ids = searcher._ranker.rank_relevant_docs(relevant_docs, query_as_list, searcher._indexer)
        break

list_of_dict = []
df = pd.read_parquet(bench_data_path, engine="pyarrow")

for tweet_id, cos_sim in ranked_doc_ids:
    row = {'tweet_id': tweet_id, 'text':df[df.tweet_id == tweet_id]['full_text'].to_list()[0], 'cos_sim': cos_sim ,'query':query_to_zoom}
    list_of_dict.append(row)


df1 = pd.DataFrame(list_of_dict)
bench_lbls = pd.read_csv(bench_lbls_path,
                         dtype={'query': int, 'tweet': str, 'y_true': int})
bench_lbls.columns = ['query','tweet_id', 'y_true']
bench_lbls_quer = bench_lbls[bench_lbls['query'] == query_to_zoom]
q_results_labeled = pd.merge(df1, bench_lbls_quer,
                                                 on=['query', 'tweet_id'], how='left', suffixes=('_result', '_bench'))
full = pd.merge(q_results_labeled, df, on=['tweet_id'],how='inner' )
full = full[['query', 'tweet_id', 'cos_sim', 'y_true', 'full_text']]
print(full)
full.to_csv(f'C:\\Users\\YigalK\\bgu\\ir\\project\\part3\\zoom_in\\query{query_to_zoom}_zoom.csv')
