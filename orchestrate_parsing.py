import contextlib
from concurrent.futures.thread import ThreadPoolExecutor
import utils
import multiprocessing

from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer


def parse_and_index(r, p, config, i):
    number_of_documents = 0
    print("task num: {}".format(i))
    # documents_list = r.read_next_file()
    # new process version
    documents_list = r.read_file_at_index(i)
    indexer = Indexer(config)
    # Iterate over every document in the file
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = p.parse_doc(document)
        number_of_documents += 1
        # index the document data
        indexer.add_new_doc(parsed_document)

    saving_dir = config.get_save_files_dir()
    print('Finished parsing and indexing. Starting to export files. task num {}'.format(i))
    utils.save_obj(indexer.inverted_idx, saving_dir + "/inverted_idx_" + str(i))
    utils.save_obj(indexer.documentDict, saving_dir + "/documentDict_" + str(i))
    utils.save_obj(indexer.postingDict, saving_dir + "/postingDict_" + str(i))

    return number_of_documents


def parse_wrapper(r, p, config):
    files_num = r.get_files_number()
    results = []
    with contextlib.closing(multiprocessing.Pool(5)) as pool:
        for i in range(files_num):
            results.append(pool.apply_async(parse_and_index, args=(r, p, config, i)))

    total_tweets = 0
    for x in results:
        total_tweets += x.get()
        #print(x.get())
    pool.join()


    # merge_evrything()
    return total_tweets
