import contextlib
import os
import string
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

    saving_dir = config.get_save_files_dir() + "/tmp"
    if not os.path.exists(saving_dir):
        os.makedirs(saving_dir)
    print('Finished parsing and indexing. Starting to export files. task num {}'.format(i))
    utils.save_obj(indexer.inverted_idx, saving_dir + "/inverted_idx_" + str(i))
    utils.save_obj(indexer.documentDict, saving_dir + "/documentDict_" + str(i))
    dump_postings(i, indexer.postingDict, saving_dir)

    return number_of_documents


def dump_postings(task_num, posting_dict, saving_dir):
    for prefix in posting_dict.keys():
        utils.save_obj(posting_dict[prefix], saving_dir + "/postingDict_" + prefix + "_" + str(task_num))


def merge_indexer(config, files_num):
    merged_index = {}
    file_prefix = config.get_save_files_dir() + "/tmp/inverted_idx_"
    for i in range(files_num):
        current_index = utils.load_obj(file_prefix + str(i))
        for term, apperances in current_index.items():
            if term not in merged_index.keys():
                merged_index[term] = apperances
            else:
                merged_index[term] += apperances
    total_terms = len(merged_index)
    print("Total num of terms: {}".format(total_terms))
    saving_dir = config.get_save_files_dir()
    utils.save_obj(merged_index, saving_dir + "/inverted_index")

    return total_terms


def merge_documents_idx(config, files_num):
    merged_docs_idx = {}
    file_prefix = config.get_save_files_dir() + "/tmp/documentDict_"
    for i in range(files_num):
        current_index = utils.load_obj(file_prefix + str(i))
        for doc_id, doc_data in current_index.items():
            # documents are unique, so doc_id can't appear twice
            merged_docs_idx[doc_id] = doc_data
    total_docs = len(merged_docs_idx)
    print("Total docs retrived: {}".format(total_docs))
    saving_dir = config.get_save_files_dir()
    utils.save_obj(merged_docs_idx, saving_dir + "/documentDict")
    return total_docs


def merge_posting_letter(saving_dir, prefix, files_num):
    print("merging posting of prefix {}, files_num: {}".format(prefix, files_num))
    loading_dir = saving_dir + '/tmp'
    file_prefix = loading_dir + "/postingDict_" + prefix + "_"
    merged_letter_posting = {}
    for i in range(files_num):
        current_letter_posting = utils.load_obj(file_prefix + str(i))
        for term, apperances in current_letter_posting.items():
            if term not in merged_letter_posting.keys():
                merged_letter_posting[term] = apperances
            else:
                merged_letter_posting[term] += apperances
    for postings_entry in merged_letter_posting.values():
        postings_entry.sort(key=lambda x: x[0])

    utils.save_obj(merged_letter_posting, saving_dir + "/postingDict_" + prefix)
    print("saved {} postingDict".format(prefix))
    return prefix


def parse_wrapper(r, p, config):
    files_num = r.get_files_number()
    results = []
    with contextlib.closing(multiprocessing.Pool(5)) as pool:
        for i in range(files_num):
            results.append(pool.apply_async(parse_and_index, args=(r, p, config, i)))

    total_tweets = 0
    for x in results:
        total_tweets += x.get()
    pool.join()

    # merge index and doc_idx
    merge_indexer(config, files_num)
    merge_documents_idx(config, files_num)
    prefixes = list(string.ascii_lowercase) + ["sign"]

    with contextlib.closing(multiprocessing.Pool(5)) as pool:
        for prefix in prefixes:
            results.append(pool.apply_async(merge_posting_letter, args=(config.get_save_files_dir(), prefix, files_num)))
    pool.join()

    return total_tweets
