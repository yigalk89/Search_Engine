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
import datetime as dt


def parse_and_index(r, p, config, i):
    start = dt.datetime.now()
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
    utils.save_obj(indexer.documentDict, saving_dir + "/document_dict_" + str(i))
    utils.save_obj(indexer.entities_idx, saving_dir + "/entities_idx_" + str(i))
    dump_postings(i, indexer.postingDict, saving_dir, "postingDict")
    dump_postings(i, indexer.entities_posting, saving_dir, "entitiesDict")
    end = dt.datetime.now()
    total_task_time = (end - start).total_seconds() / 60.0
    print("Task {}, total taks time {} minutes".format(i, total_task_time))
    return number_of_documents


def dump_postings(task_num, posting_dict, saving_dir, posting_type):
    for prefix in posting_dict.keys():
        utils.save_obj(posting_dict[prefix], saving_dir + "/" + posting_type + "_" + prefix + "_" + str(task_num))


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

    # Handle the capital restriction
    merged_index_after_cap = {}
    for term, value in merged_index.items():
        if term[0].islower():
            if term not in merged_index_after_cap.keys():
                merged_index_after_cap[term] = value
            else:
                merged_index_after_cap[term] += value
        else: # case it contains uppercase
            if term.lower() in merged_index.keys(): # case there is the same term in lower somewhere in the corpus
                if term.lower() not in merged_index_after_cap.keys():
                    merged_index_after_cap[term.lower()] = value
                else:
                    merged_index_after_cap[term.lower()] += value
            else: # case it is actually capital only
                merged_index_after_cap[term.upper()] = value

    # entities
    entities_idxs_prefix = config.get_save_files_dir() + "/tmp/entities_idx_"
    for i in range(files_num):
        current_entities = utils.load_obj(entities_idxs_prefix + str(i))
        for term, apperances in current_entities.items():
            if apperances > 1:
                merged_index_after_cap[term] = apperances

    total_terms = len(merged_index)
    print("Total num of terms: {}".format(total_terms))
    saving_dir = config.get_save_files_dir()
    utils.save_obj(merged_index_after_cap, saving_dir + "/inverted_index")

    return total_terms


def merge_documents_idx(config, files_num):
    merged_docs_idx = {}
    file_prefix = config.get_save_files_dir() + "/tmp/document_dict_"
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


def merge_posting_letter(saving_dir, prefix, files_num, inverted_idx):
    print("merging posting of prefix {}, files_num: {}".format(prefix, files_num))
    loading_dir = saving_dir + '/tmp'
    file_prefix = loading_dir + "/postingDict_" + prefix + "_"
    entities_prefix = loading_dir + "/entitiesDict_" + prefix + "_"
    merged_letter_posting = {}
    for i in range(files_num):
        current_letter_posting = utils.load_obj(file_prefix + str(i))
        for term, apperances in current_letter_posting.items():
            if term in merged_letter_posting.keys(): # already found term
                merged_letter_posting[term] += apperances
            else:
                if term in inverted_idx.keys(): # term capital that is valid, or a lower one
                    merged_letter_posting[term] = apperances
                else: # capital term candidate that haven't made it, will be lowered
                    merged_letter_posting[term.lower()] = apperances

        # load entities_posting and merge it
        curent_entity_posting = utils.load_obj(entities_prefix + str(i))
        for term, apperances in curent_entity_posting.items():
            if term in inverted_idx.keys():  # Valid entity
                merged_letter_posting[term] = apperances

    for postings_entry in merged_letter_posting.values():
        postings_entry.sort(key=lambda x: x[0])

    utils.save_obj(merged_letter_posting, saving_dir + "/postingDict_" + prefix)
    print("saved {} posting dict".format(prefix))
    return prefix


def parse_wrapper(r, p, config):
    files_num = r.get_files_number()
    results = []
    # processes = int((multiprocessing.cpu_count() / 2) - 1)
    processes = 5
    with contextlib.closing(multiprocessing.Pool(processes)) as pool:
        for i in range(files_num):
            results.append(pool.apply_async(parse_and_index, args=(r, p, config, i)))

    total_tweets = 0
    for x in results:
        total_tweets += x.get()
    pool.join()

    # merge index and doc_idx
    merge_indexer(config, files_num)
    merge_documents_idx(config, files_num)
    inverted_idx = utils.load_obj(config.get_save_files_dir() + "/inverted_index")
    prefixes = list(string.ascii_lowercase) + ["sign"]

    with contextlib.closing(multiprocessing.Pool(processes)) as pool:
        for prefix in prefixes:
            pool.apply_async(merge_posting_letter,
                                            args=(config.get_save_files_dir(), prefix, files_num, inverted_idx))

    pool.join()

    return total_tweets
