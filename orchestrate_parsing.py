import contextlib
import os
import string
import utils
import multiprocessing

from indexer import Indexer
import datetime as dt


def parse_and_index(r, p, config, i):
    """
    This function goes through the entire journey of dealing with an input file.
    Reading it from disk, parsing it, indexing it and writing the temporary index files to disk.
    It is reading the ith file from the reader list of files
    :param r: Reader class, list the files to read and deal with reading them
    :param p: Parse class, deals with parsing a document
    :param config: Config class, contains info about stemming and where to save files
    :param i: index of file to deal with from the entire list of files
    :return: number of tweets read in the specific file
    """
    start = dt.datetime.now()
    number_of_documents = 0
    #print("task num: {}".format(i))
    # obtain relevant tweets list
    documents_list = r.read_file_at_index(i)
    indexer = Indexer(config)

    # Iterate over every document in the file
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = p.parse_doc(document)
        number_of_documents += 1
        # index the document data
        indexer.add_new_doc(parsed_document)

    # save all the temporary files from indexer in tmp directory
    saving_dir = config.get_save_files_dir() + "/tmp"
    if not os.path.exists(saving_dir):
        os.makedirs(saving_dir)
    #print('Finished parsing and indexing. Starting to export files. task num {}'.format(i))
    utils.save_obj(indexer.inverted_idx, saving_dir + "/inverted_idx_" + str(i))
    utils.save_obj(indexer.documentDict, saving_dir + "/document_dict_" + str(i))
    utils.save_obj(indexer.entities_idx, saving_dir + "/entities_idx_" + str(i))
    dump_postings(i, indexer.postingDict, saving_dir, "postingDict")
    dump_postings(i, indexer.entities_posting, saving_dir, "entitiesDict")
    end = dt.datetime.now()
    total_task_time = (end - start).total_seconds() / 60.0
    #print("Task {}, total taks time {} minutes".format(i, total_task_time))
    return number_of_documents


def dump_postings(task_num, posting_dict, saving_dir, posting_type):
    # Dump all the posting files by letter
    for prefix in posting_dict.keys():
        utils.save_obj(posting_dict[prefix], saving_dir + "/" + posting_type + "_" + prefix + "_" + str(task_num))


def merge_index(config, files_num):
    """
    The function loads all the temporary index files that was made by the parse_and_index function and merge them into
    a united index.
    The function deals with the capital letters rule, where all the occurences of a term are starting with capital
    letters, it will be save in all capital. Otherwise it will be saved in the lower version.
    The function also merge the entites into the inverted index in case they appear in the corpus more than once.
    The function save the merged index to the disk for future use.
    :param config: config class that contains info about where to retrieve the saved files
    :param files_num: How many temporary files to merge in each category
    :return: Number of total terms in the index
    """
    merged_index = {}

    # Just merge all the terms in the index into one index
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

    # Check if an entity appears more than once in the corpus it's being added to the index
    entities_idxs_prefix = config.get_save_files_dir() + "/tmp/entities_idx_"
    for i in range(files_num):
        current_entities = utils.load_obj(entities_idxs_prefix + str(i))
        for term, apperances in current_entities.items():
            if apperances > 1:
                merged_index_after_cap[term] = apperances

    total_terms = len(merged_index)
    #print("Total num of terms: {}".format(total_terms))
    # Save the merged index to disk
    saving_dir = config.get_save_files_dir()
    utils.save_obj(merged_index_after_cap, saving_dir + "/inverted_index")

    return total_terms


def merge_documents_idx(config, files_num):
    """
    Merge all the temporart document indices to one document index.
    Since there is no overlap between doc_ids in different file of input, it is simply reading all the entries
    from file and write them to the new index.
    :param config: config class that contains info about where to retrieve the saved files
    :param files_num: How many temporary files to merge in each category
    :return: total docs in the corpus
    """
    merged_docs_idx = {}
    file_prefix = config.get_save_files_dir() + "/tmp/document_dict_"
    for i in range(files_num):
        current_index = utils.load_obj(file_prefix + str(i))
        for doc_id, doc_data in current_index.items():
            # documents are unique, so doc_id can't appear twice
            merged_docs_idx[doc_id] = doc_data
    total_docs = len(merged_docs_idx)
    #print("Total docs retrived: {}".format(total_docs))
    saving_dir = config.get_save_files_dir()
    utils.save_obj(merged_docs_idx, saving_dir + "/documentDict")
    return total_docs


def merge_posting_letter(saving_dir, prefix, files_num, inverted_idx):
    """
    Merge one posting file, by it's prefix. (This task is dispatched to several processes so it runs in parallel)
    It reads all the posting dict and the entities candidate_dicts with the relevant prefix and merge them into one.
    It also makes sure that entities and capital letters are aligned with the way we dealt with it in the inverted idx
    :param saving_dir: Where to save the output and find the temp files
    :param prefix: Which posting prefix this task is being applied to
    :param files_num: How many temp files to read
    :param inverted_idx: The inverted index of the corpus that contains all the final version ok keys
    :return: Which prefix this task worked on
    """

    #print("merging posting of prefix {}, files_num: {}".format(prefix, files_num))
    loading_dir = saving_dir + '/tmp'
    file_prefix = loading_dir + "/postingDict_" + prefix + "_"
    entities_prefix = loading_dir + "/entitiesDict_" + prefix + "_"
    merged_letter_posting = {}

    # Merge all the posting entries
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

    # Sort every posting entry by it doc_id
    for postings_entry in merged_letter_posting.values():
        postings_entry.sort(key=lambda x: x[0])

    # Save relevant posting dict
    utils.save_obj(merged_letter_posting, saving_dir + "/postingDict_" + prefix)
    #print("saved {} posting dict".format(prefix))
    return prefix


def parse_wrapper(r, p, config):
    """
    This function distribute the work on parsing and merging the temporary outputs to several processes using
    processes pool.
    :param r: Reader class, list the files to read and deal with reading them
    :param p: Parse class, deals with parsing a document
    :param config: Config class, contains info about stemming and where to save files
    :return: total tweets that where parsed and indexed
    """
    # Obtain the  files number we split the tasks into
    files_num = r.get_files_number()
    results = []
    # processes = int((multiprocessing.cpu_count() / 2) - 1)
    processes = 5 # Result of trial and error
    # dispatch the parse and index to the processes pool, each task with it's id
    with contextlib.closing(multiprocessing.Pool(processes)) as pool:
        for i in range(files_num):
            results.append(pool.apply_async(parse_and_index, args=(r, p, config, i)))

    total_tweets = 0
    for x in results:
        total_tweets += x.get()
    pool.join()

    # merge index and doc_idx
    merge_index(config, files_num)
    merge_documents_idx(config, files_num)
    inverted_idx = utils.load_obj(config.get_save_files_dir() + "/inverted_index")
    prefixes = list(string.ascii_lowercase) + ["sign"]

    # dispatch the posting merging task to the processes pool. Every task with it's id
    with contextlib.closing(multiprocessing.Pool(processes)) as pool:
        for prefix in prefixes:
            pool.apply_async(merge_posting_letter,
                                            args=(config.get_save_files_dir(), prefix, files_num, inverted_idx))

    pool.join()

    return total_tweets
