import numpy as np
import math
import utils
from configuration import ConfigClass


class Ranker:
    def __init__(self):
        self.posting = {}
        pass

    @staticmethod
    def calc_tf_idf(term_freq_in_doc, max_freq_doc, corpus_size, doc_num_for_term):
        """
        Calculate the tf-idf for a term.
        """
        return (term_freq_in_doc / max_freq_doc) * math.log2(corpus_size / doc_num_for_term)

    @staticmethod
    def find_term_freq_id_doc(doc_id, posting_entry):
        """
        This function gets the doc_id and the posting entry for the term and look for the value of the
        term freq in the specific document.
        This is binary search so the search will be shorter, since the posting entry is sorted we can do it.
        :return: Term freq of the term that called the function in doc doc_id
        """
        size = len(posting_entry)
        steps = 0
        start = 0
        end = len(posting_entry) - 1
        while start <= end:
            steps += 1
            mid = int((start + end) / 2)
            if posting_entry[mid][0] < doc_id:
                start = mid + 1
            elif posting_entry[mid][0] > doc_id:
                end = mid - 1
            else:
                return posting_entry[mid][1][0]
        return 0

    def rank_relevant_doc(self, relevant_doc, config, query_as_list):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The score is calculated with the cosine similarity between the query and doc tf-idf vectors
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :param config: configurations class, include all relevant paths
        :param query_as_list: list of terms in the query
        :return: sorted list of documents by score
        """
        documents_dict = utils.load_obj(config.get_save_files_dir() + "/documentDict")
        terms_list = list(self.posting.keys())
        tf_idf_dict = {}

        # build a dictionary of doc_id: tf-idf vector
        for doc_id in relevant_doc.keys():
            tf_idf_dict[doc_id] = np.zeros(len(terms_list))
            for i in range(len(terms_list)):
                term = terms_list[i]
                term_freq_in_doc = self.find_term_freq_id_doc(doc_id, self.posting[term])
                tf_idf_dict[doc_id][i] = \
                    self.calc_tf_idf(term_freq_in_doc, documents_dict[doc_id][0], len(documents_dict), len(self.posting[term]))

        # Build a query posting
        query_posting = {}
        for i in range(len(query_as_list)):
            term = query_as_list[i]
            effective_term = term
            if term not in terms_list:
                effective_term = term.lower()
            if effective_term not in query_posting.keys():
                query_posting[effective_term] = [1, [i]]
            else:
                query_posting[effective_term][0] += 1
                query_posting[effective_term][1].append(i)
        frequencies = [x[0] for x in query_posting.values()]
        if len(frequencies) > 0:
            max_freq_query = max(frequencies)
        else:
            max_freq_query = 0

        # Build the tf-idf vector for the query
        query_tf_idf = np.zeros(len(terms_list))
        for i in range(len(terms_list)):
            term = terms_list[i]
            query_tf_idf[i] = \
                self.calc_tf_idf(query_posting[term][0], max_freq_query, len(documents_dict), len(self.posting[term]))
        # build the similarity dictionary between the query and doc_id
        similarity_dict = {}
        for doc_id, tf_vect in tf_idf_dict.items():
            tf_v_size =  np.linalg.norm(tf_vect)
            quer_size =  np.linalg.norm(query_tf_idf)
            if tf_v_size == 0 or quer_size == 0:
                similarity_dict[doc_id] = 0
            else:
                similarity_dict[doc_id] = np.dot(tf_vect, query_tf_idf) / (tf_v_size *quer_size)

        return sorted(similarity_dict.items(), key=lambda item: item[1], reverse=True)

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]

    def add_posting(self, posting):
        self.posting = posting
