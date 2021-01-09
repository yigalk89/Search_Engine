# you can change whatever you want in this module, just make sure it doesn't 
# break the searcher module
import numpy as np
import math
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


    def rank_relevant_docs(self,relevant_docs, query_as_list,indexer,k=None):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param k: number of most relevant docs to return, default to everything.
        :param relevant_docs: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        """
        ranked_results = sorted(relevant_docs.items(), key=lambda item: item[1], reverse=True)
        if k is not None:
            ranked_results = ranked_results[:k]
        return [d[0] for d in ranked_results]
        """
        documents_dict = indexer.documentDict
        terms_list = list(indexer.postingDict.keys())

        # Build a query posting
        query_posting = {}
        for i, term in enumerate(query_as_list):
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
        for term in terms_list:
            if term in query_posting.keys():
                query_tf_idf[indexer.term_to_idx[term]] = \
                    self.calc_tf_idf(query_posting[term][0], max_freq_query,
                                     len(documents_dict), len(indexer.postingDict[term]))
            else:
                query_tf_idf[indexer.term_to_idx[term]] = 0

        # build the similarity dictionary between the query and doc_id
        similarity_dict = {}
        for doc_id in relevant_docs:
            tf_vect = indexer.tf_idf[doc_id]
            sim = np.dot(tf_vect, query_tf_idf) / (
                        np.linalg.norm(tf_vect) * np.linalg.norm(query_tf_idf))
            if sim > 0.0:
                similarity_dict[doc_id] = sim

        sorted_tups = sorted(similarity_dict.items(), key=lambda item: item[1], reverse=True)
        return sorted_tups

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
