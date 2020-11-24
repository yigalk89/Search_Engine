from numpy import dot
from numpy.linalg import norm
import math
import utils
from configuration import ConfigClass


class Ranker:
    def __init__(self):
        self.posting = {}
        pass

    def calc_tf_idf(self, term_freq_in_doc, max_freq_doc, corpus_size, doc_num_for_term):
        return (term_freq_in_doc / max_freq_doc) * math.log2(corpus_size / doc_num_for_term)

    def rank_relevant_doc(self, relevant_doc,  config, query_as_list):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param corpus_size:
        :param config: configurations class, include all relevant paths
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        documents_dict = utils.load_obj(config.get_save_files_dir() + "/documentDict")
        terms_list = self.posting.keys()
        tf_idf_dict = {}

        for doc_id in relevant_doc.keys():
            tf_idf_dict[doc_id] = []
            for term in terms_list:
                term_freq_in_doc = 0
                for tuple in self.posting[term]:
                    if tuple[0] == doc_id:
                        term_freq_in_doc = tuple[1]
                        break
                tf_idf_dict[doc_id].append(
                self.calc_tf_idf(term_freq_in_doc, documents_dict[doc_id][0], len(documents_dict), len(self.posting[term])))
        query_tf_idf = []
        quert_posting = {}
        for term in query_as_list:
            effective_term = term
            if term not in terms_list:
                effective_term = term.lower()
            if effective_term not in quert_posting.keys():
                quert_posting[effective_term] = 1
            else:
                quert_posting[effective_term] += 1
        max_freq_query = max(quert_posting.values())
        for term in terms_list:
            query_tf_idf.append(

                self.calc_tf_idf(quert_posting[term], max_freq_query, len(documents_dict), len(self.posting[term])))
        similarity_dict = {}
        for doc_id, tf_vect in tf_idf_dict.items():
            similarity_dict[doc_id] = dot(tf_vect, query_tf_idf) / (norm(tf_vect) * norm(query_tf_idf))

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
