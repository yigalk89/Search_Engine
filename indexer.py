import utils
import numpy as np
import math
# DO NOT MODIFY CLASS NAME
class Indexer:
    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.documentDict = {}  # key - doc_id, value - tuple of( max_tf, unique terms in doc)
        self.config = config

        self.entities_idx = {}
        self.entities_posting = {}
        self.tf_idf = {}
        self.term_to_idx = {}

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        document_dictionary = document.term_doc_dictionary
        max_tf = 0
        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = 1
                    self.postingDict[term] = []
                else:
                    self.inverted_idx[term] += 1

                if document_dictionary[term][0] > max_tf:
                    max_tf = document_dictionary[term][0]

                self.postingDict[term].append((document.tweet_id, document_dictionary[term]))

            except:
                print('problem with the following key {}'.format(term))

        entities_dict = document.entities_dict
        for term in entities_dict.keys():
            try:
                # Update inverted index and posting
                if term not in self.entities_idx.keys():
                    self.entities_idx[term] = 1
                    self.entities_posting[term] = []
                else:
                    self.entities_idx[term] += 1

                if entities_dict[term][0] > max_tf:
                    max_tf = entities_dict[term][0]

                self.entities_posting[term].append((document.tweet_id, entities_dict[term]))

            except:
                print('problem with the following key {}'.format(term))
        self.documentDict[document.tweet_id] = (max_tf, document.unique_terms)


    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_index(self, fn):
        """
        Loads a pre-computed index (or indices) so we can answer queries.
        Input:
            fn - file name of pickled index.
        """
        self.inverted_idx, self.postingDict, self.term_to_idx, self.tf_idf = utils.load_obj(fn)

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def save_index(self, fn):
        """
        Saves a pre-computed index (or indices) so we can save our work.
        Input:
              fn - file name of pickled index.
        """
        utils.save_obj([self.inverted_idx, self.postingDict, self.term_to_idx, self.tf_idf], fn)

    # feel free to change the signature and/or implementation of this function 
    # or drop altogether.
    def _is_term_exist(self, term):
        """
        Checks if a term exist in the dictionary.
        """
        return term in self.postingDict

    # feel free to change the signature and/or implementation of this function 
    # or drop altogether.
    def get_term_posting_list(self, term):
        """
        Return the posting list from the index for a term.
        """
        return self.postingDict[term] if self._is_term_exist(term) else []

    def treat_cap_and_entities(self):
        """
        This function make sures entities are brought inside the index only
        if they appear more than once.
        It makes sure that terms that starts with capital letters stays this way only
        if they dont appear with lowercase somewhere else.
        It also sorts the posting by tweet id


        """
        # Make sure capital letters is being take care of
        new_idx = {}
        new_posting = {}
        for term, val in self.inverted_idx.items():
            if term[0].islower():
                new_idx[term] = val
                new_posting[term] = self.postingDict[term]
            else: # case it contains uppercase
                if term.lower() in self.inverted_idx:
                    new_idx[term.lower()] = val
                    new_posting[term.lower()] = self.postingDict[term]
                else:
                    new_idx[term.upper()] = val
                    new_posting[term.upper()] = self.postingDict[term]

        # Add entities potential that appear more than twice
        for term, val in self.entities_idx.items():
            if val > 1:
                new_idx[term] = val
                new_posting[term] = self.entities_posting[term]

         # sort postings by tweet_id
        for posting_entry in new_posting.values():
            posting_entry.sort(key=lambda x: x[0])

        if len(new_idx.keys()) != len(new_posting.keys()):
            raise Exception(f"new idx and new postings aren't the same length. New index is: {len(new_idx.keys())} "\
            f"new posting is: {len(new_posting.keys())}")
        self.inverted_idx = new_idx
        self.postingDict = new_posting

    @staticmethod
    def calc_tf_idf(term_freq_in_doc, max_freq_doc, corpus_size, doc_num_for_term):
        """
        Calculate the tf-idf for a term.
        """
        return (term_freq_in_doc / max_freq_doc) * math.log2(corpus_size / doc_num_for_term)

    def create_tf_idf(self):
        term_to_idx_dict = {}
        for i, term in enumerate(self.inverted_idx.keys()):
            term_to_idx_dict[term] = i

        tf_idf_vector_per_doc_id = {}
        terms_num = len(self.inverted_idx.keys())
        for doc_id in self.documentDict.keys():
            tf_idf_vector_per_doc_id[doc_id] = np.zeros(terms_num)
        for term, posting in self.postingDict.items():
            for post_entry in posting:
                doc_id = post_entry[0]
                term_freq_in_doc = post_entry[1][0]
                tf_idf_vector_per_doc_id[doc_id][term_to_idx_dict[term]] = \
                    self.calc_tf_idf(term_freq_in_doc, self.documentDict[doc_id][0], terms_num, len(posting))
        self.tf_idf = tf_idf_vector_per_doc_id
        self.term_to_idx = term_to_idx_dict
