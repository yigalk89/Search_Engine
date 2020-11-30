import re


class Indexer:

    def __init__(self, config):
        self.inverted_idx = {}
        self.entities_idx = {}
        self.entities_posting = {}

        # the postingDict keys are the prefix of the posting 'a-z' and signs. And the values are dictionaries of terms.
        # e.g. {'a':
        # 'admit': [(tweet_id1, [occ_num, [pos_idx_list]]), (tweet_id2, [occ_num, [pos_idx_list]])],
        # 'another_term_starting_with_a': [(tweet_id1, [occ_num, [pos_idx_list]]), (tweet_id2, [occ_num, [pos_idx_list]])]
        # 'b':
        # 'b_starting_term': ....
        # }
        self.postingDict = {}

        self.documentDict = {}  # key - doc_id, value - tuple of( max_tf, unique terms in doc)
        self.config = config

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via several dictionaries ('inverted index', 'document' dictionary ,
        'entities_idx', 'entities_posting' and 'postingDict').
        This function obtain all the data from a doc and updates the relevant dictionaries.
        Important to bare in mind that it works only on a portion of the data, and being merge later on
        :param document: a document need to be indexed.
        """

        document_dictionary = document.term_doc_dictionary
        max_tf = 0
        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                dict_pref = obtain_dict_prefix(term)
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = 1
                    # posting relevant code
                    # check if the prefix exist and if the term already exist
                    if dict_pref not in self.postingDict.keys():
                        self.postingDict[dict_pref] = {}

                    self.postingDict[dict_pref][term] = []
                else:
                    self.inverted_idx[term] += 1

                if document_dictionary[term][0] > max_tf:
                    max_tf = document_dictionary[term][0]
                # add posting entry to the posting dict.
                self.postingDict[dict_pref][term].append((document.tweet_id, document_dictionary[term]))

            except:
                print('problem with the following key {}'.format(term))

        # Deal with entities dictionary
        entities_dict = document.entities_dict
        for term in entities_dict.keys():
            try:
                dict_pref = obtain_dict_prefix(term)
                # Update inverted index and posting
                if term not in self.entities_idx.keys():
                    self.entities_idx[term] = 1
                    # posting relevant code
                    # check if the prefix exist and if the term already exist
                    if dict_pref not in self.entities_posting.keys():
                        self.entities_posting[dict_pref] = {}

                    self.entities_posting[dict_pref][term] = []
                else:
                    self.entities_idx[term] += 1
                if entities_dict[term][0] > max_tf:
                    max_tf = entities_dict[term][0]
                self.entities_posting[dict_pref][term].append((document.tweet_id, entities_dict[term]))

            except:
                print('problem with the following key {}'.format(term))

        self.documentDict[document.tweet_id] = (max_tf, document.unique_terms)


def obtain_dict_prefix(token):
    """
    Returns the relevant prefix for the term
    :param token:
    :return: prefix for token, use in the posting dicts
    """
    # Also to check in terms of performance
    # if token[0] in string.ascii_letters:
    if re.match(r'[A-Za-z]', token[0]):
        return token[0].lower()
    else:
        return 'sign'
