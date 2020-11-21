import re


class Indexer:

    def __init__(self, config):
        self.inverted_idx = {}
        self.entities_idx = {}
        self.entities_posting = {}
        self.postingDict = {}
        self.documentDict = {}  # key - doc_id, value - tuple of( max_tf, unique terms in doc)
        self.config = config

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
                if document_dictionary[term] > max_tf:
                    max_tf = document_dictionary[term]
                self.postingDict[dict_pref][term].append((document.tweet_id, document_dictionary[term]))

            except:
                print('problem with the following key {}'.format(term))
        self.documentDict[document.tweet_id] = (max_tf, document.unique_terms)

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
                self.entities_posting[dict_pref][term].append((document.tweet_id, entities_dict[term]))

            except:
                print('problem with the following key {}'.format(term))


def obtain_dict_prefix(token):
    # Also to check in terms of performance
    # if token[0] in string.ascii_letters:
    if re.match(r'[A-Za-z]', token[0]):
        return token[0].lower()
    else:
        return 'sign'
