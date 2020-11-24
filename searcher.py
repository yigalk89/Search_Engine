from parser_module import Parse
from ranker import Ranker
import utils
from indexer import obtain_dict_prefix
from configuration import ConfigClass


class Searcher:

    def __init__(self, inverted_index):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index

    def relevant_docs_from_posting(self, query, config):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        terms_by_letter = {}
        for term in query:
            pref = obtain_dict_prefix(term)
            if pref not in terms_by_letter.keys():
                terms_by_letter[pref] = []
            terms_by_letter[pref].append(term)

        files_prefix = config.get_save_files_dir() + "/postingDict_"
        posting = {}
        # Load the relevant posting entries for the query
        for letter, terms in terms_by_letter.items():
            current_posting = utils.load_obj(files_prefix+letter)
            for term in terms:
                if term in current_posting.keys():
                    posting[term] = current_posting[term]
                elif term.lower() in current_posting.keys():
                    posting[term.lower()] = current_posting[term.lower()]

        relevant_docs = {}
        for term in query:
            try: # an example of checks that you have to do
                if term.lower() in posting.keys():
                    posting_doc = posting[term.lower()]
                else:
                    posting_doc = posting[term]
                for doc_tuple in posting_doc:
                    doc = doc_tuple[0]
                    if doc not in relevant_docs.keys():
                        relevant_docs[doc] = 1
                    else:
                        relevant_docs[doc] += 1
            except:
                print('term {} not found in posting'.format(term))
        self.ranker.add_posting(posting)
        return relevant_docs # key doc_id, val num of relevant terms
