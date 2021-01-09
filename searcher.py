from ranker import Ranker
import utils


# DO NOT MODIFY CLASS NAME
class Searcher:
    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit. The model 
    # parameter allows you to pass in a precomputed model that is already in 
    # memory for the searcher to use such as LSI, LDA, Word2vec models. 
    # MAKE SURE YOU DON'T LOAD A MODEL INTO MEMORY HERE AS THIS IS RUN AT QUERY TIME.
    def __init__(self, parser, indexer, model=None):
        self._parser = parser
        self._indexer = indexer
        self._ranker = Ranker()
        self._model = model

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def search(self, query, k=None):
        """ 
        Executes a query over an existing index and returns the number of 
        relevant docs and an ordered list of search results (tweet ids).
        Input:
            query - string.
            k - number of top results to return, default to everything.
        Output:
            A tuple containing the number of relevant search results, and 
            a list of tweet_ids where the first element is the most relavant 
            and the last is the least relevant result.
        """
        query_as_list = self._parser.parse_query(query)

        relevant_docs = self._relevant_docs_from_posting(query_as_list)
        n_relevant = len(relevant_docs)
        ranked_doc_ids_with_ranks = self._ranker.rank_relevant_docs(relevant_docs, query_as_list, self._indexer)
        ranked_doc_ids = [tup[0] for tup in ranked_doc_ids_with_ranks]
        if k:
            ret = self._ranker.retrieve_top_k(ranked_doc_ids, k)
            n_relevant = len(ret)
        else:
            ret = ranked_doc_ids
        return n_relevant, ret

    # feel free to change the signature and/or implementation of this function 
    # or drop altogether.
    def _relevant_docs_from_posting(self, query_as_list):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query_as_list: parsed query tokens
        :return: dictionary of relevant documents mapping doc_id to document frequency.
        """
        posting = {}
        for term in query_as_list:
            if term in self._indexer.inverted_idx.keys():
                posting[term] = self._indexer.postingDict[term]
            elif term.lower() in self._indexer.inverted_idx.keys():
                posting[term.lower()] = self._indexer.postingDict[term.lower()]

        relevant_docs = {}
        for term in query_as_list:
            try:
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
                continue
                # print('term {} not found in posting'.format(term))
            # save the relevant posting for the use of the ranker
        return relevant_docs
