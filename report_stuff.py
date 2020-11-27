import string

import utils
import re
import matplotlib.pyplot as plt


# a + b + c
def return_total_num_of_terms(stem, number=False):
    prefix = './WithStem' if stem else './WithoutStem'
    filename = prefix + '/inverted_index'
    idx = utils.load_obj(filename)
    if not number:
        return len(idx)
    else:
        numbers = 0
        for term in idx.keys():
            res = re.search("^\d+\,?\d*\.?\d{1,3}?[K,M,B]?$", term)
            if res:
                numbers += 1
        return numbers

def total_occurrences_of_term_in_corpus():
    terms_occ = {}
    prefixes = list(string.ascii_lowercase) + ["sign"]
    for prefix in prefixes:
        filename = "./WithoutStem/postingDict_" + prefix
        relevant_posting = utils.load_obj(filename)
        for term, posting in relevant_posting.items():
            terms_occ[term] = sum([pair[1] for pair in posting])

    return sorted(terms_occ.items(), key=lambda item: item[1], reverse=True)







if __name__ == "__main__":
    #a
    print("Total terms without stemming: {}".format(return_total_num_of_terms(False)))
    # b
    print("Total terms with stemming: {}".format(return_total_num_of_terms(True)))
    # c
    print("Total of terms which are numbers: {}".format(return_total_num_of_terms(False, True)))

    # d
    occur_list = total_occurrences_of_term_in_corpus()
    print("First 10 common words in corpus are: {}".format(occur_list[:10]))
    print("Last 10 common words in corpus are: {}".format(occur_list[-10:]))
    plt.plot([term_and_occ[1] for term_and_occ in occur_list])
    plt.yscale('log')
    plt.savefig("terms_occur.jpg")


