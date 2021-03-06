import pandas as pd
from functools import reduce

df = pd.DataFrame(
    {'query': [1, 1, 2, 2, 3], 'tweet': [12345, 12346, 12347, 12348, 12349], 'label': [1, 0, 1, 1, 0]})
#df = pd.read_csv('../Part2/310282025.csv')

test_number = 0
results = []


# precision(df, True, 1) == 0.5
# precision(df, False, None) == 0.5
def precision(df, single=False, query_number=None):
    """
        This function will calculate the precision of a given query or of the entire DataFrame
        :param df: DataFrame: Contains tweet ids, their scores, ranks and relevance
        :param single: Boolean: True/False that tell if the function will run on a single query or the entire df
        :param query_number: Integer/None that tell on what query_number to evaluate precision or None for the entire DataFrame
        :return: Double - The precision
    """
    queries_list = df['query']
    labels = df['label']
    queries_agg_data = {}

    if single and query_number is not None:
        queries_agg_data[query_number] = {'relevant_docs':0, 'total_retrived':0}
        for i, q in enumerate(queries_list):
            if q == query_number:
                queries_agg_data[query_number]['total_retrived'] += 1
                queries_agg_data[query_number]['relevant_docs'] += labels[i]
        res = float(queries_agg_data[query_number]['relevant_docs']) / queries_agg_data[query_number]['total_retrived'] \
            if queries_agg_data[query_number]['total_retrived'] > 0 else 0
        return res

    for i, q in enumerate(queries_list):
        if q not in queries_agg_data.keys():
            queries_agg_data[q] = {'relevant_docs':0, 'total_retrived':0}
        queries_agg_data[q]['total_retrived'] += 1
        queries_agg_data[q]['relevant_docs'] += labels[i]
    precisions = []
    for i, query_agg in enumerate(queries_agg_data.values()):
        res = float(query_agg['relevant_docs']) / query_agg['total_retrived'] if \
            query_agg['total_retrived'] > 0 else 0
        precisions.append(res)

    res = sum(precisions) / len(precisions) if len(precisions) > 0 else 0
    return res


# recall(df, {1:2}, True) == 0.5
# recall(df, {1:2, 2:3, 3:1}, False) == 0.388
def recall(df, num_of_relevant):
    """
        This function will calculate the recall of a specific query or of the entire DataFrame
        :param df: DataFrame: Contains tweet ids, their scores, ranks and relevance
        :param num_of_relevant: Integer: number of relevant tweets
        :param single: Boolean: True/False that tell if the function will run on a single query or the entire df
        :param query_number: Integer/None that tell on what query_number to evaluate precision or None for the entire DataFrame
        :return: Double - The recall
    """
    queries_list = df['query']
    labels = df['label']
    if 1 == len(num_of_relevant.values()):
        query_number = list(num_of_relevant.keys())[0]
        relevant_docs = 0
        for i, q in enumerate(queries_list):
            if q == query_number:
                relevant_docs += labels[i]
        res = float(relevant_docs) / list(num_of_relevant.values())[0] if \
            list(num_of_relevant.values())[0] > 0 else 0
        return res
    else:
        queries_rel_found = {}
        for i, q in enumerate(queries_list):
            if q not in queries_rel_found.keys():
                queries_rel_found[q] = 0
            if labels[i] > 0:
                queries_rel_found[q] += 1
        recalls = []
        for q, r in num_of_relevant.items():
            cur_recall = float(queries_rel_found[q]) / r if r > 0 else 0
            recalls.append(cur_recall)
        res = sum(recalls) / len(recalls) if len(recalls) > 0 else 0
        return res




# precision_at_n(df, 1, 2) == 0.5
# precision_at_n(df, 3, 1) == 0
def precision_at_n(df, query_number=1, n=5):
    """
        This function will calculate the precision of the first n files in a given query.
        :param df: DataFrame: Contains tweet ids, their scores, ranks and relevance
        :param query_number: Integer/None that tell on what query_number to evaluate precision or None for the entire DataFrame
        :param n: Total document to splice from the df
        :return: Double: The precision of those n documents
    """
    queries_list = df['query']
    labels = df['label']
    rel_docs = 0
    query_total = 0
    for i, q in enumerate(queries_list):
        if q == query_number:
            query_total += 1
            rel_docs += labels[i]
        if query_total == n:
            if n > 0:
                return float(rel_docs) / n
    if query_total > 0:
        return float(rel_docs) / query_total
    else:
        return 0

# map(df) == 2/3
def map(df):
    """
        This function will calculate the mean precision of all the df.
        :param df: DataFrame: Contains tweet ids, their scores, ranks and relevance
        :return: Double: the average precision of the df
    """
    queries_list = df['query']
    labels = df['label']
    queries_agg_data = {}
    avgs = []
    for i, q in enumerate(queries_list):
        if q not in queries_agg_data.keys():
            queries_agg_data[q] = []

        queries_agg_data[q].append(labels[i])

    for labels_for_query in queries_agg_data.values():
        query_total_prec = 0
        query_total_pos = 0
        for j, label in enumerate(labels_for_query):
            if label == 1:
                query_total_pos += 1
                query_total_prec += float(query_total_pos) / (j + 1)
        if 0 == query_total_pos:
            avgs.append(0)
        else:
            avgs.append(query_total_prec / query_total_pos)
    if 0 == len(avgs) : return 0
    return sum(avgs) / len(avgs)





def test_value(func, expected, variables):
    """
        This function is used to test your code. Do Not change it!!
        :param func: Function: The function to test
        :param expected: Float: The expected value from the function
        :param variables: List: a list of variables for the function
    """
    global test_number, results
    test_number += 1
    result = func(*variables)  # Run functions with the variables
    try:
        result = float(result)  # All function should return a number
        if result == expected:
            results.extend([f'Test: {test_number} passed'])
        else:
            results.extend([f'Test: {test_number} Failed running: {func.__name__}'
                            f' expected: {expected} but got {result}'])
    except ValueError:
        results.extend([f'Test: {test_number} Failed running: {func.__name__}'
                        f' value return is not a number'])


test_value(precision, 0.5, [df, True, 1])
test_value(precision, 0, [df, True, 4])
test_value(precision, 0.5, [df, False, None])
test_value(recall, 0.5, [df, {1: 2}])
test_value(recall, 0.388, [df, {1: 2, 2: 3, 3: 1}])
test_value(precision_at_n, 0.5, [df, 1, 2])
test_value(precision_at_n, 0, [df, 3, 1])
test_value(map, 2 / 3, [df])
#
for res in results:
    print(res)