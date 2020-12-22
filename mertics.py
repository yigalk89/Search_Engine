import pandas as pd

df = pd.DataFrame(
    {'query_num': [1, 1, 2, 2, 3], 'Tweet_id': [12345, 12346, 12347, 12348, 12349], 'label': [1, 0, 1, 1, 0]})

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
    queries_list = df['query_num']
    labels = df['label']
    queries_agg_data = {}

    if single and query_number is not None:
        queries_agg_data[query_number] = {'relevant_docs':0, 'total_retrived':0}
        for i, q in enumerate(queries_list):
            if q == query_number:
                queries_agg_data[query_number]['total_retrived'] += 1
                queries_agg_data[query_number]['relevant_docs'] += labels[i]
        return float(queries_agg_data[query_number]['relevant_docs']) / queries_agg_data[query_number]['total_retrived']

    for i, q in enumerate(queries_list):
        if q not in queries_agg_data.keys():
            queries_agg_data[q] = {'relevant_docs':0, 'total_retrived':0}
        queries_agg_data[q]['total_retrived'] += 1
        queries_agg_data[q]['relevant_docs'] += labels[i]
    precisions = []
    for i, query_agg in enumerate(queries_agg_data.values()):
        precisions.append(float(query_agg['relevant_docs']) / query_agg['total_retrived'])

    return sum(precisions) / len(precisions)


# recall(df, 2, True, 1) == 0.5
# recall(df, 5, False, None) == 0.6
def recall(df, num_of_relevant, single=False, query_number=None):
    """
        This function will calculate the recall of a specific query or of the entire DataFrame
        :param df: DataFrame: Contains tweet ids, their scores, ranks and relevance
        :param num_of_relevant: Integer: number of relevant tweets
        :param single: Boolean: True/False that tell if the function will run on a single query or the entire df
        :param query_number: Integer/None that tell on what query_number to evaluate precision or None for the entire DataFrame
        :return: Double - The recall
    """
    queries_list = df['query_num']
    labels = df['label']
    if single and query_number is not None:
        relevant_docs = 0
        for i, q in enumerate(queries_list):
            if q == query_number:
                relevant_docs += labels[i]
        return float(relevant_docs) / num_of_relevant

    return float(sum(labels)) / num_of_relevant



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
    pass


# map(df) == 0.5
def map(df):
    """
        This function will calculate the mean precision of all the df.
        :param df: DataFrame: Contains tweet ids, their scores, ranks and relevance
        :return: Double: the average precision of the df
    """
    queries_list = df['query_num']
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
test_value(precision, 0.5, [df, False, None])
test_value(recall, 0.5, [df, 2, True, 1])
test_value(recall, 0.6, [df, 5, False, None])
# test_value(precision_at_n, 0.5, [df, 1, 2])
# test_value(precision_at_n, 0, [df, 3, 1])
test_value(map, 2/3, [df])
#
for res in results:
    print(res)