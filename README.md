# Search_Engine
To use the search engine import "search_engine".
The entry point would be the main function.
Function main signature is: main(corpus_path='', output_path='.', stemming=False, queries='', num_docs_to_retrive=0)
The parameters are as follow:
corpus_path - points to a folder where the parquet files lays, in the folder or in nested folders. The parquet files should have 14 fields.
    tweet_id, tweet_date, full_text, url, url_indices, retweet_text, retweet_url, retweet_url_indices, quote_text, quote_url, quote_url_indices, retweet_quoted, retweet_quoted_urls, retweet_quoted_url_indices.
output_path - Top folder for output. Inside a withstem / withoutstem folder will be created and the posting, index and documnet index will be written. Also the csv file with the results (with or without stemming) would be writen.
stemming - Boolean, True to apply stemming, False not to apply.
queries - 2 options: 1) list of textual queries. 2) path of queries file that contains a query in every line.
num_docs_to_retrive - integer that sets the maximun of tweets to retrive per query.
