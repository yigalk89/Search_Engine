from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
from urllib.parse import urlparse

class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')

    def apply_rules(self, tokens_list):
        tokens_list = self.hashtag(tokens_list)
        tokens_list = self.tags(tokens_list)
        tokens_list = self.percents(tokens_list)
        tokens_list = self.numbers(tokens_list)
        tokens_list = self.urls(tokens_list)

        return tokens_list

    def hastag(self, tokens_list):

        """
        Do work
        """
        return tokens_list

    def urls(self, tokens_list):
        for term in tokens_list:
            parsed_url = urlparse(term)
            if parsed_url.scheme != '':
                




    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        text_tokens = word_tokenize(text)
        text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        return text_tokens_without_stopwords

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]
        term_dict = {}
        tokenized_text = self.parse_sentence(full_text)

        doc_length = len(tokenized_text)  # after text operations.

        tokenized_text_w_rules = self.aplly_rules(tokenized_text)

        for term in tokenized_text_w_rules:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document
