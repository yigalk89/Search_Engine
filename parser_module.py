from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
from urllib.parse import urlparse
import re


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')

    def apply_rules(self, tokens_list):
        # tokens_list = self.hashtag(tokens_list)
        # tokens_list = self.tags(tokens_list)
        # tokens_list = self.percents(tokens_list)
        # tokens_list = self.numbers(tokens_list)


        return tokens_list

    def remove_raw_urls(self, text, indices_str):
        if indices_str == '[]': return text # 117,140 are prolematic
        out = text
        indices = indices_str.replace('[', '').replace(']', '').split(',')
        offset = 0
        for i in range(int(len(indices) / 2)):
            start = int(indices[i*2])
            end = int(indices[i*2 + 1])
            out = out[0:start - offset] + out[end - offset:]
            offset += end - start
        return out

    def parse_url_field(self, org_url):
        tokens_extracted = []
        delimiters = "/", "-", "_", "=", "?", ",", "&"
        regex_pattern = '|'.join(map(re.escape, delimiters))

        urls = org_url.replace('{', '').replace('}', '').split(';')
        for url in urls:
            if '' == url: continue  # ignore empty entry
            url_to_parse = url.split('\":\"')[1].replace('\'','').replace('\"','')
            parsed_url = urlparse(url_to_parse)
            tokens_extracted.append(parsed_url.scheme)
            # handle domain
            if parsed_url.netloc.startswith('www.'):
                tokens_extracted.append(parsed_url.netloc[:3])
                tokens_extracted.append(parsed_url.netloc[4:])
            else:
                tokens_extracted.append(parsed_url.netloc)
            tokens_extracted += filter(None, re.split(regex_pattern, parsed_url.path))
            tokens_extracted += filter(None, re.split(regex_pattern, parsed_url.params))
            tokens_extracted += filter(None, re.split(regex_pattern, parsed_url.query))
            tokens_extracted += filter(None, re.split(regex_pattern, parsed_url.fragment))
        return tokens_extracted

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
        url_indices = doc_as_list[4]
        retweet_text = doc_as_list[5]
        retweet_url = doc_as_list[6]
        retweet_url_indices = doc_as_list[7]
        quote_text = doc_as_list[8]
        quote_url = doc_as_list[9]
        quote_url_indices = doc_as_list[10]
        retweet_quoted = doc_as_list[11]
        retweet_quoted_urls = doc_as_list[12]
        retweet_quoted_url_indices = doc_as_list[13]

        term_dict = {}
        # Remove raw URLs from the terms list (they aren't informative, deal with them later in the flaw)
        text_wo_urls = self.remove_raw_urls(full_text, url_indices)
        tokenized_text = self.parse_sentence(text_wo_urls)

        doc_length = len(tokenized_text)  # after text operations.

        tokenized_text_w_rules = self.apply_rules(tokenized_text)

        tokenized_text_w_rules += self.parse_url_field(url)

        for term in tokenized_text_w_rules:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document
