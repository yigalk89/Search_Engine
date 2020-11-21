from nltk.corpus import stopwords
from nltk import ne_chunk, pos_tag, word_tokenize
from nltk.tree import Tree
from document import Document
from urllib.parse import urlparse
import re
from string import punctuation


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')



    # caring off tags
    def tags(self,text):

        #tags=[text[i+1] for i in range(0,len(text)) if text[i] == '@']

        tags=[]

        for i in range(0, len(text)):                      #the last term in the document is @
            if (text[i] == '@' and i == (len(text) - 1)):
                break
            if text[i] == '@' and text[i + 1] not in punctuation:                            #add the tag term
                tags.append("@{}".format(text[i + 1]))

            if (text[i - 1] != '@' and text[i] != '@'):   #add the rest terms in the document
                tags.append(text[i])

        return tags

    # caring off percent
    def percentage(self,text):

        percent = []

        for i in range(0, len(text)):  # the last term in the document are ['%', 'percent', 'percentage']
            if (i == (len(text) - 1)):
                if (text[i] not in ['%', 'percent', 'percentage']):
                    percent.append(text[i])
                    break
                else:
                    break
            if text[i] in ['%', 'percent', 'percentage'] and text[i - 1].isdigit():  # add the percent term to document
                percent.append("{}%".format(text[i - 1]))

            if (text[i] not in ['%', 'percent', 'percentage']):  # add the rest terms in the document
                percent.append(text[i])



        return percent

    # caring off hashtag
    def hashtag(self,text):

            terms = []
            punctutaion1 = ['!', '"', '$', '#', '%', '&', "'", '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=',
                      '>',
                      '?', '[', '\\', ']', '^', '_', '`', '{', '|', '}', '~']  ##puncutation

            for i in range(0, len(text)):

                if (text[i] == '#' and i == (len(text) - 1)): # the last term in the document is #
                    break

                if (text[i] == '#'):
                    curren_hashtag = text[i + 1]

                    if (curren_hashtag[0].isupper() and curren_hashtag != curren_hashtag.isupper()):  #words that seperated by uppercase like #StayAtHome
                        terms.append("#{}".format(text[i + 1].lower()))
                        for w1 in re.findall('[A-Z][^A-Z]*', text[i + 1]):
                            terms.append(w1.lower())

                    if (curren_hashtag.islower() and text[i - 1] != '#'):  #terms that contain only lower letters
                        terms.append(curren_hashtag)
                        terms.append("#{}".format(curren_hashtag))

                    if (curren_hashtag.isdigit()):       #terms are digits
                        terms.append(curren_hashtag)
                        terms.append("#{}".format(curren_hashtag))


                    sgn = 'false'
                    if (text[i + 1][0].islower() and text[i + 1] != text[i + 1].islower()):  # words that starts with lowercase like #stayAtHome
                        cnt1 = 0
                        indx = 0
                        for j in curren_hashtag:
                            if (j.isupper() and sgn == 'true'):
                                indx = cnt1
                                sgn = 'true'
                            cnt1 = cnt1 + 1
                        if (sgn == 'true'):
                            terms.append(text[i + 1][0:indx])
                            for k in re.findall('[A-Z][^A-Z]*', text[i + 1][indx:]):
                                terms.append(k.lower())
                            terms.append("#{}".format(text[i + 1].lower()))

                    for j in text[i + 1]:  #terms that seperated by puncutation
                        if j in punctutaion1 and j != '#':
                            for k in re.compile(r'[\s{}]+'.format(re.escape(punctutaion1))).split(text[i + 1]):
                                terms.append(k.lower())
                            terms.append("#{}".format(text[i + 1].lower()))
                            break

                if (text[i - 1] != '#' and text[i] not in punctutaion1):   #add the rest terms in the document
                    terms.append(text[i])


            return terms


    def apply_rules(self, tokens_list):
        # tokens_list = self.hashtag(tokens_list)
        tokens_list = self.tags(tokens_list)
        tokens_list = self.percentage(tokens_list)
        tokens_list = self.parse_numbers(tokens_list)

        return tokens_list

    def parse_numbers(self, tokens_list):
        suffix_to_shortcut = {'thousand': 'K', 'million': 'M', 'billion': 'B'}
        suffix_to_number = {'thousand': 1000, 'million': 1000000, 'billion': 1000000000}
        tokens_to_output = []
        tokens_iter = iter(tokens_list)
        i = 0
        tokens_num = len(tokens_list)
        for token in tokens_iter:
            current_token = tokens_list[i]
            # Check if this token contains a number
            if any(map(str.isdigit, current_token)):
                next_token = tokens_list[i + 1] if i < tokens_num - 1 else None
                if next_token is not None: next_token = next_token.lower()
                next_next_token = tokens_list[i + 2] if i < tokens_num - 2 else None
                if self.contain_letter(current_token):
                    tokens_to_output.append(current_token)
                    i += 1
                    continue

                # strip commas, check if token contains fraction and dollar (strip dollar if so)
                current_token_no_comma = self.strip_commas(current_token)
                current_token_no_comma, has_dollar = self.treat_dollar(current_token_no_comma)
                has_fraction = self.contains_fraction(current_token_no_comma)
                is_next_suffix = True if next_token in list(suffix_to_number.keys()) else False
                is_next_dollar = next_token == '$' or next_token == 'dollar'

                # Handle fraction
                if has_fraction:
                    token_to_push = current_token_no_comma
                    if is_next_suffix:
                        token_to_push += current_token_no_comma + suffix_to_shortcut[next_token]
                        tokens_iter.__next__()
                        i += 1
                        # handle edge case when there is 1/2 Million $
                        if next_next_token == '$' or next_next_token == 'dollar':
                            has_dollar = True
                            i += 1
                            tokens_iter.__next__()

                    if has_dollar:
                        token_to_push +='$'
                    if is_next_dollar:
                        token_to_push += '$'
                        i += 1
                        tokens_iter.__next__()
                    tokens_to_output.append(token_to_push)
                    i += 1
                    continue

                # Handle regular number
                # start with extracting pattern of a number
                pattern = r"\d+\.?\d*"
                reg_pattern = re.compile(pattern)
                result = re.search(reg_pattern, current_token_no_comma)
                if result is None:
                    tokens_to_output.append(current_token_no_comma)
                    i += 1
                    continue
                else:
                    num_to_evaluate = float(result.group(0))
                if num_to_evaluate < 1000:
                    # with suffix
                    if next_token in suffix_to_number.keys():
                        i += 1
                        tokens_iter.__next__()
                        num_to_evaluate *= suffix_to_number[next_token]
                        out_token = self.format_num(num_to_evaluate)
                    else:
                        out_token = self.format_num(num_to_evaluate)

                    if next_next_token == '$' or next_next_token == 'dollar':
                        has_dollar = True
                        i += 1
                        tokens_iter.__next__()
                    if has_dollar:
                        out_token += '$'
                # Number greater than 1000
                else:
                    out_token = self.format_num(num_to_evaluate)
                if is_next_dollar:
                    i += 1
                    tokens_iter.__next__()
                    out_token += '$'
                # Check if next is fraction
                if self.contains_fraction(next_token):
                    out_token += next_token
                    tokens_iter.__next__()
                    i += 1
                tokens_to_output.append(out_token)

            else:
                tokens_to_output.append(current_token)
            i += 1
        return tokens_to_output

    def format_num(self, num_to_format):
        for unit in ['', 'K', 'M', 'B']:
            if abs(num_to_format) < 1000:
                return "{:.3f}{}".format(num_to_format, unit).replace('.000', '')
            num_to_format /= 1000
        return "{:.3f}{}".format(num_to_format, unit).replace('.000', '')



    def treat_dollar(self, token):
        if token[:1] == '$':
            return token[1:], True
        elif token[-1:] == '$':
            return token[:-1], True
        else:
            return token , False

    def is_suffix(self, token, suffix_list):
        return True if token in suffix_list else False

    def contains_fraction(self, token):
        if token is None: return False
        fraction_pattern = re.compile('(\\d+)(\\s*/\\s*)(\\d+)')
        return bool(fraction_pattern.search(token))

    def contain_letter(self, token):
        return any(map(str.isalpha, token))

    def strip_commas(self, token):
        return token.replace(',', '')

    def remove_raw_urls(self, text, indices_str):
        if indices_str == '[]': return text # 117,140 are prolematic
        out = text
        # convert the string to a list of indices
        indices = indices_str.replace('[', '').replace(']', '').split(',')
        # If a url was extracted but there are more, we need to fix the indices with the offset
        offset = 0
        for i in range(int(len(indices) / 2)):
            # obtain the URL start and end indices
            start = int(indices[i*2])
            end = int(indices[i*2 + 1])
            # slice the url out of the tweet
            out = out[0:start - offset] + out[end - offset:]
            # update the offset to be the length of the text that was sliced out
            offset += end - start
        return out

    def parse_url_field(self, org_url):
        """
        This function get urls from the original data and extract the tokens from them.
        A url is a string of the short and full url. And there could be more than one.
        The function deals with splitting the urls so it can work on each of them independatly
        :param org_url:
        :return: tokens extracted from urls in a list
        """
        tokens_extracted = []
        # prepare the delimiters we can possibly encounter in url
        delimiters = "/", "-", "_", "=", "?", ",", "&"
        regex_pattern = '|'.join(map(re.escape, delimiters))
        # prepare the urls to be parsed seperetly
        urls = org_url.replace('{', '').replace('}', '').split(',')
        for url in urls:
            if '' == url: continue  # ignore empty entry
            # work only on the full url
            urls_splitted = url.split('\":\"')
            # protect urls where the full url is none
            if len(urls_splitted) % 2 != 0: continue
            url_to_parse = urls_splitted[1].replace('\'','').replace('\"','')
            parsed_url = urlparse(url_to_parse)
            # ignoring https prefix
            # tokens_extracted.append(parsed_url.scheme)
            # handle domain
            if parsed_url.netloc.startswith('www.'):
                # omit the www part
                # tokens_extracted.append(parsed_url.netloc[:3])
                tokens_extracted.append(parsed_url.netloc[4:])
            else:
                tokens_extracted.append(parsed_url.netloc)
            tokens_extracted += filter(None, re.split(regex_pattern, parsed_url.path))
            tokens_extracted += filter(None, re.split(regex_pattern, parsed_url.params))
            tokens_extracted += filter(None, re.split(regex_pattern, parsed_url.query))
            tokens_extracted += filter(None, re.split(regex_pattern, parsed_url.fragment))
        return tokens_extracted

    def find_potential_entities(self, tokens_list):
        chunked = ne_chunk(pos_tag(tokens_list))
        continuous_chunk = []
        current_chunk = []
        regular_tokens = []
        for i in chunked:
            if type(i) == Tree:
                current_chunk.append(" ".join([token for token, pos in i.leaves()]))
            if current_chunk:
                named_entity = " ".join(current_chunk)
                if named_entity not in continuous_chunk:
                    continuous_chunk.append(named_entity)
                    current_chunk = []
            else:
                regular_tokens.append(i[0])
        return regular_tokens, continuous_chunk

    def find_entities(self, token_list):
        entities = []
        regular_tokens = []
        entity_candidate = []
        for token in token_list:
            if token[0].isupper():
                entity_candidate.append(token)
            else:
                if len(entity_candidate) > 1:
                    entity = " ".join(entity_candidate)
                    entities.append(entity)
                elif len(entity_candidate) > 0:
                    regular_tokens += entity_candidate
                regular_tokens.append(token)
                entity_candidate = []
        return regular_tokens, entities

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        text_tokens = word_tokenize(text)
        text_tokens_without_stopwords = [w for w in text_tokens if w not in self.stop_words]
        return text_tokens_without_stopwords

    def parse_doc(self, doc_as_list, steming=False):
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
        entities_dict = {}
        #print("full_text: ", full_text)
        #print("url: ",url)
        # Remove raw URLs from the terms list (they aren't informative, deal with them later in the flow)
        text_wo_urls = self.remove_raw_urls(full_text, url_indices)
        text_wo_urls = text_wo_urls.replace('…', ' ')
        tokenized_text = self.parse_sentence(text_wo_urls)
        tokens_wo_entity, entity_potential = self.find_entities(tokenized_text)
        tokenized_text_w_rules = self.apply_rules(tokens_wo_entity)
        tokenized_text_w_rules += self.parse_url_field(url)
        # filter out punctuation terms
        punct = punctuation + '’”“‘'
        tokenized_text_w_rules = [token for token in tokenized_text_w_rules if token not in punct]
        #print(tokenized_text_w_rules)
        doc_length = len(tokenized_text_w_rules)  # after text operations.

        for term in tokenized_text_w_rules:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        for term in entity_potential:
            if term not in entities_dict.keys():
                entities_dict[term] = 1
            else:
                entities_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length, len(term_dict), entities_dict)
        return document
