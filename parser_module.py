from nltk.corpus import stopwords
from nltk import  word_tokenize
from nltk.corpus import lin_thesaurus as thes
from nltk.stem.porter import *
from document import Document
from urllib.parse import urlparse
import re
from string import punctuation


class Parse:
    """
    Class that takes care of transforming a document in full text into a document object with tokens list, metadata
    about the tokens and details relevant for the inverted index
    """

    def __init__(self, to_stem=False):
        self.stop_words = stopwords.words('english')
        self.punct = punctuation + "’”“‘"
        self.punct_larg = ["\"\"", "''", "‘‘", "’’", "``"]
        self.to_stem = to_stem
        if to_stem:
            self.stemmer = PorterStemmer()

    @staticmethod
    def tags(tokens_list):
        """
        Returns tokens list with tag rule applied
        """
        if len(tokens_list) == 0:
            return tokens_list
        out = []
        found_tag = False
        for i in range(1, (len(tokens_list))):
            if found_tag:
                found_tag = False
                continue
            if tokens_list[i - 1] == '@':
                out.append('@{}'.format(tokens_list[i]))
                found_tag = True
            else:
                out.append(tokens_list[i - 1])
        if not found_tag:
            out.append(tokens_list[len(tokens_list) - 1])
        return out

    @staticmethod
    def hashtag(tokens_list):
        """
        Traverse the tokens list, and when hashtag encountered save it in the relevant format.
        Separates the words using capital letters or '_'
        :param tokens_list: List of tokens that might contain hashtag
        :return: tokens list wuth all regular tokens kept and all the hastag after aplling the logic
        """

        if len(tokens_list) == 0:
            return tokens_list
        out = []
        found_hashtag = False

        for i in range(1, len(tokens_list)):
            # make sure we don't add the term after the # just is it was stored originally
            if found_hashtag:
                found_hashtag = False
                continue
            # Found a hashtag, parse it
            if tokens_list[i - 1] == '#':
                out.append(''.join(tokens_list[i - 1: i + 1]).lower())
                words = re.findall(r'[A-Z]?[a-z]+', tokens_list[i])
                out += list(map(lambda x: x.lower(), words))
                found_hashtag = True
            else:
                out.append(tokens_list[i - 1])
        # Take car of last token in the list
        if not found_hashtag:
            out.append(tokens_list[len(tokens_list) - 1])
        return out

    # caring off percent
    @staticmethod
    def percentage(text):
        """
        :returns tokens list with percentage rule applied
        """
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

            if text[i] not in ['%', 'percent', 'percentage']:  # add the rest terms in the document
                percent.append(text[i])

        return percent

    def parse_numbers(self, tokens_list):
        """
        extract tokens of numbers and apply the numbers rules on them.
        Add a suffix for K,M and B. Format to 3 digits after decimal point. Handle fractions if they follow a number
        Tread dollars.
        :param tokens_list:
        :return: tokens list updated with number rules
        """
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

            else: # just a non-numeric token
                tokens_to_output.append(current_token)
            i += 1
        return tokens_to_output

    @staticmethod
    def format_num(num_to_format):
        """
        Helper function for number parsing that formats the number with relevant suffix and decimal suffix
        """
        for unit in ['', 'K', 'M', 'B']:
            if abs(num_to_format) < 1000:
                return "{:.3f}{}".format(num_to_format, unit).replace('.000', '')
            num_to_format /= 1000
        return "{:.3f}{}".format(num_to_format, unit).replace('.000', '')

    @staticmethod
    def treat_dollar(token):
        """
        helper function that strip a dollar out of the token, and signals it did it
        """
        if token[:1] == '$':
            return token[1:], True
        elif token[-1:] == '$':
            return token[:-1], True
        else:
            return token, False

    @staticmethod
    def is_suffix(token, suffix_list):
        return True if token in suffix_list else False

    @staticmethod
    def contains_fraction(token):
        """
        Helper functions that recognize fraction pattern
        """
        if token is None: return False
        fraction_pattern = re.compile('(\\d+)(\\s*/\\s*)(\\d+)')
        return bool(fraction_pattern.search(token))

    @staticmethod
    def contain_letter(token):
        return any(map(str.isalpha, token))

    @staticmethod
    def strip_commas(token):
        return token.replace(',', '')

    @staticmethod
    def remove_raw_urls(text, indices_str):
        """
        Get the text and the url indices and returns the text without the url
        """
        if indices_str == '[]' : return text # 117,140 are prolematic and are dealt with later on
        out = text
        # convert the string to a list of indices
        indices = indices_str.replace('[', '').replace(']', '').split(',')
        # If a url was extracted but there are more, we need to fix the indices with the offset
        offset = 0
        for i in range(int(len(indices) / 2)):
            # obtain the URL start and end indices
            start = int(indices[i*2])
            end = int(indices[i*2 + 1])
            if start == 117 and end == 140: continue # from data exploration it occurs that 117 and 140 are just wrong
            # slice the url out of the tweet
            out = out[0:start - offset] + out[end - offset:]
            # update the offset to be the length of the text that was sliced out
            offset += end - start
        return out

    @staticmethod
    def parse_url_field(org_url):
        """
        This function get urls from the original data and extract the tokens from them.
        A url is a string of the short and full url. And there could be more than one.
        The function deals with splitting the urls so it can work on each of them independatly
        :param org_url:
        :return: tokens extracted from urls in a list
        """
        if org_url is None: return []
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

    @staticmethod
    def find_entities(token_list):
        """
        This function recognizes ponetial entities in the tokens list, when there is a sequence
        of more than one word that starts with capital letters
        It returns the regular tokens and the potential entites
        """
        entities = []
        regular_tokens = []
        entity_candidate = []

        for token in token_list:
            if token[0].isupper():
                entity_candidate.append(token)
            else:
                if len(entity_candidate) > 1: # case there is more than one entity in the sequence,
                                              # concat then and add to the entities
                    entity = " ".join(entity_candidate)
                    entities.append(entity)
                elif len(entity_candidate) > 0:
                    regular_tokens += entity_candidate
                regular_tokens.append(token)
                entity_candidate = []

        # Check if there were entities in the end of the text
        if len(entity_candidate) > 1:  # case there is more than one entity in the sequence,
            # concat then and add to the entities
            entity = " ".join(entity_candidate)
            entities.append(entity)
        elif len(entity_candidate) > 0:
            regular_tokens += entity_candidate
        return regular_tokens, entities

    def apply_stemming(self, tokens_list):
        """
        Apply stemming on all the tokens in the tokens list. Return the stemmed tokens list
        """
        out_list = set()
        for token in tokens_list:
            out_list.add(self.stemmer.stem(token))
        return list(out_list)

    def apply_rules(self, tokens_list):
        """
        apply parser rules on the token list
        """
        tokens_list = self.hashtag(tokens_list)
        tokens_list = self.tags(tokens_list)
        tokens_list = self.percentage(tokens_list)
        tokens_list = self.parse_numbers(tokens_list)

        return tokens_list

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        text_tokens = word_tokenize(text)
        text_tokens_without_stopwords = [w for w in text_tokens if w not in self.stop_words]
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
        entities_dict = {}
        #print("full_text: ", full_text)
        #print("url: ",url)

        # Remove raw URLs from the terms list (they aren't informative, deal with them later in the flow)
        text_wo_urls = self.remove_raw_urls(full_text, url_indices)
        text_wo_urls = text_wo_urls.replace('…', ' ')
        tokenized_text = self.parse_sentence(text_wo_urls)
        tokens_wo_entity, entity_potential = self.find_entities(tokenized_text)

        # Add the relvant info from url and rewteet url field. The url can contain a lot if info about the tweet subject
        tokens_wo_entity += self.parse_url_field(url)
        tokens_wo_entity += self.parse_url_field(retweet_url)
        # apply all the parser rules on the tokens list
        tokenized_text_w_rules = self.apply_rules(tokens_wo_entity)
        # filter out punctuation terms
        tokenized_text_w_rules = [token for token in tokenized_text_w_rules if token not in self.punct]
        tokenized_text_w_rules = [token for token in tokenized_text_w_rules if token not in self.punct_larg]
        if self.to_stem:
            tokenized_text_w_rules = self.apply_stemming(tokenized_text_w_rules)
        #print(tokenized_text_w_rules)
        doc_length = len(tokenized_text_w_rules)  # after text operations.

        # build the term dictionary that holds the info about the frequency of the term in document,
        # and the positional indexing. This goes to the postindDict
        for i in range(len(tokenized_text_w_rules)):
            term = tokenized_text_w_rules[i]
            if term not in term_dict.keys():
                term_dict[term] = [1, [i]]
            else:
                term_dict[term][0] += 1
                term_dict[term][1].append(i)

        # create posting format dictionary for entities potential
        for term in entity_potential:
            if term not in entities_dict.keys():
                entities_dict[term] = [1, [None]]
            else:
                entities_dict[term][0] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length, len(term_dict), entities_dict)
        return document

    @staticmethod
    def add_synonyms_to_list(tokens_list):
        """
        Apply thesaurus synonym addition of one synonym per token in the list. (Performance + results relevance
        are the motivation to limit to one synonym per token).
        Returns the new tokens list including the originn and the synonyms
        """
        out_list = []
        for token in tokens_list:
            out_list.append(token)
            for syn in thes.synonyms(token, fileid="simN.lsp"):
                out_list.append(syn)
                break
        return out_list

    def parse_query(self, query):
        """
        Transform a string that is a query to a tokens list, after apllying all the relevant transformations.
        """
        tokens = self.parse_sentence(query)
        tokens_wo_entity, entity_potential = self.find_entities(tokens)
        tokens_w_synonyms = self.add_synonyms_to_list(tokens_wo_entity)
        tokens_w_rules = self.apply_rules(tokens_w_synonyms)
        tokens_w_rules = [token for token in tokens_w_rules if token not in self.punct]
        tokens_w_rules = [token for token in tokens_w_rules if token not in self.punct_larg]
        if self.to_stem:
            tokens_w_rules = self.apply_stemming(tokens_w_rules)
        return tokens_w_rules + entity_potential

