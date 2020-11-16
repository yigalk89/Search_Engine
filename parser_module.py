from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
import re
from string import punctuation


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')



    # caring off tags
    def tags(self,text):

        tags=[text[i+1] for i in range(0,len(text)) if text[i] == '@']
        print (tags)
        return tags
    print (tags)
    # caring off percent
    def percentage(self,text):
        percent = []
        for prc in text:
            cnt = cnt + 1
            if prc in ('percent', 'percentage'):
                percent.append("{}%".format(text[cnt - 2]))
            if '%' in prc:
                percent.append(prc)

        print(percent)

        return percent

    # caring off hashtag
    def hashtag(self,text):
        #print(text)
        #hashtaglist = [w for w in text if w[0] == '#']
        terms = []
        #print (hashtaglist)
        trms=[]
        upcases = []

        for i in range(0, len(text) - 1):

            if (text[i] == '#'):
                curren_hashtag = text[i + 1]
                if (curren_hashtag[
                    0].isupper() and curren_hashtag != curren_hashtag.isupper()):  # words that seperated by uppercase like #StayAtHome
                    upcases.append("#{}".format(text[i + 1].lower()))
                    for w1 in re.findall('[A-Z][^A-Z]*', text[i + 1]):
                        upcases.append(w1.lower())
                flag = 'true'
                sgn = 'true'
                if (text[i + 1][0].islower() and text[i + 1] != text[
                    i + 1].islower()):  # words that starts with lowercase like #stayAtHome
                    cnt1 = 0
                    indx = 0
                    for j in curren_hashtag:
                        if (j.isupper() and sgn == 'true'):
                            indx = cnt1
                            sgn = 'false'
                        cnt1 = cnt1 + 1
                    if (sgn == 'false'):
                        terms.append(text[i + 1][0:indx])
                        for k in re.findall('[A-Z][^A-Z]*', text[i + 1][indx:]):
                            terms.append(k.lower())
                        terms.append("#{}".format(text[i + 1].lower()))

                for j in text[i + 1]:  # words that seperated by puncutation #stay_at_home
                    if j in punctuation and j not in ['@', '#']:
                        for k in re.compile(r'[\s{}]+'.format(re.escape(punctuation))).split(text[i + 1]):
                            terms.append(k.lower())
                        terms.append("#{}".format(text[i + 1].lower()))
                        break


          #for i in hashtaglist:
        print(terms)


        '''''''''
        hashtaglist = [w for w in text if w[0] == '#']

        #upper case dispute from the words
        upcases = [[re.findall('[A-Z][^A-Z]*',w[1:]),w] for w in text if w[1].isupper()]

        #punctation dispute from the words
        pctionltrs = [[re.findall(punctuation,w[1:]),w] for w in text if p in w  for p in punctuation]
        '''''''''

        return terms + upcases


    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """

        text_tokens = word_tokenize(text)
        text_tokens = self.hashtag(text_tokens)
        text_tokens = self.tags(text_tokens)
        text_tokens = self.percentage(text_tokens)
        #text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
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

        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]
        term_dict = {}

        tokenized_text = self.parse_sentence(full_text)

        doc_length = len(tokenized_text)  # after text operations.

        for term in tokenized_text:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document
