import os
import threading
import pandas as pd


class ReadFile:

    def __init__(self, corpus_path):
        self.corpus_path = ''
        self.files_list = []
        for path, subdirs, files in os.walk(corpus_path):
            for name in files:
                if os.path.isfile(os.path.join(path, name)) and os.path.splitext(name)[1] == '.parquet':
                    self.files_list.append(os.path.join(path, name))

    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """
        full_path = os.path.join(self.corpus_path, file_name)
        df = pd.read_parquet(full_path, engine="pyarrow")
        return df.values.tolist()

    def get_files_number(self):
        return len(self.files_list)

    def read_file_at_index(self, i):
        if i < len(self.files_list):
            print("reading file {} from index {}".format(self.files_list[i], i))
            return self.read_file(self.files_list[i])
        return None


