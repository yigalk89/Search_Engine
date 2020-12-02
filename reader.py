import os
import pandas as pd


def read_file_entire_path(file_name):
    """
    Reads parqeut file in filename path
    :param file_name: path to file
    :return: list of tweets read from file
    """
    df = pd.read_parquet(file_name, engine="pyarrow")
    return df.values.tolist()


class ReadFile:

    def __init__(self, corpus_path):
        self.corpus_path = corpus_path
        self.files_list = []
        # List all relevant parquet files in corpus path
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
        # Read specific file from files list obtained from corpus path
        if i < len(self.files_list):
            #print("reading file {} from index {}".format(self.files_list[i], i))
            return read_file_entire_path(self.files_list[i])
        return None


