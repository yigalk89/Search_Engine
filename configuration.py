import os

class ConfigClass:
    def __init__(self, courpus_path='', to_stem=False, output_path='.'):
        self.corpusPath = courpus_path
        self.savedFileMainFolder = output_path
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = to_stem

        #print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath

    def get_save_files_dir(self):
        return self.savedFileMainFolder
        #if self.toStem:
        #    if not os.path.exists(self.saveFilesWithStem):
        #        os.makedirs(self.saveFilesWithStem)
        #    return self.saveFilesWithStem
        #else:
        #    if not os.path.exists(self.saveFilesWithoutStem):
        #        os.makedirs(self.saveFilesWithoutStem)
        #    return self.saveFilesWithoutStem


