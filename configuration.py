class ConfigClass:
    def __init__(self, courpus_path='', to_stem=False):
        self.corpusPath = courpus_path
        self.savedFileMainFolder = ''
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = to_stem

        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath
