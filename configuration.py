class ConfigClass:
    def __init__(self, courpus_path='', to_stem=False, output_path=''):
        self.corpusPath = courpus_path
        self.savedFileMainFolder = output_path
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = to_stem

        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath
