#define group class
class Group:
    def __init__(self, file_list):
        self.file_list = file_list
    def setSubgroup(self, subgroup):
        self.subgroup = subgroup
    def setMaxWeight(self, maxweight):
        self.maxweight = maxweight
    def setDissimilar(self, dissimilar):
        self.dissimilar = dissimilar

