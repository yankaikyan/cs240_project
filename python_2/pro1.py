class Doc:
    def __init__(self, name, one_norm, tf_idf, value):
        self.name = name
        self.one_norm = one_norm
        self.tf_idf = tf_idf
        self.value = value
    def __lt__(self, other):
        return self.one_norm < other.one_norm

