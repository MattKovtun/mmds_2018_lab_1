class FileOperator:
    def __init__(self, input_file, output_file):
        self.output_file = output_file
        self.input_file = input_file
        open(output_file, "w").close()

    def read_file(self):
        """
        Any file preprocessing if needed happens here
        :return: list of words
        """
        data = []
        if self.input_file.endswith(".csv"):
            data = open(self.input_file).read().strip().split(",")
        elif self.input_file.endswith(".txt"):
            data = open(self.input_file).read().strip().split()

        return data

    @staticmethod
    def split_data(data, splits):
        # VALID SPLIT, also it doesn't care about words
        split = []
        chunk = len(data) // splits
        s = -1
        for s in range(splits - 1):
            split.append(data[s * chunk: (s + 1) * chunk])
        split.append(data[(s + 1) * chunk:])
        return split


if __name__ == "__main__":
    input_file = "shakespeare.txt"
    a = FileOperator(input_file, 213)
    d = open(input_file).read()
    print(len(FileOperator.split_data(d, 5)))


