class FileOperator:
    def __init__(self, input_file, output_file):
        self.output_file = output_file
        self.input_file = input_file
        open(output_file, "w").close()

    def split(self, splits):
        # VALID SPLIT
        data = []
        f = open(self.input_file).read()
        chunk = len(f) // splits
        for s in range(splits - 1):
            data.append(f[s * chunk: (s + 1) * chunk])
        data.append(f[(s + 1) * chunk:])
        return data


if __name__ == "__main__":
    a = FileOperator("shakespeare.txt", 213)
    print(len(a.split(5)))
