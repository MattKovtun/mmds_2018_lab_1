class Node:
    def __init__(self, map_fn):
        self.map_fn = map_fn

    def apply(self, data, output_file):
        with open(output_file, "w") as of:
            of.write(self.map_fn(data))
