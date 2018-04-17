from multiprocessing import Process
from FileOperator import FileOperator
from Node import Node
from config import NUMBER_OF_NODES, FILE_EXTENSION


class MapReduce:
    def __init__(self, file_operator):
        self.file_operator = file_operator

    def set_mapper(self, mapper):
        self.mapper = mapper
        return self

    def set_reducer(self, reducer):
        self.reducer = reducer
        return self

    def set_shuffler(self, shuffler):
        self.shuffler = shuffler
        return self

    def run(self, n_nodes):
        data = open(self.file_operator.input_file).read()
        data_split = FileOperator.split_data(data, n_nodes)

        nodes = [Node(i, self.mapper, self.shuffler) for i in range(n_nodes)]
        map_nodes = []
        tmp_data_storages = []

        for node in nodes:
            tmp_data_storage = str(node.index) + FILE_EXTENSION
            tmp_data_storages.append(tmp_data_storage)
            p = Process(target=nodes[node.index].apply, args=(data_split[node.index],))
            p.start()
            map_nodes.append(p)

        for node in map_nodes:
            node.join()

        for node in nodes:
            with open(self.file_operator.output_file) as f_res:
                result = f_res.read()

            with open(tmp_data_storages[node.index]) as data:
                result = self.reducer(result, data.read())

            open(self.file_operator.output_file, "w").write(str(result))

        return self


def mymap(data):
    data = data.strip().split()
    letters = 0
    for word in data:
        letters += len(word)
    return letters


def myreduce(result, new_data):
    res = 0
    if result:  # check if output file is not empty
        res = int(result)
    new_data = int(new_data)
    return res + new_data


def myshuffle(node_storage):  # dict
    res = 0
    for k in node_storage:
        res += node_storage[k]
    return res


if __name__ == "__main__":
    f = FileOperator("test.txt", "res.txt")

    MapReduce(f).set_mapper(mymap) \
        .set_reducer(myreduce) \
        .set_shuffler(myshuffle) \
        .run(NUMBER_OF_NODES)
