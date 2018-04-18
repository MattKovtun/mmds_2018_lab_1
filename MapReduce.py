from multiprocessing import Process
from FileOperator import FileOperator
from Node import Node
from config import NUMBER_OF_NODES, OUTPUT_FILE_EXTENSION


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

    def _map(self, nodes, data_split):
        tmp_data_storages = []
        map_nodes = []
        for node in nodes:
            tmp_data_storage = str(node.index) + OUTPUT_FILE_EXTENSION
            tmp_data_storages.append(tmp_data_storage)
            p = Process(target=nodes[node.index].apply, args=(data_split[node.index],))
            p.start()
            map_nodes.append(p)

        for node in map_nodes:
            node.join()

        return tmp_data_storages

    def _reduce(self, nodes, tmp_data_storages):
        for node in nodes:
            with open(self.file_operator.output_file) as f_res:
                result = f_res.read()

            with open(tmp_data_storages[node.index]) as data:
                result = self.reducer(result, data.read())

            open(self.file_operator.output_file, "w").write(str(result))

    def run(self, n_nodes):
        data = self.file_operator.read_file()
        data_split = FileOperator.split_data(data, n_nodes)

        nodes = [Node(i, self.mapper, self.shuffler) for i in range(n_nodes)]

        tmp_data_storages = self._map(nodes, data_split)
        self._reduce(nodes, tmp_data_storages)

        return self


def mymap(data):
    """
    Main logic of the map has to be implemented here
    :param data: list of strings
    :return:
    """

    # letters = 0
    # for word in data:
    #     letters += len(word)
    return len(data)


def myreduce(result, new_data):
    """
    Main logic of the reduce has to be implemented here
    :param result: accumulated result
    :param new_data: result of node calculation, result after shuffle
    :return: accumulated result
    """
    res = 0
    if result:  # check if output file is not empty
        res = int(result)
    new_data = int(new_data)
    return res + new_data


def myshuffle(node_storage):
    """
    Main logic of the shuffle has to be implemented here
    :param node_storage: dict of key = workers id, value = result of map calculation
    :return:
    """
    res = 0
    for k in node_storage:
        res += node_storage[k]
    return res


if __name__ == "__main__":
    f = FileOperator("100_words.txt", "res.txt")

    MapReduce(f).set_mapper(mymap) \
        .set_reducer(myreduce) \
        .set_shuffler(myshuffle) \
        .run(NUMBER_OF_NODES)
