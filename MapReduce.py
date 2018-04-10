from multiprocessing import Process
from FileOperator import FileOperator
from Node import Node


class MapReduce:
    def __init__(self, file_operator):
        self.file_operator = file_operator

    def set_mapper(self, mapper):
        self.mapper = mapper
        return self

    def set_reducer(self, reducer):
        self.reducer = reducer
        return self

    def run(self, n_workers):
        self.data_split = self.file_operator.split(n_workers)

        workers = [Node(self.mapper) for i in range(n_workers)]
        map_workers = []
        tmp_data_storages = []

        for thread_id in range(n_workers):
            tmp_data_storage = str(thread_id) + ".txt"
            tmp_data_storages.append(tmp_data_storage)
            p = Process(target=workers[thread_id].apply, args=(self.data_split[thread_id], tmp_data_storage))
            p.start()
            map_workers.append(p)
        [t.join() for t in map_workers]

        for worker in range(n_workers):
            f_res = open(self.file_operator.output_file)
            result = f_res.read()
            f_res.close()
            data = open(tmp_data_storages[worker]).read()
            result = self.reducer(result, data)
            open(self.file_operator.output_file, "w").write(str(result))

        return self

def mymap(data):
    data = data.split()
    words_any = 0  # counting word any
    for word in data:
        if word.lower() == "any":
            words_any += 1
    return str(words_any)


def myreduce(result, new_data):
    res = 0
    if result: # check if output file is not empty
        res = int(result)
    new_data = int(new_data)
    return res + new_data


if __name__ == "__main__":
    NUMBER_OF_NODES = 10
    f = FileOperator("shakespeare.txt", "res.txt")

    MapReduce(f).set_mapper(mymap)\
        .set_reducer(myreduce)\
        .run(NUMBER_OF_NODES)
