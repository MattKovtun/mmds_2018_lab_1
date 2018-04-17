import random
from multiprocessing import Process, Manager
from FileOperator import FileOperator
from Worker import Worker
from config import NUMBER_OF_WORKERS, FILE_EXTENSION


class Node:
    def __init__(self, index, map_fn, shuffle_fn):
        self.index = index
        self.map_fn = map_fn
        self.shuffle_fn = shuffle_fn
        self.number_of_workers = NUMBER_OF_WORKERS

    def spawn_workers(self, data_split):
        manager = Manager()
        node_dict = manager.dict()
        workers = []

        for worker in range(self.number_of_workers):
            p = Process(target=Worker.perform, args=((data_split[worker], worker, self.map_fn, node_dict)))
            p.start()
            workers.append(p)

        for worker in workers:
            worker.join()
        return node_dict

    def shuffle_results(self, node_storage):
        return self.shuffle_fn(node_storage)

    def apply(self, data):
        data_split = FileOperator.split_data(data, self.number_of_workers)
        node_storage = self.spawn_workers(data_split)
        result = self.shuffle_results(dict(node_storage))

        with open(str(self.index) + FILE_EXTENSION, "w") as node_store:
            node_store.write(str(result))
