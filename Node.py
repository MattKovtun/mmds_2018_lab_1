from multiprocessing import Manager, Pool
from FileOperator import FileOperator
from Worker import Worker
from config import NUMBER_OF_WORKERS, OUTPUT_FILE_EXTENSION


class Node:
    def __init__(self, index, map_fn, shuffle_fn):
        self.index = index
        self.map_fn = map_fn
        self.shuffle_fn = shuffle_fn
        self.number_of_workers = NUMBER_OF_WORKERS

    def spawn_workers(self, data_split):
        manager = Manager()
        node_dict = manager.dict()
        workers = Pool()

        for worker in range(self.number_of_workers):
            workers.apply_async(Worker.perform, args=((data_split[worker], worker, self.map_fn, node_dict)))
        workers.close()
        workers.join()

        return node_dict

    def shuffle_results(self, node_storage):
        return self.shuffle_fn(node_storage)

    def apply(self, data):
        data_split = FileOperator.split_data(data, self.number_of_workers)
        node_storage = self.spawn_workers(data_split)
        result = self.shuffle_results(dict(node_storage))

        with open(str(self.index) + OUTPUT_FILE_EXTENSION, "w") as node_store:
            node_store.write(str(result))
