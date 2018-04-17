class Worker:
    @staticmethod
    def perform(data, worker_id, fn, node_dict):
        node_dict[worker_id] = fn(data)