class NodeType(object):
    def __init__(self, cluster_name, required_available_memory, work_dir, gmond_port, start_timeout=30):
        super().__init__()
        self._cluster_name = cluster_name
        self._work_dir = work_dir
        self._required_available_memory = required_available_memory
        self._gmond_port = gmond_port
        self._start_timeout = start_timeout

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def work_dir(self):
        return self._work_dir

    @property
    def required_available_memory(self):
        return self._required_available_memory

    @property
    def gmond_port(self):
        return self._gmond_port

    @property
    def start_timeout(self):
        return self._start_timeout
