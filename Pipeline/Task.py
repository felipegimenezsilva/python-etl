from pandas import DataFrame

class TaskMemory:
    """Store information in ram"""

    def __init__(self):
        """Initial configurations of TaskMemory class"""
        self.__internal_memory__ = {}

    def add(self, key : str, value: object) -> None:
        """Store information in ram"""
        if key in self.__internal_memory__.keys():
            raise Exception(f'Key conflict in TaskMemory:',key)
        self.__internal_memory__[key] = value

    def remove(self, key : str) -> object:
        """Remove object from TaskMemory"""
        if not key in self.__internal_memory__.keys():
            raise Exception('Not found key to remove in TaskMemory:',key)
        return self.__internal_memory__.pop(key)

    def get(self, key: str) -> object:
        """Get a value from internal memory"""
        if not key in self.__internal_memory__.keys():
            raise Exception(f'Key not found in TaskMemory:', key)
        return self.__internal_memory__.get(key)
    
    def update(self, key : str , value: object) -> None:
        """Update a value stored in internal memory"""
        if not key in self.__internal_memory__.keys():
            raise Exception(f'Key not found in TaskMemory:', key)
        self.__internal_memory__[key] = value

class Task:
    """Definition of steps to automate task execution"""

    def __init__(self) -> None:
        """Initial configurations of Task class"""
        self.memory = TaskMemory()
        self.sharedMemory= None

    def setSharedMemory(self, sharedMemory : TaskMemory) -> None:
        """Connect a shared memory to the task. The shared memory """
        """is accessible for any Task in the same TaskQueue"""
        if self.sharedMemory: raise Exception('Task.sharedMemory already exists')
        self.sharedMemory = sharedMemory

    def getTaskName(self) -> str:
        """Return the Task class name"""
        return self.__class__.__name__

    def skip(self) -> bool:
        """Stop the execution of actual pipeline"""
        return False

    def setup(self) -> dict:
        """Provide initial values to Task.extract"""
        return {}

    def extract(self, data : dict = {}) -> DataFrame:
        """Collect information from other sources (DB/API/CSV)"""
        return DataFrame()
    
    def transform(self, data: DataFrame) -> DataFrame:
        """Transform information"""
        return data
    
    def load(self, data: DataFrame) -> bool:
        """Update source"""
        return True
    
    # TODO: track fails
    def run(self) -> bool:
        """Execute all steps of task"""
        data = self.setup()
        data = self.extract(data=data)
        data = self.transform(data=data)
        data = self.load(data=data)
        return data
    
class TaskQueue:
    """Organize tasks in a queue of execution"""

    def __init__(self) -> None:
        """Initial configurations of TaskQueue class"""
        self.__cache__ = TaskMemory()

    def queue(self) -> list:
        """Execution queue. This function must return a list of """
        """each class to be executed (ordered by execution)"""
        raise Exception('TaskQueue.queue not implemented yet')

    def updateCache(self, memory : TaskMemory) -> TaskMemory:
        """Rewrite the cache object in the queue"""
        self.__cache__ = memory

    def getCache(self) -> TaskMemory:
        """Returns the shared memory used by the tasks"""
        return self.__cache__

    # TODO: track fails
    def run(self) -> bool:
        """Run each task in order"""
        for task_class in self.queue():
            task = task_class()
            task.setSharedMemory(self.__cache__)
            task.run()
        return True
    
class Manager:
    """Organize TaskQueue for parallel executions"""

    def __init__(self, parallel : bool = True) -> None:
        """Initial configurations of Manager class"""
        self.parallel = parallel
        self.__task_queues__ = []

    def add(self, taskQueue : TaskQueue) -> None:
        """Includes a taskQueue in the executions"""
        self.__task_queues__.append(taskQueue)

    # TODO: track fails
    def run(self) -> bool:
        """Execute all the queues"""
        if self.parallel: self.parallel_run()
        else: self.sequencial_run() 

    # TODO
    def parallel_run(self) -> bool:
        for queue in self.__task_queues__:
            self.__execute_queue__(queueClass=queue)
        return True

    # TODO
    def sequencial_run(self) -> bool:
        for queue in self.__task_queues__:
            self.__execute_queue__(queueClass=queue)
        return True

    # TODO
    def __execute_queue__(self, queueClass : TaskQueue) -> bool:
        queue = queueClass()
        return queue.run()