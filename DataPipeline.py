
"""
Pipeline
========
The Pipeline module contains all code necessary to create
data pipelines. This module relies on the following modules:

- DAG.DirectedGraph
- DAG.Node(implicitly through importing DAG.DirectedGraph)

"""
from DAG.Graph import DirectedGraph
from abc import ABCMeta, abstractmethod

class PipelineInterface:
    """
    PipelineInterface
    -----------------
    Abstract Data Pipeline Interface.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def add_task(self, dependent_nodes=None):
        # type: (...) -> callable
        """
        Add a task into the data pipeline.
        :return:
        """
        pass

    @abstractmethod
    def run(self):
        # type: (...) -> dict
        """
        Run the data pipeline.
        :return:
        """
        pass


class Pipeline(PipelineInterface):
    """
    Pipeline
    --------
    Concrete implementation of the Pipeline Interface.
    """

    def __init__(self):
        PipelineInterface.__init__(self)
        self.tasks = DirectedGraph()

    def add_task(self, dependent_nodes=None):
        def wrapper(f):
            if dependent_nodes:
                self.tasks.add(dependent_nodes, f)
            return f
        return wrapper

    def run(self):
        scheduled = self.tasks.sort()
        completed = {}

        for task in scheduled:
            for node, values in self.tasks.graph.items():
                if task in values:
                    completed[task] = task(completed[node])
            if task not in completed:
                completed[task] = task()
        return completed
