import threading
from src.newrelic import NRQL
from src.logger import Logger


logger = Logger().get_logger(__name__)

class ThreadWithException(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exception = None

    def run(self):
        try:
            if self._target:
                self._target(*self._args, **self._kwargs)
        except Exception as e:
            self.exception = e


class NRQLRequester:
    def __init__(self, input_queue, output_queue, num_workers=5, *args, **kwargs):
        self.num_workers = num_workers
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.threads = []

        # Start worker threads
        for _ in range(self.num_workers):
            thread = ThreadWithException(target=self.worker, args=(self.input_queue, self.output_queue))
            thread.start()
            self.threads.append(thread)

    def worker(self, input_queue, output_queue):
        name = threading.current_thread().name
        nrql_obj = NRQL()
        while True:
            data = input_queue.get()
            if data is None:
                break
            # Process the data (replace this with actual processing logic)
            request_id, account_id, nrql_query = data
            nrql_obj.set_account(account_id)
            result = None
            try:
                result = nrql_obj.query(nrql_query)
            except Exception as e:
                logger.error(f"Thread {name} raised an exception: {e}")
                pass
            finally:
                output_queue.put((request_id, result))
                input_queue.task_done()

    def request(self, data):
        self.input_queue.put(data)

    def get_results(self):
        # Block until all tasks are done
        self.input_queue.join()

        # Stop workers
        for _ in range(self.num_workers):
            self.input_queue.put(None)
        for thread in self.threads:
            thread.join()
            if thread.exception:
                logger.error(f"Thread {thread.name} raised an exception: {thread.exception}")


        # Retrieve results from the output queue
        results = []
        while not self.output_queue.empty():
            results.append(self.output_queue.get())

        return results

