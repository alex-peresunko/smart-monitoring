import threading
import sys
from newrelic import NRQL


# Custom Exception Class
class MyException(Exception):
    pass


# Custom Thread Class
def process(input_queue, output_queue):
    name = threading.current_thread().name
    nrql_obj = NRQL()
    try:
        while True:
            data = input_queue.get()
            if data is None:
                break
            # Process the data (replace this with actual processing logic)
            request_id, account_id, nrql_query = data
            nrql_obj.set_account(account_id)
            result = None
            result = nrql_obj.query(nrql_query)
            output_queue.put((request_id, result))
            input_queue.task_done()
    except Exception as e:
        raise MyException("An error in thread " + name)


class MyThread(threading.Thread):
    # Function that raises the custom exception

    def run(self):
        # Variable that stores the exception, if raised by someFunction
        self.exc = None
        try:
            process()
        except BaseException as e:
            self.exc = e

    def join(self):
        threading.Thread.join(self)
        # Since join() returns in caller thread
        # we re-raise the caught exception
        # if any was caught
        if self.exc:
            raise self.exc




class NRQLRequester:
    def __init__(self, input_queue, output_queue, num_workers=5, *args, **kwargs):
        self.num_workers = num_workers
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.threads = []

        # Start worker threads
        for _ in range(self.num_workers):
            thread = threading.Thread(target=self.worker, args=(self.input_queue, self.output_queue))
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
            result = nrql_obj.query(nrql_query)
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
            try:
                thread.join()
            except Exception as e:
                print("Exception Handled in Main, Details of the Exception:", e)


        # Retrieve results from the output queue
        results = []
        while not self.output_queue.empty():
            results.append(self.output_queue.get())

        return results

