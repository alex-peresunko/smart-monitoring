class NewrelicTask:
    def __init__(self, signal_id, newrelic_account_id, week_number, nrql_query):
        self.signal_id = signal_id
        self.account_id = newrelic_account_id
        self.week_number = week_number
        self.nrql_query = nrql_query
        self.result = NewrelicTaskData()

    def export_dict(self):
        return {
            "signal_id": self.signal_id,
            "account_id": self.account_id,
            "week_number": self.week_number,
            "nrql_query": self.nrql_query,
            "result": self.result.data
        }


class NewrelicTaskData:
    def __init__(self, request_result = None):
        self.data = request_result