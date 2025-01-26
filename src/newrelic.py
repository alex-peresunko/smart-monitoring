import requests
import json
import os
import time
import zlib
from src.logger import Logger


logger = Logger().get_logger(__name__)

NERDGRAPH_URL = "https://api.newrelic.com/graphql"


class NR_REQUEST(object):
    def __init__(self, url=None, step_delay_factor = 2, attempts_max = 7):
        if not url:
            self.url = NERDGRAPH_URL
        self._step_delay_factor = step_delay_factor
        self._attempts_max = attempts_max

    def post(self, json = None, headers = None):
        delay = 0
        attempt = 1
        while attempt <= self._attempts_max:
            time.sleep(delay)
            try:
                response = requests.post(self.url, json=json, headers=headers)
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                return None
            if response.status_code == 200:
                return response
            if response.status_code == 429:
                delay += attempt * self._step_delay_factor
                attempt += 1
            else:
                raise Exception(f"Error: {response.status_code} - {response.text}")
        logger.error("ERROR: Could not obtain response from Newrelic")
        return None


class NRQL(object):
    def __init__(self):
        self.account_id = None
        self.request = NR_REQUEST()

    def set_account(self, account_id):
        self.account_id = str(account_id)
        self.api_key = os.environ.get("NR_USER_API_KEY_%s" % self.account_id)

    def _make_request(self, nrql_query):
        payload = {
            "query": f"""
                {{
                    actor {{
                        account(id: {self.account_id}) {{
                            nrql(query: "{nrql_query}") {{
                                results
                            }}
                        }}
                    }}
                }}
            """
        }
        headers = {
            "Content-Type": "application/json",
            "Api-Key": self.api_key,
        }
        response = self.request.post(json=payload, headers=headers)
        data = response.json()
        if 'errors' in data:
            for error in data['errors']:
                logger.error(json.dumps(error, indent=2))
            return None
        else:
            return data


    def query(self, nrql_query):
        if not self.account_id or not self.api_key:
            raise Exception(f"No account id or API key defined for {nrql_query}")
        response = self._make_request(nrql_query)
        try:
            if response:
                data = response['data']['actor']['account']['nrql']['results'][0]
                if len(data.keys()) != 1:
                    raise Exception(f'Expected one result, got {len(data.keys())} for: {nrql_query}')
                for key in list(data.keys()):
                    value = data[key]
                    return value
        except:
            return None

class NewrelicEvent:
    def __init__(self):
        self.payload = {}

    def set_field_value(self, field_name: str, value):
        self.payload[field_name] = value


class Newrelic:
    def __init__(self, account, api_key):
        self.logger = Logger().get_logger(__name__)
        self.account = account
        self.api_key = api_key
        self.collector_url = "https://insights-collector.newrelic.com/v1/accounts/{}/events".format(self.account)

    @staticmethod
    def deflate_string(data, level=9):
        return zlib.compress(str(data).encode(), level)

    @staticmethod
    def set_events_collection(events: list, collection_name: str) -> list:
        for event in events:
            event['eventType'] = collection_name
        return events

    def submit_data(self, events, collection, compress=False):

        events = self.set_events_collection(events, collection)

        request_headers = {
            'Content-Type': 'application/json',
            'Api-Key': self.api_key
        }

        if compress:  # compressing data before submitting
            payload = self.deflate_string(json.dumps(events))
            request_headers['Content-Encoding'] = 'deflate'
        else:
            payload = json.dumps(events)

        self.logger.debug("Submitting {} events to Newrelic collection {}".format(len(events), collection))
        try:
            response = requests.post(self.collector_url,
                                     data=payload,
                                     headers=request_headers,
                                     timeout=60)
            if response.status_code == 200:
                self.logger.info("Successfully submitted events to NR. Response: {}".format(response.text))
                return True
            else:
                self.logger.error("Could not submit events to NR. code={}, message={}"
                                  .format(response.status_code, response.text))
                return False
        except requests.exceptions.RequestException as e:
            self.logger.error("Could not connect to NR. {}".format(e))
            raise requests.exceptions.RequestException(e)
