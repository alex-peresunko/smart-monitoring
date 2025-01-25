import requests
import json
import os
import time


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
            response = requests.post(self.url, json=json, headers=headers)
            if response.status_code == 200:
                return response
            if response.status_code == 429:
                delay += attempt * self._step_delay_factor
                attempt += 1
            else:
                raise Exception(f"Error: {response.status_code} - {response.text}")
        print("ERROR: Could not obtain response from Newrelic")
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
        if response.status_code == 200:
            data = response.json()
            if 'errors' in data:
                for error in data['errors']:
                    print(json.dumps(error, indent=2))
                return None
            else:
                result_count = len(data["data"]["actor"]["account"]["nrql"]["results"])
                # print(f"Result count: {result_count}")
                return data
        else:
            raise Exception(f"Error: {response.status_code} - {response.text}")

    def query(self, nrql_query):
        if not self.account_id or not self.api_key:
            return None
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

