#!/usr/bin/env python3

import json
import queue
import time
from src.nrql_requester import NRQLRequester
from src.newrelic import Newrelic, NewrelicEvent
from src.config_parser import ConfigParser
from src.logger import Logger

config = ConfigParser().config
logger = Logger().get_logger(__name__)
CURRENT_TIME_MS = int(round(time.time() * 1000))


def nrql_add_time(nrql, start_time, end_time):
    return nrql + " SINCE " + str(start_time) + " UNTIL " + str(end_time)


def get_periods():
    periods = []
    for i in range(1, config["profile"]["last_weeks_to_check"] + 1):
        periods.append((
                       CURRENT_TIME_MS - i * 7 * 24 * 60 * 60 * 1000 - config["profile"]["last_period_min"] * 60 * 1000,
                       CURRENT_TIME_MS - i * 7 * 24 * 60 * 60 * 1000))
    return periods


def sort_by_dict_keys(arr):
    # Use sorted with a lambda function to extract the key from each dictionary
    return sorted(arr, key=lambda d: list(d.keys()))

def calc_average_historical(arr):
    # Initialize variables to store the sum of values and the count of valid dictionaries
    total_sum = 0
    count = 0

    # Iterate over each dictionary in the array
    for d in arr:
        # Check if the dictionary has a key that is not 0
        if isinstance(d, dict) and len(d) == 1:
            key = list(d.keys())[0]
            if key != 0:
                # Add the value to the total sum and increment the count
                total_sum += d[key]
                count += 1

    # Calculate the average if count is greater than 0
    if count > 0:
        return total_sum / count
    else:
        return 0  # Return 0 or handle the case where no valid dictionaries are found

def submit_events_to_newrelic(events) -> bool:
    nr = Newrelic(config["newrelic"]["nr_ingest_account_id"], config["newrelic"]["ingesting_key"])
    try:
        nr.submit_data(events, config['newrelic']['collection'], compress=True)
    except Exception as e:
        logger.error(f"Failed submitting events to NR: {e}")
    return True

def main():
    profile = json.load(open(config["profile"]["name"], "r"))

    input_queue = queue.Queue()
    output_queue = queue.Queue()

    requester = NRQLRequester(input_queue, output_queue, config["generic"]["parallelism"])

    signal_id = 0
    for signal in profile["signals"]:
        signal_id += 1
        week_num = 0
        nrql = nrql_add_time(signal["nrql"], CURRENT_TIME_MS - config["profile"]["last_period_min"] * 60 * 1000,
                             CURRENT_TIME_MS)
        account_id = signal["nr_account"]
        signal["id"] = signal_id
        request_item = (signal["id"], week_num, account_id, nrql)
        logger.debug(request_item)
        requester.request(request_item)

        for from_ms, to_ms in get_periods():
            week_num += 1
            nrql = nrql_add_time(signal["nrql"], from_ms, to_ms)
            request_item = (signal["id"], week_num, account_id, nrql)
            logger.debug(request_item)
            requester.request(request_item)

    full_data = requester.get_results()

    for item in full_data:
        logger.debug(item)

    def get_data_by_id(signal_id):
        h_data = []
        for s_id, week_num, result in full_data:
            if s_id == signal_id:
                h_data.append({week_num: result})
        return h_data

    def get_latest_value(arr):
        for item in arr:
            if 0 in item.keys():
                return item[0]
        return None

    for signal in profile["signals"]:
        signal["data"] = get_data_by_id(signal["id"])
        signal["historical_avg"] = calc_average_historical(signal["data"])
        signal["curr_value_deviation"] = get_latest_value(signal["data"]) / signal["historical_avg"] * 100

    nr_events = []
    for signal in profile["signals"]:
        nr_event = NewrelicEvent()
        nr_event.set_field_value("signalName", signal["name"])
        nr_event.set_field_value("deviation", signal["curr_value_deviation"])
        nr_events.append(nr_event.payload)

    submit_events_to_newrelic(nr_events)


if __name__ == "__main__":
    main()
