#!/usr/bin/env python3

import json
import queue
import time
from src.nrql_requester import NRQLRequester
from src.config_parser import ConfigParser
from src.logger import Logger

config = ConfigParser().config

CURRENT_TIME_MS = int(round(time.time() * 1000))


def nrql_add_time(nrql, start_time, end_time):
    return nrql + " SINCE " + str(start_time) + " UNTIL " + str(end_time)

def get_periods():
    periods = []
    for i in range(1, config["profile"]["last_weeks_to_check"] + 1):
        periods.append((CURRENT_TIME_MS - i*7*24*60*60*1000 - config["profile"]["last_period_min"]*60*1000, CURRENT_TIME_MS - i*7*24*60*60*1000 ))
    return periods

def main():

    logger = Logger().get_logger(__name__)

    profile = json.load(open(config["profile"]["name"], "r"))

    input_queue = queue.Queue()
    output_queue = queue.Queue()

    requester = NRQLRequester(input_queue, output_queue, config["generic"]["parallelism"])

    signal_id = 0
    for signal in profile["signals"]:
        signal_id += 1
        week_num = 0
        nrql = nrql_add_time(signal["nrql"], CURRENT_TIME_MS - config["profile"]["last_period_min"]*60*1000, CURRENT_TIME_MS)
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

    for signal in profile["signals"]:
        signal["data"] = {}
        for i in range(1, config["profile"]["last_weeks_to_check"] + 1):
            signal["data"][i] = []

    for signal in profile["signals"]:
        print(f"Signal: {signal["name"]}")
        print(f"Data: {get_data_by_id(signal["id"])}")



if __name__ == "__main__":
    main()