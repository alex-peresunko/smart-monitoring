#!/usr/bin/env python3

import json
import queue
import uuid
from src.nrql_requester import NRQLRequester
from src.config_parser import ConfigParser
from src.logger import Logger



def main():
    config = ConfigParser().config
    logger = Logger().get_logger(__name__)

    profile = json.load(open(config["profile"]["name"], "r"))

    input_queue = queue.Queue()
    output_queue = queue.Queue()

    requester = NRQLRequester(input_queue, output_queue, config["generic"]["parallelism"])

    for signal in profile["signals"]:
        name = signal["name"]
        nrql = signal["nrql"]
        account_id = signal["nr_account"]

        request_id = str(uuid.uuid4())
        request_item = (request_id, account_id, nrql)
        logger.debug(request_item)
        requester.request(request_item)



    for result in  requester.get_results():
        print(result)




if __name__ == "__main__":
    main()