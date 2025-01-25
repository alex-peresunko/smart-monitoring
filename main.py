from nrql_requester import NRQLRequester
import json
import queue
import uuid



def main():
    profile = json.load(open("profile.json", "r"))

    input_queue = queue.Queue()
    output_queue = queue.Queue()

    requester = NRQLRequester(input_queue, output_queue)

    for signal in profile["signals"]:
        name = signal["name"]
        nrql = signal["nrql"]
        account_id = signal["nr_account"]

        request_id = str(uuid.uuid4())
        request_item = (request_id, account_id, nrql)
        print(request_item)
        requester.request(request_item)



    for result in  requester.get_results():
        print(result)






if __name__ == "__main__":
    main()