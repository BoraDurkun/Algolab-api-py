from algolab import API
from ws import AlgoLabSocket
import json,time
from config import *

def process_msg(msg):
    try:
        t = msg["type"]
        content = msg["content"]
        print("Type: " + t +"Content: " + content)
    except Exception as e:
        print("Error processing message: ", e)

if __name__ == "__main__":

    algo = API(api_key=MY_API_KEY, username=MY_USERNAME, password=MY_PASSWORD, auto_login=True)
    soket = AlgoLabSocket(algo.api_key, algo.hash, "T")
    soket.connect()
    while not soket.connected:
        time.sleep(0.01)

    data = {"Type": "T", "Symbols": ["ALL"]}
    soket.send(data)

    i = 0
    while soket.connected:
        data = soket.recv()
        i += 1
        if data:
            try:
                msg = json.loads(data)
                print(msg)
            except:
                print("error 1")
                soket.close()
                break
