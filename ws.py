# -*- coding: utf-8 -*-
import hashlib, json, datetime, subprocess, ssl, socket
import pandas as pd
from websocket import create_connection, WebSocketTimeoutException
from config import *

class ConnectionTimedOutException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

class AlgoLabSocket():
    def __init__(self, api_key, hash, verbose=True, callback=None):
        """
        :String api_key: API_KEY
        :String hash: LoginUser'dan dönen Hash kodu
        :String type: T: Tick Paketi (Fiyat), D: Depth Paketi (Derinlik), O: Emir Statüsü
        :Obj type: callback: Soketin veriyi göndereceği fonksiyon
        """
        self.verbose = verbose
        self.callback = callback
        self.connected = False
        self.arbitraj = {}
        self.thread_running = False
        self.kurum = {}
        self.hisse = {}
        self.df = pd.DataFrame(columns=["Date", "Hisse", "Yon", "Fiyat", "Lot", "Deger", "Usd", "Alici", "Satici"])
        self.usdtry = 0.0
        self.ws = None
        self.api_key = api_key
        self.hash = hash
        self.data = self.api_key + api_hostname + "/ws"
        self.checker = hashlib.sha256(self.data.encode('utf-8')).hexdigest()
        self.request_time = datetime.datetime.now()
        self.headers = {
            "APIKEY": self.api_key,
            "Authorization": self.hash,
            "Checker": self.checker
        }

    def load_ciphers(self):
        output = subprocess.run(["openssl", "ciphers"], capture_output=True).stdout
        output_str = output.decode("utf-8")
        ciphers = output_str.strip().split("\n")
        return ciphers[0]

    def close(self):
        self.connected = False
        self.ws = None

    def connect(self):
        if self.verbose:
            print("Socket bağlantisi kuruluyor...")
        context = ssl.create_default_context()
        context.set_ciphers("DEFAULT")
        try:
            sock = socket.create_connection((hostname, 443))
            ssock = context.wrap_socket(sock, server_hostname=hostname)
            self.ws = create_connection(socket_url, socket=ssock, header=self.headers)
            self.connected = True
        except Exception as e:
            self.close()
            print(f"Socket Hatasi: {e}")
            return False
        if self.verbose and self.connected:
            print("Socket bağlantisi başarili.")
        return self.connected

    def recv(self):
        try:
            data = self.ws.recv()
        except WebSocketTimeoutException:
            data = ""
        except Exception as e:
            print("Recv Error:", e)
            data = None
            self.close()
        return data
    def send(self, d):
        """
        :param d: Dict
        """
        try:
            data = {"token": self.hash}
            for s in d:
                data[s] = d[s]
            resp = self.ws.send(json.dumps(data))
        except Exception as e:
            print("Send Error:", e)
            resp = None
            self.close()
        return resp
