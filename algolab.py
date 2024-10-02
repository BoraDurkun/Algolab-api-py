from datetime import datetime
import requests, hashlib, json, base64, inspect, time
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from threading import Thread
from config import *

last_request = 0.0
LOCK = False
class API():
    def __init__(self, api_key, username, password, auto_login=True, keep_alive=False, verbose=True):
        """
        api_key: API_KEY
        username: TC Kimlik No
        password: DENIZBANK_HESAP_ŞİFRENİZ
        verbose: True, False - İşlemlerin çiktisini yazdirir
        """
        if verbose:
            print("Sistem hazirlaniyor...")
        try:
            self.api_code = api_key.split("-")[1]
        except:
            self.api_code = api_key
        self.api_key = "API-" + self.api_code
        self.username = username
        self.password = password
        self.api_hostname = api_hostname
        self.api_url = api_url
        self.auto_login = auto_login
        self.headers = {"APIKEY": self.api_key}
        self.keep_alive = keep_alive
        self.thread_keepalive = Thread(target=self.ping)
        self.verbose = verbose
        self.ohlc = []
        self.token = ""
        self.new_hour = False
        self.sms_code = ""
        self.hash = ""
        self.start()

    def save_settings(self):
        data = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "token": self.token,
            "hash": self.hash
        }
        with open("./data.json", "w") as f:
            # Dosyaya yaz
            json.dump(data, f)

    def load_settings(self):
        try:
            with open("./data.json", "r") as f:
                data = json.load(f)
                self.token = data["token"]
                self.hash = data["hash"]
                return True
        except:
            return False

    def start(self):
        if self.auto_login:
            # önceki login bilgileri varsa yükle
            s = self.load_settings()
            if not s or not self.is_alive:
                if self.verbose:
                    print("Login zaman aşimina uğradi. Yeniden giriş yapiliyor...")
                    
                if self.LoginUser():
                    self.LoginUserControl()
            else:
                if self.verbose:
                    print("Otomatik login başarili...")
                    
        if self.keep_alive:
            self.thread_keepalive.start()

    def ping(self):
        while self.keep_alive:
            p = self.SessionRefresh(silent=True)
            time.sleep(60 * 15)

    # LOGIN

    def LoginUser(self):
        try:
            if self.verbose:
                print("Login işlemi yapiliyor...")
                
            f = inspect.stack()[0][3]
            u = self.encrypt(self.username)
            p = self.encrypt(self.password)
            payload = {"username": u, "password": p}
            endpoint = URL_LOGIN_USER
            resp = self.post(endpoint=endpoint, payload=payload, login=True)
            login_user = self.error_check(resp, f)
            if not login_user:
                return False
            login_user = resp.json()
            succ = login_user["success"]
            msg = login_user["message"]
            content = login_user["content"]
            if succ:
                self.token = content["token"]
                if self.verbose:
                    print("Login başarili.")
                return True
            else:
                if self.verbose:
                    print(f"Login Başarisiz. self.mesaj: {msg}")
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")

    def LoginUserControl(self):
        try:
            if self.verbose:
                print("Login kontrolü yapiliyor...")
                
            self.sms_code = input("Cep telefonunuza gelen SMS kodunu girin: ")
            f = inspect.stack()[0][3]
            t = self.encrypt(self.token)
            s = self.encrypt(self.sms_code)
            payload = {'token': t, 'password': s}
            endpoint = URL_LOGIN_CONTROL
            resp = self.post(endpoint, payload=payload, login=True)
            login_control = self.error_check(resp, f)
            if not login_control:
                return False
            login_control = resp.json()
            succ = login_control["success"]
            msg = login_control["message"]
            content = login_control["content"]
            if succ:
                self.hash = content["hash"]
                if self.verbose:
                    print("Login kontrolü başarili.")
                    
                self.save_settings()
                return True
            else:
                if self.verbose:
                    print(f"Login kontrolü başarisiz.\nself.mesaj: {msg}")
                    
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            

    # REQUESTS

    def SessionRefresh(self, silent=False):
        """
        Oturum süresi uzatmak için atılan bir istektir.
        Cevap olarak Success: True veya eğer hash'iniz geçerliliğini yitirmişte 401 auth hatası olarak döner.
        """
        try:
            f = inspect.stack()[0][3]
            endpoint = URL_SESSIONREFRESH
            payload = {}
            resp = self.post(endpoint, payload=payload)
            return self.error_check(resp, f, silent)
        except Exception as e:
            if not silent:
                print(f"{f}() fonsiyonunda hata oluştu: {e}")
                

    def GetEquityInfo(self, symbol):
        """
        Sembolle ilgili tavan taban yüksek düşük anlık fiyat gibi bilgileri çekebilirsiniz.
        :String symbol: Sembol Kodu Örn: ASELS
        """
        try:
            f = inspect.stack()[0][3]
            endpoint = URL_GETEQUITYINFO
            payload = {'symbol': symbol}
            resp = self.post(endpoint, payload=payload)
            return self.error_check(resp, f)
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            

    def GetSubAccounts(self, silent=False):
        try:
            f = inspect.stack()[0][3]
            end_point = URL_GETSUBACCOUNTS
            resp = self.post(end_point, {})
            return self.error_check(resp, f, silent=silent)
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            

    def GetInstantPosition(self, sub_account=""):
        """
        Yatırım Hesabınıza bağlı alt hesapları (101, 102 v.b.) ve limitlerini görüntüleyebilirsiniz.
        """
        try:
            f = inspect.stack()[0][3]
            end_point = URL_INSTANTPOSITION
            payload = {'Subaccount': sub_account}
            resp = self.post(end_point, payload)
            return self.error_check(resp, f)
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            

    def GetTodaysTransaction(self, sub_account=""):
        """
        Günlük işlemlerinizi çekebilirsiniz.(Bekleyen gerçekleşen silinen v.b.)
        """
        try:
            f = inspect.stack()[0][3]
            end_point = URL_TODAYTRANSACTION
            payload = {'Subaccount': sub_account}
            resp = self.post(end_point, payload)
            return self.error_check(resp, f)
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            

    def GetViopCustomerOverall(self, sub_account=""):
        try:
            f = inspect.stack()[0][3]
            end_point = URL_VIOPCUSTOMEROVERALL
            payload = {'Subaccount': sub_account}
            resp = self.post(end_point, payload)
            return self.error_check(resp, f)
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            

    def GetViopCustomerTransactions(self, sub_account=""):
        try:
            f = inspect.stack()[0][3]
            end_point = URL_VIOPCUSTOMERTRANSACTIONS
            payload = {'Subaccount': sub_account}
            resp = self.post(end_point, payload)
            return self.error_check(resp, f)
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            


    def ViopColleteralInfo(self, sub_account=""):
        try:
            f = inspect.stack()[0][3]
            end_point = URL_VIOPCOLLETERALINFO
            payload = {'Subaccount': sub_account}
            resp = self.post(end_point, payload)
            return self.error_check(resp, f)
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            
            
    def RiskSimulation(self, sub_account=""):
        try:
            f = inspect.stack()[0][3]
            end_point = URL_RISKSIMULATION
            payload = {'Subaccount': sub_account}
            resp = self.post(end_point, payload)
            return self.error_check(resp, f)
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            
    def AccountExtre(self, sub_account="", start_date=None, end_date=None):
        """
        start_date: başlangiç tarihi "2023-07-01 00:00:00.0000" iso formatında
        end_date: bitiş tarihi "2023-07-01 00:00:00.0000" iso formatında
        """
        try:
            f = inspect.stack()[0][3]
            end_point = URL_ACCOUNTEXTRE
            # datetime nesneleri isoformat() ile dönüştürülüyor
            payload = {
                'start': start_date.isoformat() if start_date else None,
                'end': end_date.isoformat() if end_date else None,
                'Subaccount': sub_account
            }
            resp = self.post(end_point, payload)
            return self.error_check(resp, f)
        except Exception as e:
            print(f"{f}() fonksiyonunda hata oluştu: {e}")
            
    def CashFlow(self, sub_account=""):
        try:
            f = inspect.stack()[0][3]
            end_point = URL_CASHFLOW
            payload = {'Subaccount': sub_account}
            resp = self.post(end_point, payload)
            return self.error_check(resp, f)
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}") 

    def GetCandleData(self, symbol, period):
        """
        Belirlediğiniz sembole ait son 250 barlık OHLCV verileri çekilebilir.
        symbol: Sembol kodu
        period: İstenilen bar periyodu dakikalık olarak girilir.(1,2,5,10,15,30,60,120,240,480,1440(Günlük için))

        Örnek Body:
        {
            "symbol": "TSKB",
            "period": "1440",
        }
        """
        try:
            f = inspect.stack()[0][3]
            end_point = URL_GETCANDLEDATA
            payload = {
                'symbol': symbol,
                'period': period
            }
            resp = self.post(end_point, payload)
            return self.error_check(resp, f)
        except Exception as e:
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            

    # ORDERS

    def SendOrder(self, symbol, direction, pricetype, price, lot, sms, email, subAccount):
        """
        :String symbol: Sembol Kodu
        :String direction: İşlem Yönü: BUY / SELL (Aliş/Satiş)
        :String pricetype: Emir Tipi: piyasa/limit
        :String price: Emir tipi limit ise fiyat girilmelidir. (Örn. 1.98 şeklinde girilmelidir.)
        :String lot: Emir Adeti
        :Bool sms: Sms Gönderim
        :Bool email: Email Gönderim
        :String subAccount: Alt Hesap Numarasi “Boş gönderilebilir. Boş gönderilir ise Aktif Hesap Bilgilerini getirir.”

        Örnek Body:
        {
            "symbol": "TSKB",
            "direction": "BUY",
            "pricetype": "limit",
            "price": "2.01",
            "lot": "1",
            "sms": True,
            "email": False,
            "Subaccount": ""
        }
        """
        try:
            end_point = URL_SENDORDER
            payload = {
                "symbol": symbol,
                "direction": direction,
                "pricetype": pricetype,
                "price": price,
                "lot": lot,
                "sms": sms,
                "email": email,
                "subAccount": subAccount
            }
            resp = self.post(end_point, payload)
            try:
                data = resp.json()
                return data
            except:
                f = inspect.stack()[0][3]
                print(f"{f}() fonksiyonunda veri tipi hatasi. Veri, json formatindan farkli geldi:")
                
                print(resp.text)
                
        except Exception as e:
            f = inspect.stack()[0][3]
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
             

    def ModifyOrder(self, id, price, lot, viop, subAccount):
        """
        :String id: Emrin ID’ si
        :String price: Düzeltilecek Fiyat
        :String lot: Lot Miktari (Viop emri ise girilmelidir.)
        :Bool viop: Emrin Viop emri olduğunu belirtir. “Viop emri ise true olmalidir.”
        :String subAccount: Alt Hesap Numarasi “Boş gönderilebilir. Boş gönderilir ise Aktif Hesap Bilgilerini getirir.”

        Örnek Body
        {
            "id":"001VEV",
            "price":"2.04",
            "lot":"0",
            "viop":false,
            "subAccount":""
        }
        """
        try:
            end_point = URL_MODIFYORDER
            payload = {
                'id': id,
                'price': price,
                'lot': lot,
                'viop': viop,
                'subAccount': subAccount
            }
            resp = self.post(end_point, payload)
            try:
                data = resp.json()
                return data
            except:
                f = inspect.stack()[0][3]
                print(f"{f}() fonksiyonunda veri tipi hatasi. Veri, json formatindan farkli geldi:")
                
                print(resp.text)
                
        except Exception as e:
            f = inspect.stack()[0][3]
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            

    def DeleteOrder(self, id, subAccount):
        """
        :String id: Emrin ID’ si
        :String subAccount: Alt Hesap Numarasi “Boş gönderilebilir. Boş gönderilir ise Aktif Hesap Bilgilerini getirir.”

        Örnek Body
        {
            "id":"001VEV",
            "subAccount":""
        }
        """
        try:
            end_point = URL_DELETEORDER
            payload = {
                'id': id,
                'subAccount': subAccount
            }
            resp = self.post(end_point, payload)
            try:
                data = resp.json()
                return data
            except:
                f = inspect.stack()[0][3]
                print(f"{f}() fonksiyonunda veri tipi hatasi. Veri, json formatindan farkli geldi:")
                
                print(resp.text)
                
        except Exception as e:
            f = inspect.stack()[0][3]
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            

    def DeleteOrderViop(self, id, adet, subAccount):
        """
        :String id: Emrin ID’ si
        :String adet: İptal edilecek adet
        :String subAccount: Alt Hesap Numarasi “Boş gönderilebilir. Boş gönderilir ise Aktif Hesap Bilgilerini getirir.”

        Örnek Body
        {
            "id":"001VEV",
            "adet":"1",
            "subAccount":""
        }
        """
        try:
            end_point = URL_DELETEORDER
            payload = {
                'id': id,
                'adet': adet,
                'subAccount': subAccount
            }
            resp = self.post(end_point, payload)
            try:
                data = resp.json()
                return data
            except:
                f = inspect.stack()[0][3]
                print(f"{f}() fonksiyonunda veri tipi hatasi. Veri, json formatindan farkli geldi:")
                
                print(resp.text)
                
        except Exception as e:
            f = inspect.stack()[0][3]
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            
            
    def GetEquityOrderHistory(self, id, subAccount):
        """
        :String id: Emrin ID’ si
        :String subAccount: Alt Hesap Numarasi “Boş gönderilebilir. Boş gönderilir ise Aktif Hesap Bilgilerini getirir.”

        Örnek Body
        {
            "id":"001VEV",
            "subAccount":""
        }
        """
        try:
            end_point = URL_GETEQUITYORDERHISTORY
            payload = {
                'id': id,
                'subAccount': subAccount
            }
            resp = self.post(end_point, payload)
            try:
                data = resp.json()
                return data
            except:
                f = inspect.stack()[0][3]
                print(f"{f}() fonksiyonunda veri tipi hatasi. Veri, json formatindan farkli geldi:")
                
                print(resp.text)
                
        except Exception as e:
            f = inspect.stack()[0][3]
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            

    def GetViopOrderHistory(self, id, subAccount):
        """
        :String id: Emrin ID’ si
        :String subAccount: Alt Hesap Numarasi “Boş gönderilebilir. Boş gönderilir ise Aktif Hesap Bilgilerini getirir.”

        Örnek Body
        {
            "id":"001VEV",
            "subAccount":""
        }
        """
        try:
            f = inspect.stack()[0][3]
            end_point = URL_GETVIOPORDERHISTORY
            payload = {
                'id': id,
                'subAccount': subAccount
            }
            resp = self.post(end_point, payload)
            try:
                data = resp.json()
                return data
            except:
                f = inspect.stack()[0][3]
                print(f"{f}() fonksiyonunda veri tipi hatasi. Veri, json formatindan farkli geldi:")
                
                print(resp.text)
                
        except Exception as e:
            f = inspect.stack()[0][3]
            print(f"{f}() fonsiyonunda hata oluştu: {e}")
            
    # TOOLS

    def GetIsAlive(self):
        try:
            resp = self.GetSubAccounts(silent=True)
            return resp["success"]
        except:
            return False

    def error_check(self, resp, f, silent=False):
        try:
            if resp.status_code == 200:
                data = resp.json()
                return data
            else:
                if not silent:
                    print(f"Error kodu: {resp.status_code}")
                    
                    print(resp.text)
                    
                return False
        except:
            if not silent:
                print(f"{f}() fonksiyonunda veri tipi hatasi. Veri, json formatindan farkli geldi:")
                
                print(resp.text)
                
            return False

    def encrypt(self, text):
        iv = b'\0' * 16
        key = base64.b64decode(self.api_code.encode('utf-8'))
        cipher = AES.new(key, AES.MODE_CBC, iv)
        bytes = text.encode()
        padded_bytes = pad(bytes, 16)
        r = cipher.encrypt(padded_bytes)
        return base64.b64encode(r).decode("utf-8")

    def make_checker(self, endpoint, payload):
        if len(payload) > 0:
            body = json.dumps(payload).replace(' ', '')
        else:
            body = ""
        data = self.api_key + self.api_hostname + endpoint + body
        checker = hashlib.sha256(data.encode('utf-8')).hexdigest()
        return checker

    def _request(self, method, url, endpoint, payload, headers):
        global last_request, LOCK
        while LOCK:
            time.sleep(0.01)
        LOCK = True
        try:
            response = ""
            if method == "POST":
                t = time.time()
                diff = t - last_request
                wait_for = last_request > 0.0 and diff < 5.0 # son işlemden geçen süre 1 saniyeden küçükse bekle
                if wait_for:
                    time.sleep(5 - diff + 0.01)
                response = requests.post(url + endpoint, json=payload, headers=headers)
                last_request = time.time()
        finally:
            LOCK = False
        return response

    def post(self, endpoint, payload, login=False):
        url = self.api_url
        if not login:
            checker = self.make_checker(endpoint, payload)
            headers = {"APIKEY": self.api_key,
                       "Checker": checker,
                       "Authorization": self.hash
                       }
        else:
            headers = {"APIKEY": self.api_key}
        resp = self._request("POST", url, endpoint, payload=payload, headers=headers)
        return resp

    is_alive = property(GetIsAlive)
