# USER INFO
MY_API_KEY='API-KEY' #API Key'inizi Buraya Giriniz
MY_USERNAME = "TC veya Denizbank Kullanici Adi" #TC veya Denizbank Kullanıcı Adınızı Buraya Giriniz
MY_PASSWORD = "Şifre" #Denizbank İnternet Bankacılığı Şifrenizi Buraya Giriniz

# URLS
hostname = "www.algolab.com.tr"
api_hostname = f"https://{hostname}"
api_url = api_hostname + "/api"
socket_url = f"wss://{hostname}/api/ws"

# ORDER STATUS
ORDER_STATUS = {0: "Bekleyen",
1: "Teslim Edildi",
2: "Gerçekleşti",
3: "Kismi Gerçekleşti",
4: "İptal Edildi",
5: "Değiştirildi",
6: "Askiya Alindi",
7: "Süresi Doldu",
8: "Hata"}

# Tick to OHLCV converter için takip edilmesi istenen semboller, boş olarak verilirse tüm semboller veya marketler takip edilir.
TRACKED_SYMBOLS = []
TRACKED_MARKETS = []
BUFFER_SIZE = 250  # Converter için kaç veri sayısı biriktirilip json dosyalarına aktarılmalı

# ENDPOINTS
URL_LOGIN_USER = "/api/LoginUser"
URL_LOGIN_CONTROL = "/api/LoginUserControl"
URL_GETEQUITYINFO = "/api/GetEquityInfo"
URL_GETSUBACCOUNTS = "/api/GetSubAccounts"
URL_INSTANTPOSITION = "/api/InstantPosition"
URL_TODAYTRANSACTION = "/api/TodaysTransaction"
URL_VIOPCUSTOMEROVERALL = "/api/ViopCustomerOverall"
URL_VIOPCUSTOMERTRANSACTIONS = "/api/ViopCustomerTransactions"
URL_SENDORDER = "/api/SendOrder"
URL_MODIFYORDER = "/api/ModifyOrder"
URL_DELETEORDER = "/api/DeleteOrder"
URL_DELETEORDERVIOP = "/api/DeleteOrderViop"
URL_SESSIONREFRESH = "/api/SessionRefresh"
URL_GETCANDLEDATA = "/api/GetCandleData"
URL_VIOPCOLLETERALINFO = "/api/ViopCollateralInfo"
URL_RISKSIMULATION = "/api/RiskSimulation"
URL_GETEQUITYORDERHISTORY = "/api/GetEquityOrderHistory" # 404 Hatası alıyor
URL_GETVIOPORDERHISTORY = "/api/GetViopOrderHistory" # 404 Hatası alıyor
URL_CASHFLOW = "/api/CashFlow"
URL_ACCOUNTEXTRE = "/api/AccountExtre"
