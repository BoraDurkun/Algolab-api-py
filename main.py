from algolab import API
from datetime import datetime
from config import *
import pandas as pd, numpy as np, json, os

############################################ ENDPOINT Fonksiyonları ##################################################

### Emir  Bilgisi
def send_order():
    symbol=input("Lütfen Sembol Bilgisi Girin: ")
    direction=input("Lütfen Yapacağınız işlemi giriniz(buy/sell): ")
    pricetype=input("Lütfen Emir Tipi Bilgisi Girin(limit/piyasa): ")
    lot=input("Lütfen Lot Bilgisi Girin: ")
    if pricetype=='piyasa':
        price=""
    else:
        price=input("Lütfen Fiyat Bilgisi Girin: ")
    order = Conn.SendOrder(symbol=symbol, direction= direction, pricetype= pricetype, price=price, lot=lot ,sms=True,email=False,subAccount="")
    print("Emir gönderme işlemi gerçekleştiriliyor...")    
    if order:
        try:
            succ = order["success"]
            if succ:
                content = order["content"]
                print("Emir Gönderildi, " + content)
            else: print(order["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def modify_order():
    id=input("Lütfen ID Bilgisi Girin: ")
    viop_input=input("Eğer VIOP emri silmek istiyorsanız 'true' istemiyorsanız 'false' olarak giriniz: ")
    viop = viop_input.strip().lower() == 'true'
    lot=input("Lütfen Lot Bilgisi Girin: ")
    price=input("Lütfen Yeni Fiyat Bilgisi Girin: ")
    modify=Conn.ModifyOrder(id=id,price=price,lot=lot,viop=viop,subAccount="")
    print("** işlemi gerçekleştiriliyor...")
    if modify:
        try:
            succ = modify["success"]
            if succ:
                content = modify["content"]
                print("Emir Düzeltildi")
            else: print(modify["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def delete_order():
    id=input("Lütfen ID Bilgisi Girin: ")
    delete=Conn.DeleteOrder(id=id,subAccount="")
    print("Emir silme işlemi gerçekleştiriliyor...")
    if delete:
        try:
            succ = delete["success"]
            if succ:
                content = delete["content"]
                content_json = json.dumps(content)
                print("Emir Silindi, " + content_json)
            else: print(delete["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def delete_order_viop():
    id=input("Lütfen ID Bilgisi Girin: ")
    adet=input("Lütfen Kontrat Sayısı Girin: ")
    delete=Conn.DeleteOrderViop(id=id,adet=adet,subAccount="")
    print("Viop Emir Silme işlemi gerçekleştiriliyor...")
    if delete:
        try:
            succ = delete["success"]
            if succ:
                content = delete["content"]
                content_json = json.dumps(content)
                print("Emir Silindi, " + content_json)
            else: print(delete["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

### Sembol Bilgisi
def get_candle_data():
    SYMBOL=input("Lütfen Sembol Bilgisi Girin: ")
    PERIOD=input("Lütfen Periyot Bilgisi Girin: ")
    candle = Conn.GetCandleData(SYMBOL, PERIOD)
    if candle:
        try:
            succ = candle["success"]
            if succ:
                ohlc = []
                content = candle["content"]
                for i in range(len(content)):
                    d = content[i]["date"]
                    try:
                        dt = datetime.strptime(d, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                    except:
                        dt = datetime.strptime(d, "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d %H:%M:%S")
                    o = content[i]["open"]
                    h = content[i]["high"]
                    l = content[i]["low"]
                    c = content[i]["close"]
                    ohlc.append([dt, o, h, l, c])
                # oluşturduğumuz listi pandas dataframe'e aktarıyoruz
                df = pd.DataFrame(columns=["date", "open", "high", "low", "close"], data=np.array(ohlc))
                print(df.tail())
                json_data=df.to_json(orient='records')
                with open(SYMBOL+PERIOD+'.json', 'w', encoding='utf-8') as f:
                    f.write(json_data)
            else: print(candle["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def get_equity_info():
    symbol=input("Lütfen Sembol Bilgisi Girin: ")
    print("** işlemi gerçekleştiriliyor...")
    info=Conn.GetEquityInfo(symbol=symbol)
    if info:
        try:
            succ = info["success"]
            if succ:
                content = info["content"]
                df = pd.DataFrame(content,index=[0])
                print(df)
            else: print(info["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

### Bakiye bilgisi
def get_instant_position():
    print("Bakiye görüntüleniyor...")
    bakiye=Conn.GetInstantPosition()
    if bakiye:
        try:
            succ = bakiye["success"]
            if succ:
                content = bakiye["content"]
                df = pd.DataFrame(content)
                print(df) 
            else: print(bakiye["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def get_viop_customer_overall():
    print("** işlemi gerçekleştiriliyor...")
    bakiye=Conn.GetViopCustomerOverall()
    if bakiye:
        try:
            succ = bakiye["success"]
            if succ:
                content = bakiye["content"]
                df = pd.DataFrame(content)
                print(df)  
            else: print(bakiye["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def get_subaccounts():
    print("İşlem gerçekleştiriliyor...")
    bakiye=Conn.GetSubAccounts()
    if bakiye:
        try:
            succ = bakiye["success"]
            if succ:
                content = bakiye["content"]
                df = pd.DataFrame(content)
                print(df)
            else: print(bakiye["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

### İşlem Bilgisi
def get_todays_transaction():
    islem=Conn.GetTodaysTransaction()
    print("İşlem gerçekleştiriliyor...")
    if islem:
        try:
            succ = islem["success"]
            if succ:
                content = islem["content"]
                df = pd.DataFrame(content)
                print(df)        
            else: print(islem["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def get_viop_customer_transactions():
    islem=Conn.GetViopCustomerTransactions()
    print("İşlem gerçekleştiriliyor...")
    if islem:
        try:
            succ = islem["true"]
            if succ:
                content = islem["content"]
                df = pd.DataFrame(content,index=[0])
                print(df)       
            else: print(islem["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

### Oturum Süresi Uzatma
def session_refresh():
    print("İşlem gerçekleştiriliyor...")
    islem=Conn.SessionRefresh()
    print(islem)
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

### Yeni eklenen Endpointler
def get_equity_order_history():
    print("İşlem gerçekleştiriliyor...")
    id=input("Lütfen ID Bilgisi Girin: ")
    bakiye=Conn.GetEquityOrderHistory(id=id,subAccount="")
    if bakiye:
        try:
            succ = bakiye["true"]
            if succ:
                content = bakiye["content"]
                df = pd.DataFrame(content)
                print(df) 
            else: print(bakiye["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def account_extre():
    print("İşlem gerçekleştiriliyor...")
    start_string=input("Lütfen başlangıç tarihi Girin(başlangıç tarihi '2023-07-01' formatında): ")
    start_object = datetime.strptime(start_string, "%Y-%m-%d").isoformat()
    end_string=input("Lütfen bitiş tarihi Girin(bitiş tarihi '2023-07-01' formatında): ")
    end_object = datetime.strptime(end_string, "%Y-%m-%d").isoformat()
    bakiye=Conn.AccountExtre(start_date=start_object,end_date=end_object)
    # datetime nesnesini JSON'a dönüştürürken özel seri hale getirme işlevini kullanın

    if bakiye:
        try:
            succ = bakiye["true"]
            if succ:
                content = bakiye["content"]
                df = pd.DataFrame(content)
                print(df)  
            else: print(bakiye["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def cash_flow():   
    print("İşlem gerçekleştiriliyor...")
    bakiye=Conn.CashFlow()
    if bakiye:
        try:
            if bakiye['success']:
                content = bakiye["content"]
                df = pd.DataFrame(content,index=[0])
                print(df)  
            else: print(bakiye["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def get_equity_order_history():
    print("İşlem gerçekleştiriliyor...")
    id=input("Lütfen ID Bilgisi Girin: ")
    bakiye=Conn.GetEquityOrderHistory(id=id,subAccount="")
    if bakiye:
        try:
            succ = bakiye["true"]
            if succ:
                content = bakiye["content"]
                df = pd.DataFrame(content)
                print(df)  
            else: print(bakiye["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def get_viop_order_history():
    print("İşlem gerçekleştiriliyor...")
    id=input("Lütfen ID Bilgisi Girin: ")
    bakiye=Conn.GetViopOrderHistory(id=id,subAccount="")
    if bakiye:
        try:
            succ = bakiye["success"]
            if succ:
                content = bakiye["content"]
                df = pd.DataFrame(content)
                print(df)  
            else: print(bakiye["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

def risk_simulation():
    print("İşlem gerçekleştiriliyor...")
    bakiye=Conn.RiskSimulation()
    if bakiye:
        try:
            succ = bakiye["success"]
            if succ:
                content = bakiye["content"]
                df = pd.DataFrame(content,index=[0])
                print(df)  
            else: print(bakiye["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("Ana menüye dönmek için herhangi  tuşuna basın: ")
    return

def viop_collateral_info():
    print("İşlem gerçekleştiriliyor...")
    bakiye=Conn.ViopColleteralInfo()
    if bakiye:
        try:
            succ = bakiye["success"]
            if succ:
                content = bakiye["content"]
                df = pd.DataFrame(content,index=[0])
                print(df)  
            else: print(bakiye["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("Önceki menüye dönmek için herhangi  tuşuna basın: ")
    return

############################################ MENÜLER##################################################

def main_menu():
    while True:
        print("\nAna Menüye hoş geldiniz. Lütfen yapmak istediğiniz işlemi seçin:")
        print("1. Emir Menüsü")
        print("2. Sembol Barlarını Çekme")
        print("3. Sembol Bilgisi Çekme")
        print("4. Hesap Bilgisi Menüsü")
        print("5. Günlük İşlemleri Çekme")
        print("6. Viop Günlük İşlemleri Çekme")
        print("7. Kredi Risk Simülasyonu")
        print("8. Viop Teminat Bilgisi")
        print("9. Hesap Ekstresi Bilgisi")
        print("0. Çıkış")
        secim = input("Seçiminizi yapın: ")
        
        if secim == '1':
            if order_menu():
                continue
        elif secim == '2':
            if get_candle_data():
                continue
        elif secim == '3':
            if get_equity_info():
                continue
        elif secim == '4':
            if account_menu():
                continue
        elif secim == '5':
            if get_todays_transaction():
                continue
        elif secim == '6':
            if get_viop_customer_overall():
                continue
        elif secim == '7':
            if risk_simulation():
                continue
        elif secim == '8':
            if viop_collateral_info():
                continue
        elif secim == '9':
            if account_extre():
                continue
        elif secim == '0':
            print("Çıkış yapılıyor...")
            os._exit(0)  # 0 başarılı çıkışı temsil eder
        else:
            print("Geçersiz seçim.")
            continue  # Kullanıcı hatalı seçim yaptı, ana menüye dön

def order_menu():
    while True:
        print("\nLütfen yapmak istediğiniz işlemi seçin:")
        print("1. Emir Gönder")
        print("2. Emir Düzelt")
        print("3. Emir Sil")
        print("4. Viop Emir Sil")
        print("5. Pay Emir Tarihçesi")
        print("6. Viop Emir Tarihçesi")
        print("7. Para Akışı")
        print("0. Ana Menü")
        secim = input("Seçiminizi yapın: ")
        
        if secim == '1':
            if send_order():
                continue
        elif secim == '2':
            if modify_order():
                continue
        elif secim == '3':
            if delete_order():
                continue
        elif secim == '4':
            if delete_order_viop():
                continue
        elif secim == '5':
            if get_equity_order_history():
                continue
        elif secim == '6':
            if get_viop_order_history():
                continue
        elif secim == '7':
            if cash_flow():
                continue
        elif secim == '0':
            main_menu()
        else:
            print("Geçersiz seçim.")
            continue  # Kullanıcı hatalı seçim yaptı, menüye dön

def account_menu():
    while True:
        print("\nLütfen yapmak istediğiniz işlemi seçin:")
        print("1. Alt Hesapları Görüntüle")
        print("2. Portföy Bilgisi")
        print("3. Viop Overall Bilgisi")
        print("0. Ana Menü")
        secim = input("Seçiminizi yapın: ")
        
        if secim == '1':
            if get_subaccounts():
                continue
        elif secim == '2':
            if get_instant_position():
                continue
        elif secim == '3':
            if get_viop_customer_overall():
                continue
        elif secim == '0':
            main_menu()
        else:
            print("Geçersiz seçim.")
            continue  # Kullanıcı hatalı seçim yaptı, menüye dön

if __name__ == "__main__":
    # Login olarak, token alıyoruz
    try:
        Conn = API(api_key=MY_API_KEY, username=MY_USERNAME, password=MY_PASSWORD, auto_login=True, verbose=True)
    except Exception as e:
        print(f"Hata oluştu: {e}")
    main_menu()  # ana menüye dönüyoruz