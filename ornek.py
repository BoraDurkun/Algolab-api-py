from algolab import API
from datetime import datetime, timezone, timedelta
from config import *
import pandas as pd, numpy as np, json, os

############################################ ENDPOINT Fonksiyonlari ##################################################

### Emir  Bilgisi
def send_order():
    symbol=input("Lütfen Sembol Bilgisi Girin: ")
    print("\nLütfen Yapacağiniz işlemi giriniz?")
    print("'1' Alım")
    print("'2' Satım")
    direction = input("\nSeçiminiz: ")
    if direction == '1':
        direction="Buy"
    elif direction == '2':
        direction="Sell"
    else:
        print("Geçersiz seçim.")
        send_order()  # Kullanici hatali seçim yapti, menüye dön

    print("\nLütfen Emir Tipi Bilgisi Girin?")
    print("'1' Limit")
    print("'2' Piyasa")
    pricetype = input("\nSeçiminiz: ")
    if pricetype == '1':
        pricetype="limit"
    elif pricetype == '2':
        pricetype="piyasa"
    else:
        print("Geçersiz seçim.")
        send_order()  # Kullanici hatali seçim yapti, menüye dön

    lot=input("\nLütfen Lot Bilgisi Girin: ")
    if pricetype=='piyasa':
        price=""
    else:
        price=input("Lütfen Fiyat Bilgisi Girin: ")
    order = Conn.SendOrder(symbol=symbol, direction= direction, pricetype= pricetype, price=price, lot=lot ,sms=False,email=False,subAccount="")
    print("\nEmir gönderme isteği atılıyor...\n")    
    if order:
        try:
            succ = order["success"]
            if succ:
                content = order["content"]
                print("Emir İletildi, Dönen cevap: " + content)
            else: print(order["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    order_menu()

def modify_order():
    id=input("Lütfen ID Bilgisi Girin: ")
    print("\n Viop emri mi değiştirmek istiyorsunuz?")
    print("'1' Hayır")
    print("'2' Evet")
    viop = input("\nSeçiminiz: ")
    if viop == '1':
        viop=False
    elif viop == '2':
        viop=True
    else:
        print("Geçersiz seçim.")
        modify_order()  # Kullanici hatali seçim yapti, menüye dön
    if viop:
        lot=input("Lütfen Lot Bilgisi Girin: ")
    else:
        lot=""
    price=input("Lütfen Yeni Fiyat Bilgisi Girin: ")
    modify=Conn.ModifyOrder(id=id,price=price,lot=lot,viop=viop,subAccount="")
    print("\nEmir değiştirme isteği atılıyor...\n")
    if modify:
        try:
            succ = modify["success"]
            if succ:
                content = modify["content"]
                print("Emir Düzeltildi")
            else: print(modify["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    order_menu()

def delete_order():
    id=input("Lütfen ID Bilgisi Girin: ")
    delete=Conn.DeleteOrder(id=id,subAccount="")
    print("\nEmir silme isteği atılıyor...\n")
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
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

def delete_order_viop():
    id=input("Lütfen ID Bilgisi Girin: ")
    adet=input("Lütfen Kontrat Sayisi Girin: ")
    delete=Conn.DeleteOrderViop(id=id,adet=adet,subAccount="")
    print("\nViop Emir Silme isteği atılıyor...\n")
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
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

### Sembol Bilgisi
def get_candle_data():
    symbol=input("Lütfen Sembol Bilgisi Girin: ")
    period=input("Lütfen Periyot Bilgisi Girin: ")
    candle = Conn.GetCandleData(symbol, period)
    print("\nOHLCV barları verisi görüntüleme isteği atılıyor...\n")    
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
                # oluşturduğumuz listi pandas dataframe'e aktariyoruz
                df = pd.DataFrame(columns=["date", "open", "high", "low", "close"], data=np.array(ohlc))
                print(df.tail())
                json_data=df.to_json(orient='records')
                with open(symbol+period+'.json', 'w', encoding='utf-8') as f:
                    f.write(json_data)
            else: print(candle["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

def get_equity_info():
    symbol=input("Lütfen Sembol Bilgisi Girin: ")
    print("\nSembol bilgisi görüntüleme isteği atılıyor...\n")
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
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

### result bilgisi
def get_instant_position():
    print("\nPortöy görüntüleme isteği atılıyor...\n")
    result=Conn.GetInstantPosition()
    if result:
        try:
            succ = result["success"]
            if succ:
                content = result["content"]
                df = pd.DataFrame(content)
                print(df) 
            else: print(result["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

def get_viop_customer_overall():
    print("\n Viop overall bilgisi görüntüleme isteği atılıyor...\n")
    result=Conn.GetViopCustomerOverall()
    if result:
        try:
            succ = result["success"]
            if succ:
                content = result["content"]
                df = pd.DataFrame(content)
                print(df)  
            else: print(result["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

def get_subaccounts():
    print("\nAlt Hesap görüntüleme isteği atılıyor...\n")
    result=Conn.GetSubAccounts()
    if result:
        try:
            succ = result["success"]
            if succ:
                content = result["content"]
                df = pd.DataFrame(content)
                print(df)
            else: print(result["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

### İşlem Bilgisi
def get_todays_transaction():
    islem=Conn.GetTodaysTransaction()
    print("\nGünlük işlem listesi çekme isteği atılıyor...\n")
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
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

def get_viop_customer_transactions():
    islem=Conn.GetViopCustomerTransactions()
    print("\nViop Günlük İşlemleri görüntüleme isteği atılıyor...\n")
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
    
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

### Oturum Süresi Uzatma
def session_refresh():
    print("\nOturum süresi uzatma isteği atılıyor...\n")
    islem=Conn.SessionRefresh()
    print(islem)
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

### Yeni eklenen Endpointler
def get_equity_order_history():
    id=input("Lütfen ID Bilgisi Girin: ")
    print("\nPay emir Tarihçesi isteği atılıyor...\n")
    result=Conn.GetEquityOrderHistory(id=id,subAccount="")
    if result:
        try:
            succ = result["success"]
            if succ:
                content = result["content"]
                df = pd.DataFrame(content)
                print(df) 
            else: print(result["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

def account_extre():
    days=int(input("Kaç Günlük ekstre çekmek istersiniz?: "))
    print("Hangi tipte ekstre çekmek istersiniz?")
    print("'1' Hesap Ekstresi")
    print("'2' Viop Ekstresi")
    ekstretipi = input("\nSeçiminiz: ")
    if ekstretipi == '1':
        ekstretipi="accountextre"
    elif ekstretipi == '2':
        ekstretipi="viopextre"
    else:
        print("Geçersiz seçim.")
        account_extre()  # Kullanici hatali seçim yapti, başa dön

    end_date = datetime.now(timezone(timedelta(hours=3)))
    start_date = end_date - timedelta(days=days)
    print("\nEkstre çekme isteği atılıyor...\n")
    result = Conn.AccountExtre(start_date=start_date, end_date=end_date)
    if result:
        try:
            succ = result["success"]
            if succ:
                content = result['content'][ekstretipi]
                df = pd.DataFrame(content)
                print(df)
            else:
                print(result.get('message', 'Bilinmeyen bir hata oluştu.'))
        except Exception as e:
            print(f"Hata oluştu: {e}")
    
    input("Önceki menüye dönmek için herhangi bir tuşa basın: ")
    return

def cash_flow():   
    print("\nNakit Akış tablosu görüntüleme isteği atılıyor...\n")
    result=Conn.CashFlow()
    if result:
        try:
            if result['success']:
                content = result["content"]
                df = pd.DataFrame(content,index=[0])
                print(df)  
            else: print(result["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}")
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

def get_viop_order_history():
    print("\nViop emir tarihçesi görütüleme isteği atılıyor...\n")
    id=input("Lütfen ID Bilgisi Girin: ")
    result=Conn.GetViopOrderHistory(id=id,subAccount="")
    if result:
        try:
            succ = result["success"]
            if succ:
                content = result["content"]
                df = pd.DataFrame(content)
                print(df)  
            else: print(result["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

def risk_simulation():
    print("\nKredi risk simülasyonu tablosu görüntüleme isteği atılıyor...\n")
    result=Conn.RiskSimulation()
    if result:
        try:
            succ = result["success"]
            if succ:
                content = result["content"]
                df = pd.DataFrame(content,index=[0])
                print(df)  
            else: print(result["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("Ana menüye dönmek için herhangi  tuşuna basin: ")
    return

def viop_collateral_info():
    print("\nViop Teminat tablosu görüntüleme isteği atılıyor...\n")
    result=Conn.ViopColleteralInfo()
    if result:
        try:
            succ = result["success"]
            if succ:
                content = result["content"]
                df = pd.DataFrame(content,index=[0])
                print(df)  
            else: print(result["message"]) 
        except Exception as e:
            print(f"Hata oluştu: {e}") 
    input("\nÖnceki menüye dönmek için herhangi  tuşuna basin: ")
    return

############################################ MENÜLER##################################################

def main_menu():
    while True:
        print("\nAna Menüye hoş geldiniz. Lütfen yapmak istediğiniz işlemi seçin:")
        print("'1' Emir Menüsü")
        print("'2' Sembol Barlarini Çekme")
        print("'3' Sembol Bilgisi Çekme")
        print("'4' Hesap Bilgisi Menüsü")
        print("'5' Günlük İşlemleri Çekme")
        print("'6' Viop Günlük İşlemleri Çekme")
        print("'7' Kredi Risk Simülasyonu")
        print("'8' Viop Teminat Bilgisi")
        print("'9' Hesap Ekstresi Bilgisi")
        print("'0' Oturum Süresi Uzatma")
        print("'*' Çikiş")
        secim = input("\nSeçiminiz: ")
        
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
            if get_viop_customer_transactions():
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
            if session_refresh():
                continue
        elif secim == '*':
            print("Çikiş yapiliyor...")
            os._exit(0)  # 0 başarili çikişi temsil eder
        else:
            print("Geçersiz seçim.")
            continue  # Kullanici hatali seçim yapti, ana menüye dön

def order_menu():
    while True:
        print("\nLütfen yapmak istediğiniz işlemi seçin:")
        print("'1' Emir Gönder")
        print("'2' Emir Düzelt")
        print("'3' Emir Sil")
        print("'4' Viop Emir Sil")
        print("'5' Para Akişi")
        print("'6' Pay Emir Tarihçesi")
        print("'7' Viop Emir Tarihçesi")
        print("'0' Ana Menü")
        secim = input("\nSeçiminiz: ")
        
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
            if cash_flow():
                continue
        elif secim == '6':
            if get_equity_order_history():
                continue
        elif secim == '7':
            if get_viop_order_history():
                continue
        elif secim == '0':
            main_menu()
        else:
            print("Geçersiz seçim.")
            continue  # Kullanici hatali seçim yapti, menüye dön

def account_menu():
    while True:
        print("\nLütfen yapmak istediğiniz işlemi seçin:")
        print("'1' Alt Hesaplari Görüntüle")
        print("'2' Portföy Bilgisi")
        print("'3' Viop Overall Bilgisi")
        print("'0' Ana Menü")
        secim = input("\nSeçiminiz: ")
        
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
            continue  # Kullanici hatali seçim yapti, menüye dön

if __name__ == "__main__":
    # Login olarak, token aliyoruz
    try:
        Conn = API(api_key=MY_API_KEY, username=MY_USERNAME, password=MY_PASSWORD, auto_login=True, verbose=True)
        if Conn.is_alive == True:
            main_menu()  # ana menüye dönüyoruz
        else:
            print("Giriş yapılamadı.")
    except Exception as e:
        print(f"Hata oluştu: {e}")
    