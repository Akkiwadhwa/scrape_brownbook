import concurrent.futures
import json
import pandas as pd
import requests
import urllib3
from tqdm import tqdm

requests.urllib3.disable_warnings()

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://brownbook.net',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
}


def scrap(x):
    global dict1
    dict1 = {}
    tag = ""
    try:
        http = urllib3.PoolManager(cert_reqs='CERT_NONE')
        r = http.request("GET", url=f"https://api.brownbook.net/app/api/v1/businesses/resource/{x}/revision",
                         headers=headers)
        data = json.loads(r.data.decode('utf-8'))

        bussiness_name = data["data"]["metadata"]["name"]
        c_name = data["data"]["metadata"]["claimed"]
        if c_name == True:
            contact_name = data["data"]["metadata"]["claimant"]["real_name"]
        else:
            contact_name = "NULL"
        category = data["data"]["metadata"]["category"]
        tags = data["data"]["metadata"]["tags"]
        for i in tags:
            tag += i["name"] + ","
        address = data["data"]["metadata"]["address"]
        city = data["data"]["metadata"]["city"]
        state = data["data"]["metadata"]["state"]
        country = data["data"]["metadata"]["country"]
        country_code = data["data"]["metadata"]["country_code"]
        zip = data["data"]["metadata"]["zip_code"]
        phone = data["data"]["metadata"]["phone"]
        fax = data["data"]["metadata"]["fax"]
        email = data["data"]["metadata"]["email"]
        website = data["data"]["metadata"]["website"]
        url = f"https://www.brownbook.net/business/{data['data']['id']}/{data['data']['metadata']['link']}"
        dict1 = {
            "Bussiness Name": bussiness_name,
            "Contact Name": contact_name,
            "Category": category,
            "Tags": tag,
            "Address": address,
            "City": city,
            "State": state,
            "country": country,
            "Country Code": country_code,
            "Zipcode": zip,
            "Phone": phone,
            "Fax": fax,
            "Email": email,
            "Website": website,
            "Page url": url
        }
    except:
        dict1 = {}

    return dict1


print("Welcome to the Scrapper")
from_ = int(input("From: "))
to_ = int(input("To: "))
if isinstance(from_, int) and isinstance(to_, int):
    for i in range(from_, to_):
        print(i)
        my_iter = range(10000 * i, 10000 * (i + 1))
        with concurrent.futures.ThreadPoolExecutor(16) as executor:
            results = list(tqdm(executor.map(scrap, my_iter), total=len(my_iter)))
        df = pd.DataFrame(results)
        try:
            a = df.sort_values('Country Code')
            a.to_csv(f"{10000 * i}-{10000 * (i + 1)}.csv")
        except Exception as e:
            print(f"CSV is blank {e}")
else:
    print("Program Exit.")
# import time
#
# import pyautogui
#
# while True:
#     time.sleep(3)
#     pyautogui.moveRel(0, 50, duration = 1)
#     pyautogui.moveTo(1000, 800, duration=1)
#     pyautogui.click()
#     pyautogui.moveRel(50, 0, duration=1)