import concurrent.futures
import pandas as pd
import requests
from tqdm import tqdm

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Referer': 'https://brownbook.net',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Sec-GPC': '1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
}

cookies = {
    '_session_id': 'VkpzZjhpZk95QVFhbk5WMFZ3bXhpai9vNjI4TE1vUC9IYW1PZFZzTlA0UG5GRWJtOVJ4M2lmLzlDUUxITTV2VVdhKzVGMmdGaHdUclo4ejExcVA3bEZtZTNvZGgzVW5TcFoyN21TY3U4L3B4RENUUUdMaDIyMFZSYTVmSXpOenBnZWRkZlNzTU80ZkJUUVp4NTlFY0x4LzZkMEZtR0ltaHhIeWZxcm9ocWdxR1h0U2lCU2Zmb2Y4T05oRlBrSlluLS0vc1ppUjgwam1jOXdsejRhOGdRSXZRPT0%3D--2547c4e8925e162a172ef9f417de2d6d22ad43f4',
}


def scrap(x):
    global dict1
    r = requests.get(f"https://api.brownbook.net/app/api/v1/businesses/resource/{x}/revision", headers=headers,
                     cookies=cookies)
    data = r.json()
    tag = ""
    try:
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


    except:
        pass
    else:
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
    return dict1


def run(f, my_iter):
    with concurrent.futures.ThreadPoolExecutor(32) as executor:
        results = list(tqdm(executor.map(f, my_iter), total=len(my_iter)))
    return results


final = []
for i in range((2000 // 1000)):
    print(i)
    my_iter = range(1000 * i, 1000 * (i + 1))
    final += run(scrap, my_iter)

df = pd.DataFrame(final)
df.to_csv("data1.csv")
