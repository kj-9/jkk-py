import os
import re
from unicodedata import normalize

import googlemaps
import pandas as pd
import requests
from lxml import html

page_size = 1000
url = (
    "https://as.chizumaru.com/jkktokyo/articleList"
    f"?account=jkktokyo&searchType=True&pg=1&pageSize={page_size}&pageLimit=10000"
)

session = requests.Session()
res = session.get(url)

root = html.fromstring(res.content.decode("utf8"))
table = root.xpath('//*[@id="DispListArticle"]/table/tbody/tr')

gmaps = googlemaps.Client(key=os.environ["GMAP_API_KEY"])

arr = []
for tr in table:
    row = {}

    a = tr.xpath("./td/h4/a")
    assert len(a) == 1
    row["name"] = a[0].text_content()
    row["link"] = a[0].get("href")

    ps = tr.xpath("./td/p")
    assert len(ps) <= 7

    row["address"] = normalize("NFKC", re.sub("ほか$", "", ps[0].text_content()))

    try:
        search_addr = f'東京都{row["address"]}'
        print(search_addr)
        geocode = gmaps.geocode(search_addr)
        row["lat"] = geocode[0]["geometry"]["location"]["lat"]
        row["lng"] = geocode[0]["geometry"]["location"]["lng"]
    except Exception as e:
        print(e)

    # 家賃
    row["rent_raw"] = ps[1].text_content()

    rent_mtc = re.match(r"([0-9,]+)[^0-9,]?([0-9,]+)?", row["rent_raw"])
    row["rent_from"] = int(rent_mtc.group(1).replace(",", ""))

    _gr2 = rent_mtc.group(2)
    row["rent_to"] = int(_gr2.replace(",", "")) if _gr2 else row["rent_from"]

    # 間取り
    row["layout_raw"] = ps[2].text_content()
    row["layouts"] = re.split("[~・]", normalize("NFKC", row["layout_raw"]))

    row["built_year"] = int(ps[3].text_content().replace("年", ""))

    # 管理費
    row["management_fee_raw"] = ps[4].text_content()
    # 文字は無視して金額だけ配列に入れる
    fees = re.findall(r"([0-9,]+)円", ps[4].text_content())
    row["management_fees"] = [int(fee.replace(",", "")) for fee in fees]

    row["access"] = ps[5].text_content()
    if len(ps) == 7:
        row["comment"] = ps[6].text_content()
    arr.append(row)

df = pd.DataFrame(arr)
df.to_csv(
    "flow/get_residence/residence.csv",
    index=False,
)
