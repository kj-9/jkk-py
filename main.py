import os
import re
import sys
from collections import defaultdict

import pandas as pd
import requests
from linebot import LineBotApi
from linebot.exceptions import LineBotApiError
from linebot.models import TextSendMessage
from prefect import flow, task


@task
def fetch_data():

    URL_BASE = "https://jhomes.to-kousya.or.jp/search/jkknet/service"
    URL_INIT = f"{URL_BASE}/akiyaJyoukenStartInit"
    URL_SEARCH = f"{URL_BASE}/akiyaJyoukenRef"
    URL_CHANGE_COUNT = f"{URL_BASE}/AKIYAchangeCount"

    session = requests.Session()

    # get cookies
    _ = session.get(URL_INIT)

    # init search page to get tokens embedded
    res = session.post(URL_INIT, data={"redirect": "true", "url": URL_INIT})

    html = res.content.decode("shiftjis")
    token = re.compile('name=token value="(.+)"').search(html).group(1)
    abcde = re.compile('name="abcde" value="(.+)"').search(html).group(1)

    # post search
    res = session.post(
        URL_SEARCH,
        data={
            "token": token,
            "abcde": abcde,
            "akiyaInitRM.akiyaRefM.requiredTime": "15",
            "akiyaInitRM.akiyaRefM.yachinFrom": "0",
            "akiyaInitRM.akiyaRefM.yachinTo": "120000",
            "akiyaInitRM.akiyaRefM.mensekiFrom": "50",
            "akiyaInitRM.akiyaRefM.mensekiTo": "9999.99",
            "akiyaInitRM.akiyaRefM.bus": "0",
            "akiyaInitRM.akiyaRefM.checks": [
                "01",
                "02",
                "03",
                "04",
                "05",
                "07",
                "08",
                "09",
                "11",
                "18",
                "10",
                "12",
                "13",
                "14",
                "15",
                "16",
                "17",
                "19",
                "20",
                "21",
                "22",
                "23",
                "34",
                "33",
            ],
        },
    )
    # change search results shown to 50
    res = session.post(
        URL_CHANGE_COUNT,
        data={"token": token, "abcde": abcde, "akiyaRefRM.showCount": 50},
    )

    return res


@task
def transform_data(res: requests.Response) -> pd.DataFrame:

    # extract and transform table to df
    df_fetched = (
        pd.read_html(res.content.decode("shiftjis"), header=0)[6]
        .iloc[:, 1:10]
        .astype(pd.StringDtype())  # for using as join keys
    )
    df_fetched["last_updated"] = pd.Timestamp.now("Asia/Tokyo")

    df_saved = pd.read_csv(
        "state.csv",
        dtype=defaultdict(pd.StringDtype),
        parse_dates=["last_updated"],
        date_parser=lambda x: pd.Timestamp(x, tz="Asia/Tokyo"),
    )

    # TODO: check join key is unique and not null
    df_updated = df_fetched.merge(
        df_saved,
        how="outer",
        on=[
            "住宅名",
            "地域",
            "優先 種別",
            "住宅種別",
            "間取り",
            "床面積 [m2]",
            "家賃 [円]",
            "共益費 [円]",
        ],
        suffixes=("", "_old"),
    )

    return df_updated


@task
def send_message(df_updated: pd.DataFrame, send_line: bool):
    # TODO: treat deleted
    # df_updated[df_updated.last_updated_x.isnull()]
    # and notify

    # TODO: treat existed but updated 募集戸数
    # if df_update.募集戸数 != df_state.募集戸数:
    #   return a record with df_update.募集戸数,last_updated
    #   and notify
    # else:
    #   return a record with df_state.募集戸数,last_updated

    # newly added record
    df_new = df_updated[df_updated.last_updated_old.isnull()]

    if 0 < len(df_new.index):
        msg = "新規募集がありました:\n"
        for row in df_new.iterrows():
            msg += (
                f"{row[1][0]}: {row[1][1]}: {row[1][4]}: {row[1][5]}㎡: {row[1][6]}円\n"
            )
        print(msg)

        if send_line:
            try:
                print("Sending line message...")
                LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
                line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
                line_bot_api.broadcast(messages=TextSendMessage(text=msg))
            except LineBotApiError as e:
                print(e)


@task
def save_data(df_updated: pd.DataFrame):
    print(f"Write data as csv...")
    df_save = df_updated[df_updated.last_updated.notnull()]  # take rows from df_update

    def _update_ts(x):
        if pd.isnull(x.last_updated_old):
            return x.last_updated
        elif x.募集戸数 != x.募集戸数_old:
            return x.last_updated
        else:
            return x.last_updated_old

    df_save["last_updated"] = df_save.apply(lambda x: _update_ts(x), axis=1)
    df_save = df_save.drop(columns=["募集戸数_old", "last_updated_old"])
    df_save.sort_values(by=list(df_save.columns)).to_csv("state.csv", index=False)


@flow(name="jkk-notify", version=os.getenv("GIT_COMMIT_SHA"))
def main(DOES_SEND_LINE: bool):
    """jkk notifyer: https://github.com/kj-9/jkk-py

    Args:
        DOES_SEND_LINE (bool): does send line message if new rooms are available.
    """

    res = fetch_data()
    df_updated = transform_data(res)

    send_message(df_updated, DOES_SEND_LINE)
    save_data(df_updated)


if __name__ == "__main__":
    main(bool(sys.argv[1:]))
