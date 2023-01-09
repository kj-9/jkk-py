import json
import os
import re
import sys
from collections import defaultdict

import pandas as pd
import requests
from linebot import LineBotApi
from linebot.exceptions import LineBotApiError
from linebot.models import TextSendMessage
from prefect import flow, get_run_logger, task


@task
def fetch_htmls() -> list[str]:

    SEARCH_NAMES = [
        "トミンタワーセンジュゴチョウメ",
        "トミンタワーミナミセンジュヨンチョウメ",
        "センターマチヤ",
        "コーシャハイムチハヤ",
        "トミンハイムミナミダイサンチョウメ",
        "トミンハイムヨコカワイッチョウメ",
    ]
    URL_BASE = "https://jhomes.to-kousya.or.jp/search/jkknet/service"
    URL_INIT = f"{URL_BASE}/akiyaJyoukenStartInit"
    URL_SEARCH = f"{URL_BASE}/akiyaJyoukenRef"

    logger = get_run_logger()

    session = requests.Session()

    # get cookies
    _ = session.get(URL_INIT)

    # init search page to get tokens embedded
    res = session.post(URL_INIT, data={"redirect": "true", "url": URL_INIT})

    html = res.content.decode("shiftjis")
    token = re.compile('name=token value="(.+)"').search(html).group(1)
    abcde = re.compile('name="abcde" value="(.+)"').search(html).group(1)

    htmls = []
    for search_name in SEARCH_NAMES:
        # post search"
        res = session.post(
            URL_SEARCH,
            data={
                "token": token,
                "abcde": abcde,
                "akiyaInitRM.akiyaRefM.jyutakuKanaName": search_name.encode("cp932"),
                "akiyaInitRM.akiyaRefM.yachinFrom": "0",
                "akiyaInitRM.akiyaRefM.yachinTo": "999999999",
                "akiyaInitRM.akiyaRefM.mensekiFrom": "0",
                "akiyaInitRM.akiyaRefM.mensekiTo": "9999.99",
            },
        )

        logger.info("Response received from server.")
        logger.info(f"{search_name=}, {res.request.body=}")

        html = res.content.decode("shiftjis")

        if "ご希望の住宅、またはご希望の条件の空室はございませんでした。" in html:
            logger.info("There is no Search result.")
            continue
        else:
            htmls.append(html)

    return htmls


@task
def extract_data(html: str) -> pd.DataFrame:
    # extract and transform table to df

    df = (
        pd.read_html(html, header=0)[6]
        .iloc[:, 1:10]
        .astype(pd.StringDtype())  # for using as join keys
    )

    df["last_updated"] = pd.Timestamp.now("Asia/Tokyo")

    return df


@task
def update_data(df_fetched: pd.DataFrame) -> pd.DataFrame:

    df_saved = pd.read_csv(
        "flow/notify/state.csv",
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
    logger = get_run_logger()

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
        logger.info(msg)

        if send_line:
            try:
                logger.info("Sending line message...")

                from prefect.blocks.system import Secret

                token = Secret.load("jkk-notify-line-channel-access-token")

                line_bot_api = LineBotApi(token.get())
                line_bot_api.broadcast(messages=TextSendMessage(text=msg))
            except LineBotApiError as e:
                logger.error(e)


@task
def save_data(df_updated: pd.DataFrame):
    logger = get_run_logger()
    logger.info("Write data as csv...")
    df_save = df_updated[df_updated.last_updated.notnull()]  # take rows from df_update

    def _update_ts(x):
        if pd.isnull(x.last_updated_old):
            return x.last_updated
        if x.募集戸数 != x.募集戸数_old:
            return x.last_updated
        return x.last_updated_old

    df_save["last_updated"] = df_save.apply(_update_ts, axis=1)
    df_save = df_save.drop(columns=["募集戸数_old", "last_updated_old"])
    df_save.sort_values(by=list(df_save.columns)).to_csv(
        "flow/notify/state.csv", index=False
    )


@flow(name="jkk-notify", version=os.getenv("GIT_COMMIT_SHA"))
def main(send_line: bool):
    """jkk notifyer: https://github.com/kj-9/jkk-py

    Args:
        send_line (bool): does send line message if new rooms are available.
    """

    logger = get_run_logger()
    logger.info(f"Parameters are set: {locals()}")

    htmls = fetch_htmls()

    dfs = []
    for html in htmls:
        dfs.append(extract_data(html))

    df_fetched = (
        pd.concat(dfs)
        if dfs
        else pd.DataFrame(
            columns=[
                "住宅名",
                "地域",
                "優先 種別",
                "住宅種別",
                "間取り",
                "床面積 [m2]",
                "家賃 [円]",
                "共益費 [円]",
                "募集戸数",
                "last_updated",
            ]
        )
    )
    df_updated = update_data(df_fetched)

    send_message.submit(df_updated, send_line)
    save_data.submit(df_updated)


if __name__ == "__main__":

    args = json.loads(sys.argv[1])
    main(**args)
