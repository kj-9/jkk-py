import os
import sys
from collections import defaultdict
from playwright.sync_api import Playwright, sync_playwright
import pandas as pd

from linebot import LineBotApi
from linebot.models import TextSendMessage
from linebot.exceptions import LineBotApiError


def get_update(playwright: Playwright, headless: bool = False) -> pd.DataFrame:

    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context()
    page = context.new_page()

    with page.expect_popup() as popup_info:
        # first accesss
        page.goto(
            "https://jhomes.to-kousya.or.jp/search/jkknet/service/akiyaJyoukenStartInit"
        )

    # popuped page
    popup = popup_info.value
    popup.wait_for_load_state()

    # input condition
    popup.locator("#chk_ku_all").check()  # 区部
    popup.locator('input[value="34"]').check() #三鷹市
    popup.locator('input[value="33"]').check() #武蔵野市
    
    popup.get_by_label("15分以内").check()  # 駅から15分
    popup.locator('input[name="akiyaInitRM\\.akiyaRefM\\.bus"]').first.uncheck() #バス乗車時間を含まない
    popup.locator('select[name="akiyaInitRM\\.akiyaRefM\\.mensekiFrom"]').select_option(
        "50"
    )  # 50m2以上
    popup.locator('select[name="akiyaInitRM\\.akiyaRefM\\.yachinFrom"]').select_option(
        "70000"
    )  # 7万以上    
    popup.locator('select[name="akiyaInitRM\\.akiyaRefM\\.yachinTo"]').select_option(
        "120000"
    )  # 12万位内

    # click search botton
    popup.get_by_alt_text("検索する").first.click()
    popup.wait_for_load_state()

    # expand result
    popup.get_by_role("combobox").select_option("50")
    popup.wait_for_load_state()

    # scrape table
    result_selector = "body > div:nth-child(1) > table:nth-child(1) > tbody > tr:nth-child(2) > td > form > table > tbody > tr:nth-child(11) > td"
    table = popup.locator(result_selector).inner_html()

    # close
    context.close()
    browser.close()

    # transform
    dfs = pd.read_html(
        table, header=0,
    )  # retuns list of df, length of extracted html tables
    

    assert len(dfs) == 1  # only 1 df should be extracted
    df_update = dfs[0].iloc[:, 1:10].astype(pd.StringDtype()) # slice list and filter columns
    df_update["last_updated"] = pd.Timestamp.now("Asia/Tokyo")

    return df_update


def main(argv):

    #args
    DOES_SEND_LINE = bool(argv[0])
    IS_HEADLESS = bool(argv[1])
    
    print(f"Running with arguments: {DOES_SEND_LINE=},{IS_HEADLESS=}")

    with sync_playwright() as playwright:
        df_update = get_update(playwright, IS_HEADLESS)

    df_state = pd.read_csv(
        "state.csv",
        dtype=defaultdict(pd.StringDtype),
        parse_dates=["last_updated"],
        date_parser=lambda x: pd.Timestamp(x, tz="Asia/Tokyo"),
    )

    # TODO: check join key is unique and not null
    # df_update left join df_state
    # FIXME: You are trying to merge on float64 and object columns. need to cast as string?
    df_updated_state = df_update.merge(
        df_state,
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

    # newly added record
    df_new = df_updated_state[df_updated_state.last_updated_old.isnull()]
    
    if (0 < len(df_new.index)):
        msg = "新規募集がありました:\n"
        for row in df_new.iterrows():
            msg += f"{row[1][0]}: {row[1][1]}: {row[1][4]}: {row[1][5]}㎡: {row[1][6]}円\n"
        print(msg)
        
        if DOES_SEND_LINE:
            try:
                print("Sending line message...")
                LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
                line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
                line_bot_api.broadcast(messages=TextSendMessage(text=msg))
            except LineBotApiError as e:
                print(e)

    # TODO: treat deleted
    # df_updated_state[df_updated_state.last_updated_x.isnull()]
    # and notify
    
    # TODO: treat existed but updated 募集戸数 
    # if df_update.募集戸数 != df_state.募集戸数:
    #   return a record with df_update.募集戸数,last_updated
    #   and notify
    # else: 
    #   return a record with df_state.募集戸数,last_updated

    # FIXME: save
    print(f"Write data as csv...")
    df_save = df_updated_state[df_updated_state.last_updated.notnull()].drop(
        columns=["募集戸数_old", "last_updated_old"]
    )
    df_save.sort_values(by=list(df_save.columns)).to_csv("state.csv", index=False)
    
    print("Success.")


if __name__ == "__main__":
    main(sys.argv[1:])
