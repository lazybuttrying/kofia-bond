import requests
from xml.etree import ElementTree
import pandas as pd
from datetime import datetime, timedelta

handler = {
    "issue": dict(
        init_date=datetime(2012, 10, 23), date_col="발행일",
        columns=["발행일", "발행기관", "만기일", "만기", "발행금리(%)", "발행금액(억원)",  "통화구분"]
    ),
    "trans_daily": dict(
        init_date=datetime(2003, 1, 2), date_col="거래일",
        columns=["거래일", "매도(거래량)", "매수(거래량)", "합계"]
    ),
    "trans_detail": dict(
        init_date=datetime(2010, 1, 4), date_col="거래일자",
        columns=["거래일자", "시간", "발행기관", "발행일", "만기일", "잔존기간",
                 "수익률(%)", "거래량(억원)", "거래대금(억원)", "통화구분", "거래구분", "정정여부"]
    ),
    "yield_daily": dict(
        init_date=datetime(1990, 1, 2), date_col="거래일",
        columns=["거래일", "대표수익률_시중은행", "대표수익률_특수은행", "최종호가수익률_오전_시중",
                 "최종호가수익률_오전_특수", "최종호가수익률_오후_시중", "최종호가수익률_오후_특수"]
    )
}


cookies = {
    'WMONID': '-KcsGYoCqnj',
    'bisSelectedMenu': '0',
    'JSESSIONID': 'bzKmn4IeXsFPyPece3YsaymH2ca5tIfhzSlapDwzKJTvIXsTvjhBlTg1vTN6W5jl.ap7_servlet_kofiabondEngine',
}

headers = {
    'Accept': '*/*',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    # 'Cookie': 'WMONID=-KcsGYoCqnj; bisSelectedMenu=0; JSESSIONID=z575ONs5XGOVkrOSfModgHajohtiSI9SzrdCpLQ1qrrJZYLavaunNxqsggZFfZc7.ap8_servlet_kofiabondEngine',
    'Origin': 'https://www.kofiabond.or.kr',
    'Pragma': 'no-cache',
    'Referer': 'https://www.kofiabond.or.kr/websquare/websquare.html?w2xPath=/xml/Com/Common_GnrDsp.xml&divisionId=MBIS01011010040000&serviceId=BIS0100100170&topMenuIndex=1&w2xHome=/xml/&w2xDocumentRoot=',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}


class Query:
    def issue(start_date, end_date):
        return f'<?xml version="1.0" encoding="utf-8"?>\n<message>\n  <proframeHeader>\n    <pfmAppName>BIS-KOFIABOND</pfmAppName>\n    <pfmSvcName>BISCDIssSrchSO</pfmSvcName>\n    <pfmFnName>list</pfmFnName>\n  </proframeHeader>\n  <systemHeader></systemHeader>\n    <BISComDspDatDTO>\n    <val1>{start_date}</val1>\n    <val2>{end_date}</val2>\n</BISComDspDatDTO>\n</message>\n'

    def trans_daily(start_date, end_date):
        return f'<?xml version="1.0" encoding="utf-8"?>\n<message>\n  <proframeHeader>\n    <pfmAppName>BIS-KOFIABOND</pfmAppName>\n    <pfmSvcName>BISCDTrdSrchSO</pfmSvcName>\n    <pfmFnName>listCdTrd</pfmFnName>\n  </proframeHeader>\n  <systemHeader></systemHeader>\n    <BISComDspDatDTO>\n    <val13>8</val13>\n    <val11>{start_date}</val11>\n    <val12>{end_date}</val12>\n    <val14></val14>\n</BISComDspDatDTO>\n</message>\n'

    def trans_detail(start_date, end_date):
        return f'<?xml version="1.0" encoding="utf-8"?>\n<message>\n  <proframeHeader>\n    <pfmAppName>BIS-KOFIABOND</pfmAppName>\n    <pfmSvcName>BISCDTrdSrchSO</pfmSvcName>\n    <pfmFnName>listDet</pfmFnName>\n  </proframeHeader>\n  <systemHeader></systemHeader>\n    <BISComDspDatDTO>\n    <val1>{start_date}</val1>\n    <val2>{end_date}</val2>\n</BISComDspDatDTO>\n</message>\n'

    def yield_daily(start_date, end_date):
        return f'<?xml version="1.0" encoding="utf-8"?>\n<message>\n  <proframeHeader>\n    <pfmAppName>BIS-KOFIABOND</pfmAppName>\n    <pfmSvcName>BISCDROPSrchSO</pfmSvcName>\n    <pfmFnName>listCdROP</pfmFnName>\n  </proframeHeader>\n  <systemHeader></systemHeader>\n<BISComDspDatDTO>\n<val13>8</val13>\n<val11>{start_date}</val11>\n<val12>{end_date}</val12>\n</BISComDspDatDTO></message>\n'


def monthly(init_date):
    starts, ends = [], []
    now = datetime.now()
    while init_date < now:
        starts.append(init_date)
        init_date += timedelta(days=30)
        ends.append(init_date)
        init_date += timedelta(days=1)
        while init_date.weekday() >= 5:
            init_date += timedelta(days=1)
    return starts, ends


def _fetch(start_date, end_date, query):
    start_date = start_date.strftime('%Y%m%d')
    end_date = end_date.strftime('%Y%m%d')

    response = requests.post('https://www.kofiabond.or.kr/proframeWeb/XMLSERVICES/',
                             cookies=cookies, headers=headers, data=query).text
    return ElementTree.fromstring(response)


def fetch_data(start_date, end_date, fetch_type):
    query = getattr(Query, fetch_type)(start_date, end_date)
    columns = handler[fetch_type]["columns"]
    return pd.json_normalize((
        {
            v: item.find(f'val{i}').text.strip() if item.find(
                f'val{i}').text else ""
            for i, v in enumerate(columns, start=1)
        }
        for item in _fetch(start_date, end_date, query).iter('BISComDspDatDTO')
    ))
