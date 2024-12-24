import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import math
import requests

cookies = {
    'WMONID': 'sqNQePFDVA-',
    'JSESSIONID': '9yU6pTjwoZn42aWeukqKb4FUtJuXNWBqlFUawSpo3Sso7HG1zgrR!999489011',
    'lastAccess': '1731856796778',
}

headers = {
    'Accept': 'application/xml',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/xml; charset="UTF-8"',
    # 'Cookie': 'WMONID=sqNQePFDVA-; JSESSIONID=9yU6pTjwoZn42aWeukqKb4FUtJuXNWBqlFUawSpo3Sso7HG1zgrR!999489011; lastAccess=1731856796778',
    'Origin': 'https://seibro.or.kr',
    'Pragma': 'no-cache',
    'Referer': 'https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/moneyMarke/BIP_CNTS04033V.xml&menuNo=943',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'submissionid': 'submission_shortmFnceCasebyTdDetailsListCnt',
}


trans_data = '<reqParam action="shortmFnceCasebyTdDetailsListEL1" task="ksd.safe.bip.cnts.MoneyMarke.process.ShortmFncegdStatPTask"><SHORTM_FNCEGD_CD value="13"/><STD_DT_FR value="20241109"/><STD_DT_TO value="20241115"/><TD_TPCD value=""/><SHORTM_FNCE_INDTP_TPCD value=""/><START_PAGE value="1"/><END_PAGE value="10"/><MENU_NO value="943"/></reqParam>'
list_data = '<reqParam action="shortmFnceCasebyTdDetailsListCnt" task="ksd.safe.bip.cnts.MoneyMarke.process.ShortmFncegdStatPTask"><SHORTM_FNCEGD_CD value="13"/><STD_DT_FR value="20241109"/><STD_DT_TO value="20241111"/><TD_TPCD value=""/><SHORTM_FNCE_INDTP_TPCD value=""/><START_PAGE value="1"/><END_PAGE value="10"/><MENU_NO value="943"/></reqParam>'


def request_seibro(data):
    response = requests.post(
        'https://seibro.or.kr/websquare/engine/proworks/callServletService.jsp',
        cookies=cookies,
        headers=headers,
        data=data,
    )
    time.sleep(1)
    return response.content


columns = ['date', '기준일자', 'INPUT_NOS',  'ISSUCO_INDTP_CLSF_NO', '발행인유형',
           'GOODS_CREDITRT_CD', 'GOODS_CREDITRT_CD_NM',
           '통화', '발행금액', '할인율', 'SHORTM_FNCEGD_CD', '증권구분', '종목번호', '종목명',
           '발행일', '만기일', 'GOODS_DAYOBJ_CD', '일물', 'NUM']
df = {column: [] for column in columns}

# business days
try:
    for date in [datetime.strftime(date, '%Y%m%d') for date in pd.date_range('20100102', '20120102') if date.weekday() < 5]:
        print(date)
        list_data = f'<reqParam action="repoIssueListCnt" task="ksd.safe.bip.cnts.MoneyMarke.process.ShortmFncegdStatPTask"><SHORTM_FNCEGD_CD value="13"/><STD_DT value="{date}"/><GOODS_DAVOBJ_CD value=""/><START_PAGE value="1"/><END_PAGE value="10"/><MENU_NO value="939"/></reqParam>'

        content = request_seibro(list_data)
        trans_num = ET.fromstring(content).find('.//LIST_CNT')
        if trans_num == None or trans_num.attrib["value"] == "0":
            continue
        trans_num = int(trans_num.attrib["value"])
        print(trans_num)

        contents = []

        for i in range(math.ceil(trans_num/10)):
            trans_data = f'<reqParam action="repoIssueListEL1" task="ksd.safe.bip.cnts.MoneyMarke.process.ShortmFncegdStatPTask"><SHORTM_FNCEGD_CD value="13"/><STD_DT value="{date}"/><GOODS_DAVOBJ_CD value=""/><START_PAGE value="{1+10*i}"/><END_PAGE value="{10*(i+1)}"/><MENU_NO value="939"/></reqParam>'
            content = request_seibro(trans_data).decode('utf-8')
            root = ET.fromstring(content)
            for result in root.findall('.//result'):

                df['date'].append(date)
                df['기준일자'].append(result.find("STD_DT").attrib["value"])
                df['INPUT_NOS'].append(
                    result.find("INPUT_NOS").attrib["value"])
                df['ISSUCO_INDTP_CLSF_NO'].append(result.find(
                    "ISSUCO_INDTP_CLSF_NO").attrib["value"])
                df['발행인유형'].append(
                    result.find("SORT_NM").attrib["value"])
                df['GOODS_CREDITRT_CD'].append(result.find(
                    "GOODS_CREDITRT_CD").attrib["value"])
                df['GOODS_CREDITRT_CD_NM'].append(result.find(
                    "GOODS_CREDITRT_CD_NM").attrib["value"])
                df['통화'].append(result.find(
                    "CUR_CD").attrib["value"])
                df['발행금액'].append(result.find(
                    "ISSU_AMT").attrib["value"])
                df['할인율'].append(result.find(
                    "ISSU_MR").attrib["value"])
                df['SHORTM_FNCEGD_CD'].append(result.find(
                    "SHORTM_FNCEGD_CD").attrib["value"])
                df['증권구분'].append(result.find(
                    "SHORTM_FNCEGD_CD_NM").attrib["value"])
                df['종목번호'].append(result.find(
                    "ISIN").attrib["value"])
                df['종목명'].append(result.find(
                    "SECN_NM").attrib["value"])
                df['발행일'].append(result.find(
                    "ISSU_DT").attrib["value"])
                df['만기일'].append(result.find(
                    "RED_DT").attrib["value"])
                df['GOODS_DAYOBJ_CD'].append(
                    result.find("GOODS_DAYOBJ_CD").attrib["value"])
                df['일물'].append(result.find(
                    "GOODS_DAYOBJ_CD_NM").attrib["value"])
                df['NUM'].append(result.find("NUM").attrib["value"])

    import pickle
    with open('seibro_cd_issue.pkl', 'wb') as f:
        pickle.dump(df, f)

    df = pd.DataFrame(df)
    df.to_csv('seibro_cd_issue.csv', index=False)
except Exception as e:
    print(e)
    df = pd.DataFrame(df)
    df.to_csv('seibro_half.csv', index=False)
