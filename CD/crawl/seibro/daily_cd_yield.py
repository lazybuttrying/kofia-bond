import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

cookies = {
    'WMONID': 'sqNQePFDVA-',
    'JSESSIONID': 'sJkWjgLgfrg_PKNH-xDTRkF4uHXkVSXGeiL2Rit6tdXJkwJ8NF6_!1202419841',
    'lastAccess': '1731250232205',
}

headers = {
    'Accept': 'application/xml',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/xml; charset="UTF-8"',
    # 'Cookie': 'WMONID=sqNQePFDVA-; JSESSIONID=sJkWjgLgfrg_PKNH-xDTRkF4uHXkVSXGeiL2Rit6tdXJkwJ8NF6_!1202419841; lastAccess=1731250232205',
    'Origin': 'https://seibro.or.kr',
    'Pragma': 'no-cache',
    'Referer': 'https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/moneyMarke/BIP_CNTS04016V.xml&menuNo=494',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'submissionid': 'submission_xpirEarnIrateListEL1',
}


date = '20100104'
columns = ['date', 'credit_rank', '1d', '7d', '10d', '15d',
           '1m', '2m', '3m', '4m', '6m', '1y', '2y']
df = {column: [] for column in columns}
# business days
for date in [datetime.strftime(date, '%Y%m%d') for date in pd.date_range('20100104', '20241130') if date.weekday() < 5]:
    data = f'<reqParam action="xpirEarnIrateListEL1" task="ksd.safe.bip.cnts.MoneyMarke.process.ShortmFncegdStatPTask"><STD_DT value="{date}"/><MENU_NO value="494"/></reqParam>'

    response = requests.post(
        'https://seibro.or.kr/websquare/engine/proworks/callServletService.jsp',
        cookies=cookies,
        headers=headers,
        data=data,
    )

    content = response.content
    root = ET.fromstring(content)
    for data in root.findall('.//data'):

        for result in data.findall('.//result'):
            if result.find("G1").attrib["value"] == "CD":
                df['date'].append(date)
                df['credit_rank'].append(result.find("G2").attrib["value"])
                df['1d'].append(result.find("DD1_XPIR_PRATE").attrib["value"])
                df['7d'].append(result.find("DD7_XPIR_PRATE").attrib["value"])
                df['10d'].append(result.find(
                    "DD10_XPIR_PRATE").attrib["value"])
                df['15d'].append(result.find(
                    "DD15_XPIR_PRATE").attrib["value"])
                df['1m'].append(result.find(
                    "MONS1_XPIR_PRATE").attrib["value"])
                df['2m'].append(result.find(
                    "MONS2_XPIR_PRATE").attrib["value"])
                df['3m'].append(result.find(
                    "MONS3_XPIR_PRATE").attrib["value"])
                df['4m'].append(result.find(
                    "MONS4_XPIR_PRATE").attrib["value"])
                df['6m'].append(result.find(
                    "MONS6_XPIR_PRATE").attrib["value"])
                df['1y'].append(result.find("YY1_XPIR_PRATE").attrib["value"])
                df['2y'].append(result.find("YY2_XPIR_PRATE").attrib["value"])

    time.sleep(1)

df = pd.DataFrame(df)
df.to_csv('seibro_cd_yield.csv', index=False)
