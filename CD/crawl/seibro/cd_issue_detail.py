import pickle
import pandas as pd
import xml.etree.ElementTree as ET
import requests

cookies = {
    'WMONID': 'sqNQePFDVA-',
    'JSESSIONID': 'I8pPNd41lJ9Cyef_U-u4JmreSU8yFtMpcWI5Eff0xR-tdTy5nSvO!54157346',
    'lastAccess': '1732201763319',
}

headers = {
    'Accept': 'application/xml',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/xml; charset="UTF-8"',
    # 'Cookie': 'WMONID=sqNQePFDVA-; JSESSIONID=I8pPNd41lJ9Cyef_U-u4JmreSU8yFtMpcWI5Eff0xR-tdTy5nSvO!54157346; lastAccess=1732201763319',
    'Origin': 'https://seibro.or.kr',
    'Pragma': 'no-cache',
    'Referer': 'https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/moneyMarke/BIP_CNTS04014P.xml&ISIN=KRZE00601E53',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'submissionid': 'submission_issuSecnViewEL1',
}


t = pd.read_csv('seibro_cd_issue.csv')


keys = [
    "ISSU_BANK_NM", "ISSU_MOFNO", "ISSUCO_CUSTNO", "REP_SECN_NM", "ISIN",
    "KOR_SECN_NM", "ISSU_DT", "XPIR_DT", "FACE_AMT", "SALE_AMT",
    "TOTAL_FACE_AMT", "DISCN_RATE", "REMINDE_DT", "ISSU_WHCD_NM",
    "ISSU_CUR_CD", "PAY_BANK_BRONO", "PAY_BANK_MOFNO", "CREDIT_GRD_NM",
    "BANK_NM", "CREDITRT_DT", "VALAT_GRD_CD_NM", "KSD_CFM_DTTM"
]

columns = [
    "발행은행", "발행은행코드", "ISSUCO_CUSTNO", "REP_SECN_NM", "종목코드",
    "종목명", "발행일", "만기일", "액면금액", "매출금액",
    "발행금액", "할인율", "잔존일물", "발행형태",
    "발행통화", "지급은행지점코드", "PAY_BANK_MOFNO", "발행은행신용등급",
    "지급은행", "CREDITRT_DT", "VALAT_GRD_CD_NM", "등록일자"
]

df = {
    column: [] for column in columns
}

isins = t["종목번호"].unique()
print(len(isins))
for ii, isin in enumerate(isins):
    print(ii)
    data = f'<reqParam action="issuSecnViewEL1" task="ksd.safe.bip.cnts.MoneyMarke.process.CdIssuSecnPTask"><ISIN value="{isin}"/></reqParam>'
    response = requests.post(
        'https://seibro.or.kr/websquare/engine/proworks/callServletService.jsp',
        cookies=cookies,
        headers=headers,
        data=data,
    )
    content = response.content.decode('utf-8')
    root = ET.fromstring(content)
    for result in root.findall('.//result'):
        for i, c in enumerate(columns):
            df[c].append(result.find(f'.//{keys[i]}').attrib["value"])


with open('seibro_cd_issue_detail.pkl', 'wb') as f:
    pickle.dump(df, f)


df = pd.DataFrame(df).drop_duplicates()
df.to_csv('seibro_cd_issue_detail.csv', index=False)
