import pandas as pd
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
import requests

cookies = {
    'WMONID': 'sqNQePFDVA-',
    'lastAccess': '1732969235780',
    'JSESSIONID': 'Rr59Z0zzHudWGagm_H66yfVxzxfzxv6sReaV-iKuXL20sUmfRNE2!2023453677',
    'globalDebug': 'false',
}

headers = {
    'Accept': 'application/xml',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/xml; charset="UTF-8"',
    # 'Cookie': 'WMONID=sqNQePFDVA-; lastAccess=1732969235780; JSESSIONID=Rr59Z0zzHudWGagm_H66yfVxzxfzxv6sReaV-iKuXL20sUmfRNE2!2023453677; globalDebug=false',
    'Origin': 'https://seibro.or.kr',
    'Pragma': 'no-cache',
    'Referer': 'https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/repo/BIP_CNTS09003V.xml&menuNo=236',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'submissionid': 'submission_trTermsIrateList',
}


data_range = pd.date_range('20050102', '20241231', freq="Y")
end_data_range = data_range[-1] - timedelta(days=1)
print(data_range, end_data_range)

rows = []
for i in range(len(data_range) - 1):
    start_date = (data_range[i] + timedelta(days=1)).strftime('%Y%m%d')
    end_date = data_range[i + 1].strftime('%Y%m%d')
    print(start_date, end_date)
    data = f'<reqParam action="trTermsIrateList" task="ksd.safe.bip.cnts.Repo.process.RepoIRatePTask"><CD_ENG_NM value="REPO_PURSEC_KIND_TPCD"/><MENU_NO value="236"/><START_DT value="{start_date}"/><END_DT value="{end_date}"/><REPO_PURSEC_KIND_TPCD value=""/><PAGE_NUM value="1"/><PAGE_ON_CNT value="10"/></reqParam>'
    response = requests.post(
        'https://seibro.or.kr/websquare/engine/proworks/callServletService.jsp',
        cookies=cookies,
        headers=headers,
        data=data,
    )
    content = response.content
    root = ET.fromstring(content)
    for data in root.findall('data'):
        result = data.find('result')
        row = {element.tag: element.attrib.get(
            'value', '') for element in result}
        rows.append(row)


df = pd.DataFrame(rows)
df.to_csv('seibro_repo.csv', index=False)
