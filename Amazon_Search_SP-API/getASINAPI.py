import os
import sys
import time
import datetime
import re
import urllib
import numpy as np
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from pathlib import Path
import warnings
#warnings.simplefilter('ignore')

from sp_api.api import Catalog
from sp_api.base import SellingApiException
from sp_api.base.exceptions import SellingApiException
from sp_api.base.marketplaces import Marketplaces

now = datetime.datetime.now()
today = now.strftime('%Y%m%d')

for i in range(100):
    try:
        outname = 'result%s_%s.csv'%(i + 1, today)
        os.remove(outname)
    except:
        pass

df = pd.read_excel('設定/wordlist.xlsx', header=None)
KeyWordList = df[df.columns[0]]

dfconfig = pd.read_excel('設定/config.xlsx', header=None)
APIkeys = dfconfig[dfconfig.columns[1]]

credentials=dict(
            refresh_token     = APIkeys[0], # 'リフレッシュトークン',   # Amazon Seller開発者登録後に入手可能
            lwa_app_id        = APIkeys[1], # 'クライアントID',        # Amazon Seller開発者登録後に入手可能
            lwa_client_secret = APIkeys[2], # 'クライアント機密情報',   # Amazon Seller開発者登録後に入手可能
            aws_access_key    = APIkeys[3], # 'AWS アクセスキー',      #（AWS IAMユーザーロール登録時に取得可能）
            aws_secret_key    = APIkeys[4], # 'AWS シークレットキー',  #（AWS IAMユーザーロール登録時に取得可能）
            role_arn          = APIkeys[5], # 'AWS IAM ARN',        #（AWS IAMユーザーロール登録時に取得可能）
            )

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def search_products(keyword: str):
    obj = Catalog(marketplace=Marketplaces.JP, credentials=credentials)
    result = obj.list_items(MarketplaceId=Marketplaces.JP.marketplace_id, 
                            Query=keyword, 
                            QueryContextId='All')
    return result()

index = 1
outname = 'result%s_%s.csv'%(index, today)
print('\n#################### 検索開始 ####################\n')
for idx, KeyWord in enumerate(KeyWordList, 1):
    try:
        res = search_products(KeyWord)
        if len(res) == 0:
            continue
        else:
            product = res['Items'][0]
            print(idx,product['Identifiers']['MarketplaceASIN']['ASIN'],product['AttributeSets'][0]['Title'], len(res['Items']))
            if idx%3000 == 0:
                outname = 'result%s_%s.csv'%(idx//3000 + 1, today)
            with open(outname, mode='a', newline='', errors='ignore') as f_handle:
                str1 = '{0},{1},{2},{3}'.format(idx, KeyWord, product['Identifiers']['MarketplaceASIN']['ASIN'], product['AttributeSets'][0]['Title'])
                f_handle.write(str(str1) + "\n")
    except:    
        print(idx,"ERROR You exceeded your quota for the requested resource.")

print('\n#################### 連結開始 ####################\n')
outname = 'Merge_%s.csv'%(today)
try:
    os.remove(outname)
except:
    pass
p = Path(os.getcwd())
csv_files = list(p.glob('*.csv'))
data_list = []
for file in csv_files:
    data_list.append(pd.read_csv(file, header=None, encoding="cp932"))
df = pd.concat(data_list, axis=0, sort=True)
df.to_csv(outname, header=None, index=False, encoding="cp932")



