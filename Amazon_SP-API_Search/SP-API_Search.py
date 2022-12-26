import os
import sys
import time
import datetime
from datetime import date,timedelta
import re
import json
import urllib
import numpy as np
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
import warnings
import configparser
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from sp_api.api import Products
from sp_api.api import Catalog
from sp_api.api import ProductFees
from sp_api.api import Inventories
from sp_api.api import CatalogItems
from sp_api.api import ReportsV2
from sp_api.api import Orders
from sp_api.base import SellingApiException
from sp_api.base.exceptions import SellingApiException
from sp_api.base.marketplaces import Marketplaces
from sp_api.base import Schedules
from sp_api.base import ReportType

@retry(stop=stop_after_attempt(20), wait=wait_fixed(4))
def Update_Cell(works, Column, Row, Value):
    works.update_cell(Column , Row, Value)

@retry(stop=stop_after_attempt(20), wait=wait_fixed(4))
def Append_Row(works, Rows, Index):
    works.append_row(Rows , table_range=Index, value_input_option='USER_ENTERED')

@retry(stop=stop_after_attempt(20), wait=wait_fixed(4))
def Clear_Value(gc_, sheetname_, worksheetname_, Index):
    gc_.open(sheetname_).values_clear("%s!B%s:Z%s"%(worksheetname_, Index+1, Index+1))

config_ini=configparser.ConfigParser(interpolation=None)
config_ini.read('bin/config.ini', encoding='utf-8')
KeyJsonPath=config_ini["GASkey"]["gaskeypath"]
SheetName=config_ini["GASkey"]["sheetname"]
REFRESHTOKEN=config_ini["API_Keys"]["refresh_token"]
IWA_APP_ID=config_ini['API_Keys']["lwa_app_id"]
IWA_CLIENT_SECRET=config_ini['API_Keys']["lwa_client_secret"]
AWS_ACCESS_KEY=config_ini['API_Keys']["aws_access_key"]
AWS_SECRET_KEY=config_ini['API_Keys']['aws_secret_key']
ROLE_ARN=config_ini['API_Keys']['role_arn']

now = datetime.datetime.now()
now_day1 = datetime.datetime.now() - timedelta(days=1)
today = now.strftime('%Y-%m-%d')
yesterday = now_day1.strftime('%Y-%m-%d')

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

AWS_credentials=dict(
    refresh_token     = REFRESHTOKEN, # 'リフレッシュトークン',   # Amazon Seller開発者登録後に入手可能
    lwa_app_id        = IWA_APP_ID, # 'クライアントID',        # Amazon Seller開発者登録後に入手可能
    lwa_client_secret = IWA_CLIENT_SECRET, # 'クライアント機密情報',   # Amazon Seller開発者登録後に入手可能
    aws_access_key    = AWS_ACCESS_KEY, # 'AWS アクセスキー',      #（AWS IAMユーザーロール登録時に取得可能）
    aws_secret_key    = AWS_SECRET_KEY, # 'AWS シークレットキー',  #（AWS IAMユーザーロール登録時に取得可能）
    role_arn          = ROLE_ARN, # 'AWS IAM ARN',        #（AWS IAMユーザーロール登録時に取得可能）
)

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def search_product(ASIN):    
    obj = Catalog(credentials=AWS_credentials, marketplace=Marketplaces.JP)
    result = obj.get_item(ASIN, MarketplaceId='A1VC38T7YXB528')
    return result()

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def ItemOffer_asin(ASIN, CONDITION):
    products = Products(marketplace=Marketplaces.JP, credentials=AWS_credentials)
    result = products.get_item_offers(asin=ASIN, item_condition=CONDITION, MarketplaceId='A1VC38T7YXB528')
    return result()

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def ListingOffer_Seller_sku(Seller_sku, CONDITION):
    products = Products(marketplace=Marketplaces.JP, credentials=AWS_credentials)
    result = products.get_listings_offer(asin=Seller_sku, item_condition=CONDITION, MarketplaceId='A1VC38T7YXB528')
    return result()

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def fee_estimator_asin(ASIN,productPrice,currencyCode,shippingPrice,FBA):
    productfees = ProductFees(marketplace=Marketplaces.JP, credentials=AWS_credentials)
    result = productfees.get_product_fees_estimate_for_asin(asin=ASIN, price=productPrice, currency=currencyCode, 
                                                            shipping_price=shippingPrice, is_fba=FBA)
    return result()

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def get_summaries_item_asin(ASIN):
    catalogitems = CatalogItems(marketplace=Marketplaces.JP, credentials=AWS_credentials)
    result = catalogitems.get_catalog_item(ASIN, marketplaceIds='A1VC38T7YXB528', includedData=['summaries'])
    return result()

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def get_relationships_item_asin(ASIN):
    catalogitems = CatalogItems(marketplace=Marketplaces.JP, credentials=AWS_credentials)
    result = catalogitems.get_catalog_item(ASIN, marketplaceIds='A1VC38T7YXB528', includedData=['relationships'])
    return result()

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def get_images_item_asin(ASIN):
    catalogitems = CatalogItems(marketplace=Marketplaces.JP, credentials=AWS_credentials)
    result = catalogitems.get_catalog_item(ASIN, marketplaceIds='A1VC38T7YXB528', includedData=['images'])
    return result()

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def get_salesRanks_item_asin(ASIN):
    catalogitems = CatalogItems(marketplace=Marketplaces.JP, credentials=AWS_credentials)
    result = catalogitems.get_catalog_item(ASIN, marketplaceIds='A1VC38T7YXB528', includedData=['salesRanks'])
    return result()

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def get_EAN_item_asin(ASIN):
    catalogitems = CatalogItems(marketplace=Marketplaces.JP, credentials=AWS_credentials)
    result = catalogitems.get_catalog_item(ASIN, marketplaceIds='A1VC38T7YXB528', includedData=['identifiers'])
    return result()

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def get_competitive_asin(ASIN):
    products = Products(marketplace=Marketplaces.JP, credentials=AWS_credentials)
    result = products.get_competitive_pricing_for_asins([ASIN], marketplaceIds='A1VC38T7YXB528')
    return result()

#reportsv2 = ReportsV2(marketplace=Marketplaces.JP, credentials=AWS_credentials)
#report_result = reportsv2.create_report_schedule(reportType='GET_MERCHANT_LISTINGS_ALL_DATA',
#                                                period="P3D",
#                                                nextReportCreationTime="2022-11-19T01:00:00+09:00",
#                                                marketplaceIds=["A1VC38T7YXB528"])

# 「サービスアカウントキーのJSONファイルのパス」を入力
GAS_credentials = ServiceAccountCredentials.from_json_keyfile_name(KeyJsonPath, scope)
gc = gspread.authorize(GAS_credentials)

# 自分で編集する編集箇所②'プロジェクト名'にはGoogleスプレッドシート名
# wks = gc.open(SheetName).get_worksheet(0)
print(SheetName)
WorkSheetName = 'Main Sheet'
wks = gc.open(SheetName).worksheet(WorkSheetName)

# 指定箇所削除
# gc.open(SheetName).values_clear("Main Sheet!B2:Z10000")

# データを取得する。（リストとして)
list_of_lists = wks.get_all_values()
#list_of_lists = wks.get_all_records()

reportsv2 = ReportsV2(marketplace=Marketplaces.JP, credentials=AWS_credentials)
report_result = reportsv2.get_reports(reportTypes=["GET_AFN_INVENTORY_DATA"], processingStatuses=["DONE"], createdSince="%sT00:00:00+09:00"%today, marketplaceIds=["A1VC38T7YXB528"])
try:
    print('レポート数 ', len(report_result.payload['reports']))
    reportDocumentId = report_result.payload['reports'][0]['reportDocumentId']
    report_result = reportsv2.get_report_document(reportDocumentId, download=True, file='report.tsv')
    df_tsv = pd.read_table('report.tsv', encoding='cp932')
    print(df_tsv.columns)
    df_tsv = df_tsv[['seller-sku', 'asin', 'Quantity Available']]
    asin_list = df_tsv['asin'].values
    inventry_list = df_tsv['Quantity Available'].values
    print(df_tsv)
    print(asin_list)
except:
    try:
        report_result = reportsv2.create_report(reportType="GET_FLAT_FILE_OPEN_LISTINGS_DATA", marketplaceIds=["A1VC38T7YXB528"])
        df_tsv = pd.read_table('report.tsv', encoding='cp932')
        print(df_tsv[['seller-sku', 'asin', 'Quantity Available']])
    except:
        pass

print('ASIN Length : ', len(list_of_lists))
for i in range(1, len(list_of_lists)):
    ASIN = list_of_lists[i][0]
    print(i, ASIN)
    json_result = search_product(ASIN)
    # タイトル
    try:
        title = json_result['AttributeSets'][0]['Title']
    except:
        items = [ASIN,'Not amazon.jp','None','None','None','None','None','None','None','None','None','None','None','None','None','None','None','None','None']
        Append_Row(wks, items, 'B%s'%(i + 1))
    # カテゴリー
    salesRank_result = get_salesRanks_item_asin(ASIN)
    try:
        category = salesRank_result['salesRanks'][0]['displayGroupRanks'][0]['title']
    except:
        category = 'None'
    # ブランド
    try:
        brand = json_result['AttributeSets'][0]['Brand']
    except:
        try:
            brand = json_result['AttributeSets'][0]['Label']
        except:
            brand = 'None'
    # メーカー型番
    try:
        model = json_result['AttributeSets'][0]['Model']
    except:
        model = 'None'
    # ランキング
    try:
        #rank = json_result['SalesRankings'][0]['Rank']
        rank = salesRank_result['salesRanks'][0]['ranks'][0]['rank']
    except:
        try:
            rank = salesRank_result['salesRanks'][0]['displayGroupRanks'][0]['rank']
        except:
            rank = 'None'
    # サイズ
    try:
        height = float(json_result['AttributeSets'][0]['PackageDimensions']['Height']['value'])*2.54
        width = float(json_result['AttributeSets'][0]['PackageDimensions']['Width']['value'])*2.54
        length = float(json_result['AttributeSets'][0]['PackageDimensions']['Length']['value'])*2.54
        weight = float(json_result['AttributeSets'][0]['PackageDimensions']['Weight']['value'])*453.592
    except:
        try:
            height = float(json_result['AttributeSets'][0]['ItemDimensions']['Height']['value'])*2.54
            width = float(json_result['AttributeSets'][0]['ItemDimensions']['Width']['value'])*2.54
            length = float(json_result['AttributeSets'][0]['ItemDimensions']['Length']['value'])*2.54
            weight = float(json_result['AttributeSets'][0]['ItemDimensions']['Weight']['value'])*453.592    
        except:
            try:
                height = float(json_result['AttributeSets'][0]['ItemDimensions']['Height']['value'])*2.54
            except:
                height = 'None'
            try:
                width = float(json_result['AttributeSets'][0]['ItemDimensions']['Width']['value'])*2.54
            except:
                width = 'None'
            try:
                length = float(json_result['AttributeSets'][0]['ItemDimensions']['Length']['value'])*2.54
            except:
                length = 'None'
            try:
                weight = float(json_result['AttributeSets'][0]['ItemDimensions']['Weight']['value'])*453.592
            except:
                weight = 'None'
    # カート取得資格有無
    Offer_result = ItemOffer_asin(ASIN, 'New') #'Used'
    try:
        cart_qualification = Offer_result['Offers'][0]['IsFeaturedMerchant']
    except:
        cart_qualification = 'None'
    # 出品者数
    NumberOfOffers = 0
    try:
        Offers = Offer_result['Summary']['NumberOfOffers']
        for offerlist in Offers:
            try:
                NumberOfOffers = NumberOfOffers + int(offerlist['OfferCount'])
            except:
                pass
    except:
        pass
    # 最安値、最高値
    Competitive = get_competitive_asin(ASIN)
    low_price = 100000000
    try:
        for lowprice in Offer_result['Summary']['LowestPrices']:
            if 'LandedPrice' in lowprice:
                if low_price > lowprice['LandedPrice']['Amount'] and lowprice['condition'] == 'new':
                    low_price = lowprice['LandedPrice']['Amount']
    except:
        low_price = 'None'
    try:
        Listing_Price = Offer_result['Summary']['LowestPrices'][0]['ListingPrice']['Amount']
        Shipping_price = Offer_result['Summary']['LowestPrices'][0]['Shipping']['Amount']
    except:
        Listing_Price = 'None'
        Shipping_price = 'None'
    try:
        cart_price = Competitive[0]['Product']['CompetitivePricing']['CompetitivePrices'][0]['Price']['LandedPrice']['Amount']
        Listing_Price = Competitive[0]['Product']['CompetitivePricing']['CompetitivePrices'][0]['Price']['ListingPrice']['Amount']
        Shipping_price = Competitive[0]['Product']['CompetitivePricing']['CompetitivePrices'][0]['Price']['Shipping']['Amount']
    except:
        cart_price = 'None'

    # 販売手数料, FBA手数料
    try:
        fee_result = fee_estimator_asin(ASIN, float(Listing_Price), 'JPY', float(Shipping_price), True) #商品販売価格, 発送手数料, FBA利用
        ReferralFee = fee_result['FeesEstimateResult']['FeesEstimate']['FeeDetailList'][0]['FeeAmount']['Amount']
        FBAFees = fee_result['FeesEstimateResult']['FeesEstimate']['FeeDetailList'][3]['FeeAmount']['Amount']
    except:
        try:
            fee_result = fee_estimator_asin(ASIN, float(Listing_Price), 'JPY', float(Shipping_price), False) #商品販売価格, 発送手数料, FBA利用
            ReferralFee = fee_result['FeesEstimateResult']['FeesEstimate']['FeeDetailList'][0]['FeeAmount']['Amount']
            FBAFees = 'False'  
        except:
            ReferralFee = 'None'
            FBAFees = 'None'
    # メイン画像の取得
    images = get_images_item_asin(ASIN)
    image = images['images'][0]['images'][0]['link']
    image_function = '=HYPERLINK("https://www.amazon.co.jp/dp/%s", IMAGE("%s"))'%(ASIN,image)
    #在庫数
    if ASIN in asin_list:
        inventory = int(inventry_list[np.where(asin_list==ASIN)[0][0]])
        for k in range(len(np.where(asin_list==ASIN)[0])):
            if inventory < int(inventry_list[np.where(asin_list==ASIN)[0][k]]):
                inventory = int(inventry_list[np.where(asin_list==ASIN)[0][k]])
    else:
        inventory = 0
    # バリエーションASIN
    relationships_result = get_relationships_item_asin(ASIN)
    if relationships_result['relationships'][0]['relationships'] == []:
        childAsin = ""
    elif 'parentAsins' in relationships_result['relationships'][0]['relationships'][0]:
        parentAsin = relationships_result['relationships'][0]['relationships'][0]['parentAsins'][0]
        relationships_result = get_relationships_item_asin(parentAsin)
        childAsinlist = relationships_result['relationships'][0]['relationships'][0]['childAsins']
        childAsin = ','.join(childAsinlist)
    elif 'childAsins' in relationships_result['relationships'][0]['relationships'][0]:
        childAsinlist = relationships_result['relationships'][0]['relationships'][0]['childAsins']
        childAsin = ','.join(childAsinlist)
    
    # EAN(JAN)
    try:
        EAN = get_EAN_item_asin(ASIN)['identifiers'][0]['identifiers'][0]['identifier']
    except:
        EAN = 'None'
    Clear_Value(gc, SheetName, WorkSheetName, i)
    now = datetime.datetime.now() + timedelta(hours=9)
    now_time = now.strftime('%Y-%m-%d %H:%M')
    items = [ASIN,title,low_price,cart_price,Listing_Price,category,brand,model,rank,height,width,length,weight,NumberOfOffers,ReferralFee,FBAFees,image_function,cart_qualification,inventory,childAsin,EAN, now_time]
    Append_Row(wks, items, 'B%s'%(i + 1))


# AIzaSyC_VmeC6XBUrdKmP18JOmWmus4dveTh-SI


#superkameyoshi@gmail.com
#Super@ccount00