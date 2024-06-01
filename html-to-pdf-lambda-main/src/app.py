import json
import base64
import redis
import boto3
import re
import datetime
from redis.cluster import RedisCluster, ClusterNode
from dataclasses import dataclass

import requests
import urllib
import xmltodict
import dateutil
import botocore

from botocore.client import Config
from botocore.endpoint import URLLib3Session
from botocore.credentials import Credentials
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth

import xml.etree.ElementTree as ET
from requests.exceptions import Timeout

from html_service import HtmlService
from invoice_service import InvoiceService
from pdf_service import PdfService

html_service = HtmlService()
invoice_service = InvoiceService()
pdf_service = PdfService()

@dataclass()
class Item:
    name: str
    price: int
    quantity: int



def lambda_handler(event, context):

    domain = 'sample.firestorage.jp'
    s3_data = {}

    print(event)
    print(context)

    # 入力値を取り出す
    if event.get('requestContext') is None:
        return resp_data(404, domain, "requestContext error")

    input_query = get_input_query(event)

    val = re.sub("[^'a-zA-Z0-9/']", "", input_query)
    if val != input_query:
        return resp_data(400, domain, "input query validation error")

    print(input_query)
    items = []
    invoice = invoice_service.get()

    dt_now = datetime.datetime.now()
    invoice.issued_date = dt_now.strftime('%Y年%m月%d日')

    # input_queryに綺麗な文字列が入る
    if input_query == 'mazda':
        #  仮データ
        items = [
            Item(name="URL1", price=1000, quantity=2),
            Item(name="URL2", price=2000, quantity=1),
            Item(name="URL3", price=2000, quantity=1),
            Item(name="URL4", price=2000, quantity=1),
            Item(name="URL5", price=2000, quantity=1)
        ]
        invoice.items = items

    else:
        # urlがらキーを抽出
#       input_query = 'okuru/N3SDGvAqKfQquK/ahfquZmr'
        l = input_query.split('/')
        if len(l) != 4:
            return resp_data(400, domain, "input query string error:" + input_query)

        group = l[1]
        key   = l[2]
        dir   = l[3]

        # asp or 通常
        code, asp_name = asp_domain_check(domain)

        if code == 201:
            code ,cloud_json = s3_simple_get("firestorage-index", 'setting/asp/service/' + asp_name + '/config.json')
            print("s3_simple_get:" + 'setting/asp/service/' + asp_name + '/config.json' + ":" + str(code))
            if code != 200:
                return resp_data(400, domain, "s3_simple_get cloud setting config.json error:" + str(code))
            json_data = json.loads(cloud_json)
            s3_data = json_data['setting']
        else:
            return resp_data(400, domain, "not support")

        # config.jsonを取得
        index_config           = s3_data.copy()
        index_config['bucket'] = 'firestorage-index'
        url = group + '/' + key + '/' + dir + '/config.json'
        code ,text = s3_simple_get(index_config, url)
        print("s3_simple_get:" + url + ":" + str(code))
        if code != 200:
            if code == 404:
                return resp_data(404, domain, "url error")
            else:
                return resp_data(400, domain, "config,json error")

        upload_index = json.loads(text)
        prefix = group + '/' + upload_index['upload_dir'] + '/contents'
        print("bucket_list prefix:" + prefix)

        # ファイル一覧を取得
        code, data = bucket_list(s3_data, prefix, '', '', 1000)

        if code != 200:
            return resp_data(400, domain, "bucket_list error:" + str(code))
        print("bucket_list:" + prefix + ":" + str(code))

        items = []
        for item in data['list']:
            #  [{'key': 'okuru/C3cxSrKytYuWVh/gB473UOB/config.json', 'date': '2024-05-30 15:06:23', 'etag': '"950d93364a1ce4c5579022c74e66b5b5"', 'size': '1568'}
            l = item['key'].split('/')
            items.append(Item(name=l[len(l) - 1] , price=item['size'], quantity=item['date']))

        invoice.items = items
        invoice.company.url = 'https://' + domain + '/key/' + group + '/' + key + '/' + dir
        invoice.company.exp = 'xxxxx-xx-xx xx:xx:xx'

    # テンプレートを取得
    code, text = s3_simple_get('firestorage-index', 'setting/test/template/donwload_pdf.html.json')
    if code != 200:
        return resp_data(404, domain, "donwload_pdf.html error:" + str(code))

    json_data = json.loads(text)
    body = json_data['body']
    print("s3_simple_get:donwload_pdf.pdf:" + str(code))

    html = html_service.render_invoice(invoice, body)
    pdf = pdf_service.create_from_html(html)
    pdf_base64 = base64.b64encode(pdf).decode("utf-8")
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/pdf",
            "Content-Disposition": "attachment; filename=invoice.pdf"
        },
        "body": pdf_base64,
        "isBase64Encoded": True
    }

def get_input_query(event):

    rawPath = event.get('requestContext').get('http').get('path')
    return rawPath

def resp_data(code, domain, message):

    body = {}
    body['code'] = code
    body['data'] = message
    body['version'] = "1.0"

    if domain != '*':
        domain = "https://" + domain

    return {
        'statusCode': 200,
        'headers' : {
          "Access-Control-Allow-Origin":  domain,
        },
        'body': json.dumps(body)
    }

def redis_connect():

    redis_host = 'firestorage-manage-redis.ypabtk.clustercfg.apne1.cache.amazonaws.com'
    redis_port = '6379'

    nodes = [
       {"host": redis_host, "port": redis_port}
    ]

    cluster_nodes = [ClusterNode(**node) for node in nodes]
    rc = RedisCluster(startup_nodes=cluster_nodes, read_from_replicas=True)

    return rc

def asp_domain_check(domain):

    name = domain.split('.')

    if name[0] == 'www':
       return 200, ""
    if name[0] == 'firestorage':
       return 200, ""
    elif name[0] == 'xfs.jp':
       return 200, ""

    return 201, name[0]

def s3_simple_get(s3_data, key):

    if isinstance(s3_data, dict):

        url, headers = create_proxy_url('GET', s3_data, key)

        try:
            body = ""
            r = requests.get(url, timeout=(3.0, 5.0), headers=headers, data="")
            code = r.status_code

            if code == 200:
                body = r.text
            return code, body
        except Timeout:
            return 501

    else:

        s3 = boto3.client('s3')

        try:
            response = s3.get_object(Bucket=s3_data, Key=key)
            text = response['Body'].read()
            text = text.decode('utf8')
            return 200, text
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return 404, ""
            else:
                return 500, ""

def create_proxy_url(method, s3_data, key):

#    proxy_url    = os.environ['proxy_url']
    proxy_url    = 'http://ip-172-17-100-84.ap-northeast-1.compute.internal'
    endpoint_url = s3_data['aws_host']
    Host         = endpoint_url.replace("https://", "")

    ### 1.APIエンドポイントURL
    url_ = s3_data['aws_host'] + "/" + s3_data['bucket'] + "/" + key
    ### 2. Credential生成
    credentials = Credentials(s3_data['aws_access_key_id'], s3_data['aws_secret_access_key'])
    ### 3. AWSRequest生成
    request = AWSRequest(method=method, url=url_)
    ### 4. AWSリクエスト署名
    SigV4Auth(credentials, 's3', s3_data['region']).add_auth(request)
    ### 5. API発行
    headers = {
        'Authorization': request.headers['Authorization'],
        'Host':Host,
        'X-Amz-Date':request.context['timestamp']
    }

    return proxy_url + "/" + Host + "/" + s3_data['bucket']  + "/" + key, headers


def bucket_list(s3_data, prefix: str, Delimiter: str, marker: str, MaxKeys :int):
    """S3上のファイルリスト取得
    Args:
        bucket (str): バケット名
        prefix (str): バケット以降のパス
        recursive (bool): 再帰的にパスを取得するかどうか
    """
    if MaxKeys is None:
        MaxKeys = 1000
    elif MaxKeys > 1000:
        MaxKeys = 1000

    bucket = ""

    if isinstance(s3_data, dict):

        region       = s3_data['region']
        endpoint_url = s3_data['aws_fdqn']
        bucket       = s3_data['bucket']
        access_key   = s3_data['aws_access_key_id']
        secret_key   = s3_data['aws_secret_access_key']

        list = []
        if len(Delimiter) > 0:
            list.append("delimiter=" + Delimiter)

        if MaxKeys > 0:
            list.append("max-keys=" + str(MaxKeys))

        list.append('prefix=' + urllib.parse.quote(prefix, safe=''))

        parm = "&".join(list)

        url, headers = create_proxy_url('GET', s3_data, "?" + parm)

        try:
            r = requests.get(url, headers=headers, timeout=(3.0, 5.0) )
            code = r.status_code

            if code == 200:

                xml_data = r.text
                json = xmltodict.parse(xml_data)
                print("xmltodict")
                print(json)

                response = json['ListBucketResult']
                data = {}

                if 'CommonPrefixes' in response:
                    # Delimiterが'/'のときはフォルダがKeyに含まれない
                    data['response'] = 'CommonPrefixes'
                    list = []
                    if type(response['CommonPrefixes']) is list:
                        list.append(response['CommonPrefixes'])
                        data['list']  = list
                        data['count'] = len(list)
                        return 200, data
                    else:
                        data['list'] =  response['CommonPrefixes']
                        data['count'] = len(response['CommonPrefixes'])
                        return 200, data

                elif 'Contents' in response:  # 該当する key がないと response に 'Contents' が含まれない

                    data['response'] = 'Contents'
                    keys = []

                    JST = datetime.timezone(datetime.timedelta(hours=+9), 'JST')
                    root = ET.fromstring(xml_data)
                    # 子ノードを読み込む
                    for child1 in root:
                        value = {}
                        for child2 in child1:
                            if child2.tag.find('Key') > 0:
                                value['key'] = child2.text
                            elif child2.tag.find('Size') > 0:
                                value['size'] = child2.text
                            elif child2.tag.find('LastModified') > 0:
                                date = child2.text
                                date = dateutil.parser.parse(date).astimezone(JST)
                                value['date'] = date.strftime("%Y-%m-%d %H:%M:%S")
                            elif child2.tag.find('ETag') > 0:
                                value['etag'] = child2.text
                        if 'key' in value:
                            keys.append(value)

                    data['list'] = keys
                    data['count'] = len(keys)
                    return 200, data
                else:
                    data = {}
                    data['count'] = 0
                    return code, data

        except requests.exceptions.RequestException as e:
            data = {}
            data['count'] = 0
            print("エラー : ",e)
            return 500, data

        data = {}
        data['count'] = 0
        return code, data

    else:

        s3 = boto3.client('s3')
        keys = []

        try:
            response = s3.list_objects(
                Bucket=s3_data, Prefix=prefix, Marker=marker, Delimiter=Delimiter, MaxKeys=MaxKeys, EncodingType='url')
            if len(response) > 0:
                pass
            else :
                return 404, keys
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return 404, keys
            else:
                pass
        except KeyError as e:
            return 500, keys



    if 'CommonPrefixes' in response:
        # Delimiterが'/'のときはフォルダがKeyに含まれない
        return 200, response['CommonPrefixes']

    if 'Contents' in response:  # 該当する key がないと response に 'Contents' が含まれない
        for item in response['Contents']:
            data = {}

            print(item['LastModified'])

            data['key']  = item['Key']
            data['date'] = item['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
            data['size'] = item['Size']
            data['etag'] = item['ETag']
            keys.append(data)

        return 200, keys

    return 404, keys

