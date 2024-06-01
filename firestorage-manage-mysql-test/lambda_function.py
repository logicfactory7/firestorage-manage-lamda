import json
import pymysql
import memcache
import boto3
import requests
import urllib
import xmltodict
import dateutil
import botocore

from datetime import date, datetime,timezone, timedelta
from botocore.client import Config
from botocore.endpoint import URLLib3Session
from botocore.credentials import Credentials
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth


import xml.etree.ElementTree as ET
from requests.exceptions import Timeout

# pip3 install requests -t .
# pip3 install PyMySQL -t .
# pip3 install python-memcached -t .
# pip3 install xmltodict -t .

def lambda_handler(event, context):
    # TODO implement

    domain = 'sample.firestorage.jp'

    code, asp_name = asp_domain_check(domain)

    s3_data = {}

    if code == 201:
        code ,cloud_json = s3_simple_get("firestorage-index", 'setting/asp/service/' + asp_name + '/config.json')
        print(code)
        print(cloud_json)

        json_data = json.loads(cloud_json)
        s3_data = json_data['setting']

    input_query = 'okuru/N3SDGvAqKfQquK/ahfquZmr'
    l = input_query.split('/')
    if len(l) != 3:
        return {
            'statusCode': 400,
            'body': len(l)
        }

    group = l[0]
    key   = l[1]
    dir   = l[2]

    url = group + '/' + key + '/' + dir + '/config.json'

    print(url)
    print(s3_data)

    index_config           = s3_data.copy()
    index_config['bucket'] = 'firestorage-index'

    code ,text = s3_simple_get(index_config, url)

    print(code)
    print(text)

    if code != 200:
        return {
            'statusCode': 400,
            'body': len(l)
        }

    upload_index = json.loads(text)
    prefix = group + '/' + upload_index['upload_dir'] + '/contents'

    # ファイル一覧を取得
    code, data = bucket_list(s3_data, prefix, '', '', 1000)

    if code != 200:
        return {
            'statusCode': 400,
            'body': len(l)
        }

    item_list = []

    for item in data['list']:
        #  [{'key': 'okuru/C3cxSrKytYuWVh/gB473UOB/config.json', 'date': '2024-05-30 15:06:23', 'etag': '"950d93364a1ce4c5579022c74e66b5b5"', 'size': '1568'}
        item_data = {}
        l = item['key'].split('/')
        item_data['key'] = l[len(l) - 1]
        item_data['date'] = item['date']
        item_data['size'] = item['size']
        item_list.append(item_data)

    return {
        'statusCode': 200,
        'body': json.dumps(item_list)
    }




    code, memc, message = memcached_connect("fire_main")

    if code != 200:
        return {
            'statusCode': 200,
            'body': json.dumps("memc connect NG : " + str(code) + ":" + message)
        }

    rep = memc.get("google_sp_crawler1")

    print("rep")
    print(rep.decode('utf-8'))

    return {
        'statusCode': 200,
        'body': json.dumps("ok")
    }





    # MySQLに接続
    code, db, message = mysql_db_connect("fire_slave")
    if code != 200:
        return {
            'statusCode': 200,
            'body': json.dumps("connect NG : " + str(code) + ":" + message)
        }

    cursor = db.cursor()
    # データ取得のクエリ
    data = {'id':1}
    try:

        sql = "SELECT * FROM member_tbl WHERE id = %(id)s "
        cursor.execute(sql, (data))
        result = cursor.fetchall()
        for row in result:
            print(row)

        print(json.dumps(result, default=json_serial, indent=2))
    finally:
        mysql_db_close(db, cursor)


    return {
        'statusCode': 200,
        'body': json.dumps("ok")
    }

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

                    JST = timezone(timedelta(hours=+9), 'JST')
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


def memcached_connect(name):

    ip_add   = ''

    if name == 'fire_main':
        ip_add = '192.168.100.31:11211'
    elif name == 'localhost':
        ip_add = '127.0.0.1:11211'

    if ip_add == '':
        return 404, {}, "db name not found"

    client = memcache.Client([ip_add], cache_cas=True)

    return 200, client, "OK"


def mysql_db_close(db, cursor):

    cursor.close()
    db.close()

def mysql_db_connect(db_name):

    ip_add   = ''
    user     = 'fire'
    password = 'fireaccess'
    database = 'fire'
    # charset='utf8mb4'

    if db_name == 'fire_slave':
        ip_add = '192.168.100.113'

    if ip_add == '':
        return 404, {}, "db name not found"

    try:
        db = pymysql.connect(host=ip_add,
            user=user,
            password=password,
            database=database)
    except pymysql.Error as e:
        return 500, {}, "db name not found"

    return 200, db, "OK"

# date, datetimeの変換関数
def json_serial(obj):
    # 日付型の場合には、文字列に変換します
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    # 上記以外はサポート対象外.
    raise TypeError ("Type %s not serializable" % type(obj))

