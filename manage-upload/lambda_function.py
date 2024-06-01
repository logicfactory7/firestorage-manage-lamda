import json
import fire_basic as fb
import firelib as fl
import mini_lib as ml
import logic_memcached as lm
import s3_simple as ss
import boto3
import botocore
import os
import re
import random
import urllib.parse
import xmltodict
import requests
import datetime
import base64
import hashlib
import dateutil
from dateutil.relativedelta import relativedelta
from requests.exceptions import Timeout
from botocore.client import Config
from botocore.endpoint import URLLib3Session
from botocore.credentials import Credentials
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth

import xml.etree.ElementTree as ET
from requests.exceptions import Timeout


# pip3 install redis -t .
# pip3 install requests -t .
# pip3 install xmltodict -t .
# pip3 install python-dateutil -t .
# pip3 install requests-aws4auth -t .
# pip3 install python-memcached -t .

_server_data = None
_cloud_data = None
_server_data_time = None
_cloud_data_time = None

def lambda_handler(event, context):

    body = {}
    code = 200
    rc = fb.redis_connect()

    validation_parm = [
        {'key': 'domain', 'max': 50, 'min':3, 'type': "a-z0-9."    , "req" : True},
        {'key': 'key',    'max': 40, 'min':8, 'type': "a-zA-Z0-9"  , "req" : False},
        {'key': 'group',  'max': 15, 'min':5, 'type': "a-zA-Z0-9"  , "req" : False},
        {'key': 'act',    'max': 40, 'min':3, 'type': "a-zA-Z0-9_" , "req" : False},
        {'key': 'auth',   'max': 16, 'min':3, 'type': "a-zA-Z0-9"  , "req" : False},
        {'key': 'dir',    'max': 16, 'min':3, 'type': "a-zA-Z0-9."  , "req" : False},
        {'key': 'file',   'max': 16, 'min':3, 'type': "a-zA-Z0-9"  , "req" : False},
        {'key': 'count',  'max': 3,  'min':1, 'type': "0-9"        , "req" : False},
        {'key': 'size',   'max': 5,  'min':1, 'type': "0-9"        , "req" : False},
        {'key': 'name',   'max': 1024,  'min':1,  'type': "*"       , "req" : False},
        {'key': 'type',   'max': 10, 'min': 1,    'type': "a-zA-Z0-9" , "req" : False},
        {'key': 'download_pass',   'max': 8, 'min': 0,    'type': "a-zA-Z0-9" , "req" : False},
        {'key': 'add_text', 'max': 10, 'min': 1,  'type': "*" , "req" : False},
        {'key': 'pass_code'         , 'max': 8,   'min': 4, 'type': "0-9" , "req" : False},
        {'key': 'setting_pass_code' , 'max': 8,   'min': 4, 'type': "0-9" , "req" : False},
        {'key': 'add_title_value'   , 'max': 512, 'min': 0, 'type': "*" , "req" : False},
        {'key': 'upload_title_value', 'max': 512, 'min': 0, 'type': "*" , "req" : False},
        {'key': 'add_title_value'   , 'max': 1024, 'min': 0, 'type': "*" , "req" : False},
        {'key': 'add_comment'       , 'max': 4096, 'min': 0, 'type': "*" , "req" : False},
        {'key': 'message'           , 'max': 4096, 'min': 0, 'type': "*" , "req" : False},
        {'key': 'sendmail'          , 'max': 128, 'min': 0, 'type': "mail" , "req" : False},
        {'key': 'keys'              , 'max': 4096, 'min': 0, 'type': "*" , "req" : False},
        {'key': 'server'            , 'max': 30  , 'min': 0, 'type': "a-z0-9." , "req" : False},
        {'key': 'expire'            , 'max': 4   , 'min': 0, 'type': "0-9" , "req" : False},
        {'key': 'eid'               , 'max': 45,   'min': 32, 'type': "a-f0-9" , "req" : False},
    ]

    code , form_data , error_data = fb.validation_deco(event.get('queryStringParameters'), validation_parm)

    if code != 200:
        print(error_data)
        return resp_data(400, "*", "validation error")

    response = event.get('queryStringParameters')

    code , form_data , error_data = fb.validation_deco(response, validation_parm)

    if code != 200:
        print(error_data)
        return resp_data(code, "*", error_data)

    # domain許可チェック
    domain = form_data['domain']
    if fb.domain_check(domain):
        return resp_data(404, "*", "domain error")

    # group許可チェック
    if len(form_data['group']) == 0:
        form_data['group'] = 'uketorumini'
    if form_data['group'] == 'uketorumini' or form_data['group'] == 'okurumini' or form_data['group'] == 'okuru':
        pass
    else:
        return resp_data(400, domain, "group error:" + form_data['group'])

    # eidチェック
    code, member_data = lm.check_eid(rc, form_data['eid'])

    data = {}
#   code = fb.access_text_log(event, form_data['group'] + "_top", form_data, "date,ip,act,key,dir,file,name,size,count")

    if form_data['act'] == 'crypt_data_upload_manage':
        # アップロードする前に準備をする
        return crypt_data_upload_manage(rc, domain, form_data, event, member_data)
    elif form_data['act'] == 'get_sign_upload_url':
        # ファイルのアップロードの署名作成
        return get_sign_upload_url(rc, domain, form_data)
    elif form_data['act'] == 'part_file':
        # 分割アップロード
        return part_file(rc, domain, form_data)
    elif form_data['act'] == 'upload_sendmail':
        # メールを送る
        return upload_sendmail(event, rc, domain, form_data)
    else:
        return resp_data(404, domain, "action error")

def crypt_data_upload_manage(rc, domain, form_data, event, member_data):

    body = {}
    auth          = form_data['auth']
    group         = form_data['group']
    upload_title  = form_data['upload_title_value']
    add_comment   = form_data['add_comment']
    add_title     = form_data['add_title_value']
    download_pass = form_data['download_pass']
    expire        = form_data['expire']

    if group != 'uketorumini' and group != 'okurumini' and group != 'okuru':
        code = 404
        body['message'] = 'group error:' + group
        return resp_data(code, domain, body)

    body['message'] = "OK"

    upload_service = {}
    upload_setting = {}

    asp_code, asp_name = fb.asp_domain_check(domain, member_data)

    if asp_code == 201:

        code ,cloud_json = fb.s3_simple_get("firestorage-index", 'setting/asp/service/' + asp_name + '/config.json')

        if code != 200:
            print("asp server data get error:" + str(code))
            message = "asp server data get error:"+ str(code)
            return resp_data(404, domain, message)

        server_json = json.loads(cloud_json)

        data = {}
        data['member_data'] = member_data
        data['server_json'] = server_json

        upload_service = server_json['config']
        upload_setting = server_json['setting']
 
        print("cloud_json")
        print(upload_setting)

    else:

        # サーバーリストを取得
        code, message, upload_server_list, server_json = get_upload_servers("")

        if code != 200:
            body['message'] = message
            return resp_data(code, domain, body)

        print("upload_target")
        print(upload_server_list)

        # ランダムに選択し決める
        list = upload_server_list.split(',')
        upload_service = random.choice(list)

        # debug upload
        if fb.get_logic_ip_check(event):
            print("logic_ip:" + fb.get_ip_address(event))
            for upload_server_name in server_json:
                if 'debug' in server_json[upload_server_name]:
                    if server_json[upload_server_name]['debug'] == True:
                        print("debug hit:" + upload_server_name)
                        upload_service = upload_server_name
                        break

        print("upload_target:" + upload_service)

        # 設定を取得
        upload_setting = server_json[upload_service]['setting']

    # アップロードタイトル誰用
    if len(upload_title) > 30:
        upload_title = ""
    else:
        upload_title = fb.file_name_escape(upload_title)
    body['upload_title'] = upload_title

    # 何が用
    print("add_title:" + add_title)
    if len(add_title) > 30:
        add_title = ""
    else:
        add_title = fb.file_name_escape(add_title)
    print("add_title:" + add_title)
    body['add_title'] = add_title

    # 追加コメント
    add_comment = fb.file_name_escape(add_comment)
    print("add_comment:" + add_comment)
    if len(add_comment) > 1000:
        add_comment = add_comment[0:1000]
    print("add_comment:" + add_comment)
    body['add_comment'] = add_comment

    # authから引っ張る
    code, config, message = ml.get_auth_config(rc, group, auth)
    if code != 200:
        body['message'] = message
        return resp_data(code, domain, body)

    # 正しいkey
    key    = config['key']

    # 名前が入る
    config['upload_org_service'] = upload_service

    # list
    upload_service_list = []
    upload_service_list.append(upload_service)
    config['upload_service'] = upload_service_list
    # 設定が入る
    upload_settings = {}
    upload_settings[upload_service] = upload_setting
    config['upload_setting'] = upload_settings
    print("config")
    print(config)

    user_data = {}
    user_data['upload_title'] = upload_title
    user_data['add_title'] = add_title
    config['user_data'] = user_data

    unix_time = fb.get_unixtime()
    config['create_time'] = unix_time
    config['create_date'] = fb.datetime_str(unix_time, 'yyyy-mm-dd hh:mm:ss')

    if group == 'okurumini':
        exp = 0
        # 0は期限なし
        if len(expire) > 0:
            exp = int(expire)
    else:
        exp = int(config['setting']['expire'])

    if exp < 100:
        # 100以下は日
        unix_time = unix_time + (exp * 86400)
    else:
        # 100以上は時間
        unix_time = unix_time + (exp * 60)

    config['download_limit'] = fb.datetime_str(unix_time, 'yyyy-mm-dd hh:mm:ss')
    config['download_limit_time'] = unix_time

    s3_data = ""

    if asp_code == 201:

        s3_data           = upload_setting.copy()
        s3_data['bucket'] = 'firestorage-index'

        bucket_name  = upload_setting['bucket']
        endpoint_url = upload_setting['aws_host']
        region       = upload_setting['region']
        access_key   = upload_setting['aws_access_key_id']
        secret_key   = upload_setting['aws_secret_access_key']

    else:
        # s3設定データを取得
        code, endpoint_url, region, bucket_name, access_key, secret_key = fl.get_s3_config("index")
        s3_data = bucket_name

    # ぐるぐるさせて発行にチャレンジ
    for num in range(5):

        # 認証を作成
        code1, dir        = fb.get_rand_str_expir(rc, "0-9a-zA-Z",  8, 86400)
        code2, upload_dir = fb.get_rand_str_expir(rc, "0-9a-zA-Z", 32, 86400)

        # 存在する？
        code3 = ss.s3_simple_head(s3_data, group + '/' + key + "/" + dir + "/config.json")
        code4 = ss.s3_simple_head(s3_data, group + '/download/' + upload_dir + "/config.json")

        if code3 == 403 or code4 == 403 or code3 == 400 or code4 == 400:
            print("code3:" + str(code3))
            print("code4:" + str(code4))
            break

        if code1 == 200 and code2 == 200 and code3 == 404 and code4 == 404:

            body['download_url'] = "https://" + domain + "/key/" + group + "/" + key + "/" + dir

            if asp_code != 201:
                # xfsを取り出す
                code, xfs_key = fb.get_xfs_key(event, body['download_url'])
                print("xfs:" + str(code) + ":" + xfs_key)
                if code == 200:
                    log_data = {}
                    log_data['xfs_key'] = "https://xfs.jp/" + xfs_key
                    log_data['key'] = key
                    log_data['dir'] = dir
                    log_data['url'] = body['download_url']
    ###             code = fb.access_text_log(event, group + "_xfs", log_data, "date,ip,act,key,dir,xfs_key,url")
                    config['xfs_key'] = xfs_key
                    config['xfs_url'] = "https://xfs.jp/" + xfs_key
                    body['xfs_url'] = config['xfs_url']

            # コメント
            if len(add_comment) > 0:
                comment = config['setting']['comment']
                config['setting']['comment'] = comment + add_comment

            # パスワード
            if len(download_pass) > 0:
                config['setting']['download_pass'] = download_pass
            else:
                config['setting']['download_pass'] = ""

            if group == 'okurumini':
                # オクルミニだけ書き換え
                config['setting']['expire'] = expire

            config['dir'] = dir
            config['upload_dir'] = upload_dir
            body['upload_dir'] = upload_dir

            text = fb.make_json_data(config)

            # s3に新しく保存
            code = ss.s3_simple_put(s3_data, group + '/' + key + "/" + dir + "/config.json" , text)
            if code != 200:
                body['message'] = "create config.json error"
                return resp_data(400, domain, body)

            # upload dir を作成
            data = {}
            data['create_time'] = fb.get_unixtime()
            data['group'] = group
            data['key'] = key
            data['dir'] = dir
            data['upload_dir'] = upload_dir

            json_data = fb.make_json_data(data)
            code = ss.s3_simple_put(s3_data, group + '/download/' + upload_dir + "/config.json" , json_data)
            if code != 200:
                body['message'] = "create upload_dir index.json error"
                return resp_data(400, domain, body)

            # redisに保存(アップロードの時に使う)
            text = json.dumps(config)
            rc.set(group + ":" + auth + ":" + dir, text, ex=86400)
            body['save'] = group + ":" + auth + ":" + dir

            # index.json用アップロードURLを作成
            upload_file = group + "/download/" + upload_dir + "/index.json"
            s3_client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=endpoint_url, region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            url = s3_client.generate_presigned_url('put_object', Params={'Bucket': 'firestorage-index', 'Key': upload_file }, ExpiresIn=600)

            body['upload_thumbnail'] = url
            body['file'] = dir

            # upload_dirを保存 redisのみ
            u = {}
            if 'upload_tmp' in config:
                u = config['upload_tmp']
            u[dir] = upload_dir
            config['upload_tmp'] = u
            text = json.dumps(config)
            redis_key = group + ":" + auth

            print("redis_set_config")
            print(text)

            rc.set(redis_key, text, ex=86400)

            # 削除ファイルをアップロード
            if asp_code == 201:
                delete_json_upload('firestorage-index-asp', asp_name, group, dir, key, exp, True)
            else:
                delete_json_upload('firestorage-index',           "", group, dir, key, exp, False)

            return resp_data(200, domain, body)

    body['message'] = "dir create error"
    return resp_data(400, domain, body)

def upload_sendmail(event, rc, domain, form_data):

    code = 200
    body = {}
    data = {}
    config = {}
    file_list = ""

    user_key = form_data['key']
    auth  = form_data['auth']
    dir   = form_data['dir']
    msg   = form_data['message']
    group = form_data['group']

    mail_key = group + '_uploaded'
    bucket = 'firestorage-index'
    mail_list = {}

    # authから引っ張る
    code, config, message = ml.get_auth_config(rc, group, auth + ":" + dir)
    if code != 200:
        body['message'] = message
        return resp_data(code, domain, body)

    # 正しいkey
    key = config['key']

    # 二重防止
    text = rc.get(group+ ":sendmail:" + auth + ":" + key + ":" + dir)
    if text is not None:
       body['message'] = "sendmail exist"
       return resp_data(400, domain, body)

    # 二重防止
    rc.set(group + ":sendmail:" + auth + ":" + key + ":" + dir , 1, ex=3600)

    # 送った内容
    if fb.is_json(msg):
        mail_list = json.loads(msg)
    else:
        body['message'] = "ge upload item json error"
        return resp_data(400, domain, body)

    # 管理画面通知
#    monitor_sqs(rc, event, group + '_mail', key, dir, len(mail_list))

#   ファイル削除jsonを設置
    s3_config = {}
    url_exp = int(config['setting']['expire'])
#   delete_json_upload(s3_config, group, key, url_exp, True)

    # 管理画面用、新規アップロードがわかる様に
    rc.set(group + ":upload:" + key + ":" + dir , fb.get_unixtime(), ex=86400)

#    if group == 'okurumini':
#        body['code']  = code
#        body['message']  = "OK"
#        return resp_data(code, domain, body)

    # LINEのアクセストークンあるか？
    if 'line_token' in config:
        if len(config['line_token']) > 0:
            data = {}
            data['act'] = group + "_send"
            data['key'] = user_key
            data['auth'] = auth
            data['dir'] = dir
            data['domain'] = domain
            data['group'] = group

            if 'xfs_key' in config:
                data['xfs_url'] = 'https://xfs.jp/' + config['xfs_key']

            # sqsを送る
            code = fb.put_sqs_data('https://sqs.ap-northeast-1.amazonaws.com/695355811576/firestorage-line-notfy', data)
            print("sqs:" + str(code))
            body['line'] = 'send'
        else:
            body['line'] = 'no data send'

    else:
        body['line'] = 'no send'

    # メール通知どう？
    if 'guest_upload_mail' in config['setting']:
        guest_upload_mail = config['setting']['guest_upload_mail']
        if guest_upload_mail['active'] == False:
            body['message'] = "no active mail / no send"
            return resp_data(200, domain, body)
        else:
            if len(guest_upload_mail['mail_address']) > 0:
                data['to'] = guest_upload_mail['mail_address']
            else:
                body['message'] = "mailaddress no setting / no send"
                return resp_data(200, domain, body)
        body['message'] = "send"
    else:
        #設定なし
        body['message'] = "mail no send"
        return resp_data(200, domain, body)

    email_type = 'text'
    if 'email_type' in config['setting']:
        email_type = config['setting']['email_type']

    #    {'add_title': 'まっちゃん', 'upload_title': ''}
    add_title = ''
    upload_title = 'なし'
    if 'upload_title' in config['user_data']:
        upload_title = config['user_data']['upload_title']
        if upload_title == '':
            upload_title = 'なし'

    if 'add_title' in config['user_data']:
        add_title = config['user_data']['add_title']

    print("add_title:" + add_title)
    print("upload_title:" + upload_title)

    # 送ったファイルを解析
    for item in mail_list:
        name = fb.uri_decode(item['name'])
        name = add_title + fb.file_name_escape(name)

        size = fb.convert_size(item['size'])
        if len(file_list) > 0:
            file_list += "\n" + name + "\t" + size
        else:
            file_list = name + "\t" + size
        # textありか？
        if 'text' in item:
            text = fb.file_name_escape(item['text'])
            file_list += "\n内容\n" + text

    data['upload_url']   = 'https://' + domain + '/key/' + group + '/' + key
    data['download_url'] = 'https://' + domain + '/key/' + group + '/' + key + "/" + dir

    if 'xfs_key' in config:
        data['download_url'] = 'https://xfs.jp/' + config['xfs_key']

    if 'xfs_upload' in config:
        data['xfs_upload'] = config['xfs_upload']

    data['file_list']    = file_list
    data['upload_title'] = upload_title

    code, text = sendmail_template(event, rc, mail_key, data, email_type)

    body['code']  = code
    body['sendmail_template']  = mail_key

    return resp_data(code, domain, body)

def delete_json_upload(bucket, asp_name, group, dir, key, url_exp, no_move : bool):

    # uketorumini_move.json       分散化をする
    # uketorumini_org_delete.json オリジナルアップロードだけを消す
    # uketorumini_delete.json     アップロードとindex.json/conifg.json全て消す

    if url_exp == 0:
        # 0は無期限
        url_exp = 0
    elif url_exp > 100:
        # 100以上は分なので1日
        url_exp = 1

    if len(asp_name) > 0:
        group_name = asp_name + '/' + group
    else:
        group_name = group

    delete_key = group_name + '/' + key + "/" + dir + "/" + group + "_delete.json"
    move_key   = group_name + '/' + key + "/" + dir + "/" + group + "_move.json"
    org_delete = group_name + '/' + key + "/" + dir + "/" + group + "_org_delete.json"

    if url_exp == 0:
        # 無期限
        # 1日経過したらmoveし分散化
        if no_move == False:
            code = fb.s3_simple_put(bucket, move_key, json.dumps({}))
            code = fb.s3_simple_set_tag(bucket, move_key, 'expire', "1")
            # もう1日経過したらオリジナルを消す
            code = fb.s3_simple_put(bucket, org_delete, json.dumps({}))
            code = fb.s3_simple_set_tag(bucket, org_delete, 'expire', "2")
    elif url_exp > 2:
        # 2日以上は分散化をする
        code = fb.s3_simple_put(bucket, delete_key, json.dumps({}))
        code = fb.s3_simple_set_tag(bucket, delete_key, 'expire', str(url_exp))
        # 1日経過したらmoveし分散化
        if no_move == False:
          code = fb.s3_simple_put(bucket, move_key, json.dumps({}))
          code = fb.s3_simple_set_tag(bucket, move_key, 'expire', "1")
          # もう1日経過したらオリジナルを消す
          code = fb.s3_simple_put(bucket, org_delete, json.dumps({}))
          code = fb.s3_simple_set_tag(bucket, org_delete, 'expire', "2")
    else:
        # 1から2日は分散化せずに消す
        code = fb.s3_simple_put(bucket, delete_key, json.dumps({}))
        code = fb.s3_simple_set_tag(bucket, delete_key, 'expire', str(url_exp))

def sendmail_template(event, rc, key, data, email_type):

    code = 200
    stage = fb.get_api_stage_alias(event)

    # メール送信、テンプレートを取得
    json_data = fb.get_setting_data_stage(rc, stage, "mail", key)

    if 'body' not in json_data:
        #ないぞ
        return 404, key + "body not found"

    text = json_data['body']
    # HTML5 名前付き文字参照を戻す
    text = fb.html_entities_decode(text)

    if 'to' not in data:
        return 403, "data to or from not found"
    if 'reply' not in data:
        data['reply'] = ""
    if 'from' not in data:
        data['from'] =  "info@firestorage.jp"
    if 'returns' not in data:
        data['returns'] = "info@firestorage.jp"

    # 置換
    for item in data:
        text = text.replace("<" + item + ">", data[item])

    # 外部置換
    match = re.findall(r'(#[^\s]+)', text)
    for item in match:
        get_key = item.replace("#", "")
        json = fb.get_setting_data_stage(rc, stage, "mail", get_key)
        if 'body' in json:
            print(get_key + ":OK")
            tag = json['body']
            # HTML5 名前付き文字参照を戻す
            tag = fb.html_entities_decode(tag)
            text = text.replace(item, tag)
        else:
            print(get_key + ":not found")

    # テキストを分割、１行目が題名
    list = text.split("\n")
    index = 0
    subject = ""
    bodys = ""
    for item in list:
        if index == 0:
            subject = item
        else:
            bodys += item + "\n"
        index += 1

    mail_data = {}
    mail_data['to']      = data['to']
    mail_data['from']    = data['from']
    mail_data['subject'] = subject
    mail_data['message'] = bodys
    mail_data['reply']   = data['reply']
    mail_data['returns'] = data['returns']

    mail_sqs = os.environ['mail_sqs']
    stage = fb.get_api_stage_alias(event)
    if 'mail_sqs' + "_" + stage in os.environ:
        mail_sqs = os.environ['mail_sqs' + "_" + stage]

    print(mail_sqs)

    if email_type == 'text' or email_type == '':
        # メールを送る
        code = ml.send_text_mail(mail_sqs, mail_data)
    else:
        code = ml.html_mail(rc, stage, mail_sqs, mail_data)

    return code, "OK"

def part_file(rc, domain, form_data):

    body  = {}
    auth  = form_data['auth']
    key   = form_data['key']
    name  = form_data['name']
    dir   = form_data['dir']
    file  = form_data['file']
    group = form_data['group']

    if name.find('/', 0) > 0:
        body['message'] = "input file name error"
        return resp_data(400, domain, body)

    if len(name)> 1024:
        body['message'] = "input file name length error"
        return resp_data(400, domain, body)

    code, config, message = ml.get_auth_config(rc, group, auth)
    if code != 200:
        body['message'] = message
        return resp_data(code, domain, body)

    # upload_idを取得
    json_text =rc.get(group + ":upload_id:" + auth + ":" + key + ":" + dir)

    if fb.is_json(json_text):
        upload_data = json.loads(json_text)
    else:
        body['message'] = "load json error"
        return resp_data(400, domain, body)

    upload_id  = upload_data['upload_id']
    upload_dir = upload_data['upload_dir']
    upload_service = config['upload_service']
    if upload_service is None:
        body['message'] = "no upload_service"
        return resp_data(400, domain, body)

    # proxy dir を取得
    upload_org_service = config['upload_org_service']
    proxy_dir      = upload_org_service
    upload_setting = config['upload_setting'][upload_org_service]
    bucket_name  = upload_setting['bucket']
    endpoint_url = upload_setting['aws_host']
    region       = upload_setting['region']
    access_key   = upload_setting['aws_access_key_id']
    secret_key   = upload_setting['aws_secret_access_key']

    if bucket_name is None or bucket_name == '':
        body['message'] = "s3 config error:" + upload_service
        return resp_data(400, domain, body)

    decode_name = fb.uri_decode(name)
    upload_file = group + "/" + upload_dir + "/contents/" + dir + "/" + decode_name

    url_ = endpoint_url +                             "/" + bucket_name + "/" + upload_file + "?uploadId=" + upload_id
    url = os.environ['proxy_url'] + "/" + proxy_dir + "/" + bucket_name + "/" + upload_file + "?uploadId=" + upload_id

    headers = fb.create_aws_v4(url_, "GET", region, access_key, secret_key, endpoint_url)

    r = requests.get(url, headers=headers, timeout=(3.0, 5.0) )
    print(r.request.headers)
    code = r.status_code
    xml_data = r.text

    print("code:" + str(code))
    print(xml_data)
    root = ET.fromstring(xml_data)

    body['xml_resp'] = xml_data

    send_xml = ""
    etag = []
    index = 0
    # 子ノードを読み込む
    for child1 in root:
#          print("--- member infomation")
#          print(f"{child1.tag}: {child1.attrib}")
        for child2 in child1:
            if child2.tag.find('ETag') > 0:
                tag = child2.text
                tag = re.sub(r"[^0-9a-zA-Z]", "", tag)
                index = index + 1
                send_xml += "<Part>\n"
                send_xml += "  <PartNumber>" + str(index) + "</PartNumber>\n"
                send_xml += "  <ETag>" + tag + "</ETag>\n"
                send_xml += "</Part>\n"
                etag.append(tag)


    xml_data  = "<CompleteMultipartUpload>\n"
    xml_data += send_xml
    xml_data += "</CompleteMultipartUpload>"

    body['xml_data'] = xml_data
    body['ETag'] = etag
    body['name'] = name

    # debug 消す
    body['upload_file'] = upload_file

    xml_data = xml_data.encode("utf-8")

    headers = fb.create_aws_v4(url_, "POST", region, access_key, secret_key, endpoint_url)

    try:
        r = requests.post(url, timeout=(3.0, 5.0), headers=headers, data=xml_data)
        body['post'] = r.status_code
        code = r.status_code
        text = r.text
        print(text)
    except Timeout:
        code = 501

    if code == 200 or code == 201:
        body['url'] = url_
        body['upload_id'] = upload_id
        body['message'] = "OK"
        return resp_data(200, domain, body)
    else:
        body['url'] = url_
        body['upload_id'] = upload_id
        body['message'] = "api get error:" + str(code)
        return resp_data(400, domain, body)

def get_sign_upload_url(rc, domain, form_data):

    body  = {}
    key   = form_data['key']
    auth  = form_data['auth']
    name  = form_data['name']
    dir   = form_data['dir']
    count = form_data['count']
    file  = form_data['file']
    group = form_data['group']

    if name.find('/', 0) > 0:
        body['message'] = "input file name error"
        return resp_data(400, domain, body)

    if len(name)> 1024:
        body['message'] = "input file name length error"
        return resp_data(400, domain, body)

    # authから引っ張る
    code, config, message = ml.get_auth_config(rc, group, auth)
    if code != 200:
        body['message'] = message + ":" + auth
        return resp_data(code, domain, body)

    print(config)

    if 'upload_tmp' not in config:
        body['message'] = "upload_tmp no data"
        return resp_data(code, domain, body)

    if file not in config['upload_tmp']:
        body['message'] = "upload_tmp/file no data"
        return resp_data(code, domain, body)

    upload_dir     = config['upload_tmp'][file]

    print("upload_dir:" + upload_dir)

    upload_service = config['upload_service']
    if upload_service is None:
        body['message'] = "no upload_service"
        return resp_data(400, domain, body)

    # proxy dir を取得
    proxy_dir = upload_service[0]
    # uploadの場所と設定
    upload_setting = config['upload_setting'][upload_service[0]]
    bucket_name    = upload_setting['bucket']
    endpoint_url   = upload_setting['aws_host']
    region         = upload_setting['region']
    access_key     = upload_setting['aws_access_key_id']
    secret_key     = upload_setting['aws_secret_access_key']

    print("upload_setting")
    print(upload_setting)

    if bucket_name is None or bucket_name == '':
        body['message'] = "get_s3_config error:" + upload_service
        return resp_data(400, domain, body)

    decode_name = fb.uri_decode(name)
    upload_file = group + "/" + upload_dir + "/contents/" + dir + "/" + decode_name

    print("upload_file")
    print(upload_file)

    url = ""
    if int(count) == 1:
        code, url, upload_id = s3_simple_multi_upload_init(bucket_name, upload_file, endpoint_url, region, access_key, secret_key, proxy_dir)
        print("s3_simple_multi_upload_init:" + str(code))
        body['upload_id'] = upload_id
        if code != 200:
            body['message'] = "s3_simple_multi_upload_init error:" + str(code)
            return resp_data(400, domain, body)
        upload_data = {}
        upload_data['upload_id']  = upload_id
        upload_data['upload_dir'] = upload_dir
        upload_data['count'] = 1
        json_text = json.dumps(upload_data)
        print(json_text)
        rc.set(group + ":upload_id:" + auth + ":" + key + ":" + dir , json_text, ex=86400)
    elif int(count) > 1:
        # 保存しているデータを取得
        json_text =rc.get(group + ":upload_id:" + auth + ":" + key + ":" + dir)
        upload_data = json.loads(json_text)
        upload_data['count'] = count
        upload_id = upload_data['upload_id']

        if len(upload_id) < 3 :
            body['message'] = "upload_id length error:" + str(code)
            return resp_data(400, domain, body)

        s3_client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=endpoint_url, region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        url = s3_client.generate_presigned_url('upload_part', Params={'Bucket': bucket_name, 'Key': upload_file, 'UploadId': upload_id, 'PartNumber' : int(count)}, ExpiresIn=1800)

        json_text = json.dumps(upload_data)
        rc.set(group + ":upload_id:" + auth + ":" + key + ":" + dir, json_text, ex=86400)
        print("upload_id:count:" + str(count))
        body['upload_id'] = upload_id

    else:
        s3_client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=endpoint_url, region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        url = s3_client.generate_presigned_url('put_object', Params={'Bucket': bucket_name, 'Key': upload_file }, ExpiresIn=1800)

    body['upload_file'] = upload_file
    body['url'] = url
    body['name'] = name
    body['message'] = "OK"
    body['form'] = form_data

    return resp_data(200, domain, body)

def s3_quote_plus(upload_file):
    upload_file = urllib.parse.quote(upload_file)
    upload_file = upload_file.replace("+", '%2B')
    return upload_file

def get_upload_servers(user_target_server):

    global _server_data
    global _server_data_time
    global _cloud_data
    global _cloud_data_time

    code = 200
    message = "OK"
    server_json = {}
    upload_server_list = ""
    server_cache_time = 120

    if user_target_server:

        # ダウンロードサーバーを探す
        if _cloud_data is not None:
            if _cloud_data_time < fb.get_unixtime():
                print("get_upload_servers cache clear")
                _cloud_data = None

        # 全ての server
        if _cloud_data is None:
            print("all server get s3 data")
            # サーバーデータ存在する？
            code ,cloud_json = fb.s3_simple_get("firestorage-index", 'setting/server/config.json')
            if code != 200:
                print("all server data get error")
                message = "all server data get error"
                return code, message, "", server_json
            _cloud_data = cloud_json
            _cloud_data_time = fb.get_unixtime() + server_cache_time
        else:
            print("get_upload_servers cache hit")

        server_json = json.loads(_cloud_data)

        # upload可能なサーバーを抽出
        if user_target_server in server_json:
            return 200, message, user_target_server, server_json
        else:
            return 400, "upload_service not found", "", server_json

    else:

        if _server_data is not None:
            if _server_data_time < fb.get_unixtime():
                print("get_upload_servers cache clear")
                _server_data = None

        if _server_data is None:
            print("get_upload_servers get s3 data")
            # アップロードサーバーデータ存在する？
            code ,upload_json = fb.s3_simple_get("firestorage-index", 'setting/server/sub.json')
            if code != 200:
                message = "upload server data get error"
                return code, message, "", server_json
            _server_data = upload_json
            _server_data_time = fb.get_unixtime() + server_cache_time
        else:
            print("get_upload_servers cache hit")

        server_json = json.loads(_server_data)

        # upload可能なサーバーを全て抽出
        for target_server in server_json.keys():
            if server_json[target_server]['upload'] == True:
                if len(upload_server_list) > 0:
                    upload_server_list += "," + target_server
                else:
                    upload_server_list = target_server

        if upload_server_list is None:
            code = 400
            message =  "upload_service not found"

        return code, message, upload_server_list, server_json

def s3_simple_multi_upload_init(bucket_name, upload_file, endpoint_url, region, access_key, secret_key, proxy_dir):

    print("endpoint_url:" + endpoint_url)
    print("region_name:" + region)
    print("upload_file:" + upload_file)

    ### 1.APIエンドポイントURL
    url_ = endpoint_url + "/" + bucket_name + "/" + upload_file + "?uploads"

    ### 2. Credential生成
    credentials = Credentials(access_key, secret_key)

    ### 3. AWSRequest生成
    request = AWSRequest(method="POST", url=url_)

    ### 4. AWSリクエスト署名
    SigV4Auth(credentials, 's3', region).add_auth(request)

    Host = endpoint_url.replace("https://", "")

    ### 5. API発行
    headers = {
        'Authorization': request.headers['Authorization'],
        'Host':Host,
        'X-Amz-Date':request.context['timestamp']
    }

    print(url_)
    print(headers)

    proxy_url = os.environ['proxy_url']

    url = proxy_url + "/" + proxy_dir + "/" + bucket_name  + "/" + upload_file + "?uploads"

    print("prpxy:" + url)

    try:
        r = requests.post(url, timeout=(3.0, 5.0), headers=headers, data="")
        code = r.status_code

        if code == 200 or code == 201:

            xml_data = r.text
            print(xml_data)
            root = ET.fromstring(xml_data)
            print(root)

            upload_id = ""

            for child in root:
                print(child.tag,child.text)
                if child.tag.find('UploadId') > 0:
                    upload_id = child.text

            print("upload_id:" + upload_id)

            s3_client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=endpoint_url, region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            url = s3_client.generate_presigned_url('upload_part', Params={'Bucket': bucket_name, 'Key': upload_file, 'UploadId': upload_id, 'PartNumber' : 1}, ExpiresIn=600)

            return code, url, upload_id

        else:
            return code, "" ,""
    except Timeout:
         return 501, "", ""


def resp_data(code, domain, message):

    body = {}
    body['code'] = code
    body['data'] = message
    body['version'] = "2.0"

    if domain != '*':
        domain = "https://" + domain

    return {
        'statusCode': 200,
        'headers' : {
          "Access-Control-Allow-Origin":  domain,
        },
        'body': json.dumps(body)
    }
