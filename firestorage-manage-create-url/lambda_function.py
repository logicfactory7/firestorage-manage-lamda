import os
import json
import re
import random
import string
import time
import datetime
import redis
import fire_basic as fb
import xfs_lib
import requests
import logic_memcached as lm
import s3_simple as ss

# pip3 install redis -t .
# pip3 install requests -t .

def lambda_handler(event, context):

    domain = 'https://firestorage.jp'
    bucket = 'firestorage-index'
    log_bucket = 'firestorage-data'

    # 未登録は16　
    # 36^14 =     6,140,942,214,464,815,497,216
    # 36^16 = 7,958,661,109,946,400,884,391,936
    key_length = 14
    ip_address = fb.get_ip_address(event)

    body = {}
    mem_key = "create_mini:count:" + ip_address

    rc = fb.redis_connect()
    count = rc.get(mem_key)

    if count is None:
        rc.set(mem_key, 1, ex=600, nx=True)
        count = 1
    else:
        if int(count) >= 100:
            return rest_resp(401, 'create limit error', '*')
        count = rc.incr(mem_key)

    validation_parm = [
        {'key': 'domain', 'max': 50, 'min': 3, 'type': "a-z0-9." , "req" : True},
        {'key': 'type'  , 'max': 5 , 'min': 1, 'type': "a-z0-9_" , "req" : True},
        {'key': 'group' , 'max': 15, 'min': 1, 'type': "a-z0-9_" , "req" : False},
        {'key': 'eid'   , 'max': 40, 'min': 32,'type': "a-f0-9"  , "req" : False},
        {'key': 'count' , 'max': 3,  'min': 1, 'type': "0-9"     , "req" : False},
    ]

    response = event.get('queryStringParameters')

    code , form_data , error_data = fb.validation_deco(response, validation_parm)

    if code != 200:
        print(error_data)
        return rest_resp(404, "validation error", "*")

    domain         = form_data['domain']
    body['domain'] = form_data['domain']
    type           = form_data['type']
    group          = form_data['group']

    if fb.domain_check(body['domain']):
        return rest_resp(404, "domain error", "*")

    if len(group) == 0:
        group = 'uketorumini'

    if group == 'uketorumini' or group == 'okurumini' or group == 'okuru' or group == 'uketoru':
        pass
    else:
        return rest_resp(400, "group error", domain)

    org_key = group + "/org/index.json"

    # 日本時間にセット
    dt_now_jst_aware = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=9))
    )

    # eidチェック いまはログイン会員のみ
    code, member_data = lm.check_eid(rc, form_data['eid'])
    if code != 200:
        return rest_resp(400, "member error", domain)

    # コピー元
    code, json_text2 = fb.s3_simple_get(bucket, org_key)
    if code!= 200:
       return rest_resp(code, org_key + ' not found', domain)
    json_data2 = json.loads(json_text2)

    json_data2['user_id'] = member_data['id']
    json_data2['asp'] = member_data['asp']
    json_data2['ip_address'] = ip_address
    json_data2['create_time'] = int(time.time())
    json_data2['create_date'] = dt_now_jst_aware.strftime('%Y-%m-%d %H:%M:%S')
    json_data2['upload_service'] = ""
    json_data2['group'] = group

    asp_code, asp_name = fb.asp_domain_check(domain, member_data)

    s3_setting = bucket

    if asp_code == 201:

        code ,cloud_json = fb.s3_simple_get("firestorage-index", 'setting/asp/service/' + asp_name + '/config.json')

        if code != 200:
            print("asp server data get error:" + str(code))
            message = "asp server data get error:"+ str(code)
            return rest_resp(404, message, domain)

        server_json = json.loads(cloud_json)
        s3_setting = server_json['setting']
        s3_setting['bucket'] = bucket # 'firestorage-index' へ変更

    for num in range(7):

        code1, randlst = fb.get_rand_str_expir(rc, "0-9a-zA-Z", key_length, 86400)
        code2 = ss.s3_simple_head(s3_setting, group + '/' + randlst  + '/index.json')

        body['code'] = code2

        print("code1:" + str(code1) + ":" + randlst)
        print("code2:" + str(code2))

        if code1 == 200 and code2 == 404:
            json_data2['key'] = randlst
            body['group_key'] = group
            body['key'] = randlst
            body['url'] = 'https://' + domain + '/key/' + group + '/' + randlst

            code, xfs_url= xfs_lib.create_xfs_url(rc, body['url'], "", 0, 0, ip_address)
            if code == 200:
                body['url'] = xfs_url
                json_data2['xfs_upload'] = xfs_url

            # 管理画面で24Hだけ分かる様に
            unix_time = fb.get_unixtime()
            rc.set(group + ":create:" + randlst , fb.get_unixtime() , ex=86400)

            if type == 'who':
                # 誰？
                json_data2['setting']['upload_title'] = 1
            elif type =='what':
                json_data2['setting']['add_title'] = 1
                json_data2['setting']['add_title_value'] = ""
            elif type =='0':
                pass
            else:
                return rest_resp(400, "type error", domain)

            json_text = json.dumps(json_data2, indent=2)

            if member_data['id'] > 0:
                count = 0
                if len(form_data['count']) > 0:
                    count = int(form_data['count'])
                date_time = 9999999999 - unix_time
                user_dir = group + '/user/' + str(member_data['id']) + "/" + str(date_time) + "/" + str(count) + "/" + randlst + '/index.json'
                json_data2['user_dir'] = user_dir
                json_text = json.dumps(json_data2, indent=2)
                code = ss.s3_simple_put(s3_setting, user_dir, json_text)

            code = ss.s3_simple_put(s3_setting, group + '/' + randlst + '/index.json', json_text)
            body['code'] = code

            # log
            yyyymmdd = fb.datetime_str(unix_time, 'yyyymmdd')

            log = {}
            log['ip_address'] = ip_address
            log['create_time'] = int(time.time())
            log['create_date'] = dt_now_jst_aware.strftime('%Y-%m-%d %H:%M:%S')
            text = json.dumps(log, indent=2)

            # 作成記録
#           code = fb.s3_simple_put(log_bucket, group + '/create/' + yyyymmdd  + '/' + randlst + '/index.json', text)

            if group == 'okurumini' or group == 'okuru' or group == 'uketoru':
                # オクルはauthを作成
                auth_len = 26
                # 認証を作成
                code, auth = fb.get_rand_str_expir(rc, "0-9a-zA-Z", auth_len, 86400 * 3)
                auth_time = 86400
                rc.set(group + ":" + auth, json_text, ex=auth_time)
                body['code'] = code
                body['auth'] = auth

            break

    else:
        body['code'] = 400

    body['create_count'] = count

    if domain != '*':
        domain = "https://" + domain

    return {
        'statusCode': code,
        'headers' : {
          "Access-Control-Allow-Origin": domain,
        },
        'body': json.dumps(body, indent=2)
    }


def rest_resp(code, message, domain):
    body = {}
    body['code'] = code
    body['message'] = message

    if domain != '*':
        domain = "https://" + domain

    return {
        'statusCode': 200,
        'headers' : {
          "Access-Control-Allow-Origin": domain,
        },
        'body': json.dumps(body, indent=2)
    }


