import json
import re
import fire_basic  as fb
import manage as mg
import logic_memcached as lm
import s3_simple as ss

# pip3 install redis -t .
# pip3 install requests -t .
# pip3 install xmltodict -t .
# pip3 install python-dateutil -t .

def lambda_handler(event, context):
    # TODO implement

    # テンプレートを出力
    domain = ''
    body = {}
    server_config = {}

    validation_parm = [
        {'key': 'domain', 'max': 50, 'min': 3,  'type': "a-z0-9."   , "req" : True},
        {'key': 'act',    'max': 32, 'min': 4,  'type': "a-z0-9_"   , "req" : True},
        {'key': 'eid',    'max': 40, 'min': 32, 'type': "a-f0-9"    , "req" : False},
        {'key': 'group',  'max': 4,  'min': 8,  'type': "a-z0-9"    , "req" : False},
        {'key': 'key',    'max': 8,  'min': 16, 'type': "a-zA-Z0-9" , "req" : False},
        {'key': 'auth',   'max': 128,'min': 16, 'type': "*"         , "req" : False},
    ]

    response = event.get('queryStringParameters')
    code , form_data , error_data = fb.validation_deco(response, validation_parm)

    print(form_data)

    if code != 200:
        print(error_data)
        return resp_data(404, "*", error_data)

    # domain許可チェック
    domain = form_data['domain']
    if fb.domain_check(domain):
        return resp_data(404, "*", "domain error")

    # eidなし
    if form_data['eid'] == '':
        body['url'] = mg.get_asp_login_url(domain)
        resp_data(code, domain, body)

    # group check
    if form_data['group'] == 'okuru' or form_data['group'] == 'uketoru':
        pass
    else:
        resp_data(code, domain, "group error")

    rc = fb.redis_connect()

    # eidチェック いまはログイン会員のみ
    code, member_data = lm.check_eid(rc, form_data['eid'])
    if code != 200:
        return resp_data(400, domain, "member error")

    asp_code, asp_name = fb.asp_domain_check(domain, member_data)

    if asp_code == 201:
        code ,cloud_json = fb.s3_simple_get("firestorage-index", 'setting/asp/service/' + asp_name + '/config.json')
        if code != 200:
            return resp_data(400, domain, "firestorage-index get error")
        config = json.loads(cloud_json)
        server_config = config['setting']
    else:
        server_config = "firestorage-index"

    if form_data['act'] == 'list':
        # 一覧
        return upload_dir_list(domain, form_data['group'], member_data, server_config)
    elif form_data['act'] == 'view':
        # 詳細
        return upload_dir_view(domain, form_data['group'], form_data['key'], form_data['auth'], member_data, server_config)
    elif form_data['act'] == 'delete_key':
        # 詳細
        return upload_delete_key(rc, domain, form_data['group'], form_data['key'], form_data['auth'], asp_code, server_config, member_data, asp_name)
    else:
        return resp_data(404, domain, "act error")

def upload_delete_key(rc, domain, group, key, auth, asp_code, server_config, member_data, asp_name):

    body = {}

    s3_data           = server_config.copy()
    s3_data['bucket'] = 'firestorage-index'

    # 認証 group/user_id/count/key
    code, msg = check_user_file_auth(s3_data, auth, member_data)
    if code != 200:
        return resp_data(400, domain, msg)

    prefix = '/' + group + "/" + key + "/"
    code, data = ss.bucket_list(s3_data, prefix, '', '', 1000)
    if code != 200:
        return resp_data(400, domain, 'bucket list error')

    # dirリストを引っ張る
    list = []
    for item in data['list']:
        l = item['key'].split('/')
        if l[len(l) - 1] == 'config.json':
            item_list = {}
            dir = l[len(l) - 2]
            if asp_code == 201:
                item_list['key'] = asp_name + '/' + group + '/' + key + '/' + dir + '/' + group + '_delete.json'
                code = fb.s3_simple_put('firestorage-index-asp', item_list['key'] , '{}')
                item_list['code'] = code
                code = fb.s3_simple_delete_single('firestorage-index-asp', item_list['key'])
                item_list['delete_code'] = code
            else:
                pass
#               code = fb.s3_simple_put('firestorage-index', group + '/' + dir + 'index.json', '{}')
            list.append(item_list)

    body['dir_code'] = code
    body['dir'] = list

    return resp_data(200, domain, body)

def upload_dir_view(domain, group, key, auth, member_data, server_config):

    if len(key) == 0:
        return resp_data(400, domain, 'key error')

    s3_data           = server_config.copy()
    s3_data['bucket'] = 'firestorage-index'

    # 認証 group/user_id/count/key
    code, msg = check_user_file_auth(s3_data, auth, member_data)
    if code != 200:
        return resp_data(400, domain, msg)

    # ファイル一覧を取得
    Delimiter = '%2F'
    prefix = '/' + group + "/" + key + "/"
    code, data = ss.bucket_list(s3_data, prefix, '', '', 1000)
    if code != 200:
        return resp_data(400, domain, 'bucket list error')

    print(data)
    data_list = []

    # dirリストを引っ張る
    for item in data['list']:
        data_item = {}
        l = item['key'].split('/')
        if l[len(l) - 1] == 'config.json':
            print("config.json")
            data_item['dir']  = l[len(l) - 2]
            data_item['date'] = item['date']
            code, text = ss.s3_simple_get(s3_data, item['key'])
            data_item['code'] = code
            if code == 200:
                if fb.is_json(text):
                   json_data = json.loads(text)
                   data_item['upload_dir'] = json_data['upload_dir']
                   prefix = '/' + group + "/" + data_item['upload_dir'] + "/"
                   code, dir = ss.bucket_list(server_config, prefix, '', '', 1000)
                   print(dir)
                   data_item['dir_code'] = code
                   if 'list' in dir:
                       list_item = {}
                       for item2 in dir['list']:
                            l = item2['key'].split('/')
                            id = l[len(l) - 2]
                            ty = l[len(l) - 3]

                            items = {}
                            if id in list_item:
                                items = list_item[id]

                            if ty == 'contents':
                                items['name'] = l[len(l) - 1]
                                items['size'] = int(item2['size'])
                                items['date'] = item2['date']
                                items['url']  = ss.s3_simple_sign(server_config, 'get_object', item2['key'], 1800)
                            elif ty == 'clamd':
                                # 数字に変換
                                s = l[len(l) - 1]
                                s = re.sub(r"\D", "", s)
                                items['clamd'] = int(s)
                            list_item[id] = items

                       data_item['dir_list'] = list_item
                   else:
                       data_item['dir_code'] = 404

            data_list.append(data_item)


    body = {}
    body['list']  = data_list
    body['count'] = len(data_list)
    body['auth']  = auth

    return resp_data(200, domain, body)

def check_user_file_auth(s3_data, auth, member_data):

    if len(auth) == 0:
        return 400, 'auth error'

    user_id = member_data['id']

    # 認証 group/user_id/count/key
    l = auth.split('/')
    print(l)
    if int(l[2]) != user_id:
        return 400, 'member auth error'
    if l[len(l) - 1] != 'index.json':
        return 400, 'member index.json error:' + l[len(l) - 1]

    code, text = ss.s3_simple_get(s3_data, auth)

    if code != 200:
        return 200,'auth file error'

    return 200, ""

def upload_dir_list(domain, group, member_data, server_config):

    s3_data           = server_config.copy()
    s3_data['bucket'] = 'firestorage-index'

    # ファイル一覧を取得
    Delimiter = '%2F'
    prefix = '/' + group + "/user/" + str(member_data['id']) + "/"
    code, data = ss.bucket_list(s3_data, prefix, '', '', 1000)

    if code != 200:
        return resp_data(400, domain, 'bucket list error')

    data_list = []

    print("upload_dir_list:" + str(code))
    print(data)

    if data['count'] == 0:
        data = {}
        data['list'] = data_list
        data['count'] = 0
        return resp_data(200, domain, data)


    for item in data['list']:
        data_item = {}
        l = item['key'].split('/')

        if len(l) > 5:
            dates = 9999999999 - int(l[3])
            data_item['date']  = fb.datetime_str(dates, 'yyyy-mm-dd hh:mm:ss')
            data_item['count'] = l[4]
            data_item['key']   = l[5]
            data_item['auth']  = fb.uri_encode(item['key'])
            data_list.append(data_item)

    data = {}
    data['list'] = data_list
    data['count'] = len(data_list)

    return resp_data(200, domain, data)

def resp_data(code, domain, message):

    body = {}
    body['code'] = code
    body['data'] = message

    if domain != '*':
        domain = "https://" + domain

    return {
        'statusCode': 200,
        'headers' : {
          "Access-Control-Allow-Origin":  domain,
        },
        'body': json.dumps(body)
    }

