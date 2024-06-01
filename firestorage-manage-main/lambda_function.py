import json
import fire_basic as fb
import logic_memcached as lm
import manage as mg
import pymysql

# pip3 install PyMySQL -t .
# pip3 install python-memcached -t .

def lambda_handler(event, context):
    # TODO implement

    # テンプレートを出力
    domain = ''
    body = {}
    stage = 'test'
    validation_parm = [
        {'key': 'domain', 'max': 50, 'min': 3,  'type': "a-z0-9."  , "req" : True},
        {'key': 'act',    'max': 32, 'min': 4,  'type': "a-z0-9_"  , "req" : True},
        {'key': 'manage', 'max': 32, 'min': 4,  'type': "a-z0-9_"  , "req" : False},
        {'key': 'eid',    'max': 40, 'min': 32, 'type': "a-f0-9"   , "req" : False},
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

    code, memc, message = lm.memcached_connect("fire_main")

    if code != 200:
        return resp_data(400, domain, "memc error:" + message)

    print("memc:" + str(code))

    rc = fb.redis_connect()

    body['eid'] = form_data['eid']
    code, member_data = lm.get_member_data(memc, form_data['eid'])
    print("get_member_data:" + str(code) + ":eid:" + form_data['eid'])

    if code == 200:

        code, menu = get_member_menu(rc, member_data, domain, '', event)

        rep = {}
        rep['code'] = code
        rep['data'] = menu
        body['menu'] = rep

        if form_data['act'] == 'top':
            # domainチェック
            code, url = mg.asp_domain_check(domain, member_data)

            if code != 200:
                body['code'] = code
                body['url']  = url
                return resp_data(code, domain, body)

            body['asp_domain_check'] = code
            body['member_key'] = member_data
            body['act'] = form_data['act']

            code, data, header  = get_sub_main(rc, stage, 'actmenu', 'top', event, member_data)
            if code == 200:
                body['main'] = data
                body['header'] = header
            else:
                body['code'] = code
                body['main'] = 'get top data error'

        else:

            # メニューと画面を持ってくる
            code, data, header = get_sub_menu_main(rc, stage, member_data, domain, form_data['act'], event)

            if code == 200:
                body['act']  = form_data['act']
                body['body'] = data
                body['header'] = header
            else:
                body['code'] = code
                body['body'] = 'get ' + form_data['act'] + ' data error'

    else:
        code = 301
        body['url'] = mg.get_asp_login_url(domain)

    return resp_data(code, domain, body)

def get_sub_main(rc, stage, type, action, event, member_data):

    bucket = 'firestorage-index'
    header = ""

    mem_key = 'setting/' + stage + '/' + type + '/' + action + '.json'
    code = 200

    json_text = rc.get(mem_key)
    if fb.get_logic_ip_check(event) == True:
       # ロジックIPは無視
       json_text = None

    if json_text is not None:
        # キャッシュから
        json_data = json.loads(json_text)
        if 'header' in json_data:
            header = json_data['header']
            if 'extra' in json_data:
                code, json_data = extra_data(rc, stage, json_data, member_data)
                print("extra_data:" + str(code))
        return 200, json_data['body'], header
    else:
        # s3から
        code, json_text = fb.s3_simple_get(bucket, mem_key)
        if code == 200:
            rc.set(mem_key, json_text, ex=600, nx=True)
            json_data = json.loads(json_text)
            if 'header' in json_data:
                header = json_data['header']
            if 'extra' in json_data:
                code, json_data = extra_data(rc, stage, json_data, member_data)
                print("extra_data:" + str(code))
            return 200, json_data['body'], header

    return 400, "NG" + action, ""

def extra_data(rc, stage, json_data, member_data):

    # 置換すべきデータがあるか

    body  = ""
    body2 = ""
    extra_data = {}
    user_id = member_data['id']
    db_code = 500

    if 'extra' not in json_data:
        return 404, json_data
    else:
        extra = json_data['extra']
        if extra is None:
            return 403, json_data
        if fb.is_json(extra) == False:
            return 400, json_data
        extra_data = json.loads(extra)

    if 'body' in json_data:
        body = json_data['body']
    if 'body2' in json_data:
        body2 = json_data['body2']

    if 'mail_group' in extra_data:

        lang = {}
        mail_group = extra_data['mail_group']
        tag = '<mail_group>'

        if 'tag' in mail_group:
            tag = mail_group['tag']

        if 'lang' in mail_group:
            lang = mail_group['lang']
            if 'no_list' not in lang:
                lang['no_list']['jp'] = 'リストはありません'
                lang['no_list']['en'] = 'no list'
            if 'select_list' not in lang:
                lang['select_list']['jp'] = '選択可能です'
                lang['select_list']['en'] = 'selectable list'

        group_list_jp = ""
        group_list_en = ""

        # MySQLに接続
        if db_code == 500:
            db_code, db, message = mg.mysql_db_connect("fire_slave")

        if db_code == 200:

            cursor = db.cursor()
            data = {'id': user_id}
            result = {}

            try:

                sql = "select id,group_name,counts,timestamp from user_group_tbl where user_id = %(id)s order by id desc";
                cursor.execute(sql, (data))
                result = cursor.fetchall()

                for list in result:
                    # {'id': 320493, 'group_name': 'aaaaadsads', 'counts': None, 'timestamp': datetime.datetime(2019, 12, 17, 16, 56)}

                    counts = 0
                    if list['counts'] is not None:
                        counts = int(list['counts'])

                    if counts > 0:
                        group_name = fb.uri_decode(list['group_name']);
                        group_name = fb.html_entities_encode(group_name)
                        group_list_jp += '<option value="' + str(list['id']) + '">' + group_name + '</option>' + "\n"
                        group_list_en += '<option value="' + str(list['id']) + '">' + group_name + '</option>' + "\n"

                    print(list)

            finally:
                pass

            if group_list_jp is None:
                group_list_jp = '<option value="0">' + lang['no_list']['jp'] + '</option>'
                group_list_en = '<option value="0">' + lang['no_list']['en'] + '</option>'
            else:
                group_list_jp = '<option value="0">' + lang['select_list']['jp'] + '</option>' + "\n" + group_list_jp
                group_list_en = '<option value="0">' + lang['select_list']['en'] + '</option>' + "\n" + group_list_en

            print("tag:" + tag)

            if body is not None:
                body  = body.replace(tag, group_list_jp)
            if body2 is not None:
               body2 = body2.replace(tag, group_list_en)

    if 'file_group' in extra_data:

        lang = {}
        file_group = extra_data['file_group']
        tag = '<file_group>'

        if 'tag' in file_group:
            tag = file_group['tag']

        if 'lang' in file_group:
            lang = file_group['lang']
            if 'no_list' not in lang:
                lang['no_list']['jp'] = 'リストはありません'
                lang['no_list']['en'] = 'no list'
            if 'select_list' not in lang:
                lang['select_list']['jp'] = '選択可能です'
                lang['select_list']['en'] = 'selectable list'

        group_list_jp = ""
        group_list_en = ""

        # MySQLに接続
        if db_code == 500:
            db_code, db, message = mg.mysql_db_connect("fire_slave")

        if db_code == 200:

            cursor = db.cursor()
            data = {'id': user_id}
            result = {}

            try:

                sql = "select id,group_name,counts,sha1 from user_file_group_tbl where user_id = %(id)s order by id desc";
                cursor.execute(sql, (data))
                result = cursor.fetchall()

                for list in result:

                    group_name = fb.uri_decode(list['group_name'])
                    group_name = fb.html_entities_encode(group_name)
                    group_list_jp += '<option value="' + str(list['id']) + '">' + group_name + '</option>' + "\n"
                    group_list_en += '<option value="' + str(list['id']) + '">' + group_name + '</option>' + "\n"

                    print(list)
            finally:
                pass

            if group_list_jp is None:
                group_list_jp = '<option value="0">' + lang['no_list']['jp'] + '</option>'
                group_list_en = '<option value="0">' + lang['no_list']['en'] + '</option>'
            else:
                group_list_jp = '<option value="0">' + lang['select_list']['jp'] + '</option>' + "\n" + group_list_jp
                group_list_en = '<option value="0">' + lang['select_list']['en'] + '</option>' + "\n" + group_list_en

            print("tag:" + tag)

            if body is not None:
                body  = body.replace(tag, group_list_jp)
            if body2 is not None:
               body2 = body2.replace(tag, group_list_en)

    if db_code == 500:
        mg.mysql_db_close(db, cursor)

    if 'body' in json_data:
        json_data['body'] = body
    if 'body2' in json_data:
        json_data['body2'] = body2

    return 200, json_data

def get_sub_menu_main(rc, stage, member_data, domain, action, event):

    bucket = 'firestorage-index'
    data = {}
    act_check = action + ":true"

    if act_check.find('_'):
        result = act_check.split('_')
        act_check = result[0]

    code, data = get_member_menu(rc, member_data, domain, 'check', event)
    if code != 200:
        return 400, "", ""

    if act_check in data:
        # 画面を持ってくる
        code, data, header = get_sub_main(rc, stage, 'actmenu', action, event, member_data)
        return code, data, header
    else:
        return 200, "menu auth check", ""

def get_member_menu(rc, member_data, domain, type, event):

    if 'id' not in member_data:
        return 404, ""

    data = ""

    menu = {}
    menu['okuru']   = 'オクル,upload file'
    menu['uketoru'] = 'ウケトル,upload page'
    menu['miseru']  = 'ミセル,photo'
    menu['tameru']  = 'タメル,storage'
    menu['config']  = '設定,storage'
    menu['asp']     = 'ライセンス/ASP,Admin'

    clas = {}
    clas['okuru']   = 'fa-solid fa-cloud-arrow-up pe-1 text-primary'
    clas['uketoru'] = 'fa-solid fa-cloud-arrow-down pe-1 text-primary'
    clas['miseru']  = 'fa-solid fa-camera pe-1 text-primary'
    clas['tameru']  = 'fa-solid fa-box-archive pe-1 text-primary'
    clas['config']  = 'fa-solid fa-gears pe-1 text-primary'
    clas['asp']     = 'fa-solid fa-building pe-1 text-primary'

    if member_data['payed'] == '4' or member_data['payed'] == '8':
        # ウケトル
        data = "top,okuru:true,uketoru:false,miseru:false,tameru:false,config:false"

    elif member_data['payed'] == '5' or member_data['payed'] == '6' or member_data['payed'] == '7':
#       アーカイブ
        data = "top,okuru:true,archive:false,config:false"

    elif member_data['payed'] == '1' or member_data['payed'] == '2' or member_data['payed'] == '3':
#       有料
        data = "top,okuru:true,uketoru:false,miseru:false,tameru:false,config:false"

    elif member_data['asp'] == '-':
#       無料
        data = "top,okuru:true,uketoru:false,miseru:false,tameru:false,config:false"

    elif member_data['asp'] != '-' and member_data['asp'] != '':
        # メニューリストを入手
        data = get_asp_menu_list(rc, member_data['asp'], member_data['asp_level'], event)

    else:
        return 400, data

    if type == 'check':
        # リストのみ
        return 200, data

    result = data.split(',')

    country = mg.get_domain_country(domain)

    print(data)

    list = []
    for str in result:
        if str != 'top':

            l = str.split(':')
            value  = l[0]
            active = 'false'
            url = ''
            print(l)
            print(len(l))
            if len(l) > 1:
                active = l[1]
            if len(l) > 2:
                url = l[2]

            data = menu[value]
            jp, com = data.split(',')
            title = ""
            if country == 'jp':
                title = jp
            else:
                title = com
            c = {}
            c['title']  = title
            c['action'] = value
            c['url'] = url
            c['class']  = clas[value]

            if active == 'true' or active == 'True':
                c['active'] = True
            else:
                c['active'] = False

            list.append(c)

    return 200, list

def get_asp_menu_list(rc, member_asp_name, member_asp_level, event):

    bucket = 'firestorage-index'
    # デフォルト
    data = "top,okuru,uketoru,miseru,tameru,config"

    mem_key = 'setting/production/createasp/' + member_asp_name + '.json'
    json_text = rc.get(mem_key)

    if fb.get_logic_ip_check(event) == True:
#        pass
       json_text = None

    if json_text is None:
        code, json_text = fb.s3_simple_get(bucket, mem_key)
        if code == 200:
            rc.set(mem_key, json_text, ex=600, nx=True)
    else:
        print("redis")

    if json_text is not None:
        json_data = json.loads(json_text)
        if 'body' in json_data:
            json_data = json.loads(json_data['body'])
            if 'data' in json_data:
                list = json_data['data']
                if 'asp_level' in list[0]:
                    asp_level = list[0]['asp_level']
                    if 'all' in asp_level:
                        data = asp_level['all']
                        print("asp_level:all" + ":" + data)

                    if member_asp_level in asp_level:
                        data = asp_level[member_asp_level]
                        print("asp_level:" + member_asp_level + ":" + data)

    return data

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
