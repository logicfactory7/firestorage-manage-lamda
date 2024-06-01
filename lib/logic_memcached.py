import memcache
import json

# pip3 install python-memcached -t .

def memcached_connect(name):

    ip_add   = ''

    if name == 'fire_main':
        ip_add = '192.168.100.31:11211'

    if ip_add == '':
        return 404, {}, "db name not found"

    client = memcache.Client([ip_add], cache_cas=True)

    return 200, client, "OK"

def get_member_data(memc, eid):

    data = {}
    code = 200
    member_key = 'cookie-' + eid

    rep = memc.get(member_key)

    print(rep)

    if rep:
        str = rep.decode('utf-8')
        result = str.split('\t')
        data['id']        = int(result[0])
        data['asp_level'] = result[1]
        data['payed']     = result[2]
        data['current_volume_id'] = result[3]
        data['asp']       = result[4]
        data['stime']     = int(result[5])
        data['amazons3']  = result[6]
    else:
        code = 404

    return code, data

def check_eid(rc, eid):

    if eid is None:
        data = {}
        data['id'] = 0
        return 404, data
    elif len(eid) < 40:
        data = {}
        data['id'] = 0
        return 404, data

    key = "member_data:" + eid

    text = rc.get(key)

    if is_jsonx(text):
#       print("member_data:rc" + text)
        member_data = json.loads(text)
        return 200, member_data

    code, memc, message = memcached_connect("fire_main")

    if code != 200:
        print("member_data:memc_connect_err")
        data = {}
        data['id'] = 0
        return 404, data

    code, member_data = get_member_data(memc, eid)

    if code == 200:
        text = json.dumps(member_data)
        print("member_data:memc" + text)
        rc.set(key, text, ex=120)
        return code, member_data
    else:
        print("member_data:memc:no_data")
        data = {}
        data['id'] = 0
        return 404, data

def is_jsonx(json_str):
   try:
        json.loads(json_str)
   except json.JSONDecodeError as e:
        return False
   # 以下の例外でも捕まえるので注意
   except ValueError as e:
        return False
   except Exception as e:
        return False
   return True
