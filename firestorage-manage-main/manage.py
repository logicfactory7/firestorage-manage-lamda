import pymysql
from datetime import date, datetime

# pip3 install PyMySQL -t .
# pip3 install python-memcached -t .

def mysql_db_close(db, cursor):

    cursor.close()
    db.close()

def mysql_db_connect(db_name):

    ip_add   = ''
    user     = 'fire'
    password = 'fireaccess'
    database = 'fire'
    # charset='utf8mb4'

    # まだ仮
    if db_name == 'fire_slave':
        ip_add = '192.168.100.77'

    if ip_add == '':
        return 404, {}, "db name not found"

    try:
        db = pymysql.connect(host=ip_add,
            user=user,
            password=password,
            database=database, connect_timeout=3,cursorclass=pymysql.cursors.DictCursor)
    except pymysql.Error as e:
        print(e)
        return 500, {}, "pymysql %d: %s" %(e.args[0], e.args[1])

    return 200, db, "OK"

# date, datetimeの変換関数
def json_serial(obj):
    # 日付型の場合には、文字列に変換します
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    # 上記以外はサポート対象外.
    raise TypeError ("Type %s not serializable" % type(obj))

def get_asp_login_url(domain):

    url = ''

    if domain == 'firestorage.jp' or  domain == 'firestorage.com':
        url = 'https://login.' + domain + '/login/login.cgi?act=login_form'
    else:
        url = '/main.cgi?act=login_form'

    return url

def get_domain_country(domain):

    domain1, domain2, domain3 = domain.split('.')

    if domain1 == 'firestorage':
        if domain2 == 'jp':
            return 'jp'
        else:
            return 'com'
    elif domain2 == 'firestorage':
        if domain3 == 'jp':
            return 'jp'
        else:
            return 'com'

    return 'jp'

def asp_domain_check(domain, data):

    domain1, domain2, domain3 = domain.split('.')

    if domain1 == 'firestorage':
        if data['asp'] == '' or  data['asp']  == '-':
            return 200, ""
        else:
            url = get_asp_login_url(domain)
            return 301 , url
    else:
        if domain1 == data['asp']:
            return 200, ""
        else:
            url = get_asp_login_url(domain)
            return 301 , url



def get_member_data_db(user_id):

    # MySQLに接続
    code, db, message = mysql_db_connect("fire_slave")
    if code != 200:
        return 400, "", message

    cursor = db.cursor()
    data = {'id': user_id}
    result = {}

    try:

        sql = "SELECT * FROM member_tbl WHERE id = %(id)s limit 1 "
        cursor.execute(sql, (data))
        result = cursor.fetchall()

    except Exception as e:
        return 400, "", "pymysql %d: %s" %(e.args[0], e.args[1])
    finally:
        mg.mysql_db_close(db, cursor)

    return 200, result, "OK"
