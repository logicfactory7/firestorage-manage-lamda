import json
import fire_basic as fb
import s3_simple  as ss

# pip3 install requests -t .
# pip3 install xmltodict -t .
# pip3 install redis -t .

def lambda_handler(event, context):
    # TODO implement

    # {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'ap-northeast-1', 'eventTime': '2024-05-30T05:21:01.842Z', 'eventName': 'ObjectRemoved:Delete', 'userIdentity': {'principalId': 'AWS:AIDA2DZTPU34MFV7QLCV3'}, 'requestParameters': {'sourceIPAddress': '39.110.234.252'}, 'responseElements': {'x-amz-request-id': 'EZE0FQSC4EQK2G01', 'x-amz-id-2': 'jN8FnOyrpKpHyNJmEoHLQoWcRI9WlVzPKNssjNpADhIDM34cj0xOha8cywqOI66K+Ielgc4HR9iyqVYgnx5HprybSET1g8GkRh02vIJu4yk='}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': 'firestorage-index-asp-delete', 'bucket': {'name': 'firestorage-index-asp', 'ownerIdentity': {'principalId': 'A39Z59BZNDNTAV'}, 'arn': 'arn:aws:s3:::firestorage-index-asp'}, 'object': {'key': 'delete.json', 'sequencer': '0066580CBDD0C8773E'}}}]}

    print(event)

    for message_unicode in event['Records']:
        s3_delete_action(message_unicode['s3'])

    return {
        'statusCode': 200,
        'body': json.dumps("OK")
    }

def s3_delete_action(json_data):

    data_config = {}

    if 'bucket' not in json_data:
        print("no bucket tag")
        return 404
    if 'object' not in json_data:
        print("no object tag")
        return 404

    bucket = json_data['bucket']['name']
    object = json_data['object']['key']

    if bucket != 'firestorage-index-asp':
        print("bad bucket name:" + bucket)
        return 400

    print(bucket)
    print(object)

    # sample/okuru/C3cxSrKytYuWVh/gB473UOB/okuru_delete.json
    l = object.split('/')
    if len(l) < 3:
        return 400

    asp_name = l[0]
    group    = l[1]
    key      = l[2]
    dir      = l[3]

    if bucket == 'firestorage-index-asp':
        # asp
        code ,cloud_json = fb.s3_simple_get("firestorage-index", 'setting/asp/service/' + asp_name + '/config.json')
        if code != 200:
            print("s3_simple_get error:" + 'setting/asp/service/' + asp_name + '/config.json')
            return 400
        config = json.loads(cloud_json)
        data_config = config['setting']
    else:
        data_config = bucket

    index_config           = data_config.copy()
    index_config['bucket'] = 'firestorage-index'

    prefix = group + '/' + key + '/' + dir + '/'

    # ファイル一覧を取得
    code, data = ss.bucket_list(index_config, prefix, '', '', 1000)

    print(code)
    print(prefix)
    print(data)

    if code != 200:
        print("bucket_list error:" + str(code))
        return code

    for item in data['list']:
        if item['key'].find('config.json') >= 0:
            #  [{'key': 'okuru/C3cxSrKytYuWVh/gB473UOB/config.json', 'date': '2024-05-30 15:06:23', 'etag': '"950d93364a1ce4c5579022c74e66b5b5"', 'size': '1568'}
            code = delete_files(index_config, data_config, item['key'], group)
            break



#{
#	's3SchemaVersion': '1.0',
#	 'configurationId': 'firestorage-index-asp-delete',
#	 'bucket': {
#		'name': 'firestorage-index-asp',
#		 'ownerIdentity': {
#			'principalId': 'A39Z59BZNDNTAV'
#		},
#		 'arn': 'arn : aws : s3 : ::firestorage-index-asp'
#	},
#	 'object': {
#		'key': 'delete.json',
#		 'sequencer': '0066580E10E97DF2FE'
#	}
#}

def delete_files(index_config, data_config, config_file, group):

    code ,text = ss.s3_simple_get(index_config, config_file)
    print(config_file + ":" + str(code))

    if code != 200:
        print("delete_files s3_simple_get error:" + str(code) + ":" + config_file)
        return code

    if fb.is_json(text):

        json_data = json.loads(text)
        upload_dir = json_data['upload_dir']
        prefix = group + '/' + upload_dir + '/'

        # ファイル一覧を取得
        code, data = ss.bucket_list(data_config, prefix, '', '', 1000)
        print("bucket_list upload_dir:"  + str(code) + ":" + prefix)
        print(data)

        data_list = []
        index_list = []

        if data['count'] > 0:
            for item in data['list']:
                data_list.append(item['key'])

        # サムネ
        index_list.append(group + '/download/' + upload_dir + '/index.json')
        # ユーザーリスト
        index_list.append(json_data['user_dir'])

        print("data_list")
        print(data_list)
        print("index_list")
        print(index_list)

        code, body = ss.s3_simple_delete_multi(data_config, data_list)
        print("s3_simple_delete_multi:" + str(code))

        code, body = ss.s3_simple_delete_multi(index_config, index_list)
        print("s3_simple_delete_multi:" + str(code))

        # log関係

    else:
        print("is_json error" )
        return 400

