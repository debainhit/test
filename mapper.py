#!/usr/bin/env python

import sys
import json
import time
import logging
import hashlib
import base64

def desensitization(val):
    if not val:
        return val
    return hashlib.md5(base64.b64encode(hashlib.sha512(val).hexdigest()).replace('\r', '').replace('\n', '')[::-1]).hexdigest()


def parse(record):
    try:
        package = json.loads(record.decode('utf-8'))
        identifier = desensitization(package['identifier'])
        user_id = package['user_id']
        app_name = package['app_name']
        app_version = package['app_version']
        ip = package['ip']
        channel_code = package['channel_code']
        usages = package['request']['data']
        for usage in usages:
            path = usage['path']
            if app_version == "6266" and path in ('path_login_original', 'path_login_step'):
                values = json.loads(usage['value'])
#                event = values['event_name']
#                if event == 'event_android_login':
                values['path'] = path
                values['identifier'] = identifier
                values['user_id'] = user_id
                values['app_version'] = app_version
                values['channel_code'] = channel_code
                values['app_name'] = app_name
                values['ip'] = ip
                timestamp = values['timestamp'] / 1000
                values['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
                sys.stdout.write('%s\n' % json.dumps(values))
    except:
        logging.exception('error in parsing data')
#        assert False
        pass


if __name__ == '__main__':
    for line in sys.stdin:
       parse(line)
