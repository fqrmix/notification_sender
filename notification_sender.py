"""
    Notification sender script for YooKassa by @fqrmix
    
    Kibana search query:
        message.message: "SHOP_ID" && message.methodid: "QUEUE/sendNotificationQueue" && NOT message.message: "url=apiV3_URL" && message.message: "status*EVENT_TYPE"
    
    Usage: python ./notification_sender.py [PATH_TO_JSON.json] [merchant_url]
    
    @ 2023
"""
import json
import requests
import re
import time
import logging
import uuid
from sys import argv, stdout
from urllib.parse import urlparse
from dataclasses import dataclass


logging.basicConfig(filename='./notification_sender.log',
                    filemode='a',
                    format='[%(asctime)s,%(msecs)d] [%(levelname)s] %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger('urbanGUI')
logging.getLogger().addHandler(logging.StreamHandler(stdout))

raw_data = dict()
message_dict = dict()
sended_objects = list()

usage_text = 'Usage:\n\npython ./notification_sender.py \
[PATH_TO_JSON.json] [merchant_url]'

@dataclass
class k8s_Message:
    traceid: str
    level: str
    paymentid: str
    name: str
    methodid: str
    shopid: str
    thread: str
    message: str

@dataclass
class Notification:
    url: str
    body: str
    headers: list

try:
    try:
        json_filepath = argv[1]
        assert json_filepath.endswith('.json'), \
        f'У указанного файла расширение не .json!\n\n{usage_text}'
    except IndexError:
        raise Exception(f'Не указан путь к .json файлу!\n\n{usage_text}')
    
    try:
        merchant_apiV3_url = urlparse(argv[2])
        assert all(
            [
                merchant_apiV3_url.scheme,
                merchant_apiV3_url.netloc,
                merchant_apiV3_url.path
            ]
        ) is True, \
        f'Невалидный URL мерчанта!\n\n{usage_text}'

    except IndexError:
        raise Exception(
            f'Не указан URL мерчанта, на который нужно отправить уведомления\n\n'\
            f'{usage_text}'
        )

    with open(json_filepath, encoding='utf-8') as json_file:
        raw_data = json.load(json_file)

    for hit in raw_data['hits']['hits']:
        message_dict = hit['_source']['message']
        message_object = k8s_Message(
            traceid = message_dict['traceid'],
            level = message_dict['level'],
            paymentid = message_dict['paymentid'],
            name = message_dict['name'],
            methodid = message_dict['methodid'],
            shopid = message_dict['shopid'],
            thread = message_dict['thread'],
            message = message_dict['message'],
        )

        notification = Notification(
            url=re.search(
                r'(?<=url=)(.*)(?=, )', 
                message_object.message
            ).group(),

            body=re.search(
                r'(?s)(?<=object=)(.*)(?=, headers=)', 
                message_object.message
            ).group(),

            headers=re.search(
                r'(?<=headers=\[)(.*)(?=\])', 
                message_object.message
            ).group().split(sep=', ')
        )

        notification.body = json.loads(notification.body)
        
        try:
            assert notification.body['payment_id'] is not None
            event_type = 'refund' + '.' + notification.body['status']
        except KeyError:
            event_type = 'payment' + '.' + notification.body['status']
        
        notification.body = {
            "type": "notification",
            "event": event_type,
            "object": notification.body
        }

        notification.body = json.dumps(
                                notification.body,
                                indent=2, 
                                separators=(',', ' : '),
                                ensure_ascii=False,
                            ).encode('utf-8')

        notification.headers = {
            x.split(sep=': ')[0]:x.split(sep=': ')[1] \
            for x in notification.headers
        }

        if notification.body not in sended_objects:
            trace_id = uuid.uuid4()
            logging.info(f'[{trace_id}] Notification body:\n{notification.body.decode("utf-8")}')

            response = requests.post(
                url=merchant_apiV3_url.geturl(), 
                data=notification.body,
                headers=notification.headers
            )
            
            if response.status_code == 200:
                logging.info(f'[{trace_id}] Notification was successfully sended. Response: {response}')
            else:
                logging.warning(f'[{trace_id}] Notification was not successfully sended. Response: {response}')
            
            sended_objects.append(notification.body)
            time.sleep(2)
        else:
            logger.info(f"Notification for ID [{json.loads(notification.body)['object']['id']}] was already sended. Continue.")
            continue

except Exception as error:
    logging.error(error)
    raise Exception(error)
