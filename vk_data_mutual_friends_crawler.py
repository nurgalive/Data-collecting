import sys
import os 
import time
import vk_api
import pandas as pd
from tqdm import tnrange, tqdm_notebook, tqdm
from time import sleep
import django

# указывается адрес проекта
VKData = '/home/rust/Documents/Azat/vk_crawler/code/VKData'
sys.path.insert(0, VKData)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "VKData.settings.py")
django.setup()

from getData.models import getData
from getData.models import MutualFriends

login, password = '$$$', '$$$'
vk_session = vk_api.VkApi(login, password)
try:
        vk_session.auth()
except vk_api.AuthError as error_msg:
        print(error_msg)

vk = vk_session.get_api()

getData.objects.count() # размер БД

vk_ids = getData.objects.values('vk_id', 'id_crm', 'timestamp_friends')
total = getData.objects.count() 
vk_set_all = set()

for i in tqdm(vk_ids, desc='Making set'):
    vk_set_all.add(i['vk_id'])

print(len(vk_set_all))

log = 0 # вывод логов в консоль. Если "0" не выводить логи. "1" выводить логи.
error_store = { 'id_crm': [],
                'error': [], }


vk_objects = getData.objects.all()
if log == 1:
    print('Total records in DB ',total)
error_store = {} # словарь для хранения возникших ошибок
for i in tqdm(vk_objects, desc='Total progress'): #итерартор по записям БД с испольованием плагина на статус бар
    if i.timestamp_friends is None:
        if log == 1:
            print('Is data collected:',i.timestamp_friends)
        if log == 1: 
            print('Cheking vk_id ---->',i.vk_id)  
        
        try:
            res = vk.friends.get(user_id = i.vk_id) # запрос к vk api
            if log == 1:
                print('List of all friends -------------------------------------------------------------------->',res)
            
            vk_set_to = set(res['items'])  # множество друзей из запроса к vk api
            res_set = vk_set_all.intersection(vk_set_to) # пересечения множества друзей с множеством из БД
            if log == 1:
                print('------------------------------------------------------------------------------------------')
                print('resulted set of mutual friends --->', res_set)
            
            if res_set is not None:  # проверка на наличие общих друзей
                 #for r in tqdm(res_set, desc='Friends gathering', leave = False): # итератор по общим друзьям
                for r in res_set: # итератор по общим друзьям
                    if log == 1:
                        print('vk_id added in DB ----->',r)
                    cur = getData.objects.filter(vk_id = r) # находти нужный id_crm в БД
                    # записываем данные в БД
                    gd, is_create  = MutualFriends.objects.get_or_create(friend_vk_id=r, vk_id=i.vk_id, 
                                                                 id_crm_id = i.id_crm, 
                                                                 friend_id_crm_id = cur.values('id_crm')[0]['id_crm'])
                    gd.save()
                    #gd2, is_create2 = getData.objects.get_or_create(timestamp_friends = time.asctime( time.localtime(time.time()) ) )
                i.timestamp_friends = time.asctime( time.localtime(time.time()) )
                i.save()
            if log == 1:
                print('___________________________________________________________________________________________________')
                print(' ')
        except Exception as msg:
            #  появляющиеся ошибки записываем в словарь
            #error_store['id_crm'].append(i.id_crm)
            #error_store['error'].append(msg)
            if log == 1:
                print(msg, i.id_crm)
    else:
        if log == 1:
            print('Is data collected:',i.timestamp_friends)
        pass
