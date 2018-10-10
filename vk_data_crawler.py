import sys
import os
import time
import vk_api
import pandas as pd
from tqdm import tnrange, tqdm
from time import sleep
import django

#project adress
VKData = '/home/rust/Documents/Azat/VKCrawler/VKData'
sys.path.insert(0, VKData)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "VKData.settings.py")
django.setup()

from getData.models import getData
df = pd.read_csv('../raw_data/azat_clean.csv')

#authorization
login, password = '$$$', '$$$'
vk_session = vk_api.VkApi(login, password)
try:
        vk_session.auth()
except vk_api.AuthError as error_msg:
        print(error_msg)

vk = vk_session.get_api()

#changing the data type
df['VK_PROFILE'] = df['VK_PROFILE'].astype(int) 
df['id_crm'] = df['id_crm'].astype(str)

d = df.apply(lambda x: pre_save(x['VK_PROFILE'], x['id_crm']), axis=1)

log = 0 #put 0 for logs


#overalll number of records calculated from input
total = getData.objects.count() 
print('Всего записей ',total)

# load all data from DB
vk_objects = getData.objects.all() 

# loop on rows on DB
for o in tqdm(vk_objects, desc='Total progress'):           
    if o.timestamp is None: #check if data already collected
        # chekcing is api available
        try:
            # query to vk api
            result = vk.users.get(user_ids = o.vk_id,fields='bdate,city,country,counters')
            if log == 1:
                print('Обрабатываемый vk_id --->', o.vk_id)
            # adding the required data from response
            o.last_name = (result[0]['last_name'])
            o.first_name = (result[0]['first_name'])
            if 'bdate' in result[0].keys():    
                o.bdate = (result[0]['bdate'])
            else:
                pass
            if 'city' in result[0].keys():
                o.city = (result[0]['city']['title'])
            else:
                pass
            if 'country' in result[0].keys():
                o.country = (result[0]['country']['title'])
            else:
                pass
            if 'friends' in result[0].keys():
                o.friends = (result[0]['counters']['friends'])
            else:
                pass
            if 'followers' in result[0].keys():
                o.followers = (result[0]['counters']['followers'])
            else:
                pass
            o.timestamp = time.asctime( time.localtime(time.time()) )
            o.save() #save data to DB
        except Exception as q:
            # if it error, wait 2 seconds
            print('Error. Wait 2 seconds.', o.vk_id)
            time.sleep(2)
            print(q)
    else:
        # output on screen, if record already exist
        if log == 1:
            print('The record is exist', o.timestamp)
        pass

#from DB to csv
vk_objects = getData.objects.all() 
data = { "vk id": [], "id crm": [], "last name": [], 
        "first name": [], "bdate": [], "city": [], 
        "country": [], 'friends': [], "followers": []}

for o in tqdm(vk_objects, desc='Total progress'):
        data["vk id"].append(o.vk_id)
        data["id crm"].append(o.id_crm)
        data["last name"].append(o.last_name)
        data["first name"].append(o.first_name)
        data["bdate"].append(o.bdate)
        data["city"].append(o.city)
        data["country"].append(o.country)
        data["friends"].append(o.friends)
        data["followers"].append(o.followers)
df = pd.DataFrame.from_dict(data)
df.to_csv("../final_data/result.csv", index=False, encoding='utf-8')        
