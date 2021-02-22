import pandas as pd
import vk_api
import numpy as np
import requests
import json

# Считываем данные

path = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR-ti6Su94955DZ4Tky8EbwifpgZf_dTjpBdiVH0Ukhsq94jZdqoHuUytZsFZKfwpXEUCKRFteJRc9P/pub?gid=889004448&single=true&output=csv'
df = pd.read_csv(path, parse_dates=[0])

print('Данные считаны')

# Рассчитываем метрики

metrics_by_day = df.groupby(['date', 'event'], as_index=False)\
                   .agg({'ad_id': 'count'})\
                   .pivot(index='date', columns='event', values='ad_id')

metrics_by_day['ctr'] = (100 * metrics_by_day['click'] / metrics_by_day['view']).round(2)
metrics_by_day['view_cost'] = df['ad_cost'].unique()[0] / 1000
metrics_by_day['total_cost'] = (metrics_by_day['view_cost'] * metrics_by_day['view']).round(2)


def count_change(column):
    """
    Эта функция считает темп прироста показателя для каждого дня.
    В качестве аргумента принимает столбец (серию) со значениями показателя.
    Возвращает список с темпами прироста для каждого дня.
    """
    change_list = [0]
    for i in range(1, len(column)):
        change_list.append(int((100 * column[i] / column[i-1] - 100).round()))
    return change_list


# Вызываем функцию от нужных нам столбцов и помещаем полученные значения темпов прироста в новые столбцы

metrics_by_day['click_change'] = count_change(metrics_by_day['click'])
metrics_by_day['view_change'] = count_change(metrics_by_day['view'])
metrics_by_day['ctr_change'] = count_change(metrics_by_day['ctr'])
metrics_by_day['total_cost_change'] = count_change(metrics_by_day['total_cost'])

print('Метрики рассчитаны')

# Заводим переменные, необходимые для оформления отчета

current_date = metrics_by_day.index[-1]
ad_id = df['ad_id'].unique()[0]
click = metrics_by_day.loc[current_date, 'click']
click_change = metrics_by_day.loc[current_date, 'click_change']
view = metrics_by_day.loc[current_date, 'view']
view_change = metrics_by_day.loc[current_date, 'view_change']
costs = metrics_by_day.loc[current_date, 'total_cost']
costs_change = metrics_by_day.loc[current_date, 'total_cost_change']
ctr = metrics_by_day.loc[current_date, 'ctr']
ctr_change = metrics_by_day.loc[current_date, 'ctr_change']

# Создаем текстовый файл с отчетом

with open('metrics_report.txt', 'w') as f:
    print('{:%B %d, %Y}: '.format(current_date) + f'отчет по объявлению {ad_id}',
          '',
          f'Траты: {costs} руб. ' + '({0:+d}%)'.format(costs_change),
          f'Показы: {view} ' + '({0:+d}%)'.format(view_change),
          f'Клики: {click} ' + '({0:+d}%)'.format(click_change),
          f'CTR: {ctr}% ' + '({0:+d}%)'.format(ctr_change),
          sep='\n', file=f)

# Отправляем текст отчета в ВК

with open('metrics_report.txt', 'r') as f:
    text = f.readlines()
message_text = ''.join(text)

app_token = '9b74092af5460e8ef9cc47402fc4453866526af9979a98a9fea5b6341fe2e78b9998ceea2996f06bad0cb'
chat_id = 1
my_id = 1073831
vk_session = vk_api.VkApi(token=app_token)
vk = vk_session.get_api()

vk.messages.send(
    chat_id=chat_id,
    random_id=np.random.randint(1, 2 ** 31),
    message=message_text)

# Отправляем файл с отчетом в ВК

path_to_file = '/home/jupyter-d.kuznetsov-5/airflow-3/dags/d.kuznetsov/metrics_report.txt'
file_name = 'metrics_report.txt'

upload_url = vk.docs.getMessagesUploadServer(peer_id=my_id)["upload_url"]
file = {'file': (file_name, open(path_to_file, 'rb'))}

response = requests.post(upload_url, files=file)

json_data = json.loads(response.text)

saved_file = vk.docs.save(file=json_data['file'], title=file_name)
attachment = 'doc{}_{}'.format(saved_file['doc']['owner_id'], saved_file['doc']['id'])

vk.messages.send(
    chat_id=chat_id,
    random_id=np.random.randint(1, 2 ** 31),
    message='',
    attachment=attachment)

print('Отчет отправлен')
