import csv
import pandas as pd
import datetime
import time
import random
from dateutil.relativedelta import relativedelta

action_list = [['浏览', 1], ['收藏', 2], ['分享', 3], ['推荐', 4], ['购买', 5]]


def get_action_day_list():
    # 1.获取当前日期
    end_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    # 2.获取当然日期的两年前
    begin_date = (datetime.datetime.strptime(end_date, '%Y-%m-%d') - relativedelta(months=6)).strftime('%Y-%m-%d')
    # 1.定义一个list
    date_list = []
    # 2.获取开始时间
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    # 3.获取结束时间
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    # 3.对时间进行遍历
    while begin_date <= end_date:
        # 4.对时间进行格式化
        date_str = begin_date.strftime("%Y-%m-%d")
        # 5.添加到列表当中
        date_list.append(date_str)
        # 6.对时间进行加1
        begin_date += datetime.timedelta(days=1)
    # 7.返回列表
    return date_list


def etl_product_type_list(product_type_list):
    return product_type_list.replace('[', '').replace(']', '').replace(' ', '').replace("'", '').split(',')


def get_action_time(action_day, action_hour, action_minute):
    if action_hour < 10:
        return action_day + " 0" + str(action_hour) + ":" + str(action_minute)
    else:
        return action_day + " " + str(action_hour) + ":" + str(action_minute)


def get_action_range(action_hour):
    if action_hour <= 8:
        return "00~08时"
    elif action_hour <= 13:
        return "08~13时"
    elif action_hour <= 20:
        return "13-20时"
    else:
        return "20-23时"


def start_etl_action_info():
    action_day_list = get_action_day_list()
    # 打开文件，追加a
    out = open(r'data_etl/action_info0310.csv', 'w', newline='', encoding='utf-8')
    # 设置写入模式
    csv_write = csv.writer(out, dialect='excel')
    csv_write.writerow([
        'user_id',
        'user_name',
        'user_age',
        'user_gender',
        'user_wage',
        'product_id',
        'product_type',
        'user_type',
        'action',
        'action_score',
        'action_time',
        'action_range'
    ])
    # 读取用户数据
    user_info_csv = pd.read_csv('data_etl/user_info0310.csv')
    # 读取商品信息
    product_info_csv = pd.read_csv('data_etl/product_info0310.csv')
    # 遍历用户数据
    for user_index in user_info_csv.index:
        # 获取用户编号
        user_id = user_info_csv['user_id'][user_index]
        # 获取用户姓名
        user_name = user_info_csv['user_name'][user_index]
        # 获取用户年龄
        user_age = int(user_info_csv['user_age'][user_index])
        # 获取用户性别
        user_gender = user_info_csv['user_gender'][user_index]
        # 获取用户工资
        user_wage = int(user_info_csv['user_wage'][user_index])
        # 获取用户访问时间限制
        user_action_hour_list = range(
            int(user_info_csv['user_action_start'][user_index]), int(user_info_csv['user_action_end'][user_index]))
        # 获取用户可选择商品标签
        product_type_list = etl_product_type_list(user_info_csv['product_type_list'][user_index])
        # 遍历商品标签
        for user_product_type in product_type_list:
            # 过滤出某个商品标签的商品并随机取数
            frac = random.randint(5, 10) / 10.0
            usr_product_info_csv = product_info_csv[product_info_csv['product_type'] == user_product_type].sample(
                frac=frac)
            # 遍历商品信息
            for product_index in usr_product_info_csv.index:
                # 获取商品编号
                product_id = product_info_csv['product_id'][product_index]
                # 获取商品类别
                product_type = product_info_csv['product_type'][product_index]
                # 获取用户类型
                user_type = product_info_csv['user_type'][product_index]
                # 获取行为分数
                action_dic = random.choice(action_list)
                # 获取行为时间
                action_hour = int(random.choice(user_action_hour_list))
                action_time = get_action_time(
                    action_day=random.choice(action_day_list),
                    action_hour=action_hour,
                    action_minute=random.choice(range(10, 59))
                )
                action_range = get_action_range(action_hour=action_hour)
                # 构建对象
                action_info = [
                    user_id,
                    user_name,
                    user_age,
                    user_gender,
                    user_wage,
                    product_id,
                    product_type,
                    user_type,
                    action_dic[0],
                    action_dic[1],
                    action_time,
                    action_range
                ]
                # 输出对象
                print(action_info)
                # 写入对象
                csv_write.writerow(action_info)
