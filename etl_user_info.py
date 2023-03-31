import csv
import random

import pandas as pd


def get_user_gae_flag(user_age):
    if user_age <= 18:
        return "青少年"
    elif 19 < user_age <= 24:
        return "青年"
    elif 25 < user_age <= 35:
        return "中青年"
    elif 36 < user_age <= 45:
        return "中年"
    elif 46 < user_age <= 55:
        return "中老年"
    else:
        return "老年"


def get_user_action_hour(user_age):
    if user_age <= 18:
        return [10, 19]
    elif 19 < user_age <= 24:
        return [10, 22]
    elif 25 < user_age <= 35:
        return [5, 23]
    elif 36 < user_age <= 45:
        return [0, 23]
    elif 46 < user_age <= 55:
        return [10, 21]
    else:
        return [10, 19]


def get_user_wage_flag(user_wage):
    if user_wage < 5200:
        return "低收入人群"
    elif user_wage <= 6500:
        return "普通收入人群"
    elif user_wage <= 10000:
        return "中收入人群"
    else:
        return "高收入人群"


def get_product_type_list(user_gender, user_age, user_wage):
    product_type_list = []
    # 根据性别过滤商品类别
    if user_gender == '男':
        product_type_list = ["时尚服装", "美食", "娱乐", "程序", "退休礼物", "影视", "健身"]
    else:
        product_type_list = ["时尚服装", "美食", "娱乐", "宝妈", "程序", "退休礼物", "影视", "健身"]
    # 根据年龄过滤商品类别
    if user_age <= 35:
        product_type_list.remove("退休礼物")
    if 35 < user_age < 50:
        product_type_list.remove("退休礼物")
        if user_gender == '女':
            product_type_list.remove("宝妈")
    if user_age >= 50:
        product_type_list = ["美食", "退休礼物", "影视"]
    # 根据工资过滤商品类别
    if user_wage < 6000:
        if user_age >= 50:
            product_type_list = ["美食", "退休礼物", "影视"]
        else:
            product_type_list.remove("程序")
    # 返回
    if len(product_type_list) < 4:
        return product_type_list
    else:
        product_type_list = random.sample(product_type_list, 4)
    return product_type_list


def start_etl_user_info():
    # 打开文件，追加a
    out = open(r'data_etl/user_info0310.csv', 'w', newline='', encoding='utf-8')
    # 设置写入模式
    csv_write = csv.writer(out, dialect='excel')
    csv_write.writerow([
        'user_id',
        'user_name',
        'user_age',
        'user_gender',
        'user_city',
        'user_city_type',
        'user_wage',
        'user_age_flag',
        'user_wage_flag',
        'user_action_start',
        'user_action_end',
        'product_type_list'
    ])
    # 读取用户数据
    user_info_csv = pd.read_csv('data/user_info.csv')
    # 遍历用户数据
    for user_index in user_info_csv.index:
        # 获取用户编号
        user_id = user_info_csv['user_id'][user_index]
        # 获取用户名称
        user_name = user_info_csv['user_name'][user_index]
        # 获取用户年龄
        user_age = int(user_info_csv['user_age'][user_index])
        # 获取用户性别
        user_gender = user_info_csv['user_gender'][user_index]
        # 获取用户城市
        user_city = user_info_csv['user_city'][user_index]
        # 获取用户城市类别
        user_city_type = user_info_csv['user_city_type'][user_index]
        # 获取哦用户工资
        user_wage = int(user_info_csv['user_wage'][user_index])
        # 获取年龄标签
        user_age_flag = get_user_gae_flag(user_age=user_age)
        # 获取用户收入标签
        user_wage_flag = get_user_wage_flag(user_wage=user_wage)
        # 获取用户访问时间限制
        user_action_hour = get_user_action_hour(user_age=user_age)
        user_action_start = user_action_hour[0]
        user_action_end = user_action_hour[1]
        # 获取用户可选择商品标签
        product_type_list = get_product_type_list(user_gender=user_gender, user_age=user_age, user_wage=user_wage)
        # 过滤数据
        if user_age is not None and user_wage is not None and user_gender is not None:
            # 构建用户数据
            user_info = [
                user_id,
                user_name,
                user_age,
                user_gender,
                user_city,
                user_city_type,
                user_wage,
                user_age_flag,
                user_wage_flag,
                user_action_start,
                user_action_end,
                product_type_list
            ]
            # 输出
            print(user_info)
            # 写入数据
            csv_write.writerow(user_info)
