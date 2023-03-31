import re
import csv
import jieba
import pandas as pd


def stop_words_list():
    stopwords = [line.strip() for line in open('stopwords/stopwords.txt', encoding='UTF-8').readlines()]
    return stopwords


def get_cut_word(sentence, stopwords):
    # 1.定义接收分词后的评论
    result_word_list = []
    # 2.对文档中的每一行进行中文分词
    sentence_depart = jieba.cut("".join(re.compile('[^\u4e00-\u9fa5]').split(sentence.strip())).strip())
    # 3.去停用词
    for word in sentence_depart:
        if word not in stopwords:
            result_word_list.append(word)
    # 4.返回分词后的列表
    return result_word_list


def start_etl_product_word_info():
    stopwords = stop_words_list()
    # 打开文件，追加a
    out = open(r'data_etl/product_word_info0310.csv', 'w', newline='', encoding='utf-8')
    # 设置写入模式
    csv_write = csv.writer(out, dialect='excel')
    csv_write.writerow([
        'product_id',
        'product_name',
        'product_price',
        'product_type',
        'user_type',
        'product_word']
    )
    # 读取商品信息
    product_info_csv = pd.read_csv('data_etl/product_info0310.csv')
    # 遍历用户数据
    for product_index in product_info_csv.index:
        # 获取商品编号
        product_id = product_info_csv['product_id'][product_index]
        # 获取商品名称
        product_name = product_info_csv['product_name'][product_index]
        # 获取商品价格
        product_price = product_info_csv['product_price'][product_index]
        # 获取商品类型
        product_type = product_info_csv['product_type'][product_index]
        # 获取用户标签
        user_type = product_info_csv['user_type'][product_index]
        # 遍历商品信息
        for product_word in get_cut_word(sentence=product_name, stopwords=stopwords):
            # 构建对象
            product_word_data = [
                product_id,
                product_name,
                product_price,
                product_type,
                user_type,
                product_word
            ]
            # 输出
            print(product_word_data)
            # 写入数据
            csv_write.writerow(product_word_data)
