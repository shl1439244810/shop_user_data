from etl_action_info import start_etl_action_info
from etl_product_info import start_etl_product_info
from etl_product_word_info import start_etl_product_word_info
from etl_system_user import start_etl_system_user
from etl_user_info import start_etl_user_info
from spark_task import start_etl_spark

if __name__ == '__main__':
    # 1.爬取商品信息
    # start_etl_product_info()
    # 2.对用户信息进行清洗
    # start_etl_user_info()
    # 3.对商品信息进行分词
    # start_etl_product_word_info()
    # 4.对用户行为进行清洗
    # start_etl_action_info()
    # 5.构建系统用户
    # start_etl_system_user()
    # 读取数据进行计算
    start_etl_spark()
