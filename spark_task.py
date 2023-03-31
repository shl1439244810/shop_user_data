import findspark

findspark.init()
from pyspark.sql.types import *
from pyspark.sql import SparkSession

mysql_url = "jdbc:mysql://192.168.118.168:3306/shop2?serverTimezone=UTC&useUnicode=true&zeroDateTimeBehavior" \
            "=convertToNull&autoReconnect=true&characterEncoding=utf-8"

prop = {'user': 'root', 'password': 'root', 'driver': "com.mysql.jdbc.Driver"}

schema_action_info = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("user_age", IntegerType(), True),
    StructField("user_gender", StringType(), True),
    StructField("user_wage", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("user_type", StringType(), True),
    StructField("action", StringType(), True),
    StructField("action_score", IntegerType(), True),
    StructField("action_time", StringType(), True),
    StructField("action_range", StringType(), True)
])

schema_product_info = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("product_type", StringType(), True),
    StructField("user_type", StringType(), True)
])

schema_product_word_info = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("product_type", StringType(), True),
    StructField("user_type", StringType(), True),
    StructField("product_word", StringType(), True)
])

schema_system_user = StructType([
    StructField("userid", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("password", StringType(), True),
    StructField("realname", StringType(), True)
])

schema_user_info = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("user_age", StringType(), True),
    StructField("user_gender", StringType(), True),
    StructField("user_city", StringType(), True),
    StructField("user_city_type", StringType(), True),
    StructField("user_wage", StringType(), True),
    StructField("user_age_flag", StringType(), True),
    StructField("user_wage_flag", StringType(), True),
    StructField("user_action_start", StringType(), True),
    StructField("user_action_end", StringType(), True),
    StructField("product_type_list", StringType(), True)
])


def start_etl_spark():
    # 1.创建SparkSession
    spark = SparkSession.builder.appName("spark_etl").getOrCreate()
    # 2.读取本地数据集
    action_info = spark.read.option("header", True) \
        .csv('data_etl/action_info0307.csv', schema=schema_action_info)
    product_info = spark.read.option("header", True) \
        .csv('data_etl/product_info0307.csv', schema=schema_product_info)
    product_word_info = spark.read.option("header", True) \
        .csv('data_etl/product_word_info0307.csv', schema=schema_product_word_info)
    system_user = spark.read.option("header", True) \
        .csv('data_etl/system_user0307.csv', schema=schema_system_user)
    user_info = spark.read.option("header", True)\
        .csv('data_etl/user_info0307.csv', schema=schema_user_info)
    # 3.写入Mysql
    action_info.write.jdbc(mysql_url, 'action_info', 'overwrite', prop)
    product_info.write.jdbc(mysql_url, 'product_info', 'overwrite', prop)
    product_word_info.write.jdbc(mysql_url, 'product_word_info', 'overwrite', prop)
    system_user.write.jdbc(mysql_url, 'system_user', 'overwrite', prop)
    user_info.write.jdbc(mysql_url, 'user_info', 'overwrite', prop)
    # 4.将合并数据并保存到hdfs中
    action_info.write.mode("overwrite").options(header="true") \
        .csv("hdfs://192.168.118.166:50070/shop/action_info")
    product_info.write.mode("overwrite").options(header="true") \
        .csv("hdfs://192.168.118.166:50070/shop/product_info")
    product_word_info.write.mode("overwrite").options(header="true") \
        .csv("hdfs://192.168.118.166:50070/shop/product_word_info")
    # 5.读取创建表
    spark.read.option("header", True) \
        .csv("hdfs://192.168.118.166:50070/shop/action_info", schema=schema_action_info)\
        .createOrReplaceTempView("action_info")
    spark.read.option("header", True) \
        .csv("hdfs://192.168.118.166:50070/shop/product_info", schema=schema_action_info)\
        .createOrReplaceTempView("product_info")
    spark.read.option("header", True) \
        .csv("hdfs://192.168.118.166:50070/shop/product_word_info", schema=schema_action_info)\
        .createOrReplaceTempView("product_word_info")
    # 6.身份画像标签
    tag_sql1 = """
        select
            user_id,
            user_type
        from
            (
            select
                user_id,
                user_type,
                action_score,
                row_number()over(partition by user_id order by action_score desc) as rank
            from
                (
                select
                    user_id,
                    user_type,
                    sum(action_score) as action_score
                from
                    action_info
                group by
                    user_id,
                    user_type
                ) as t1
            ) as t2
        where
            rank <= 3
    """
    spark.sql(tag_sql1).write.jdbc(mysql_url, 'tag_user_type', 'overwrite', prop)
    # 7.消费能力标签
    tag_sql2 = """
        with t1 as (
            select
                user_id,
                counts,
                row_number() over(order by counts desc) as rank
            from
                (
                select
                    user_id,
                    count(1) as counts
                from
                    action_info
                where
                    action = '购买'
                group by
                    user_id
                ) as temp1 
        ),
        t2 as (
            select max(rank) as max_rank from t1
        )
        select
            t1.user_id,
            case when t1.rank <= t2.max_rank * 0.1 then '鲸鱼'
                 when t1.rank <= t2.max_rank * 0.3 then '海豚'
                 when t1.rank <= t2.max_rank * 0.5 then '虾米'
            else '普通' end as user_sale_type
        from
            t1
            cross join t2
    """
    spark.sql(tag_sql2).write.jdbc(mysql_url, 'tag_user_consumption', 'overwrite', prop)
    # 8.访问时间喜好标签
    tag_sql3 = """
        select
            user_id,
            case when action_range = '00~08时' then '凌晨访客'
                 when action_range = '08~13时' then '上午访客'
                 when action_range = '13-20时' then '下午访客'
            else '晚上访客' end as user_time_type
        from
            (
            select
                user_id,
                action_range,
                row_number()over(partition by user_id order by counts desc) as rank
            from
                (
                select
                    user_id,
                    action_range,
                    count(1) as counts
                from
                    action_info
                group by
                    user_id,
                    action_range
                ) as temp
            ) as t1
        where
            rank = 1
    """
    spark.sql(tag_sql3).write.jdbc(mysql_url, 'tag_user_time', 'overwrite', prop)
    # 9.释放资源
    spark.stop()

