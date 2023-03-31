import csv
import random
import time
import traceback
from lxml import etree
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# 不加载图片
options = webdriver.ChromeOptions()
options.add_experimental_option('prefs', {'profile.managed_default_content_settings.images': 2})
browser = webdriver.Chrome(options=options, executable_path='driver/chromedriver.exe')
# 设置等待时间
wait = WebDriverWait(browser, 10)
# 设置全局变量用来存储数据
data_list = []
# 打开文件，追加a
global csv_write


def search(product_type, user_type):
    browser.get('https://www.jd.com/')
    try:
        # 等到搜索框加载出来
        search_box = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#key")))
        # 等到搜索按钮可以被点击
        action1 = "#search > div > div.form > button"
        submit = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, action1)))
        # 向搜索框内输入关键词
        search_box[0].send_keys(product_type)
        # 点击
        submit.click()
        # 记录一下总页码,等到总页码加载出来
        action2 = "#J_bottomPage > span.p-skip > em:nth-child(1) > b"
        total = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, action2)))
        time.sleep(5)
        # 获取网页信息
        html = browser.page_source
        # 调用提取数据的函数
        parse_html(html, product_type, user_type)
        # 返回总页数
        return total[0].text
    except StaleElementReferenceException:
        search(product_type, user_type)
    except TimeoutError:
        search(product_type, user_type)
    except TimeoutException:
        search(product_type, user_type)


def next_page(page_number, product_type, user_type):
    # 设置进入页面加载速度为1S
    browser.set_page_load_timeout(3)
    try:
        # 滑动到底部
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # 设置随机延迟
        time.sleep(random.randint(5, 10))
        # 翻页按钮
        action1 = "#J_bottomPage > span.p-num > a.pn-next > em"
        button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, action1)))
        # 翻页动作
        button.click()
        # 等到30个商品都加载出来
        action2 = "#J_goodsList > ul > li:nth-child(30)"
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, action2)))
        # 滑动到底部，加载出后三十个货物信息
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # 等到60个商品都加载出来
        action3 = "#J_goodsList > ul > li:nth-child(60)"
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, action3)))
        # 判断翻页成功,高亮的按钮数字与设置的页码一样
        action4 = "#J_bottomPage > span.p-num > a.curr"
        wait.until(EC.text_to_be_present_in_element((By.CSS_SELECTOR, action4), str(page_number)))
        time.sleep(3)
        # 获取网页信息
        html = browser.page_source
        # 调用提取数据的函数
        parse_html(html, product_type, user_type)
    except Exception as e1:
        traceback.print_exc()


# 检查商品名称，如果不能获取，则返回None
def check_phone_name(li):
    try:
        phone_name_value = li.find_element_by_xpath('.//div[@class="p-name p-name-type-2"]//a//em').text
        return phone_name_value.replace('\r', '').replace('\n', '')
    except:
        return 'None'


# 检查详情URL是否有效，无效返回NONE
def check_detail_url(li):
    try:
        detail_rul = li.find_element_by_xpath('.//div[@class="p-name p-name-type-2"]//a').get_attribute('href')
        return detail_rul
    except:
        return 'None'


# 初始化标签请求接口
def get_flag_url(phone_id):
    return "https://club.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98&productId={}&score=0" \
           "&sortType=5&page=0&pageSize=10&isShadowSku=0&fold=1".format(phone_id)


# 开始解析HTML
def parse_html(html, product_type, user_type):
    global csv_write
    html = etree.HTML(html)
    # 开始提取信息,找到ul标签下的全部li标签
    try:
        lis = browser.find_elements_by_class_name('gl-item')
        # 遍历
        for li in lis:
            try:
                # 获取商品标题
                product_name = check_phone_name(li)
                # 获取详情页地址
                detail_rul = check_detail_url(li)
                if product_name != 'None' and detail_rul != 'None' and (not ('二手' in product_name or '拍拍' in product_name)):
                    # 获取商品价格
                    product_price = float(li.find_element_by_xpath('.//div[@class="p-price"]//i').text)
                    # 商品编号
                    product_id = detail_rul.replace("https://item.jd.com/", "").replace(".html", "")
                    # 对数据进行过滤
                    if product_name is not None and product_price is not None and product_type is not None:
                        # 创建对象
                        phone_info_data = [product_id, product_name, product_price, product_type, user_type]
                        # 输出对象信息
                        print(phone_info_data)
                        # 写入对象
                        csv_write.writerow(phone_info_data)
            except Exception as e1:
                continue
    except TimeoutError:
        parse_html(html, product_type, user_type)


def start_etl_product_info():
    global csv_write
    # 打开文件，追加a
    out = open(r"data_etl/product_info0310.csv", 'w', newline='', encoding='utf-8')
    csv_write = csv.writer(out, dialect='excel')
    csv_write.writerow(['product_id', 'product_name', 'product_price', 'product_type', 'user_type'])
    # 初始化商品品牌
    product_type_list = ["时尚服装", "美食", "娱乐", "宝妈", "程序", "退休礼物", "影视", "健身"]
    user_type_list = ["时尚达人", "饮食男女", "娱乐至上", "全职宝妈", "IT精英", "功成退休", "影视星迷", "健身达人"]
    print("1.爬取商品数据")
    for index in range(len(product_type_list)):
        product_type = product_type_list[index]
        user_type = user_type_list[index]
        print(product_type, user_type)
        total = int(search(product_type, user_type))
        for i in range(1, 11):
            time.sleep(random.randint(1, 3))
            print("第", i, "页：")
            next_page(i, product_type, user_type)
    browser.quit()
