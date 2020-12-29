import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = "SimHei"  # 解决中文乱码问题

df_train = pd.read_csv(r'D:\ubuntu_share\data_format1\train_format1.csv')
df_test = pd.read_csv(r'D:\ubuntu_share\data_format1\test_format1.csv')
user_info = pd.read_csv(r'D:\ubuntu_share\data_format1\user_info_format1.csv')
user_log = pd.read_csv(r'D:\ubuntu_share\data_format1\user_log_format1.csv')

user_info['age_range'].replace(0.0, np.nan, inplace=True)
user_info['gender'].replace(2.0, np.nan, inplace=True)

# user_info = user_info.dropna()
# user_info.info()
# user_log = user_log.dropna()

'''
特征建立

需要根据user_id，和merchant_id（seller_id）,从用户画像表以及用户日志表中提取特征，填写到df_train这个数据框中，从而训练评估模型 需要建立的特征如下：

用户的年龄(age_range)
用户的性别(gender)
某用户在该商家日志的总条数(total_logs)
用户浏览的商品的数目，就是浏览了多少个商品(unique_item_ids)
浏览的商品的种类的数目，就是浏览了多少种商品(categories)
用户浏览的天数(browse_days)
用户单击的次数(one_clicks)
用户添加购物车的次数(shopping_carts)
用户购买的次数(purchase_times)
用户收藏的次数(favourite_times)

'''

# age_range,gender特征添加
df_train = pd.merge(df_train, user_info, on="user_id", how="left")

# total_logs特征添加
total_logs_temp = user_log.groupby([user_log["user_id"], user_log["seller_id"]]).count().reset_index()[
    ["user_id", "seller_id", "item_id"]]
total_logs_temp.rename(columns={"seller_id": "merchant_id", "item_id": "total_logs"}, inplace=True)
df_train = pd.merge(df_train, total_logs_temp, on=["user_id", "merchant_id"], how="left")

# unique_item_ids特征添加
unique_item_ids_temp = \
user_log.groupby([user_log["user_id"], user_log["seller_id"], user_log["item_id"]]).count().reset_index()[
    ["user_id", "seller_id", "item_id"]]
unique_item_ids_temp1 = unique_item_ids_temp.groupby(
    [unique_item_ids_temp["user_id"], unique_item_ids_temp["seller_id"]]).count().reset_index()
unique_item_ids_temp1.rename(columns={"seller_id": "merchant_id", "item_id": "unique_item_ids"}, inplace=True)
df_train = pd.merge(df_train, unique_item_ids_temp1, on=["user_id", "merchant_id"], how="left")

# categories特征构建
categories_temp = \
user_log.groupby([user_log["user_id"], user_log["seller_id"], user_log["cat_id"]]).count().reset_index()[
    ["user_id", "seller_id", "cat_id"]]
categories_temp1 = categories_temp.groupby(
    [categories_temp["user_id"], categories_temp["seller_id"]]).count().reset_index()
categories_temp1.rename(columns={"seller_id": "merchant_id", "cat_id": "categories"}, inplace=True)
df_train = pd.merge(df_train, categories_temp1, on=["user_id", "merchant_id"], how="left")

# browse_days特征构建
browse_days_temp = \
user_log.groupby([user_log["user_id"], user_log["seller_id"], user_log["time_stamp"]]).count().reset_index()[
    ["user_id", "seller_id", "time_stamp"]]
browse_days_temp1 = browse_days_temp.groupby(
    [browse_days_temp["user_id"], browse_days_temp["seller_id"]]).count().reset_index()
browse_days_temp1.rename(columns={"seller_id": "merchant_id", "time_stamp": "browse_days"}, inplace=True)
df_train = pd.merge(df_train, browse_days_temp1, on=["user_id", "merchant_id"], how="left")

# one_clicks、shopping_carts、purchase_times、favourite_times特征构建
one_clicks_temp = \
user_log.groupby([user_log["user_id"], user_log["seller_id"], user_log["action_type"]]).count().reset_index()[
    ["user_id", "seller_id", "action_type", "item_id"]]
one_clicks_temp.rename(columns={"seller_id": "merchant_id", "item_id": "times"}, inplace=True)
one_clicks_temp["one_clicks"] = one_clicks_temp["action_type"] == 0
one_clicks_temp["one_clicks"] = one_clicks_temp["one_clicks"] * one_clicks_temp["times"]
one_clicks_temp["shopping_carts"] = one_clicks_temp["action_type"] == 1
one_clicks_temp["shopping_carts"] = one_clicks_temp["shopping_carts"] * one_clicks_temp["times"]
one_clicks_temp["purchase_times"] = one_clicks_temp["action_type"] == 2
one_clicks_temp["purchase_times"] = one_clicks_temp["purchase_times"] * one_clicks_temp["times"]
one_clicks_temp["favourite_times"] = one_clicks_temp["action_type"] == 3
one_clicks_temp["favourite_times"] = one_clicks_temp["favourite_times"] * one_clicks_temp["times"]
four_features = one_clicks_temp.groupby(
    [one_clicks_temp["user_id"], one_clicks_temp["merchant_id"]]).sum().reset_index()
four_features = four_features.drop(["action_type", "times"], axis=1)
df_train = pd.merge(df_train, four_features, on=["user_id", "merchant_id"], how="left")

'''
建立好特征的缺失值处理

'''
# 缺失值向前填充
df_train = df_train.fillna(method='ffill')

# 保存到本地
df_train.to_csv(r'D:\ubuntu_share\df_train_noindex.csv', sep=',', header=True, index=False)
