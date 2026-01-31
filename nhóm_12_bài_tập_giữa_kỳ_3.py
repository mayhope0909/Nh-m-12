
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://loint22406_db_user:175200@cluster175.qdjbnkk.mongodb.net/?appName=Cluster175"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Re-using the URI that successfully connected in the previous cell
uri = "mongodb+srv://loint22406_db_user:175200@cluster175.qdjbnkk.mongodb.net/?appName=Cluster175"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Access the database named 'dtb'
db = client["dtb"]

# Access the collection named 'dtb'
collection = db["dtb"]

# Query the first 100 documents in the collection
documents = list(collection.find({}).limit(100))

# Print the retrieved documents
if documents:
    print(f"Found {len(documents)} documents in the 'dtb.dtb' collection:")
    for doc in documents:
        print(doc)
else:
    print("No documents found in the 'dtb.dtb' collection.")

"""Preview dữ liệu từ MongoDB"""

import pandas as pd

# Convert the list of documents to a Pandas DataFrame
df_documents = pd.DataFrame(documents)



"""**CÂU 2. VAEX XÂY DỰNG ỨNG DỤNG PHÂN TÍCH DATASET**"""

import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://loint22406_db_user:175200@cluster175.qdjbnkk.mongodb.net/?appName=Cluster175"

client = MongoClient(uri, server_api=ServerApi('1'))
db = client["dtb"]
collection = db["dtb"]

documents = list(collection.find({}, {"_id": 0}))

df = pd.DataFrame(documents)
print(df.shape)
df.head()

df["Zipcode"] = df["Zipcode"].astype(str)
df.to_parquet("dtb_data.parquet")
print("✅ Đã xuất dữ liệu sang Parquet")


import vaex

dfv = vaex.open("dtb_data.parquet")

"""Phân tích mức độ tai nạn - số lượng tai nạn theo mức độ nghiêm trọng"""

severity_stats = dfv.groupby("Severity", agg=vaex.agg.count())
severity_stats

"""Phân tích tai nạn theo thời gian (năm)"""

dfv['Start_Time'] = dfv['Start_Time'].astype('datetime64')
dfv['Year'] = dfv['Start_Time'].dt.year

accidents_per_year = dfv.groupby("Year", agg=vaex.agg.count()).sort("Year")
accidents_per_year

""" Phân tích ảnh hưởng ngày / đêm"""

day_night_stats = dfv.groupby("Sunrise_Sunset", agg=vaex.agg.count())


"""Phân tích khoảng cách ảnh hưởng của tai nạn"""

dfv.mean(dfv["Distance(mi)"]), dfv.max(dfv["Distance(mi)"])

"""Phân tích vai trò của hạ tầng giao thông"""

# Tai nạn tại khu vực có đèn giao thông
traffic_signal_stats = dfv.groupby("Traffic_Signal", agg=vaex.agg.count())
traffic_signal_stats

# Tai nạn tại khu vực có biển Stop
stop_stats = dfv.groupby("Stop", agg=vaex.agg.count())
stop_stats
