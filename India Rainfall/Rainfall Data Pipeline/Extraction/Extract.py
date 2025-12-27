%pip install python-dotenv
import requests
from pyspark.sql.types import *
from datetime import datetime
import os
from dotenv import load_dotenv

metric_struct = StructType([
    StructField("avg", DoubleType(), True),
    StructField("max", DoubleType(), True),
    StructField("min", DoubleType(), True),
    StructField("sum", DoubleType(), True),
    StructField("count", IntegerType(), True),
    StructField("stddev", DoubleType(), True),
    StructField("LandAreaWeight", DoubleType(), True),
    StructField("TotalPopulationWeight", DoubleType(), True),
    StructField("NumberOfHouseholdsWeight", DoubleType(), True),
    StructField("TotalPopulationMaleWeight", DoubleType(), True),
    StructField("TotalPopulationFemaleWeight", DoubleType(), True)
])

# Full schema
Data_schema = StructType([
    StructField("Country", StringType(), True),
    StructField("StateName", StringType(), True),
    StructField("StateCode", IntegerType(), True),
    StructField("DistrictName", StringType(), True),
    StructField("DistrictCode", IntegerType(), True),
    StructField("Year", StringType(), True),
    StructField("CalendarDay", StringType(), True),
    StructField("D7319_1", StringType(), True),
    StructField("D7319_9", StringType(), True),
    StructField("D7319_10", StringType(), True),
    StructField("D7319_14", StringType(), True),
    StructField("D7319_15", StringType(), True),
    StructField("D7319_19", StringType(), True),
    StructField("D7319_20", StringType(), True),
    StructField("D7319_24", StringType(), True),

    StructField("I7319_6", metric_struct, True),
    StructField("I7319_7", metric_struct, True),
    StructField("I7319_8", metric_struct, True),
    StructField("I7319_11", metric_struct, True),
    StructField("I7319_12", metric_struct, True),
    StructField("I7319_13", metric_struct, True),
    StructField("I7319_16", metric_struct, True),
    StructField("I7319_17", metric_struct, True),
    StructField("I7319_18", metric_struct, True),
    StructField("I7319_21", metric_struct, True),
    StructField("I7319_22", metric_struct, True),
    StructField("I7319_23", metric_struct, True)
])

load_dotenv()

page = 1
base_url = os.getenv("URL")
if base_url is not None:
    API_KEY = os.getenv("API_KEY")
    DIM_COL = os.getenv("Dim_Col")
    FACT_COL = os.getenv("Fact_Col")
    
else:
    raise ValueError("URL is not set in the environment variables.")

parameters = {
    "API_Key":API_KEY,
    "ind":FACT_COL,
    "dim":DIM_COL
}
page_data = []
header_data = []
start_date = datetime.strptime("2025-12-26", "%Y-%m-%d").date()
CurrentDate = datetime.now().date()

print("Extraction Start")
while True:
    response = requests.get(base_url+f"?pageno={page}",params=parameters)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        break

    data = response.json()
    Header_API_Data = data.get('Headers', {}).get('Items', [])
    Page_API_Data = data.get('Data', [])

    if not Page_API_Data:
        print("No more pages.")
        break

    for row in Page_API_Data:
        cd = row.get("CalendarDay")
        if cd:
            cd_date = datetime.strptime(cd, "%Y-%m-%d").date()
            if start_date <= cd_date <= CurrentDate:
                    print("Included")
                    page_data.append(row)
                    header_data.extend(Header_API_Data)
            else:
                pass
    
    page += 1
    print(page)

print("Extraction End")

spark.createDataFrame(page_data,schema=Data_schema).write.mode("append").parquet("/Volumes/workspace/rainfall_data/bronze_layer/RainFall_data.parquet")
spark.createDataFrame(header_data).write.mode("append").parquet("/Volumes/workspace/rainfall_data/bronze_layer/RainFall_data.parquet")

print("Data written to parquet")