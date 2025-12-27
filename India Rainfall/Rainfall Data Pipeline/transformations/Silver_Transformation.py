from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, split
from pyspark.sql.types import StructType, StructField, DoubleType, LongType


print("Silver Transformation Start")

Rain_Data = spark.read.parquet("/Volumes/workspace/rainfall_data/bronze_layer/RainFall_data.parquet")
Rain_Header = spark.read.parquet("/Volumes/workspace/rainfall_data/bronze_layer/RainFall_header.parquet")

#droping duplicates from data
Rain_Data = Rain_Data.dropDuplicates()
Rain_Header = Rain_Header.dropDuplicates()

def Column_Rename(Prefix:dict,Data_Table:DataFrame)->DataFrame:
    df_renamed = Data_Table

    for old, new in Prefix.items():
        df_renamed = df_renamed.withColumnRenamed(old, new)
    return df_renamed

def dict_to_Col(df:DataFrame,prefix_map: dict = None)->DataFrame:
    result_df = df
    
    # Automatically find all STRUCT type columns
    struct_columns = [field.name for field in df.schema.fields 
                      if isinstance(field.dataType, StructType)]
    
    # Process each struct column
    for col_name in struct_columns:
        # Determine the prefix for this column
        if prefix_map and col_name in prefix_map:
            prefix = prefix_map[col_name]
        else:
            prefix = col_name
        
        # Get all field names within the struct
        struct_fields = df.schema[col_name].dataType.fieldNames()
        
        # Create new columns for each struct field
        for struct_field in struct_fields:
            result_df = result_df.withColumn(
                f"{prefix}_{struct_field}",
                result_df[col_name][struct_field]
            )
        
        # Drop the original column
        result_df = result_df.drop(col_name)
    
    return result_df

prefix_map = Rain_Header.select("ID", "DisplayName").toPandas().set_index("ID")["DisplayName"].to_dict()

df = Column_Rename(prefix_map,Rain_Data)

df = dict_to_Col(df,prefix_map=prefix_map)

df = df.withColumn("Year", split(col("Year"), ",").getItem(1).cast('int'))

print("Silver Transformation End")

df.write.mode("overwrite").parquet("/Volumes/workspace/rainfall_data/silver_layer/RainFall_data")
Rain_Header.write.csv("/Volumes/workspace/rainfall_data/silver_layer/RainFall_header", mode="overwrite")

print("Silver Tables saved")