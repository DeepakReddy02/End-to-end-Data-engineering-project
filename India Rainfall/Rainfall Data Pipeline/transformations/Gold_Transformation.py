RainDF = spark.read.format("parquet").load(
    "/Volumes/workspace/rainfall_data/silver_layer/RainFall_data/"
)

RainDF = RainDF.dropDuplicates()

State_Table = RainDF.select(
    "Country",
    "State",
    "State lgd code",
    "District",
    "District lgd code"
).distinct().withColumnRenamed(
    "State lgd code", "State_Code"
).withColumnRenamed(
    "District lgd code", "District_Code"
).orderBy(
    "State_Code", "District_Code"
)

Rain_Fact_Table = RainDF.select(
    "State lgd code",
    "District lgd code",
    "Year",
    "Calendar Day",
    "Daily category",
    "Weekly date",
    "Weekly category",
    "Cumulative date",
    "Cumulative category",
    "Monthly date",
    "Monthly category",
    "Daily actual_avg",
    "Daily normal_avg",
    "Percentage of daily departure_avg",
    "Weekly actual_avg",
    "Weekly normal_avg",
    "Percentage of weekly departure_avg",
    "Cumulative actual_avg",
    "Cumulative normal_avg",
    "Percentage of cumulative departure_avg",
    "Monthly acutual_avg",
    "Monthly normal_avg",
    "Percentage of monthly departure_avg"
).withColumnRenamed(
    "State lgd code", "State_Code"
).withColumnRenamed(
    "District lgd code", "District_Code"
).withColumnRenamed(
    "Calendar Day", "Calendar_Day"
).withColumnRenamed(
    "Daily category", "Daily_category"
).withColumnRenamed(
    "Weekly date", "Weekly_date"
).withColumnRenamed(
    "Weekly category", "Weekly_category"
).withColumnRenamed(
    "Cumulative date", "Cumulative_date"
).withColumnRenamed(
    "Cumulative category", "Cumulative_category"
).withColumnRenamed(
    "Monthly date", "Monthly_date"
).withColumnRenamed(
    "Monthly category", "Monthly_category"
).withColumnRenamed(
    "Daily actual_avg", "Daily_actual"
).withColumnRenamed(
    "Daily normal_avg", "Daily_normal"
).withColumnRenamed(
    "Percentage of daily departure_avg", "Percentage_of_daily_departure"
).withColumnRenamed(
    "Weekly actual_avg", "Weekly_actual"
).withColumnRenamed(
    "Weekly normal_avg", "Weekly_normal"
).withColumnRenamed(
    "Percentage of weekly departure_avg", "Percentage_of_weekly_departure"
).withColumnRenamed(
    "Cumulative actual_avg", "Cumulative_actual"
).withColumnRenamed(
    "Cumulative normal_avg", "Cumulative_normal"
).withColumnRenamed(
    "Percentage of cumulative departure_avg", "Percentage_of_cumulative_departure"
).withColumnRenamed(
    "Monthly acutual_avg", "Monthly_acutual"
).withColumnRenamed(
    "Monthly normal_avg", "Monthly_normal"
).withColumnRenamed(
    "Percentage of monthly departure_avg", "Percentage_of_monthly_departure"
)

State_Table.write.mode("overwrite").saveAsTable("workspace.rainfall_data.State_Table")
Rain_Fact_Table.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("workspace.rainfall_data.Rain_Fact_Table")