"""
Regularity Analyzer Task
# 1. Regularity analysis
# 1-1. If ts is regular, calc period
# 2. Save regularity and period to DWH (Now Hive concrete)
# 3. todo: Propagate report
"""

from func import *
from pyspark.sql import SparkSession
from univariate.analyzer import AnalysisReport

if __name__ == "__main__":
    # Initialize app
    app_conf = get_conf_from_evn()

    SparkSession.builder.config(
        "spark.hadoop.hive.exec.dynamic.partition", "true"
    ).config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

    # [AICNS-61]
    if app_conf["SPARK_EXTRA_CONF_PATH"] != "":
        config_dict = parse_spark_extra_conf(app_conf)
        for conf in config_dict.items():
            SparkSession.builder.config(conf[0], conf[1])

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    app_conf["sql_context"] = spark

    # Get feature metadata
    sensor = get_feature_metadata(app_conf)
    data_col_name = (
        "input_data"  # todo: metadata concern or strict validation column names
    )
    time_col_name = "event_time"

    # Load data  # todo: will validated data go dwh?
    ts = load_raw_data(app_conf, sensor, time_col_name, data_col_name)

    # Analyze regularity
    report: AnalysisReport = analyze_regularity(ts=ts, time_col_name=time_col_name)

    # Save regularity and period to DWH
    save_regularity_to_dwh(regularity_report=report)

    # todo: store offline report

    spark.stop()
