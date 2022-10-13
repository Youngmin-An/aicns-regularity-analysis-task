"""
Function level adapters
"""
import os
import pendulum
from utils.feature_util import mongo_to_dict
from featureMetadataFetcher import FeatureMetadataFetcher, SensorMetadataFetcher
from univariate.analyzer import AnalysisReport, RegularityAnalyzer, Analyzer
from univariate.strategy.period import PeriodCalcType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    FloatType,
    IntegerType,
    StringType,
)
import pyspark.sql.functions as F
import logging

__all__ = [
    "get_feature_metadata",
    "get_conf_from_evn",
    "parse_spark_extra_conf",
    "load_validated_data",
    "analyze_regularity",
    "save_regularity_to_dwh",
]

logger = logging.getLogger()


def get_feature_metadata(app_conf):
    fetcher: FeatureMetadataFetcher = SensorMetadataFetcher()
    fetcher.get_or_create_conn(app_conf)
    sensors, positions = fetcher.fetch_metadata()
    sensor = next(
        filter(lambda sensor: sensor.ss_id == int(app_conf["FEATURE_ID"]), sensors)
    )  # todo: AICNS-33
    print("sensor: ", mongo_to_dict(sensor))
    return sensor


def get_conf_from_evn():
    """
        Get conn info from env variables
    :return:
    """
    conf = dict()
    try:
        # Feature id
        conf["FEATURE_ID"] = os.getenv("FEATURE_ID")
        # Metadata
        conf["METADATA_HOST"] = os.getenv("METADATA_HOST")
        conf["METADATA_PORT"] = os.getenv("METADATA_PORT")
        conf["METADATA_TYPE"] = os.getenv("METADATA_TYPE", default="sensor")
        conf["METADATA_BACKEND"] = os.getenv("METADATA_BACKEND", default="MongoDB")
        # Data source
        conf["SOURCE_HOST"] = os.getenv("SOURCE_HOST")
        conf["SOURCE_PORT"] = os.getenv("SOURCE_PORT")
        conf["SOURCE_DATA_PATH_PREFIX"] = os.getenv(
            "SOURCE_DATA_PATH_PREFIX", default=""
        )
        conf["SOURCE_BACKEND"] = os.getenv("SOURCE_BACKEND", default="HDFS")
        # Raw data period
        start_datetime = os.getenv("APP_TIME_START")  # yyyy-MM-dd'T'HH:mm:ss
        end_datetime = os.getenv("APP_TIME_END")  # yyyy-MM-dd'T'HH:mm:ss
        conf["APP_TIMEZONE"] = os.getenv("APP_TIMEZONE", default="UTC")

        conf["SPARK_EXTRA_CONF_PATH"] = os.getenv(
            "SPARK_EXTRA_CONF_PATH", default=""
        )  # [AICNS-61]
        conf["start"] = pendulum.parse(start_datetime).in_timezone(conf["APP_TIMEZONE"])
        conf["end"] = pendulum.parse(end_datetime).in_timezone(conf["APP_TIMEZONE"])

        # todo: temp patch for day resolution parsing, so later with [AICNS-59] resolution will be subdivided.
        conf["end"] = conf["end"].subtract(minutes=1)

    except Exception as e:
        print(e)
        raise e
    return conf


def parse_spark_extra_conf(app_conf):
    """
    Parse spark-default.xml style config file.
    It is for [AICNS-61] that is spark operator take only spark k/v confs issue.
    :param app_conf:
    :return: Dict (key: conf key, value: conf value)
    """
    with open(app_conf["SPARK_EXTRA_CONF_PATH"], "r") as cf:
        lines = cf.read().splitlines()
        config_dict = dict(
            list(
                filter(
                    lambda splited: len(splited) == 2,
                    (map(lambda line: line.split(), lines)),
                )
            )
        )
    return config_dict


def load_validated_data(app_conf, time_col_name, data_col_name) -> DataFrame:
    """
    Validated data from DWH(Hive)
    :param app_conf:
    :param feature:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    table_name = "validated_" + app_conf['FEATURE_ID']
    SparkSession.getActiveSession().sql(f"REFRESH TABLE validated_{table_name}")
    query = f'''
    SELECT v.{time_col_name}, v.{data_col_name}  
        FROM (
            SELECT {time_col_name}, {data_col_name}, concat(concat(cast(year as string), lpad(cast(month as string), 2, '0')), lpad(cast(day as string), 2, '0')) as date 
            FROM {table_name}
            ) v 
        WHERE v.date  >= {app_conf['start'].format('YYYYMMDD')} AND v.date <= {app_conf['end'].format('YYYYMMDD')} 
    '''
    logger.info("load_validated_data query: " + query)
    ts = SparkSession.getActiveSession().sql(query)
    logger.info(ts.show())
    return ts.sort(F.col(time_col_name).desc())


def analyze_regularity(ts: DataFrame, time_col_name: str) -> AnalysisReport:
    """

    :return:
    """
    analyzer: Analyzer = RegularityAnalyzer(
        period_strategy_type=PeriodCalcType.ClusteringAndApproximateGCD
    )  # todo: who has responsibility about automated dependency injection
    report: AnalysisReport = analyzer.analyze(ts=ts, time_col_name=time_col_name)
    return report


def save_regularity_to_dwh(
    ts: DataFrame, time_col_name: str, regularity_report: AnalysisReport, app_conf
):
    """

    :return:
    """
    regularity = regularity_report.parameters["regularity"]
    period = None
    periodic_error = None
    if regularity == "regular":
        period = regularity_report.parameters["period"]
        periodic_error = regularity_report.parameters["periodic_error"]
    cnt = ts.count()
    millis = (
        ts.orderBy(F.col(time_col_name).desc()).first()[0]
        - ts.select(time_col_name).first()[0]
    ) if cnt > 0 else None

    table_name = "regularity_" + app_conf["FEATURE_ID"]
    SparkSession.getActiveSession().sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} (regularity CHAR (9), period DOUBLE, periodic_error DOUBLE, start_date DATE, end_date DATE, sample_duration_millisec INT, sample_size INT) STORED AS PARQUET"
    )

    schema = StructType(
        [
            StructField("reglarity", StringType(), True),
            StructField("period", FloatType(), True),
            StructField("periodic_error", FloatType(), True),
            StructField("start", StringType(), True),
            StructField("end", StringType(), True),
            StructField("sample_duration_millisec", IntegerType(), True),
            StructField("sample_size", IntegerType(), True),
        ]
    )
    data = [
        (
            regularity,
            period,
            periodic_error,
            app_conf["start"].format("YYYY-MM-DD"),
            app_conf["end"].format("YYYY-MM-DD"),
            millis,
            cnt,
        )
    ]
    df = SparkSession.getActiveSession().createDataFrame(data=data, schema=schema)
    df = df.select(
        "reglarity",
        "period",
        "periodic_error",
        F.to_date("start").alias("start_date"),
        F.to_date("end").alias("end_date"),
        "sample_duration_millisec",
        "sample_size",
    )
    df.write.option("nullValue", None).format("hive").insertInto(table_name)


def propagate_regularity_report():
    """
    # todo:
    :return:
    """
    pass
