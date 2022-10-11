"""
Function level adapters
"""
from rawDataLoader import RawDataLoader, DatePartitionedRawDataLoader
import os
import pendulum
from utils.feature_util import mongo_to_dict
from featureMetadataFetcher import FeatureMetadataFetcher, SensorMetadataFetcher
from univariate.analyzer import AnalysisReport, RegularityAnalyzer, Analyzer
from univariate.strategy.period import PeriodCalcType
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

__all__ = ['get_feature_metadata', 'get_conf_from_evn', 'parse_spark_extra_conf', 'load_raw_data', 'analyze_regularity', 'save_regularity_to_dwh']


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

        conf["SPARK_EXTRA_CONF_PATH"] = os.getenv("SPARK_EXTRA_CONF_PATH", default="")  # [AICNS-61]
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
        config_dict = dict(list(filter(lambda splited: len(splited) == 2, (map(lambda line: line.split(), lines)))))
    return config_dict


def load_raw_data(app_conf, feature, time_col_name, data_col_name):
    loader: RawDataLoader = DatePartitionedRawDataLoader()
    loader.prepare_to_load(**app_conf)
    feature_raw_df = (
        loader.load_feature_data_by_object(
            start=app_conf["start"], end=app_conf["end"], feature=feature
        )
        .select(time_col_name, data_col_name)
        .sort(time_col_name)
    )
    return feature_raw_df


def analyze_regularity(ts: DataFrame, time_col_name: str) -> AnalysisReport:
    """

    :return:
    """
    analyzer: Analyzer = RegularityAnalyzer(period_strategy_type=PeriodCalcType.ClusteringAndApproximateGCD)  # todo: who has responsibility about automated dependency injection
    report: AnalysisReport = analyzer.analyze(ts=ts, time_col_name=time_col_name)
    return report


def save_regularity_to_dwh(ts: DataFrame, time_col_name: str, regularity_report: AnalysisReport, app_conf):
    """

    :return:
    """
    regularity = regularity_report.parameters["regularity"]
    period = None
    periodic_error = None
    if regularity == "regular":
        period = regularity_report.parameters["period"]
        periodic_error = regularity_report.parameters["periodic_error"]
    millis = ts.orderBy(F.col(time_col_name).desc()).first()[0] - ts.select(time_col_name).first()[0]
    cnt = ts.count()

    table_name = "regularity_" + app_conf["FEATURE_ID"]
    SparkSession.getActiveSession().sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} (regularity CHAR (9), period DOUBLE, periodic_error DOUBLE, start_date DATE, end_date DATE, sample_duration_millisec INT, sample_size INT) STORED AS PARQUET")

    SparkSession.getActiveSession().sql(f"INSERT INTO {table_name} VALUES ({regularity}, {period}, {periodic_error}, '{app_conf['start'].format('YYYY-MM-DD')}', '{app_conf['end'].format('YYYY-MM-DD')}', {millis}, {cnt})")


def propagate_regularity_report():
    """
    # todo:
    :return:
    """
    pass
