from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from typing import List
import argparse


def read_input(spark: SparkSession, inputPaths: List[str]) -> DataFrame:
    """
    Reads input events.
    Parses the user.token json string and exposes its fields as
    dataframe columns under parsedToken column.
    Removes duplicate events based on id field

    :param spark: Spark Session used to create the DataFrame
    :param inputPath: the path can be either a single json file or a directory storing json files
    :return: events as a dataFrame
    """
    # Define custom schema for events
    schema = StructType([
        StructField("datetime",TimestampType(),True),
        StructField("id",StringType(),True),
        StructField("type",StringType(),True),
        StructField("domain",StringType(),True),
        StructField("user",StructType([
            StructField('id', StringType(), True),
            StructField('country', StringType(), True),
            StructField('token', StringType(), True)
        ]))
    ])

    # Define a custom schema for user.token column
    tokenSchema = StructType([
        StructField('vendors', StructType([
            StructField('enabled', ArrayType(StringType(), True)),
            StructField('disabled',ArrayType(StringType(), True))
        ])),
        StructField('purposes', StructType([
            StructField('enabled', ArrayType(StringType(), True)),
            StructField('disabled',ArrayType(StringType(), True))
        ]))
    ])

    df = spark.read.schema(schema).json(inputPaths)
    # Add a column with parsed token json string
    df_parsed = df.withColumn("parsedToken",from_json(df.user.token,tokenSchema))
    df_parsed_deduplicated = df_parsed.dropDuplicates(["id"])
    return df_parsed_deduplicated

def event_type_count(df: DataFrame, type: str) -> DataFrame:
    """
    Generic function to compute the number of events of any type. The count is grouped by the following dimensions:
        - Date and hour (YYYY-MM-DD-HH)
        - Domain
        - User country

    :param df: input DataFrame
    :param type: type for which to execute the computation
    :return: a DataFrame containing the result of the computation
    """
    return df.where(df.type == type).groupBy(date_format("datetime", "yyyy-MM-dd-hh").alias("dateHour"), "domain", "user.country").count()

def event_type_with_consent_count(df: DataFrame, type: str) -> DataFrame:
    """
    Generic function to compute the number of events of any type with consent. The count is grouped by the following dimensions:
        - Date and hour (YYYY-MM-DD-HH)
        - Domain
        - User country

    :param df: input DataFrame
    :param type: type for which to execute the computation
    :return: a DataFrame containing the result of the computation
    """
    return df.where((df.type == type) & (size(df.parsedToken.purposes.enabled) > 0)).groupBy(date_format("datetime", "yyyy-MM-dd-hh").alias("dateHour"), "domain", "user.country").count()

def avg_event_type_per_user(df: DataFrame, type:  str) -> DataFrame:
    """
    Generic function to compute the average number of events of any type per user. The average is grouped by the following dimensions:
        - Date and hour (YYYY-MM-DD-HH)
        - Domain
        - User country

    :param df: input DataFrame
    :param type: type for which to execute the computation
    :return: a DataFrame containing the result of the computation
    """
    count_event_type_per_user = df.where(df.type == type).groupBy(date_format("datetime", "yyyy-MM-dd-hh").alias("dateHour"), "domain", "user.country", "user.id").count()
    return count_event_type_per_user.groupBy("dateHour", "domain", "country").avg("count")

def pageviews(df: DataFrame) -> DataFrame:
    """
    Computes the number of events of type pageview
    :param df: input DataFrame
    :return: a DataFrame containing the result of the computation
    """
    return event_type_count(df, "pageview")


def consents_asked(df: DataFrame) -> DataFrame:
    """
    Computes the number of events of type consent.asked
    :param df: input DataFrame
    :return: a DataFrame containing the result of the computation
    """
    return event_type_count(df, "consent.asked")

def consents_given(df:  DataFrame) -> DataFrame:
    """
    Computes the number of events of type consent.given
    :param df: input DataFrame
    :return: a DataFrame containing the result of the computation
    """
    return event_type_count(df, "consent.given")


def pageviews_with_consent(df: DataFrame) -> DataFrame:
    """
    Computes the number of events of type pageview with consent
    :param df: input DataFrame
    :return: a DataFrame containing the result of the computation
    """
    return event_type_with_consent_count(df, "pageview")

def consents_given_with_consent(df: DataFrame) -> DataFrame:
    """
    Computes the number of events of type consent.given with consent
    :param df: input DataFrame
    :return: a DataFrame containing the result of the computation
    """
    return event_type_with_consent_count(df, "consent.given")

def avg_pageview_per_user(df: DataFrame) -> DataFrame:
    """
    Computes the average number of events of type pageview per user
    :param df: input DataFrame
    :return: a DataFrame containing the result of the computation
    """
    return avg_event_type_per_user(df, "pageview")






