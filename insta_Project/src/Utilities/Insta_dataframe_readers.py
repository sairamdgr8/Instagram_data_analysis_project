# dataframe_operations.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,size,explode_outer,split,trim,regexp_replace
import json,os
#from Configurations import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Insta_dataframe_operations") \
    .getOrCreate()



def read_data(file_path):
    """
    Function to read data from a file into a DataFrame.
    """
    df = spark.read.json(file_path, multiLine=True)
    return df



def process_data_followers(df):
    """
    Function to perform specific processing on the DataFrame followers.
    """
    # Example: Perform some data transformation or analysis
    processed_followers_df = df.select(col("string_list_data")[0].getField("value").alias("followers"))
    return processed_followers_df

def process_data_following(df):
    """
    Function to perform specific processing on the DataFrame following.
    """
    # Example: Perform some data transformation or analysis
    processed_following_df = df.select(explode("relationships_following.string_list_data").alias("x"))
    processed_following_df = processed_following_df.select(explode("x").alias("y"))
    processed_following_df = processed_following_df.select(col("y").getField("value").alias("following"))
    return processed_following_df

def process_data_website_activity(df):
    """
    Function to perform specific processing on the DataFrame following.
    """

    df_web_market_activity = df.select(
        explode("apps_and_websites_off_meta_activity").alias("apps_and_websites_off_meta_activity"))
    df_web_market_activity = df_web_market_activity.select("apps_and_websites_off_meta_activity.name",
                                                           "apps_and_websites_off_meta_activity.events")
    df_web_market_activity = df_web_market_activity.withColumn("events_count", size("events"))
    processed_web_market_activity_df = df_web_market_activity.select("name", "events_count")
    return processed_web_market_activity_df


def process_data_audience_city_insights(df):
    """
    Function to perform specific processing on the DataFrame following.
    """
    df_audience_insights_city_percentage_wise = df.select(
        col("organic_insights_audience.string_map_data")[0].getField("Follower Percentage by City").alias(
            "city_wise_percentage"))
    df_audience_insights_city_percentage_wise.printSchema()
    # df_audience_insights_percentage_wise.show(truncate=False)

    df_audience_insights_city_percentage_wise = df_audience_insights_city_percentage_wise.select(
        col("city_wise_percentage.value").alias("city_wise_percentage"))

    df_audience_insights_city_percentage_wise = df_audience_insights_city_percentage_wise.select(
        split(col("city_wise_percentage"), ",").alias("city_wise_percentage"))
    df_audience_insights_city_percentage_wise.printSchema()
    df_audience_insights_city_percentage_wise = df_audience_insights_city_percentage_wise.select(
        explode(df_audience_insights_city_percentage_wise.city_wise_percentage).alias("city_wise_percentage_combine"))

    df_audience_insights_city_percentage_wise = df_audience_insights_city_percentage_wise.select(
        (split(df_audience_insights_city_percentage_wise.city_wise_percentage_combine, ':', -1)[0].alias('city')),
        split(df_audience_insights_city_percentage_wise.city_wise_percentage_combine, ':', -1)[1].alias('percentage'))
    df_audience_insights_city_percentage_wise_final = df_audience_insights_city_percentage_wise.select(
        trim(col("city")).alias("city"), regexp_replace(col("percentage"), "%", "").cast('double').alias("percentage%"))
    return df_audience_insights_city_percentage_wise_final

def process_data_audience_country_insights(df):
    """
    Function to perform specific processing on the DataFrame following.
    """
    df_audience_insights_country_percentage_wise = df.select(
        col("organic_insights_audience.string_map_data")[0].getField("Follower Percentage by country").alias(
            "country_wise_percentage"))
    df_audience_insights_country_percentage_wise.printSchema()
    # df_audience_insights_percentage_wise.show(truncate=False)

    df_audience_insights_country_percentage_wise = df_audience_insights_country_percentage_wise.select(
        col("country_wise_percentage.value").alias("country_wise_percentage"))

    df_audience_insights_country_percentage_wise = df_audience_insights_country_percentage_wise.select(
        split(col("country_wise_percentage"), ",").alias("country_wise_percentage"))
    df_audience_insights_country_percentage_wise.printSchema()
    df_audience_insights_country_percentage_wise = df_audience_insights_country_percentage_wise.select(
        explode(df_audience_insights_country_percentage_wise.country_wise_percentage).alias(
            "country_wise_percentage_combine"))

    df_audience_insights_country_percentage_wise = df_audience_insights_country_percentage_wise.select((split(
        df_audience_insights_country_percentage_wise.country_wise_percentage_combine, ':', -1)[0].alias('country')),\
        split(df_audience_insights_country_percentage_wise.country_wise_percentage_combine,':', -1)[1].alias('percentage'))

    df_audience_insights_country_percentage_wise_final = df_audience_insights_country_percentage_wise.select(
        trim(col("country")).alias("country"),
        regexp_replace(col("percentage"), "%", "").cast('double').alias("percentage%"))

    return df_audience_insights_country_percentage_wise_final

def process_data_audience_age_insights(df):
    df_audience_insights_age_percentage_wise = df.select(
        col("organic_insights_audience.string_map_data")[0].getField(
            "Follower Percentage by Age for All Genders").alias("age_wise_percentage"))
    df_audience_insights_age_percentage_wise.printSchema()
    # df_audience_insights_percentage_wise.show(truncate=False)

    df_audience_insights_age_percentage_wise = df_audience_insights_age_percentage_wise.select(
        col("age_wise_percentage.value").alias("age_wise_percentage"))

    df_audience_insights_age_percentage_wise = df_audience_insights_age_percentage_wise.select(
        split(col("age_wise_percentage"), ",").alias("age_wise_percentage"))
    df_audience_insights_age_percentage_wise.printSchema()
    df_audience_insights_age_percentage_wise = df_audience_insights_age_percentage_wise.select(
        explode(df_audience_insights_age_percentage_wise.age_wise_percentage).alias("age_wise_percentage_combine"))

    df_audience_insights_age_percentage_wise = df_audience_insights_age_percentage_wise.select(
        (split(df_audience_insights_age_percentage_wise.age_wise_percentage_combine, ':', -1)[0].alias('age')),
        split(df_audience_insights_age_percentage_wise.age_wise_percentage_combine, ':', -1)[1].alias('percentage'))

    df_audience_insights_age_percentage_wise_final = df_audience_insights_age_percentage_wise.select(
        trim(col("age")).alias("age"), regexp_replace(col("percentage"), "%", "").cast('double').alias("percentage%"))

    return df_audience_insights_age_percentage_wise_final

def process_data_audience_men_insights(df):
    df_audience_insights_gender_men_percentage_wise = df.select(
        col("organic_insights_audience.string_map_data")[0].getField("Follower Percentage by Age for Men").alias(
            "men_age_wise_percentage"))
    df_audience_insights_gender_men_percentage_wise.printSchema()
    # df_audience_insights_percentage_wise.show(truncate=False)

    df_audience_insights_gender_men_percentage_wise = df_audience_insights_gender_men_percentage_wise.select(
        col("men_age_wise_percentage.value").alias("men_age_wise_percentage"))

    df_audience_insights_gender_men_percentage_wise = df_audience_insights_gender_men_percentage_wise.select(
        split(col("men_age_wise_percentage"), ",").alias("men_age_wise_percentage"))
    df_audience_insights_gender_men_percentage_wise.printSchema()
    df_audience_insights_gender_men_percentage_wise = df_audience_insights_gender_men_percentage_wise.select(
        explode(df_audience_insights_gender_men_percentage_wise.men_age_wise_percentage).alias(
            "age_wise_percentage_combine"))

    df_audience_insights_gender_men_percentage_wise = df_audience_insights_gender_men_percentage_wise.select((split(
        df_audience_insights_gender_men_percentage_wise.age_wise_percentage_combine, ':', -1)[0].alias('men_age')),\
                    split(df_audience_insights_gender_men_percentage_wise.age_wise_percentage_combine,':',-1)[1].alias('men_percentage'))

    df_audience_insights_gender_men_percentage_wise_final = df_audience_insights_gender_men_percentage_wise.select(
        trim(col("men_age")).alias("men_age"),
        regexp_replace(col("men_percentage"), "%", "").cast('double').alias("men_percentage%"))

    return df_audience_insights_gender_men_percentage_wise_final

def process_data_audience_women_insights(df):
    df_audience_insights_gender_women_percentage_wise = df.select(
        col("organic_insights_audience.string_map_data")[0].getField("Follower Percentage by Age for Women").alias(
            "women_age_wise_percentage"))
    df_audience_insights_gender_women_percentage_wise.printSchema()
    # df_audience_insights_percentage_wise.show(truncate=False)

    df_audience_insights_gender_women_percentage_wise = df_audience_insights_gender_women_percentage_wise.select(
        col("women_age_wise_percentage.value").alias("women_age_wise_percentage"))

    df_audience_insights_gender_women_percentage_wise = df_audience_insights_gender_women_percentage_wise.select(
        split(col("women_age_wise_percentage"), ",").alias("women_age_wise_percentage"))
    df_audience_insights_gender_women_percentage_wise.printSchema()
    df_audience_insights_gender_women_percentage_wise = df_audience_insights_gender_women_percentage_wise.select(
        explode(df_audience_insights_gender_women_percentage_wise.women_age_wise_percentage).alias(
            "age_wise_percentage_combine"))

    df_audience_insights_gender_women_percentage_wise = df_audience_insights_gender_women_percentage_wise.select((split(
        df_audience_insights_gender_women_percentage_wise.age_wise_percentage_combine, ':', -1)[0].alias('women_age')),\
                 split(df_audience_insights_gender_women_percentage_wise.age_wise_percentage_combine,':',-1)[1].alias('women_percentage'))


    df_audience_insights_gender_women_percentage_wise_final = df_audience_insights_gender_women_percentage_wise.select(
        trim(col("women_age")).alias("women_age"),
        regexp_replace(col("women_percentage"), "%", "").cast('double').alias("women_percentage%"))

    return df_audience_insights_gender_women_percentage_wise_final


def process_data_liked_comments(df):
    """
    Function to perform specific processing on the DataFrame following.
    """
    df_liked_comments = df.select(col("likes_comment_likes").getField("title").alias("x"))
    df_liked_comments = df_liked_comments.select(explode("x").alias("liked_comments"))
    return df_liked_comments

def process_data_liked_posts(df):
    """
    Function to perform specific processing on the DataFrame following.
    """
    df_liked_posts = df.select(col("likes_media_likes").getField("title").alias("x"))
    df_liked_posts = df_liked_posts.select(explode("x").alias("liked_posts"))
    return df_liked_posts

def process_data_ads_viewed(df):
    """
    Function to perform specific processing on the DataFrame following.
    """
    df_ads_viewed = df.select(col("impressions_history_ads_seen").getField("string_map_data").alias("x"))
    df_ads_viewed = df_ads_viewed.select(explode("x").alias("y"))
    df_ads_viewed = df_ads_viewed.select(col("y").getField("Author").alias("ads_viewed"))
    df_ads_viewed = df_ads_viewed.select(col("ads_viewed.*")).alias("ads_viewed")
    df_ads_viewed = df_ads_viewed.distinct()
    return df_ads_viewed


def process_data_logged_past_insights_reels(df):
    """
    Function to perform specific processing on the DataFrame following.
    """

    def child_struct(nested_df):
        # Creating python list to store dataframe metadata
        list_schema = [((), nested_df)]
        # Creating empty python list for final flattern columns
        flat_columns = []

        while len(list_schema) > 0:
            # Removing latest or recently added item (dataframe schema) and returning into df variable
            parents, df = list_schema.pop()
            flat_cols = [col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],))) for c in df.dtypes if
                         c[1][:6] != "struct"]

            struct_cols = [c[0] for c in df.dtypes if c[1][:6] == "struct"]

            flat_columns.extend(flat_cols)
            for i in struct_cols:
                projected_df = df.select(i + ".*")
                list_schema.append((parents + (i,), projected_df))
        return nested_df.select(flat_columns)

    def master_array(df):
        array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
        while len(array_cols) > 0:
            for c in array_cols:
                df = df.withColumn(c, explode_outer(c))
            df = child_struct(df)
            array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
        return df

    df_logged_past_insights_reels= master_array(df)
    df_logged_past_insights_reels = df_logged_past_insights_reels.withColumn("tags_array",split(df_logged_past_insights_reels["organic_insights_reels_string_map_data_Caption_value"], " "))
    df_logged_past_insights_reels = df_logged_past_insights_reels.select((explode(df_logged_past_insights_reels["tags_array"])).alias("tag"))
    df_logged_past_insights_reels=df_logged_past_insights_reels.select('tag').distinct()
    return df_logged_past_insights_reels


def process_data_folder_count(path):
    """
    Function to perform specific processing on the DataFrame following.
    """
    path=path
    def count_folders(path):
        reels_count = 0
        for root, dirs, files in os.walk(path):
            reels_count += len(files)
        return reels_count

    total_folders = count_folders(path)
    print("********************* total_folders *************** ",total_folders)

    return total_folders

def media_reels_create_read_df(number):
    """
    Function to read and create data
    """
    df=spark.createDataFrame([(number,)], ["total_reels_count"])
    return df

def media_stories_create_read_df(number):
    """
    Function to read and create data
    """
    df=spark.createDataFrame([(number,)], ["total_stories_count"])
    return df

def message_inbox_acitivity_create_read_df(number):
    """
    Function to read and create data
    """
    df=spark.createDataFrame([(number,)], ["total_message_inbox_acitivity_count"])
    return df

def media_write_data_local(df, output_path):
    """
    Function to write DataFrame to a local directory file.
    """
    return df.write.csv(output_path, header=True, mode="overwrite")


def write_data_local(df, output_path):
    """
    Function to write DataFrame to a local directory file.
    """
    return df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")


def write_data_snowflake(df,tbl_name):
    """
    Function to write DataFrame to a snowflake
    # Read Snowflake connection parameters from config file
    """
    snowflake_config_path = os.path.join(os.getcwd(), 'insta_Project\src\config', 'snowflake_config.json')
    with open(snowflake_config_path, "r") as snowflake_config_file:
        snowflake_conn = json.load(snowflake_config_file)
        print(snowflake_conn)
    return df.write.format("snowflake") \
        .options(**snowflake_conn) \
        .option("dbtable", tbl_name) \
        .mode("overwrite") \
        .options(header=True) \
        .save()





