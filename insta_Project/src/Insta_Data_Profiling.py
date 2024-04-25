# main_script.py

from Utilities.Insta_dataframe_readers import (spark,read_data, process_data_followers, \
      write_data_local,write_data_snowflake, \
      process_data_website_activity, \
      process_data_following,\
      process_data_folder_count,\
      process_data_logged_past_insights_reels,\
      process_data_audience_city_insights,\
      process_data_audience_country_insights,\
      process_data_audience_age_insights,\
      process_data_audience_men_insights,\
      process_data_audience_women_insights,\
      process_data_liked_comments,process_data_liked_posts,\
      process_data_ads_viewed,\
      media_reels_create_read_df,media_stories_create_read_df,\
      message_inbox_acitivity_create_read_df, \
      media_write_data_local
       )

from Utilities.Configurations import *




# main usage
if __name__ == "__main__":

      try:

            #### followers


            # Read data from file
            followers_input_file = followers_input_file_path
            data_df = read_data(followers_input_file)

            # Process data
            process_data_followers_df = process_data_followers(data_df)

            # Write processed data to output file to local target
            followers_output_file = followers_target_file_path
            write_data_local(process_data_followers_df, followers_output_file)

            # Write processed data to output file to sowflake target
            write_data_snowflake(process_data_followers_df,"followers_tbl")

            #### following

            # Read data from file
            data_df = read_data(following_input_file_path)
            # Process data
            process_data_following_df = process_data_following(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_following_df, following_target_file_path)
            # Write processed data to output file to sowflake target
            write_data_snowflake(process_data_following_df, "following_tbl")

            #### web_activity

            # read data from file
            data_df = read_data(web_activity_input_file_path)
            # Process data
            process_data_web_acitivity_df = process_data_website_activity(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_web_acitivity_df, web_activity_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(process_data_web_acitivity_df, "web_activity_tbl")

            #### logged_past_insights_reels

            # read data from file
            data_df = read_data(logged_past_insights_reels_input_file_path)
            # Process data
            process_data_logged_past_insights_reels_df = process_data_logged_past_insights_reels(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_logged_past_insights_reels_df, logged_past_insights_reels_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(process_data_logged_past_insights_reels_df, "logged_past_insights_reels_tbl")

            #### audience_insights_city_wise

            # read data from file
            data_df = read_data(audience_insights_input_file_path)
            # Process data
            process_data_audience_city_insights_df = process_data_audience_city_insights(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_audience_city_insights_df, audience_insights_city_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(process_data_audience_city_insights_df, "audience_insights_city_tbl")

            #### audience_insights_country_wise

            # read data from file
            data_df = read_data(audience_insights_input_file_path)
            # Process data
            process_data_audience_country_insights_df = process_data_audience_country_insights(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_audience_country_insights_df, audience_insights_country_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(process_data_audience_country_insights_df, "audience_insights_country_tbl")

            #### audience_insights_age_wise

            # read data from file
            data_df = read_data(audience_insights_input_file_path)
            # Process data
            process_data_audience_age_insights_df = process_data_audience_age_insights(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_audience_age_insights_df, audience_insights_age_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(process_data_audience_age_insights_df, "audience_insights_age_tbl")

            #### audience_insights_men_wise

            # read data from file
            data_df = read_data(audience_insights_input_file_path)
            # Process data
            process_data_audience_men_insights_df = process_data_audience_men_insights(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_audience_men_insights_df, audience_insights_men_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(process_data_audience_men_insights_df, "audience_insights_men_tbl")

            #### audience_insights_women_wise

            # read data from file
            data_df = read_data(audience_insights_input_file_path)
            # Process data
            process_data_audience_women_insights_df = process_data_audience_women_insights(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_audience_women_insights_df, audience_insights_women_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(process_data_audience_women_insights_df, "audience_insights_women_tbl")


            #### instagram_activity_liked_comments

            # read data from file
            data_df = read_data(your_instagram_activity_liked_comments_input_file_path)
            # Process data
            process_data_liked_comments_df = process_data_liked_comments(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_liked_comments_df, your_instagram_activity_liked_comments_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(process_data_liked_comments_df, "liked_comments_tbl")

            #### instagram_activity_liked_posts

            # read data from file
            data_df = read_data(your_instagram_activity_liked_posts_input_file_path)
            # Process data
            process_data_liked_posts_df = process_data_liked_posts(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_liked_posts_df, your_instagram_activity_liked_posts_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(process_data_liked_posts_df, "liked_posts_tbl")

            #### ads_info_ads_viewed

            # read data from file
            data_df = read_data(ads_viewed_input_file_path)
            # Process data
            process_data_ads_viewed_df = process_data_ads_viewed(data_df)
            # Write processed data to output file to local target
            write_data_local(process_data_ads_viewed_df, ads_viewed_target_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(process_data_ads_viewed_df, "ads_viewed_tbl")





            ### media_reels
            # Process data
            process_data_media_reels_df = process_data_folder_count(path=media_reels_input_file_path)
            ##create dataframe before writing
            media_reels_create_df=media_reels_create_read_df(process_data_media_reels_df)
            # Write processed data to output file to local target
            media_write_data_local(media_reels_create_df, media_reels_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(media_reels_create_df, "media_reels_tbl")

            ### media_stories
            # Process data
            process_data_media_stories_df = process_data_folder_count(path=media_stories_input_file_path)
            ##create dataframe before writing
            media_stories_create_df=media_stories_create_read_df(process_data_media_stories_df)
            # Write processed data to output file to local target
            media_write_data_local(media_stories_create_df, media_stories_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(media_stories_create_df, "media_stories_tbl")

            ### message_inbox_activity
            # Process data
            process_data_message_inbox_activity_df = process_data_folder_count(path=your_instagram_activity_messages_inbox_input_file_path)
            ##create dataframe before writing
            message_inbox_activity_create_df=message_inbox_acitivity_create_read_df(process_data_message_inbox_activity_df)
            # Write processed data to output file to local target
            media_write_data_local(message_inbox_activity_create_df, your_instagram_activity_messages_inbox_target_file_path)
            # Write processed data to output file to snowflake target
            write_data_snowflake(message_inbox_activity_create_df, "message_inbox_activity_tbl")


      except Exception as e:
            print("An error occurred:", str(e))

      finally:
            print("Spark Session Stopping...........")
            spark.stop()
