import os
import configparser
import json

config = configparser.ConfigParser()
config_path = os.path.join(os.getcwd(),'insta_Project\src\config','config.ini')
print("********os.getcwd() *******",os.getcwd())
print("**************** config_path  **********",config_path)
config.read(config_path)

############ parent path
sec_parent_path = config.get('sec_parent_path','parent_path')
sec_parent_path = os.path.join(os.getcwd(),sec_parent_path)
print("*****sec_parent_path *******",sec_parent_path)

############## followers
sec_followers_and_following_path_child_path = config.get('sec_followers_and_following_path','child_path')
sec_followers_and_following_path_followers_path = config.get('sec_followers_and_following_path','followers_path')

followers_input_file_path = os.path.join(sec_parent_path, sec_followers_and_following_path_child_path,sec_followers_and_following_path_followers_path)
print("*****followers_input_file_path *******",followers_input_file_path)
#  C:\Users\Hp X360\learn_powerBI\building_Insta_FB_Project_X\insta_Project\src\data\source_data\instagram-sairamdgr8-2024-02-28-JIlQW2Wf\connections\followers_and_following\followers_1.json
followers_target_file_path=config.get('sec_followers_and_following_path','target_followers_path')


#### following
sec_followers_and_following_path_following_path = config.get('sec_followers_and_following_path','following_path')
following_input_file_path = os.path.join(sec_parent_path, sec_followers_and_following_path_child_path,sec_followers_and_following_path_following_path)
following_target_file_path=config.get('sec_followers_and_following_path','target_following_path')

#### web_activity


sec_web_activity_path = config.get('sec_web_activity_path','web_activity_path')
web_activity_input_file_path = os.path.join(sec_parent_path, sec_web_activity_path)
print(web_activity_input_file_path)
web_activity_target_file_path=config.get('sec_web_activity_path','target_web_activity_path')

#### logged_past_insights_reels

sec_logged_past_insights_reels_child_path = config.get('sec_logged_past_insights_reels_path','logged_past_insights_reels_child_path')
sec_logged_past_insights_reels_path = config.get('sec_logged_past_insights_reels_path','reels_path')
logged_past_insights_reels_input_file_path = os.path.join(sec_parent_path, sec_logged_past_insights_reels_child_path,sec_logged_past_insights_reels_path)
print(logged_past_insights_reels_input_file_path)
logged_past_insights_reels_target_file_path=config.get('sec_logged_past_insights_reels_path','target_logged_past_insights_reels_path')

## audience_insights_city,country,age,men,women
sec_audience_insights_path = config.get('sec_audience_insights_path','audience_insights_path')
audience_insights_input_file_path=os.path.join(sec_parent_path, sec_logged_past_insights_reels_child_path,sec_audience_insights_path)
print(audience_insights_input_file_path)
audience_insights_city_path=config.get('sec_audience_insights_path','target_audience_insights_city_path',)
audience_insights_insights_reels_target_file_path=config.get('sec_audience_insights_path','target_audience_insights_path')
audience_insights_city_target_file_path=os.path.join(audience_insights_insights_reels_target_file_path,audience_insights_city_path)
print(audience_insights_city_target_file_path)
##### country
audience_insights_country_path=config.get('sec_audience_insights_path','target_audience_insights_country_path',)
audience_insights_country_target_file_path=os.path.join(audience_insights_insights_reels_target_file_path,audience_insights_country_path)
print(audience_insights_country_target_file_path)
#### age
audience_insights_age_path=config.get('sec_audience_insights_path','target_audience_insights_age_path',)
audience_insights_age_target_file_path=os.path.join(audience_insights_insights_reels_target_file_path,audience_insights_age_path)
print(audience_insights_age_target_file_path)
#### men
audience_insights_men_path=config.get('sec_audience_insights_path','target_audience_insights_men_path',)
audience_insights_men_target_file_path=os.path.join(audience_insights_insights_reels_target_file_path,audience_insights_men_path)
print(audience_insights_men_target_file_path)
#### women
audience_insights_women_path=config.get('sec_audience_insights_path','target_audience_insights_women_path',)
audience_insights_women_target_file_path=os.path.join(audience_insights_insights_reels_target_file_path,audience_insights_women_path)
print(audience_insights_women_target_file_path)

#### media_reels

sec_media_reels_path = config.get('sec_media_reels_path','media_reels_path')
media_reels_input_file_path = os.path.join(sec_parent_path, sec_media_reels_path)
print(media_reels_input_file_path)
media_reels_target_file_path=config.get('sec_media_reels_path','target_media_reels_path')

#### media_stories

sec_media_stories_path = config.get('sec_media_stories_path','media_stories_path')
media_stories_input_file_path = os.path.join(sec_parent_path, sec_media_stories_path)
print(media_stories_input_file_path)
media_stories_target_file_path=config.get('sec_media_stories_path','target_media_stories_path')

#### your_instagram_activity
## message_inbox_path

sec_your_instagram_activity_path = config.get('sec_your_instagram_activity_path','your_instagram_activity_path')
sec_your_instagram_activity_messages_inbox_child_path = config.get('sec_your_instagram_activity_path','your_instagram_activity_messages_inbox_child_path')
your_instagram_activity_messages_inbox_input_file_path = os.path.join(sec_parent_path, sec_your_instagram_activity_path,sec_your_instagram_activity_messages_inbox_child_path)
print(your_instagram_activity_messages_inbox_input_file_path)
your_instagram_activity_messages_inbox_target_path=config.get('sec_your_instagram_activity_path','target_your_instagram_activity_path')
your_instagram_activity_messages_inbox_target_child_file_path=config.get('sec_your_instagram_activity_path','target_your_instagram_activity_messages_inbox_path')
your_instagram_activity_messages_inbox_target_file_path=os.path.join(your_instagram_activity_messages_inbox_target_path,your_instagram_activity_messages_inbox_target_child_file_path)
print(your_instagram_activity_messages_inbox_target_file_path)

## liked_comments

sec_your_instagram_activity_liked_comments_child_path=config.get('sec_your_instagram_activity_path','your_instagram_activity_liked_comments_child_path')
your_instagram_activity_liked_comments_input_file_path = os.path.join(sec_parent_path, sec_your_instagram_activity_path,sec_your_instagram_activity_liked_comments_child_path)
print(your_instagram_activity_liked_comments_input_file_path)
your_instagram_activity_liked_comments_target_path=config.get('sec_your_instagram_activity_path','target_your_instagram_activity_path')
your_instagram_activity_liked_comments_target_child_file_path=config.get('sec_your_instagram_activity_path','target_your_instagram_activity_liked_comments_path')
your_instagram_activity_liked_comments_target_file_path=os.path.join(your_instagram_activity_liked_comments_target_path,your_instagram_activity_liked_comments_target_child_file_path)
print(your_instagram_activity_liked_comments_target_file_path)

## liked_posts

sec_your_instagram_activity_liked_posts_child_path=config.get('sec_your_instagram_activity_path','your_instagram_activity_liked_posts_child_path')
your_instagram_activity_liked_posts_input_file_path = os.path.join(sec_parent_path, sec_your_instagram_activity_path,sec_your_instagram_activity_liked_posts_child_path)
print(your_instagram_activity_liked_posts_input_file_path)
your_instagram_activity_liked_posts_target_path=config.get('sec_your_instagram_activity_path','target_your_instagram_activity_path')
your_instagram_activity_liked_posts_target_child_file_path=config.get('sec_your_instagram_activity_path','target_your_instagram_activity_liked_posts_path')
your_instagram_activity_liked_posts_target_file_path=os.path.join(your_instagram_activity_liked_posts_target_path,your_instagram_activity_liked_posts_target_child_file_path)
print(your_instagram_activity_liked_posts_target_file_path)

## ads_info

sec_ads_path=config.get('sec_ads_path','ads_path')
sec_ads_viewed_child_path=config.get('sec_ads_path','ads_viewed_child_path')
ads_viewed_input_file_path=os.path.join(sec_parent_path,sec_ads_path,sec_ads_viewed_child_path)
print(ads_viewed_input_file_path)
ads_target_path=config.get('sec_ads_path','target_ads_path')
target_ads_viewed_child_path=config.get('sec_ads_path','target_ads_viewed_child_path')
ads_viewed_target_path=os.path.join(ads_target_path,target_ads_viewed_child_path)
print(ads_viewed_target_path)


###########
#Snowflake

snowflake_config_path = os.path.join(os.getcwd(),'insta_Project\src\config','snowflake_config.json')
print("**************** snowflake_config_path  **********",snowflake_config_path)
#config.read(snowflake_config_path)



