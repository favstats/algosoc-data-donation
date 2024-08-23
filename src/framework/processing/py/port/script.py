import logging
import json
import io
import random
from typing import Optional, Literal
import pandas as pd

import port.api.props as props
import port.helpers as helpers
import port.youtube as youtube
import port.validate as validate
import port.tiktok as tiktok
import port.twitter as twitter
import port.facebook as facebook
import port.google as google
import port.instagram as instagram
import port.linkedin as linkedin

from port.api.commands import (CommandSystemDonate, CommandUIRender, CommandSystemExit)

LOG_STREAM = io.StringIO()

logging.basicConfig(
    #stream=LOG_STREAM,
    level=logging.DEBUG,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")


def process(session_id):
    global validation

    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platforms = [
        ("Facebook", facebook.process_facebook_data, facebook.validate),
        ("Google", google.process_google_data, google.validate),
        ("Instagram", instagram.process_instagram_data, instagram.validate),
        ("TikTok", tiktok.process_tiktok_data, tiktok.validate)
    ]

    # Randomize the order of the platforms
    random.shuffle(platforms)

    subflows = len(platforms)
    steps = 2
    step_percentage = (100 / subflows) / steps
    progress = 0

    while True:
        LOGGER.info("Prompt for file")
        yield donate_logs(f"{session_id}-tracking")
    
        promptFile = prompt_file("application/zip", "Social Media")
        file_result = yield render_donation_page("Social Media", promptFile, progress)
    
        LOGGER.info("Uploaded a file")
        if file_result.__type__ == "PayloadString":
            table_list = None  # Initialize table_list before the loop
            for idx, (platform_name, extraction_fun, validation_fun) in enumerate(platforms):
                LOGGER.info(f"Attempting to process as {platform_name} data")
                validation = validation_fun(file_result.value)
                
                if validation.status_code.id == 0:
                  
                    yield donate_logs(f"{session_id}-tracking")
    
                    table_list = extraction_fun(file_result.value)
    
                    if table_list and len(table_list) > 0:
                        LOGGER.info(f"Successfully processed as {platform_name} data")
                        break
                else:
                    LOGGER.info(f"Not a valid {platform_name} zip; trying next platform")
    
            if table_list and len(table_list) > 0:
                break
            else:
                LOGGER.info("No valid data found; prompt retry_confirmation")
                # Check if it's the final platform or if it's the last successful validation
                LOGGER.error("All platforms failed. Showing file names now.")
                validation = validation_fun(file_result.value)
                for p in validation.validated_paths:
                    LOGGER.debug("Found: %s in zip", p)
                yield donate_logs(f"{session_id}-tracking")
                retry_result = yield render_donation_page("Social Media", retry_confirmation("Social Media"), progress)
    
                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    LOGGER.info("Skipped during retry")
                    yield donate_logs(f"{session_id}-tracking")
                    break
        else:
            LOGGER.info("Skipped file upload")
            yield donate_logs(f"{session_id}-tracking")
            break


    progress += step_percentage

    if table_list and len(table_list) > 0:
        LOGGER.info("Prompt consent")
        yield donate_logs(f"{session_id}-tracking")

        prompt = assemble_tables_into_form(table_list)
        consent_result = yield render_donation_page("Social Media", prompt, progress)

        if consent_result.__type__ == "PayloadJSON":
            LOGGER.info("Data donated")
            yield donate_logs(f"{session_id}-tracking")
            yield donate("Social Media", consent_result.value)
        else:
            LOGGER.info("Skipped after reviewing consent")
            yield donate_logs(f"{session_id}-tracking")

    yield exit(0, "Success")
    yield render_end_page()



##################################################################

def assemble_tables_into_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    return props.PropsUIPromptConsentForm(table_list, [])


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def create_empty_table(platform_name: str) -> props.PropsUIPromptConsentFormTable:
    """
    Show something in case no data was extracted
    """
    title = props.Translatable({
       "en": "Er ging niks mis, maar we konden niks vinden",
       "nl": "Er ging niks mis, maar we konden niks vinden"
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table



##################################################################
# Extraction functions

def extract_youtube(youtube_zip: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    """
    Main data extraction function
    Assemble all extraction logic here
    """
    tables_to_render = []

    # Extract comments
    df = youtube.my_comments_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube comments", "nl": "Youtube comments"})
        table =  props.PropsUIPromptConsentFormTable("youtube_comments", table_title, df) 
        tables_to_render.append(table)

    # Extract Watch later.csv
    df = youtube.watch_later_to_df(youtube_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube watch later", "nl": "Youtube watch later"})
        table =  props.PropsUIPromptConsentFormTable("youtube_watch_later", table_title, df) 
        tables_to_render.append(table)

    # Extract subscriptions.csv
    df = youtube.subscriptions_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube subscriptions", "nl": "Youtube subscriptions"})
        table =  props.PropsUIPromptConsentFormTable("youtube_subscriptions", table_title, df) 
        tables_to_render.append(table)

    # Extract subscriptions.csv
    df = youtube.watch_history_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube watch history", "nl": "Youtube watch history"})
        vis = [create_chart("area", "Youtube videos bekeken", "Youtube videos watched", "Date standard format", y_label="Aantal videos", date_format="auto"),
               create_chart("bar", "Activiteit per uur van de dag", "Activity per hour of the day", "Date standard format", y_label="Aantal videos", date_format="hour_cycle"),
               create_wordcloud("Meest bekeken kanalen", "Most watched channels", "Channel")]
        table =  props.PropsUIPromptConsentFormTable("youtube_watch_history", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    # Extract live chat messages
    df = youtube.my_live_chat_messages_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube my live chat messages", "nl": "Youtube my live chat messages"})
        table =  props.PropsUIPromptConsentFormTable("youtube_my_live_chat_messages", table_title, df) 
        tables_to_render.append(table)

    return tables_to_render


def extract_tiktok(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    

    
    df = tiktok.video_browsing_history_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok video browsing history", "nl": "Tiktok video browsing history"})
        table =  props.PropsUIPromptConsentFormTable("tiktok_video_browsing_history", table_title, df) 
        tables_to_render.append(table)

    df = tiktok.favorite_videos_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok favorite videos", "nl": "Tiktok favorite videos"})
        table =  props.PropsUIPromptConsentFormTable("tiktok_favorite_videos", table_title, df) 
        tables_to_render.append(table)

    df = tiktok.following_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok following", "nl": "Tiktok following"})
        table =  props.PropsUIPromptConsentFormTable("tiktok_following", table_title, df) 
        tables_to_render.append(table)

    df = tiktok.like_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok likes", "nl": "Tiktok likes"})
        table =  props.PropsUIPromptConsentFormTable("tiktok_like", table_title, df) 
        tables_to_render.append(table)

    df = tiktok.search_history_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok search history", "nl": "Tiktok search history"})
        table =  props.PropsUIPromptConsentFormTable("tiktok_search_history", table_title, df) 
        tables_to_render.append(table)

    df = tiktok.share_history_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok share history", "nl": "Tiktok share history"})
        table =  props.PropsUIPromptConsentFormTable("tiktok_share_history", table_title, df) 
        tables_to_render.append(table)

    df = tiktok.comment_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok comment history", "nl": "Tiktok comment history"})
        table =  props.PropsUIPromptConsentFormTable("tiktok_comment", table_title, df) 
        tables_to_render.append(table)

    df = tiktok.watch_live_history_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok watch live history", "nl": "Tiktok watch live history"})
        table =  props.PropsUIPromptConsentFormTable("tiktok_watch_live_history", table_title, df) 
        tables_to_render.append(table)

    return tables_to_render


def extract_twitter(twitter_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = twitter.following_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter following", "nl": "Twitter following"})
        table =  props.PropsUIPromptConsentFormTable("twitter_following", table_title, df) 
        tables_to_render.append(table)

    df = twitter.like_to_df(twitter_zip)
    if not df.empty:
        vis = [create_wordcloud("Liked Tweets", "Liked Tweets", "Tweet", tokenize=True)]
        table_title = props.Translatable({"en": "Twitter likes", "nl": "Twitter likes"})
        table =  props.PropsUIPromptConsentFormTable("twitter_like", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = twitter.tweets_to_df(twitter_zip)
    if not df.empty:
        vis = [create_wordcloud("Eigen Tweets", "Own Tweets", "Tweet", tokenize=True)]
        table_title = props.Translatable({"en": "Twitter tweets", "nl": "Twitter tweets"})
        table =  props.PropsUIPromptConsentFormTable("twitter_tweets", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = twitter.user_link_clicks_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter user link clicks", "nl": "Twitter user link clicks"})
        table =  props.PropsUIPromptConsentFormTable("twitter_user_link_clicks", table_title, df) 
        tables_to_render.append(table)

    df = twitter.block_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter block", "nl": "Twitter block"})
        table =  props.PropsUIPromptConsentFormTable("twitter_block", table_title, df) 
        tables_to_render.append(table)

    df = twitter.mute_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter mute", "nl": "Twitter mute"})
        table =  props.PropsUIPromptConsentFormTable("twitter_mute", table_title, df) 
        tables_to_render.append(table)

    return tables_to_render


def extract_facebook(facebook_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = facebook.group_interactions_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook group interactions", "nl": "Facebook group interactions"})
        vis = [create_wordcloud("Groepen met meeste interacties", "Groups with most interactions", "Group name", value_column="Times Interacted")]
        table =  props.PropsUIPromptConsentFormTable("facebook_group_interactions", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = facebook.comments_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook comments", "nl": "Facebook comments"})
        vis = [create_wordcloud("Meest voorkomende woorden in comments", "Most common words in comments", "Comment", tokenize=True)]
        table =  props.PropsUIPromptConsentFormTable("facebook_comments", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = facebook.likes_and_reactions_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook likes and reactions", "nl": "Facebook likes and reactions"})
        vis = [create_chart('bar', "Meest gebruikte reacties", "Most used reactions", "Reaction")]
        table =  props.PropsUIPromptConsentFormTable("facebook_likes_and_reactions", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = facebook.your_badges_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your badges", "nl": "Facebook your badges"})
        table =  props.PropsUIPromptConsentFormTable("facebook_your_badges", table_title, df) 
        tables_to_render.append(table)

    df = facebook.your_posts_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your posts", "nl": "Facebook your posts"})
        table =  props.PropsUIPromptConsentFormTable("facebook_your_posts", table_title, df) 
        tables_to_render.append(table)

    df = facebook.your_search_history_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your searh history", "nl": "Facebook your search history"})
        vis = [create_wordcloud("Meest gebruikte zoektermen", "Most used search terms", "Search Term", tokenize=True)]
        table =  props.PropsUIPromptConsentFormTable("facebook_your_search_history", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = facebook.recently_viewed_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook recently viewed", "nl": "Facebook recently viewed"})
        table =  props.PropsUIPromptConsentFormTable("facebook_recently_viewed", table_title, df) 
        tables_to_render.append(table)

    df = facebook.recently_visited_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook recently visited", "nl": "Facebook recently visited"})
        table =  props.PropsUIPromptConsentFormTable("facebook_recently_visited", table_title, df) 
        tables_to_render.append(table)

    df = facebook.feed_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook feed", "nl": "Facebook feed"})
        table =  props.PropsUIPromptConsentFormTable("facebook_feed", table_title, df) 
        tables_to_render.append(table)

    df = facebook.controls_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook controls", "nl": "Facebook controls"})
        table =  props.PropsUIPromptConsentFormTable("facebook_controls", table_title, df) 
        tables_to_render.append(table)

    df = facebook.group_posts_and_comments_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook group posts and comments", "nl": "Facebook group posts and comments"})
        table =  props.PropsUIPromptConsentFormTable("facebook_group_posts_and_comments", table_title, df) 
        tables_to_render.append(table)
        
    df = facebook.your_posts_check_ins_photos_and_videos_1_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your posts check ins photos and videos", "nl": "Facebook group posts and comments"})
        table =  props.PropsUIPromptConsentFormTable("facebook_your_posts_check_ins_photos_and_videos", table_title, df) 
        tables_to_render.append(table)

    return tables_to_render

def extract_chrome(chrome_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = chrome.browser_history_to_df(chrome_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Chrome browser history", "nl": "Chrome browser history"})
        vis = [create_chart("area", "Chrome internet activiteit", "Chrome internet activity", "Date", y_label="Aantal URLs geopend", date_format="auto", addZeroes=True),
               create_chart("bar", "Activiteit per uur van de dag", "Activity per hour of the day", "Date", y_label='Aantal URLs geopend', date_format="hour_cycle", addZeroes=True),
               create_wordcloud("Meest bezochte websites", "Most visited websites", "Url", extract='url_domain')]
        table =  props.PropsUIPromptConsentFormTable("chrome_browser_history", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = chrome.bookmarks_to_df(chrome_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Chrome bookmarks", "nl": "Chrome bookmarks"})
        vis = [create_wordcloud("Meest voorkomende websites in bookmarks", "Most common websites in bookmarks", "Url", extract='url_domain')]
        table =  props.PropsUIPromptConsentFormTable("chrome_bookmarks", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = chrome.omnibox_to_df(chrome_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Chrome omnibox", "nl": "Chrome omnibox"})
        table =  props.PropsUIPromptConsentFormTable("chrome_omnibox", table_title, df) 
        tables_to_render.append(table)

    return tables_to_render


def extract_instagram(instagram_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = instagram.accounts_not_interested_in_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram accounts not interested in", "nl": "Instagram accounts not interested in"})
        table =  props.PropsUIPromptConsentFormTable("instagram_accounts_not_interested_in", table_title, df) 
        tables_to_render.append(table)

    df = instagram.ads_viewed_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram ads viewed", "nl": "Instagram ads viewed"})
        table =  props.PropsUIPromptConsentFormTable("instagram_ads_viewed", table_title, df) 
        tables_to_render.append(table)

    df = instagram.posts_viewed_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram posts viewed", "nl": "Instagram posts viewed"})
        vis = [
            create_chart("area", "Instagram posts bekeken", "Instagram posts viewed", "Date", y_label="Posts", date_format="auto"),
            create_chart("bar", "Hoe laat bekijk jij Instagram posts", "At what time do you view Instagram posts", "Date", y_label="Posts per uur", date_format="hour_cycle")
            ]
        table =  props.PropsUIPromptConsentFormTable("instagram_posts_viewed", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = instagram.posts_not_interested_in_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram posts not interested in", "nl": "Instagram posts not interested in"})
        table =  props.PropsUIPromptConsentFormTable("instagram_posts_not_interested_in", table_title, df) 
        tables_to_render.append(table)

    df = instagram.videos_watched_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram videos_watched", "nl": "Instagram posts videos_watched"})
        vis = [create_chart("area", "Instagram videos bekeken", "Instagram videos watched", "Date", y_label="Videos", date_format="auto")]
        table =  props.PropsUIPromptConsentFormTable("instagram_videos_watched", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = instagram.post_comments_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram post_comments", "nl": "Instagram posts post_comments"})
        vis = [create_wordcloud("Meest voorkomende woorden in comments", "Most common words in comments", "Comment", tokenize=True)]
        table =  props.PropsUIPromptConsentFormTable("instagram_post_comments", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = instagram.following_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram following", "nl": "Instagram posts following"})
        table =  props.PropsUIPromptConsentFormTable("instagram_following", table_title, df) 
        tables_to_render.append(table)

    df = instagram.liked_comments_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram liked_comments", "nl": "Instagram posts liked_comments"})
        vis = [create_chart("area", "Instagram comments geliked", "Instagram comments liked", "Date", y_label="Comments", date_format="auto")]
        table =  props.PropsUIPromptConsentFormTable("instagram_liked_comments", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = instagram.liked_posts_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram liked_posts", "nl": "Instagram posts liked_posts"})
        vis = [create_chart("area", "Instagram posts geliked", "Instagram posts liked", "Date", y_label="Posts", date_format="auto")]
        table =  props.PropsUIPromptConsentFormTable("instagram_liked_posts", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    return tables_to_render


def extract_linkedin(zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = linkedin.company_follows_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin company_follows", "nl": "Linkedin company_follows"})
        table =  props.PropsUIPromptConsentFormTable("linkedin_company_follows", table_title, df) 
        tables_to_render.append(table)

    df = linkedin.member_follows_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin member_follows", "nl": "Linkedin member_follows"})
        table =  props.PropsUIPromptConsentFormTable("linkedin_member_follows", table_title, df) 
        tables_to_render.append(table)

    df = linkedin.reactions_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin reactions", "nl": "Linkedin reactions"})
        vis = [create_chart('bar', "LinkedIn Reactions", "Linkedin Reactions", "Type")]
        table =  props.PropsUIPromptConsentFormTable("linkedin_reactions", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = linkedin.ads_clicked_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin ads_clicked", "nl": "Linkedin ads_clicked"})
        table =  props.PropsUIPromptConsentFormTable("linkedin_ads_clicked", table_title, df) 
        tables_to_render.append(table)

    df = linkedin.search_queries_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin search_queries", "nl": "Linkedin search_queries"})
        vis = [create_wordcloud("Meest gebruikte zoektermen", "Most used search terms", "Search Query", tokenize=True)]
        table =  props.PropsUIPromptConsentFormTable("linkedin_search_queries", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = linkedin.shares_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin shares", "nl": "Linkedin shares"})
        table =  props.PropsUIPromptConsentFormTable("linkedin_shares", table_title, df) 
        tables_to_render.append(table)

    df = linkedin.comments_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin comments", "nl": "Linkedin comments"})
        table =  props.PropsUIPromptConsentFormTable("linkedin_comments", table_title, df) 
        tables_to_render.append(table)

    return tables_to_render


##########################################
# Functions provided by Eyra did not change

def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_donation_page(platform, body, progress):
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform}))

    footer = props.PropsUIFooter(progress)
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(extensions, platform):
    description = props.Translatable(
        {
            "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a file from {platform}.",
            "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Als u geen {platform} bestand heeft klik dan op “Overslaan” rechts onder."
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)

def exit(code, info):
    return CommandSystemExit(code, info)
