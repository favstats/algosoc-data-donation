import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
import re
from lxml import html  # Make sure this import is present
from pathlib import Path
import port.api.props as props
import port.helpers as helpers
import port.vis as vis

from port.validate import (
    DDPCategory,
    StatusCode,
    ValidateInput,
    Language,
    DDPFiletype,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files = [
            "accounts_you're_not_interested_in.json",
            "ads_viewed.json",
            "posts_viewed.json",
            "videos_watched.json",
            "your_topics.json",
            "post_comments.json",
            "liked_posts.json",
            "following.json",
            "ads_clicked.json",
            "liked_comments.json",
            "live_videos.json",
            "posts.json",
            "reels.json",
            "stories.json",
            "word_or_phrase_searches.json"
        ],
    ),
    DDPCategory(
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
            "accounts_you're_not_interested_in.html",
            "ads_viewed.html",
            "posts_viewed.html",
            "videos_watched.html",
            "your_topics.html",
            "post_comments.html",
            "liked_posts.html",
            "following.html",
            "ads_clicked.html",
            "liked_comments.html",
            "live_videos.html",
            "posts.html",
            "reels.html",
            "stories.html",
            "word_or_phrase_searches.html"
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message="Valid DDP"),
    StatusCode(id=1, description="Not a valid DDP", message="Not a valid DDP"),
    StatusCode(id=2, description="Bad zipfile", message="Bad zip"),
]

def parse_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    
    required_columns = ['data_type', 'Action', 'title', 'URL', 'Date', 'details']
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA
    
    return df

def validate(file: Path) -> ValidateInput:
    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)
    
    try:
        paths = []
        file_name = file.lower()  # Convert file name to lowercase for consistent checks
        
        with zipfile.ZipFile(file, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".json", ".html"):
                    paths.append(p.name.lower())  # Convert to lowercase for consistent checks

        validation.infer_ddp_category(paths)
        
        if validation.ddp_category is None:
            logger.warning("Could not infer DDP category")
            validation.set_status_code(1)  # Not a valid DDP
        elif validation.ddp_category.ddp_filetype in (DDPFiletype.JSON, DDPFiletype.HTML):
            # Check if the file name indicates Facebook or Instagram
            if "facebook" in file_name and "instagram" not in file_name:
                validation.set_status_code(1)  # Not a valid DDP for Instagram
                logger.warning("Found Facebook in zip file so can't be Instagram!")
            # elif "instagram" in file_name and "facebook" not in file_name:
            #     validation.set_status_code(0)  # Valid DDP for Instagram
            #     
            #     # Log the valid Instagram files found
            #     for p in paths:
            #         logger.debug("Found: %s in zip", p)
            # 
            # If file name does not indicate, fallback to checking paths
            elif any("facebook" in path for path in paths) and not any("instagram" in path for path in paths):
                validation.set_status_code(1)  # Not a valid DDP for Instagram
                logger.warning("Found Facebook in file names so can't be Instagram!")
            # 
            # elif any("instagram" in path for path in paths) and not any("facebook" in path for path in paths):
            #     validation.set_status_code(0)  # Valid DDP for Instagram
            #     
            else:
                validation.set_status_code(0)  # Assume it is a valid DDP 
                # Log the valid Instagram files found
                # for p in paths:
                #     logger.debug("Found: %s in zip", p)
              
        else:
            logger.warning("Could not infer DDP category")
            validation.set_status_code(1)  # Not a valid DDP

    except zipfile.BadZipFile:
        logger.error("Bad zip file")
        validation.set_status_code(2)  # Bad zipfile
    except Exception as e:
        logger.error(f"Unexpected error during validation: {str(e)}")
        validation.set_status_code(1)  # Not a valid DDP
    
    validation.validated_paths = paths  # Store the validated paths
    return validation


def extract_instagram_data(instagram_zip: str) -> Dict[str, Any]:
    global DATA_FORMAT
    # validation = validate(Path(instagram_zip))
    # if validation.status_code is None or validation.status_code.id != 0:
    #     logger.error(f"Invalid zip file: {validation.status_code.description if validation.status_code else 'Unknown error'}")
    #     return {}

    data = {}
    try:
        with zipfile.ZipFile(instagram_zip, "r") as zf:
            json_files = [f for f in zf.namelist() if f.endswith('.json')]
            html_files = [f for f in zf.namelist() if f.endswith('.html')]
            
            # Determine data format based on majority file type
            DATA_FORMAT = "json" if len(json_files) > len(html_files) else "html"
            
            files_to_process = json_files if DATA_FORMAT == "json" else html_files
            
            for file in files_to_process:
                with zf.open(file) as f:
                    if DATA_FORMAT == "json":
                        data[Path(file).name] = json.load(io.TextIOWrapper(f, encoding='utf-8'))
                    elif DATA_FORMAT == "html":
                        data[Path(file).name] = f.read().decode('utf-8')
        logger.info(f"Extracted data from {len(data)} files. Data format: {DATA_FORMAT}")
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
    return data



  
def parse_ads_clicked(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        # Extract the relevant items from the JSON structure
        ads_clicked = helpers.find_items_bfs(data, "impressions_history_ads_clicked")
        parsed_data = []

        for ad in ads_clicked:
            title = ad.get("title", "Unknown Ad")
            timestamp = ad.get("string_list_data", [{}])[0].get("timestamp", 0)
            date = helpers.robust_datetime_parser(timestamp) if timestamp else ""

            parsed_item = {
                'title': title,
                'url': '',  # No URL data in the JSON structure provided
                'Date': date,
                'detail': ''  # No additional details
            }
            parsed_data.append(parsed_item)

        return parsed_data
    elif DATA_FORMAT == "html":
        ads_clicked = helpers.find_items_bfs(data, "ads_clicked.html")
        # logger.debug("HTML content fetched for ads clicked.")

        if not ads_clicked:
            logger.warning("No content found for 'ads_clicked.html'.")
            return []

        try:
            # Parse the HTML content
            tree = html.fromstring(ads_clicked)
            # logger.debug("Successfully parsed HTML content into an element tree.")

            parsed_data = []

            # Look for the main content area, and then find divs that contain ads clicked
            ad_elements = tree.xpath('//div[@role="main"]//div[count(div) > 1]')
        
            for ad in ad_elements:
                author = ad.xpath('.//div[1]/text()')
                date = ad.xpath('.//div[2]//text()')

                parsed_item = {
                    'data_type': 'instagram_ad_clicked',
                    'Action': 'AdClick',
                    'title': author[0].strip() if author else 'Unknown Ad',
                    'URL': '',
                    'Date': helpers.robust_datetime_parser(date[0].strip()),
                    'details': json.dumps({})
                }
                # logger.debug(f"Constructed parsed item: {parsed_item}")
                parsed_data.append(parsed_item)

            # logger.info(f"Refined parsed data: {parsed_data}")
            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'ads_clicked.html': {str(e)}")
            return []



# def parse_ads_viewed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
#     if DATA_FORMAT == "json":
#         fin =  parse_json_generic(data, "impressions_history_ads_seen")
#         # logger.info(f"File returned {fin} items")
#         return fin
#     else:
#         fin = parse_html_generic(data)
#         logger.info(f"File returned {fin} items")
#         return fin
def parse_ads_viewed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        ads = helpers.find_items_bfs(data, "impressions_history_ads_seen")
        if not ads:
            return []
        return [{
            'data_type': 'instagram_ad_viewed',
            'Action': 'AdView',
            'title': ad.get("string_map_data", {}).get("Author", {}).get("value", "Unknown Ad"),
            'URL': '',
            'Date': helpers.robust_datetime_parser(ad.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
            'details': json.dumps({})
        } for ad in ads]
    elif DATA_FORMAT == "html":
        ads_viewed = helpers.find_items_bfs(data, "ads_viewed.html")
        if not ads_viewed:
            logger.warning("No content found for 'ads_viewed.html'.")
            return []

        try:
            tree = html.fromstring(ads_viewed)
            main_content = tree.xpath('//div[@role="main"]')

            if not main_content:
                logger.warning("No main content found in the HTML document.")
                return []

            ad_elements = main_content[0]
            logger.debug(f"Found {len(ad_elements)} ad views.")

            parsed_data = []
            for ad in ad_elements:
                try: 
                    author = ad.xpath('.//div[1]//text()')[1]
                    try: 
                        date = ad.xpath('.//div[1]//text()')[3]
                    except Exception as e:
                        date = ad.xpath('.//div[1]//text()')[1]
                        author = 'Unknown Author'

                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Unknown Author'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_ad_viewed',
                        'Action': 'AdView',
                        'title': title_text,
                        'URL': '',
                        'Date': date_text,
                        'details': json.dumps({})
                    }
                    # print(f"Constructed parsed item: {parsed_item}")
                    parsed_data.append(parsed_item)

                except Exception as e:
                    logger.error(f"Error parsing ad element: {str(e)}")

            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'ads_viewed.html': {str(e)}")
            return []

      


def parse_posts_viewed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      posts = helpers.find_items_bfs(data, "impressions_history_posts_seen")

      if not posts:
        return []
      return [{
          'data_type': 'instagram_post_viewed',
          'Action': 'PostView',
          'title': post.get("string_map_data", {}).get("Author", {}).get("value", "Unknown Author"),
          'URL': '',
          'Date': helpers.robust_datetime_parser(post.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
          'details': json.dumps({})
      } for post in posts]
    elif DATA_FORMAT == "html":
        posts_viewed = helpers.find_items_bfs(data, "posts_viewed.html")
        if not posts_viewed:
            logger.warning("No content found for 'posts_viewed.html'.")
            return []

        try:
            tree = html.fromstring(posts_viewed)
            main_content = tree.xpath('//div[@role="main"]')

            if not main_content:
                logger.warning("No main content found in the HTML document.")
                return []

            post_elements = main_content[0]
            logger.debug(f"Found {len(post_elements)} post views.")

            parsed_data = []
            for post in post_elements:
                try: 
                    author = post.xpath('.//div[1]//text()')[1]
                    try: 
                        date = post.xpath('.//div[1]//text()')[3]
                    except Exception as e:
                        date = post.xpath('.//div[1]//text()')[1]
                        author = 'Unknown Author'

                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Unknown Author'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_post_viewed',
                        'Action': 'PostView',
                        'title': title_text,
                        'URL': '',
                        'Date': date_text,
                        'details': json.dumps({})
                    }
                    # print(f"Constructed parsed item: {parsed_item}")
                    parsed_data.append(parsed_item)

                except Exception as e:
                    logger.error(f"Error parsing ad element: {str(e)}")

            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'posts_viewed.html': {str(e)}")
            return []
      

def parse_videos_watched(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      videos = helpers.find_items_bfs(data, "impressions_history_videos_watched")
      
      if not videos:
        return []
      return [{
          'data_type': 'instagram_video_watched',
          'Action': 'VideoWatch',
          'title': video.get("string_map_data", {}).get("Author", {}).get("value", "Unknown Author"),
          'URL': '',
          'Date': helpers.robust_datetime_parser(video.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
          'details': json.dumps({})
      } for video in videos]
    elif DATA_FORMAT == "html":
        videos_watched = helpers.find_items_bfs(data, "videos_watched.html")
        if not videos_watched:
            logger.warning("No content found for 'videos_watched.html'.")
            return []

        try:
            tree = html.fromstring(videos_watched)
            main_content = tree.xpath('//div[@role="main"]')

            if not main_content:
                logger.warning("No main content found in the HTML document.")
                return []

            post_elements = main_content[0]
            logger.debug(f"Found {len(post_elements)} post views.")

            parsed_data = []
            for post in post_elements:
                try: 
                    author = post.xpath('.//div[1]//text()')[1]
                    try: 
                        date = post.xpath('.//div[1]//text()')[3]
                    except Exception as e:
                        date = post.xpath('.//div[1]//text()')[1]
                        author = 'Unknown Author'

                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Unknown Author'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_videos_watched',
                        'Action': 'VideoWatch',
                        'title': title_text,
                        'URL': '',
                        'Date': date_text,
                        'details': json.dumps({})
                    }
                    # print(f"Constructed parsed item: {parsed_item}")
                    parsed_data.append(parsed_item)

                except Exception as e:
                    logger.error(f"Error parsing ad element: {str(e)}")

            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'videos_watched.html': {str(e)}")
            return []
      
def parse_advertisers_using_activity(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        advertisers = helpers.find_items_bfs(data, "ig_custom_audiences_all_types")
        if not advertisers:
          return []
        return [{
            'data_type': 'instagram_advertiser_activity',
            'Action': 'AdvertiserActivity',
            'title': advertiser.get("advertiser_name", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps({
                'has_data_file_custom_audience': advertiser.get("has_data_file_custom_audience", False),
                'has_remarketing_custom_audience': advertiser.get("has_remarketing_custom_audience", False),
                'has_in_person_store_visit': advertiser.get("has_in_person_store_visit", False)
            })
        } for advertiser in advertisers]
    elif DATA_FORMAT == "html":
        html_content = helpers.find_items_bfs(data,"advertisers_using_your_activity_or_information.html")
        if not html_content:
          return []
        
        try: 
            
          parsed_table = helpers.html_tables(html_content)
          df = pd.DataFrame(parsed_table[1])
          result = []
          
          for _, row in df.iterrows():
              result.append({
                  'data_type': 'instagram_advertiser_activity',
                  'Action': 'AdvertiserActivity',
                  'title': row[0],
                  'URL': '',
                  'Date': '',
                  'details': json.dumps({
                      'has_data_file_custom_audience': row[1] == 'x' if len(row) > 1 else False ,
                      'has_remarketing_custom_audience': row[2] == 'x' if len(row) > 2 else False ,
                      'has_in_person_store_visit': row[3] == 'x' if len(row) > 3 else False 
                  })
              })
            
          return result
        except Exception as e:
            logger.error(f"Error parsing 'advertisers_using_your_activity_or_information.html': {str(e)}")
            return []

## todo: fix html
def parse_subscription_for_no_ads(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        subscriptions = helpers.find_items_bfs(data, "subscription_for_no_ads.json")
        subscriptions = helpers.find_items_bfs(subscriptions, "label_values")
        if not subscriptions:
          return []
        return [{
            'data_type': 'instagram_subscription',
            'Action': 'SubscriptionStatus',
            'title': "Your Ad-Opt-Out Subscription Status" + ": " + sub.get("value", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps({})
        } for sub in subscriptions]
    elif DATA_FORMAT == "html":
        html_content = helpers.find_items_bfs(data, "subscription_for_no_ads.html")
        if not html_content:
          return []
        
        try: 
        
          tree = html.fromstring(html_content)
          subscriptions = []
          
          # Find all table rows in the main content
          subscription_rows = tree.xpath('//div[@role="main"]//table//tr')
          
          for row in subscription_rows:
              label = row.xpath('.//td[1]/text()')[0].strip() if row.xpath('.//td[1]/text()') else ""
              value = row.xpath('.//td[2]/text()')[0].strip() if row.xpath('.//td[2]/text()') else ""
              
              subscriptions.append({
                  'data_type': 'instagram_subscription',
                  'Action': 'SubscriptionStatus',
                  'title': 'Your Ad-Opt-Out Subscription' + ": " + value,
                  'URL': '',
                  'Date': '',
                  'details': json.dumps({})
              })
        
          return subscriptions
        
        except Exception as e:
            logger.error(f"Error parsing 'subscription_for_no_ads.html': {str(e)}")
            return []      
      
      
      
def parse_post_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      comments = helpers.find_items_bfs(data, "comments_media_comments") or helpers.find_items_bfs(data, "post_comments_1.json")

      if not comments:
        return []
      return [{
          'data_type': 'instagram_post_comment',
          'Action': 'Comment',
          'title': comment.get("string_map_data", {}).get("Comment", {}).get("value", ""),
          'URL': "https://www.instagram.com/" + comment.get("string_map_data", {}).get("Media Owner", {}).get("value", ""),
          'Date': helpers.robust_datetime_parser(comment.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
          'details': json.dumps({})
          
      } for comment in comments]
    elif DATA_FORMAT == "html":
        elements = helpers.find_items_bfs(data, "post_comments_1.html")
        if not elements:
            logger.warning("No content found for 'post_comments_1.html'.")
            return []

        try:
            tree = html.fromstring(elements)
            main_content = tree.xpath('//div[@role="main"]')

            if not main_content:
                logger.warning("No main content found in the HTML document.")
                return []

            post_elements = main_content[0]
            logger.debug(f"Found {len(post_elements)} post views.")

            parsed_data = []
            for post in post_elements:
                try: 
                    title = post.xpath('.//div[1]//text()')[1]
                    author = post.xpath('.//div[1]//text()')[3]

                    try: 
                        date = post.xpath('.//div[1]//text()')[5]
                    except Exception as e:
                        date = post.xpath('.//div[1]//text()')[3]
                        author = ''

                    # Ensure lists are not empty before accessing elements
                    author_text = author.strip() if author else ''
                    title_text = title.strip() if title else 'Unknown Text'

                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_post_comment',
                        'Action': 'Comment',
                        'title': title_text,
                        'URL': "https://www.instagram.com/" + author_text,
                        'Date': date_text,
                        'details': json.dumps({})
                    }
                    # print(f"Constructed parsed item: {parsed_item}")
                    parsed_data.append(parsed_item)

                except Exception as e:
                    logger.error(f"Error parsing ad element: {str(e)}")

            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'post_comments_1.html': {str(e)}")
            return []

def parse_liked_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      liked_posts = helpers.find_items_bfs(data, "likes_media_likes")

      if not liked_posts:
        return []
      return [{
          'data_type': 'instagram_liked_post',
          'Action': 'LikePost',
          'title': helpers.find_items_bfs(post, "title"),
          'URL': post.get("string_list_data", [{}])[0].get("href", ""),
          'Date': helpers.robust_datetime_parser(post.get("string_list_data", [{}])[0].get("timestamp", 0)),
          'details': json.dumps({})
      } for post in liked_posts]
    elif DATA_FORMAT == "html":
        elements = helpers.find_items_bfs(data, "liked_posts.html")
        if not elements:
            logger.warning("No content found for 'liked_posts.html'.")
            return []

        try:
            tree = html.fromstring(elements)
            main_content = tree.xpath('//div[@role="main"]')

            if not main_content:
                logger.warning("No main content found in the HTML document.")
                return []

            post_elements = main_content[0]
            logger.debug(f"Found {len(post_elements)} post views.")

            parsed_data = []
            for post in post_elements:
                try: 
                    try: 
                        author = post.xpath('.//div[1]//text()')[0]
                        date = post.xpath('.//div[1]//text()')[2]
                    except Exception as e:
                        author = 'Unknown Author'
                        date = post.xpath('.//div[1]//text()')[1]
                        
                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Unknown Author'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_liked_post',
                        'Action': 'LikePost',
                        'title': title_text,
                        'URL': '',
                        'Date': date_text,
                        'details': json.dumps({})
                    }
                    # print(f"Constructed parsed item: {parsed_item}")
                    parsed_data.append(parsed_item)

                except Exception as e:
                    logger.error(f"Error parsing ad element: {str(e)}")

            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'liked_posts.html': {str(e)}")
            return []


def parse_liked_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      liked_comments = helpers.find_items_bfs(data, "likes_comment_likes")

      if not liked_comments:
        return []
      return [{
          'data_type': 'instagram_liked_comment',
          'Action': 'LikeComment',
          'title': helpers.find_items_bfs(comment, "title"),
          'URL': comment.get("string_list_data", [{}])[0].get("href", ""),
          'Date': helpers.robust_datetime_parser(comment.get("string_list_data", [{}])[0].get("timestamp", 0)),
          'details': json.dumps({})
      } for comment in liked_comments]
    elif DATA_FORMAT == "html":
        elements = helpers.find_items_bfs(data, "liked_comments.html")
        if not elements:
            logger.warning("No content found for 'liked_comments.html'.")
            return []

        try:
            tree = html.fromstring(elements)
            main_content = tree.xpath('//div[@role="main"]')

            if not main_content:
                logger.warning("No main content found in the HTML document.")
                return []

            comment_elements = main_content[0]
            logger.debug(f"Found {len(comment_elements)} comment views.")

            parsed_data = []
            for comment in comment_elements:
                try: 
                    try: 
                        author = comment.xpath('.//div[1]//text()')[0]
                        date = comment.xpath('.//div[1]//text()')[2]
                    except Exception as e:
                        author = 'Unknown Author'
                        date = comment.xpath('.//div[1]//text()')[1]
                        
                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Unknown Author'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_liked_comment',
                        'Action': 'LikeComment',
                        'title': title_text,
                        'URL': '',
                        'Date': date_text,
                        'details': json.dumps({})
                    }
                    # print(f"Constructed parsed item: {parsed_item}")
                    parsed_data.append(parsed_item)

                except Exception as e:
                    logger.error(f"Error parsing ad element: {str(e)}")

            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'liked_comments.html': {str(e)}")
            return []
          
def parse_story_likes(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      liked_stories = helpers.find_items_bfs(data, "story_activities_story_likes") or helpers.find_items_bfs(data, "story_likes.json")

      if not liked_stories:
        return []
      return [{
          'data_type': 'instagram_liked_story',
          'Action': 'LikeStory',
          'title': helpers.find_items_bfs(story, "title"),
          'URL': "https://www.instagram.com/" + helpers.find_items_bfs(story, "title"),
          'Date': helpers.robust_datetime_parser(story.get("string_list_data", [{}])[0].get("timestamp", 0)),
          'details': json.dumps({})
      } for story in liked_stories]
    elif DATA_FORMAT == "html":
        elements = helpers.find_items_bfs(data, "story_likes.html")
        if not elements:
            logger.warning("No content found for 'story_likes.html'.")
            return []

        try:
            tree = html.fromstring(elements)
            main_content = tree.xpath('//div[@role="main"]')

            if not main_content:
                logger.warning("No main content found in the HTML document.")
                return []

            story_elements = main_content[0]
            logger.debug(f"Found {len(story_elements)} story likes.")

            parsed_data = []
            for story in story_elements:
                try: 
                    try: 
                        author = story.xpath('.//div[1]//text()')[0]
                        date = story.xpath('.//div[1]//text()')[1]
                    except Exception as e:
                        author = 'Unknown Author'
                        date = story.xpath('.//div[1]//text()')[0]
                        
                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Unknown Author'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_liked_story',
                        'Action': 'LikeStory',
                        'title': title_text,
                        'URL': "https://www.instagram.com/" + title_text,
                        'Date': date_text,
                        'details': json.dumps({})
                    }
                    # print(f"Constructed parsed item: {parsed_item}")
                    parsed_data.append(parsed_item)

                except Exception as e:
                    logger.error(f"Error parsing ad element: {str(e)}")

            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'story_likes.html': {str(e)}")
            return []

def parse_following(data: Dict[str, Any]) -> List[Dict[str, Any]]:

    if DATA_FORMAT == "json":
      following = helpers.find_items_bfs(data, "relationships_following")
      if not following:
        return []
      return [{
          'data_type': 'instagram_following',
          'Action': 'Follow',
          'title': account.get("string_list_data", [{}])[0].get("value", "Unknown Account"),
          'URL': account.get("string_list_data", [{}])[0].get("href", ""),
          'Date': helpers.robust_datetime_parser(account.get("string_list_data", [{}])[0].get("timestamp", 0)),
          'details': json.dumps({})
      } for account in following]
    elif DATA_FORMAT == "html":
        elements = helpers.find_items_bfs(data, "following.html")
        if not elements:
            logger.warning("No content found for 'following.html'.")
            return []

        try:
            tree = html.fromstring(elements)
            main_content = tree.xpath('//div[@role="main"]')

            if not main_content:
                logger.warning("No main content found in the HTML document.")
                return []

            post_elements = main_content[0]
            logger.debug(f"Found {len(post_elements)} post views.")

            parsed_data = []
            for post in post_elements:
                try: 
                    try: 
                        author = post.xpath('.//div[1]//text()')[0]
                        date = post.xpath('.//div[1]//text()')[1]
                    except Exception as e:
                        author = 'Unknown Author'
                        date = post.xpath('.//div[1]//text()')[0]
                        
                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Unknown Author'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_following',
                        'Action': 'Follow',
                        'title': title_text,
                        'URL':  "https://www.instagram.com/" + title_text,
                        'Date': date_text,
                        'details': json.dumps({})
                    }
                    # print(f"Constructed parsed item: {parsed_item}")
                    parsed_data.append(parsed_item)

                except Exception as e:
                    logger.error(f"Error parsing ad element: {str(e)}")

            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'following.html': {str(e)}")
            return []
          
def parse_account_searches(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      searches = helpers.find_items_bfs(data, "searches_user")
      if not searches:
          return []
      
      return [{
          'data_type': 'instagram_account_search',
          'Action': 'Search',
          'title': search_word,
          'URL': "https://www.instagram.com/explore/search/keyword/?q=" + search_word,
          'Date': helpers.robust_datetime_parser(
              search['string_map_data'].get('Time', {}).get('timestamp') or 
              search['string_map_data'].get('Tijd', {}).get('timestamp')
          ),
          'details': json.dumps({})
      } for search in searches if (search_word := (
          search['string_map_data'].get('Search', {}).get('value') or 
          search['string_map_data'].get('Zoekopdracht', {}).get('value') or 
          search['string_map_data'].get('Zoeken', {}).get('value')
      ))]
    elif DATA_FORMAT == "html":
        searches = helpers.find_items_bfs(data, "account_searches.html")
        if not searches:
            logger.warning("No content found for 'account_searches.html'.")
            return []

        try:
            tree = html.fromstring(searches)
            main_content = tree.xpath('//div[@role="main"]')

            if not main_content:
                logger.warning("No main content found in the HTML document.")
                return []

            post_elements = main_content[0]
            logger.debug(f"Found {len(post_elements)} post views.")

            parsed_data = []
            for post in post_elements:
                try: 
                    author = post.xpath('.//div[1]//text()')[1]
                    try: 
                        date = post.xpath('.//div[1]//text()')[3]
                    except Exception as e:
                        date = post.xpath('.//div[1]//text()')[1]
                        author = 'Unknown Author'

                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Unknown Author'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_account_search',
                        'Action': 'Search',
                        'title': title_text,
                        'URL': "https://www.instagram.com/explore/search/keyword/?q=" + title_text,
                        'Date': date_text,
                        'details': json.dumps({})
                    }
                    # print(f"Constructed parsed item: {parsed_item}")
                    parsed_data.append(parsed_item)

                except Exception as e:
                    logger.error(f"Error parsing ad element: {str(e)}")

            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'account_searches.html': {str(e)}")
            return []
          
def parse_searches(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      searches = helpers.find_items_bfs(data, "searches_keyword")
      if not searches:
          return []
            
      return [{
          'data_type': 'instagram_search',
          'Action': 'Search',
          'title': search_word,
          'URL': "https://www.instagram.com/explore/search/keyword/?q=" + search_word,
          'Date': helpers.robust_datetime_parser(
              search['string_map_data'].get('Time', {}).get('timestamp') or 
              search['string_map_data'].get('Tijd', {}).get('timestamp')
          ),
          'details': json.dumps({})
      } for search in searches if (search_word := (
          search['string_map_data'].get('Search', {}).get('value') or 
          search['string_map_data'].get('Zoekopdracht', {}).get('value') or 
          search['string_map_data'].get('Zoeken', {}).get('value')
      ))]

    elif DATA_FORMAT == "html":
        searches = helpers.find_items_bfs(data, "word_or_phrase_searches.html")
        if not searches:
            logger.warning("No content found for 'word_or_phrase_searches.html'.")
            return []

        try:
            tree = html.fromstring(searches)
            main_content = tree.xpath('//div[@role="main"]')

            if not main_content:
                logger.warning("No main content found in the HTML document.")
                return []

            post_elements = main_content[0]
            logger.debug(f"Found {len(post_elements)} post views.")

            parsed_data = []
            for post in post_elements:
                try: 
                    author = post.xpath('.//div[1]//text()')[1]
                    try: 
                        date = post.xpath('.//div[1]//text()')[3]
                    except Exception as e:
                        date = post.xpath('.//div[1]//text()')[1]
                        author = 'Unknown Author'

                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Unknown Author'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_search',
                        'Action': 'Search',
                        'title': title_text,
                        'URL': "https://www.instagram.com/explore/search/keyword/?q=" + title_text,
                        'Date': date_text,
                        'details': json.dumps({})
                    }
                    # print(f"Constructed parsed item: {parsed_item}")
                    parsed_data.append(parsed_item)

                except Exception as e:
                    logger.error(f"Error parsing ad element: {str(e)}")

            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing 'word_or_phrase_searches.html': {str(e)}")
            return []

def parse_posted_reels_insights(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      reels = helpers.find_items_bfs(data, "organic_insights_reels")
      if not reels:
          return []
      
      return [{
          'data_type': 'instagram_posted_reel',
          'Action': 'PostedReel',
          'title': reel['string_map_data'].get('Caption', {}).get('value', '') or reel['media_map_data']['Media Thumbnail'].get('title', ''),
          'URL': '',
          'Date': helpers.robust_datetime_parser(reel['string_map_data']['Upload Timestamp']['timestamp']),
          'details': json.dumps({
              'duration': reel['string_map_data'].get('Duration', {}).get('value', ''),
              'accounts_reached': reel['string_map_data'].get('Accounts reached', {}).get('value', ''),
              'plays': reel['string_map_data'].get('Instagram Plays', {}).get('value', ''),
              'likes': reel['string_map_data'].get('Instagram Likes', {}).get('value', ''),
              'comments': reel['string_map_data'].get('Instagram Comments', {}).get('value', ''),
              'shares': reel['string_map_data'].get('Instagram Shares', {}).get('value', ''),
              'saves': reel['string_map_data'].get('Instagram Saves', {}).get('value', ''),
          })
      } for reel in reels]
    elif DATA_FORMAT == "html":
        reels = helpers.find_items_bfs(data, "reels.html")
        if not reels:
            logger.warning("No content found for 'reels.html'.")
            return []
        try:
            tree = html.fromstring(reels)
            reels_data = []
    
            # Extract the necessary information from the HTML structure
            try:
                reel_elements = tree.xpath('//div[@role="main"]//div[div/div]')  # General structure to locate posts
                
                for reel in reel_elements:
                    try:
                        title = reel.xpath('.//div[1]/text()')[1] if reel.xpath('.//div[1]/text()') else ''
                        date = reel.xpath('.//td[normalize-space(text())="Upload Timestamp"]/following-sibling::td/text()')[0]

                        
                        # Extract each detail with individual error handling
                        try:
                            duration = reel.xpath('.//div[contains(text(), "Duration")]/following-sibling::div/text()')[0].strip()
                        except Exception as e:
                            logger.error(f"Error extracting duration: {str(e)}")
                            duration = ''
                        
                        try:
                            accounts_reached = reel.xpath('.//div[contains(text(), "Accounts reached")]/following-sibling::div/text()')[0].strip()
                        except Exception as e:
                            logger.error(f"Error extracting accounts reached: {str(e)}")
                            accounts_reached = ''
                        
                        try:
                            plays = reel.xpath('.//div[contains(text(), "Plays")]/following-sibling::div/text()')[0].strip()
                        except Exception as e:
                            logger.error(f"Error extracting plays: {str(e)}")
                            plays = ''
                        
                        try:
                            likes = reel.xpath('.//div[contains(text(), "Likes")]/following-sibling::div/text()')[0].strip()
                        except Exception as e:
                            logger.error(f"Error extracting likes: {str(e)}")
                            likes = ''
                        
                        try:
                            comments = reel.xpath('.//div[contains(text(), "Comments")]/following-sibling::div/text()')[0].strip()
                        except Exception as e:
                            logger.error(f"Error extracting comments: {str(e)}")
                            comments = ''
                        
                        try:
                            shares = reel.xpath('.//div[contains(text(), "Shares")]/following-sibling::div/text()')[0].strip()
                        except Exception as e:
                            logger.error(f"Error extracting shares: {str(e)}")
                            shares = ''
                        
                        try:
                            saves = reel.xpath('.//div[contains(text(), "Saves")]/following-sibling::div/text()')[0].strip()
                        except Exception as e:
                            logger.error(f"Error extracting saves: {str(e)}")
                            saves = ''
                        
                        reel_data = {
                            'data_type': 'instagram_posted_reel',
                            'Action': 'PostedReel',
                            'title': title,
                            'URL': '',
                            'Date': date,
                            'details': json.dumps({
                                'duration': duration,
                                'accounts_reached': accounts_reached,
                                'plays': plays,
                                'likes': likes,
                                'comments': comments,
                                'shares': shares,
                                'saves': saves
                            })
                        }
                        reels_data.append(reel_data)
                    except Exception as e:
                        logger.error(f"Error parsing a reel element: {str(e)}")
            except Exception as e:
                logger.error(f"Error parsing reel elements: {str(e)}")
            return reels_data
        except Exception as e:
            logger.error(f"Error parsing HTML content for reels: {str(e)}")
            return []

def parse_posted_posts_insights(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      posts = helpers.find_items_bfs(data, "organic_insights_posts")
      if not posts:
          return []
      
      return [{
          'data_type': 'instagram_posted_post',
          'Action': 'PostedPost',
          'title': post['media_map_data']['Media Thumbnail'].get('title', ''),
          'URL': '',
          'Date': helpers.robust_datetime_parser(post['string_map_data']['Creation Timestamp']['timestamp']),
          'details': json.dumps({
              'profile_visits': post['string_map_data'].get('Profile visits', {}).get('value', ''),
              'impressions': post['string_map_data'].get('Impressions', {}).get('value', ''),
              'follows': post['string_map_data'].get('Follows', {}).get('value', ''),
              'accounts_reached': post['string_map_data'].get('Accounts reached', {}).get('value', ''),
              'saves': post['string_map_data'].get('Saves', {}).get('value', ''),
              'likes': post['string_map_data'].get('Likes', {}).get('value', ''),
              'comments': post['string_map_data'].get('Comments', {}).get('value', ''),
              'shares': post['string_map_data'].get('Shares', {}).get('value', ''),
          })
      } for post in posts]
    if DATA_FORMAT == "html":
      return []


def parse_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      posts = data.get("posts_1.json", []) or helpers.find_items_bfs(item, "title")
      if isinstance(posts, dict):
          posts = [posts]
      return [{
          'data_type': 'instagram_post',
          'Action': 'Post',
          'title': helpers.find_items_bfs(item, "title"),
          'URL': '',
          'Date': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "creation_timestamp")),
          'details': json.dumps({})
      } for item in posts]
    elif DATA_FORMAT == "html":
        posts_html = helpers.find_items_bfs(data, "posts_1.html")
        if not posts_html:
          logger.warning("No content found for 'posts_1.html'.")
          return []
        try:
          tree = html.fromstring(posts_html)
          posts = tree.xpath('//div[@role="main"]/div')
          return [{
              'data_type': 'instagram_post',
              'Action': 'Post',
              'title': "No Text" if (post.xpath('div/text()') and post.xpath('div[last()]/text()') 
                                  and post.xpath('div/text()')[0] == post.xpath('div[last()]/text()')[0]) 
                    else (post.xpath('div/text()')[0].strip() if post.xpath('div/text()') else ''),              'URL': '',
              'Date': helpers.robust_datetime_parser(post.xpath('div[last()]/text()')[0] if post.xpath('div[last()]/text()') else ''),
              'details': json.dumps({})
          } for post in posts]
        except Exception as e:
            logger.error(f"Error parsing 'posts_1.html': {str(e)}")
            return []   
    
    
def parse_reels(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      posts = data.get("reels.json", []) or helpers.find_items_bfs(item, "ig_reels_media")
      if isinstance(posts, dict):
          posts = [posts]
      return [{
          'data_type': 'instagram_reel',
          'Action': 'Post',
          'title': helpers.find_items_bfs(item, "title"),
          'URL': '',
          'Date': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "creation_timestamp")),
          'details': json.dumps({})
      } for item in posts]
    elif DATA_FORMAT == "html":
        posts_html = helpers.find_items_bfs(data, "reels.html")
        if not posts_html:
          logger.warning("No content found for 'reels.html'.")
          return []
        try:
          tree = html.fromstring(posts_html)
          posts = tree.xpath('//div[@role="main"]/div')
          return [{
              'data_type': 'instagram_reel',
              'Action': 'Post',
              'title': "No Text" if (post.xpath('div/text()') and post.xpath('div[last()]/text()') 
                                  and post.xpath('div/text()')[0] == post.xpath('div[last()]/text()')[0]) 
                    else (post.xpath('div/text()')[0].strip() if post.xpath('div/text()') else ''),
              'URL': '',
              'Date': helpers.robust_datetime_parser(post.xpath('div[last()]/text()')[0] if post.xpath('div[last()]/text()') else ''),
              'details': json.dumps({})
          } for post in posts]
        except Exception as e:
            logger.error(f"Error parsing 'reels.html': {str(e)}")
            return []    
          
def parse_stories(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      posts = data.get("stories.json", []) or helpers.find_items_bfs(item, "ig_stories")
      if isinstance(posts, dict):
          posts = [posts]
      return [{
          'data_type': 'instagram_story',
          'Action': 'Post',
          'title': helpers.find_items_bfs(item, "title"),
          'URL': '',
          'Date': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "creation_timestamp")),
          'details': json.dumps({})
      } for item in posts]
    elif DATA_FORMAT == "html":
        posts_html = helpers.find_items_bfs(data, "stories.html")
        if not posts_html:
          logger.warning("No content found for 'stories.html'.")
          return []
        try:
          tree = html.fromstring(posts_html)
          posts = tree.xpath('//div[@role="main"]/div')
          return [{
              'data_type': 'instagram_story',
              'Action': 'Post',
              'title': "No Text" if (post.xpath('div/text()') and post.xpath('div[last()]/text()') 
                                  and post.xpath('div/text()')[0] == post.xpath('div[last()]/text()')[0]) 
                    else (post.xpath('div/text()')[0].strip() if post.xpath('div/text()') else ''),              'URL': '',
              'Date': helpers.robust_datetime_parser(post.xpath('div[last()]/text()')[0] if post.xpath('div[last()]/text()') else ''),
              'details': json.dumps({})
          } for post in posts]
        except Exception as e:
            logger.error(f"Error parsing 'stories.html': {str(e)}")
            return []


def get_json_keys(data, prefix=''):
    """
    Retrieve all unique keys and subkeys from a JSON object, combining them with '__' for each sub-level.

    Parameters:
    - data (dict): The JSON object (Python dictionary) to traverse.
    - prefix (str): The prefix to use for nested keys (used in recursive calls).

    Returns:
    - keys (set): A set of unique key paths.
    """
    keys = set()
    
    if isinstance(data, dict):
        for key, value in data.items():
            # Form the new prefix by appending current key with '__'
            new_prefix = f"{prefix}__{key}" if prefix else key
            keys.add(new_prefix)
            # Recursively get keys for nested dictionaries
            keys.update(get_json_keys(value, new_prefix))
    elif isinstance(data, list):
        for item in data:
            # For lists, we don't append any index to the prefix, just proceed with the elements
            keys.update(get_json_keys(item, prefix))
    
    return keys


def process_instagram_data(instagram_zip: str) -> List[props.PropsUIPromptConsentFormTable]:
    logger.info("Starting to extract Instagram data.")   

    extracted_data = extract_instagram_data(instagram_zip)
    # Assuming `extracted_data` is a dictionary where keys are the file paths or names.
    filtered_extracted_data = {
        k: v for k, v in extracted_data.items() if not re.match(r'^\d+\.html$', k.split('/')[-1])
    }
    
    # Logging only the filtered keys
    logger.info(f"Extracted data keys: {helpers.get_json_keys(filtered_extracted_data) if filtered_extracted_data else 'None'}")   
    
    all_data = []
    parsing_functions = [
        parse_ads_viewed, 
        parse_posts_viewed, 
        parse_videos_watched,
        parse_post_comments,
        parse_liked_posts, 
        parse_liked_comments,
        parse_story_likes,
        parse_following,
        parse_searches, 
        parse_account_searches,
        parse_posted_reels_insights,
        parse_posted_posts_insights,
        parse_posts,
        parse_reels,
        parse_stories,
        parse_ads_clicked
        
    ]
    
    for parse_function in parsing_functions:
        try:
            parsed_data = parse_function(extracted_data)
            logger.info(f"{parse_function.__name__} returned {len(parsed_data)} items")
            all_data.extend(parsed_data)
        except Exception as e:
            logger.error(f"Error in {parse_function.__name__}: {str(e)}")
    
    tables_to_render = []
    
    if all_data:
        combined_df = parse_data(all_data)
        try: 
          combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
          combined_df = combined_df.dropna(subset=['Date'])  # Drop rows where 'Date' is NaN
          
          # Convert all datetime objects to timezone-naive
          combined_df['Date'] = combined_df['Date'].dt.tz_convert(None)
          # Check for entries with dates before 2016
          pre_2000_count = (combined_df['Date'] < pd.Timestamp('2000-01-01')).sum()
          if pre_2000_count > 0:
              logger.info(f"Found {pre_2000_count} entries with dates before 2000.")
          
              # Filter out dates before 2000
              try:
                  # Filter out dates before 2000
                  combined_df = combined_df[combined_df['Date'] >= pd.Timestamp('2000-01-01')]  
                  # Confirm deletion
                  if pre_2000_count > 0:
                      post_filter_count = (combined_df['Date'] < pd.Timestamp('2000-01-01')).sum()
                      if post_filter_count == 0:
                          logger.info(f"Successfully deleted {pre_2000_count} entries with dates before 2000.")
                      else:
                          logger.info(f"Failed to delete some entries with dates before 2000. Remaining: {post_filter_count}.")
                      
              except Exception as e:
                  logger.info(f"Error filtering dates before 2000: {e}")

          combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
          combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
          logger.error(f"Error parsing or sorting date: {str(e)}")
        combined_df['Count'] = 1
        
        table_title = props.Translatable({"en": "Instagram Activity Data", "nl": "Instagram Gegevens"})
        visses = [vis.create_chart(
            "line", 
            "Instagram Activity Over Time", 
            "Instagram Activity Over Time", 
            "Date", 
            y_label="Number of Observations", 
            date_format="auto"
        )]
        
        table = props.PropsUIPromptConsentFormTable("instagram_all_data", table_title, combined_df, visualizations=visses)
        tables_to_render.append(table)
        
        logger.info(f"Successfully processed {len(combined_df)} total entries from Instagram data")
    else:
        logger.warning("No data was successfully extracted and parsed")
   
   
    ### this is for all things without dates
    all_data = []
    parsing_functions = [
        parse_subscription_for_no_ads, parse_advertisers_using_activity
    ]
    
    for parse_function in parsing_functions:
        try:
            parsed_data = parse_function(extracted_data)
            logger.info(f"{parse_function.__name__} returned {len(parsed_data)} items")
            all_data.extend(parsed_data)
        except Exception as e:
            logger.error(f"Error in {parse_function.__name__}: {str(e)}")
    
    
    if all_data:
        combined_df = parse_data(all_data)
        logger.info(f"Combined data frame shape: {combined_df.shape}")
        
        if not combined_df.empty:
            # Remove the 'Date' column if it exists
            if 'Date' in combined_df.columns:
                combined_df = combined_df.drop(columns=['Date'])
            # combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
            # combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
            # combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
       
            # if 'details' in combined_df.columns:
            #     combined_df = combined_df.drop(columns=['details'])
            # Create a single table with all data
            table_title = props.Translatable({"en": "Instagram Ad Info", "nl": "Instagram Gegevens"})


            # Pass the ungrouped data for the table and grouped data for the chart
            table = props.PropsUIPromptConsentFormTable("instagram_all_data2", table_title, combined_df)
            tables_to_render.append(table)
            
            logger.info(f"Successfully processed Second {len(combined_df)} total entries from Instagram data")
        else:
            logger.warning("Second Combined DataFrame is empty")
    else:
        logger.warning("Second Combined DataFrame: No data was successfully extracted and parsed")
    
    return tables_to_render

# Helper functions for specific data types
def posts_to_df(instagram_zip: str) -> pd.DataFrame:
    tables = process_instagram_data(instagram_zip)
    df = tables[0].df if tables else pd.DataFrame()
    return df[df['data_type'].isin(['instagram_post_viewed', 'instagram_liked_post'])].drop(columns=['data_type'])

def likes_to_df(instagram_zip: str) -> pd.DataFrame:
    tables = process_instagram_data(instagram_zip)
    df = tables[0].df if tables else pd.DataFrame()
    return df[df['data_type'] == 'instagram_liked_post'].drop(columns=['data_type'])

def ads_viewed_to_df(instagram_zip: str) -> pd.DataFrame:
    tables = process_instagram_data(instagram_zip)
    df = tables[0].df if tables else pd.DataFrame()
    return df[df['data_type'] == 'instagram_ad_viewed'].drop(columns=['data_type'])

def posts_viewed_to_df(instagram_zip: str) -> pd.DataFrame:
    tables = process_instagram_data(instagram_zip)
    df = tables[0].df if tables else pd.DataFrame()
    return df[df['data_type'] == 'instagram_post_viewed'].drop(columns=['data_type'])

def videos_watched_to_df(instagram_zip: str) -> pd.DataFrame:
    tables = process_instagram_data(instagram_zip)
    df = tables[0].df if tables else pd.DataFrame()
    return df[df['data_type'] == 'instagram_video_watched'].drop(columns=['data_type'])

def following_to_df(instagram_zip: str) -> pd.DataFrame:
    tables = process_instagram_data(instagram_zip)
    df = tables[0].df if tables else pd.DataFrame()
    return df[df['data_type'] == 'instagram_following'].drop(columns=['data_type'])

def post_comments_to_df(instagram_zip: str) -> pd.DataFrame:
    tables = process_instagram_data(instagram_zip)
    df = tables[0].df if tables else pd.DataFrame()
    return df[df['data_type'] == 'instagram_post_comment'].drop(columns=['data_type'])

def accounts_not_interested_in_to_df(instagram_zip: str) -> pd.DataFrame:
    tables = process_instagram_data(instagram_zip)
    df = tables[0].df if tables else pd.DataFrame()
    return df[df['data_type'] == 'instagram_account_not_interested'].drop(columns=['data_type'])

def liked_comments_to_df(instagram_zip: str) -> pd.DataFrame:
    tables = process_instagram_data(instagram_zip)
    df = tables[0].df if tables else pd.DataFrame()
    return df[df['data_type'] == 'instagram_liked_comment'].drop(columns=['data_type'])

def ads_clicked_to_df(instagram_zip: str) -> pd.DataFrame:
    tables = process_instagram_data(instagram_zip)
    df = tables[0].df if tables else pd.DataFrame()
    return df[df['data_type'] == 'instagram_ad_clicked'].drop(columns=['data_type'])
