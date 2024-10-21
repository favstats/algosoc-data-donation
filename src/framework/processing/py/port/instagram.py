import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
import re
import os
from bs4 import UnicodeDammit
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
            "post_comments_1.json",
            "liked_posts.json",
            "following.json",
            "ads_clicked.json",
            "liked_comments.json",
            "live_videos.json",
            "posts_1.json",
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
            "posts_1.html",
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
    
    required_columns = ['Type', 'Actie', 'URL', 'Datum', 'Details']
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA
    
    return df

def validate(file: Path) -> ValidateInput:
    global validation
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
                logger.info(f"Valid DDP inferred")
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
    global the_username  
    
    try:
        # Extract username from the zip file name
        zip_filename = Path(instagram_zip).stem  # Extract the base name of the file without extension
        pattern = r'^(instagram)-([a-zA-Z0-9]+)-(\d{4}-\d{1,2}-\d{1,2}|\d{1,2}-\d{1,2}-\d{4})$'
        
        match = re.match(pattern, zip_filename)
        if match:
            the_username = match.group(2)  # Extract the username from the pattern
        else:
            the_username = None
    except Exception as e:
        logger.error(f"Could not find username in file data {str(e)}")
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
              
                # file_info = zf.getinfo(file)
                # # Log the file size
                # file_size_gb = file_info.file_size / (1024 ** 3)  # Convert bytes to GB
                # logger.info(f"{Path(file).name}: {file_size_gb} GB")
                
                with zf.open(file) as f:
                    raw_data = f.read()
                    
                    
                    
                    
                    # Use UnicodeDammit to detect the encoding
                    suggestion = UnicodeDammit(raw_data)
                    encoding = suggestion.original_encoding
                    # logger.debug(f"Encountered encoding: {encoding}.")

                    try:
                        if DATA_FORMAT == "json":
                            data[Path(file).name] = json.loads(raw_data.decode(encoding))
                        elif DATA_FORMAT == "html":
                            data[Path(file).name] = raw_data.decode(encoding)
                    except (UnicodeDecodeError, json.JSONDecodeError) as e:
                        logger.error(f"Error processing file {file} with encoding {encoding}: {str(e)}")
                        continue  # Skip the problematic file and continue with othersr(e)}")

        the_user = helpers.find_items_bfs(data, "author")
        if not the_user:
            the_user = helpers.find_items_bfs(data, "actor")
            
        logger.info(f"Extracted data from {len(data)} files. Data format: {DATA_FORMAT}")
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
    return data


def replace_username_in_dataframe(df):

    if not the_username:
        # logger.warning("Username not found; skipping replacement.")
        return df

    # Function to replace the username in the DataFrame
    def replace_username(value):
        if the_username in value:
            return value.replace(the_username, "the_username")
        return value
    
    # Apply the function to all 'Actie' and 'Details' columns
    for column in df.columns:
        if column in ['Actie', 'Details']:
            df[column] = df[column].apply(replace_username)
    
    return df
  
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
                'Type': 'Aangeklikte Advertenties',
                'Actie': title,
                'URL': 'Geen URL',  # No URL data in the JSON structure provided
                'Datum': date,
                'Details': 'Geen Details',   # No additional Details
                'Bron': 'Instagram: Ads Clicked'
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
                    'Type': 'Aangeklikte Advertenties',
                    'Actie': author[0].strip() if author else 'Unknown Ad',
                    'URL': 'Geen URL',
                    'Datum': helpers.robust_datetime_parser(date[0].strip()),
                    'Details': 'Geen Details',   # No additional Details
                    'Bron': 'Instagram: Ads Clicked'
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
            'Type': 'Gezien Advertenties',
            'Actie': "'Bekeken:' " + ad.get("string_map_data", {}).get("Author", {}).get("value", "Unknown Ad"),
            'URL': 'Geen URL',
            'Datum': helpers.robust_datetime_parser(ad.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
            'Details': 'Geen Details',   # No additional Details
            'Bron': 'Instagram: Ads Viewed'
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
                        author = 'Geen Auteur'

                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Geen Auteur'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'Type': 'Gezien Advertenties',
                        'Actie': "'Bekeken:' " + title_text,
                        'URL': 'Geen URL',
                        'Datum': date_text,
                        'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Ads Viewed'
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
          'Type': 'Gezien Posts',
          'Actie': "'Bekeken:' " + post.get("string_map_data", {}).get("Author", {}).get("value", "Geen Auteur"),
          'URL': 'Geen URL',
          'Datum': helpers.robust_datetime_parser(post.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
          'Details': 'Geen Details',   # No Gezien Additional Details
                        'Bron': 'Instagram: Posts Viewed'
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
                        author = 'Geen Auteur'

                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Geen Auteur'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'Type': 'Gezien Posts',
                        'Actie': "'Bekeken:' " + title_text,
                        'URL': 'Geen URL',
                        'Datum': date_text,
                        'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Posts Viewed'
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
          'Type': 'Gezien Posts',
          'Actie': "'Bekeken:' " + video.get("string_map_data", {}).get("Author", {}).get("value", "Geen Auteur"),
          'URL': 'Geen URL',
          'Datum': helpers.robust_datetime_parser(video.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
          'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Videos Watched'
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
                        author = 'Geen Auteur'

                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Geen Auteur'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'Type': 'Gezien Posts',
                        'Actie': "'Bekeken:' " + title_text,
                        'URL': 'Geen URL',
                        'Datum': date_text,
                        'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Videos Watched'
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
            'Type': 'Advertentie Data',
            'Actie': "'Gebruikte jouw gegevens': " + advertiser.get("advertiser_name", ""),
            'URL': 'Geen URL',
            'Datum': 'Geen Datum',
            'Details': json.dumps({
                'has_data_file_custom_audience': advertiser.get("has_data_file_custom_audience", False),
                'has_remarketing_custom_audience': advertiser.get("has_remarketing_custom_audience", False),
                'has_in_person_store_visit': advertiser.get("has_in_person_store_visit", False)
            }),   # No additional Details
                        'Bron': 'Instagram: Advertiser Activity'
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
                  'Type': 'Advertentie Data',
                  'Actie': "'Gebruikte jouw gegevens': " + row[0],
                  'URL': 'Geen URL',
                  'Datum': 'Geen Datum',
                  'Details': json.dumps({
                      'has_data_file_custom_audience': row[1] == 'x' if len(row) > 1 else False ,
                      'has_remarketing_custom_audience': row[2] == 'x' if len(row) > 2 else False ,
                      'has_in_person_store_visit': row[3] == 'x' if len(row) > 3 else False 
                  }),   # No additional Details
                        'Bron': 'Instagram: Advertiser Activity'
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
            'Type': 'Advertentie Data',
            'Actie': "Uw status van advertentie-opt-out abonnement" + ": " + sub.get("value", ""),
            'URL': 'Geen URL',
            'Datum': 'Geen Datum',
            'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Subscription Status'
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
                  'Type': 'Advertentie Data',
                  'Actie': 'Uw status van advertentie-opt-out abonnement' + ": " + value,
                  'URL': 'Geen URL',
                  'Datum': 'Geen Datum',
                  'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Subscription Status'
              })
        
          return subscriptions
        
        except Exception as e:
            logger.error(f"Error parsing 'subscription_for_no_ads.html': {str(e)}")
            return []      
      
      

def parse_post_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    comments = []

    if DATA_FORMAT == "json":
        # Loop through all paths that match the exact pattern 'post_comments_*.json'
        for path in validation.validated_paths:
            if path.endswith(".json") and os.path.basename(path).startswith("post_comments_"):
                current_comments = helpers.find_items_bfs(data, path)
                
                if not current_comments:
                    continue
                
                comments.extend([{
                    'Type': 'Reacties',
                    'Actie': "'Gereageerd': " + comment.get("string_map_data", {}).get("Comment", {}).get("value", "Geen Tekst"),
                    'URL': "Geen URL",
                    'Datum': helpers.robust_datetime_parser(comment.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
                    'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Post Comments'
                } for comment in current_comments])

    elif DATA_FORMAT == "html":
        for path in validation.validated_paths:
            if path.endswith(".html") and os.path.basename(path).startswith("post_comments_"):
                html_content = data.get(path, "")
                if not html_content:
                    logger.warning(f"No content found for '{path}'.")
                    continue

                try:
                    tree = html.fromstring(html_content)
                    main_content = tree.xpath('//div[@role="main"]')

                    if not main_content:
                        logger.warning(f"No main content found in '{path}'.")
                        continue

                    post_elements = main_content[0]
                    logger.debug(f"Found {len(post_elements)} post views in '{path}'.")

                    for post in post_elements:
                        try:
                            title = post.xpath('.//div[1]//text()')[1]
                            author = post.xpath('.//div[1]//text()')[3]

                            try:
                                date = post.xpath('.//div[1]//text()')[5]
                            except Exception:
                                date = post.xpath('.//div[1]//text()')[3]
                                author = ''

                            author_text = author.strip() if author else ''
                            title_text = title.strip() if title else 'Unknown Text'
                            date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                            parsed_item = {
                                'Type': 'Reacties',
                                'Actie': "'Gereageerd': " + title_text,
                                'URL': "Geen URL",
                                'Datum': date_text,
                                'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Post Comments'
                            }

                            comments.append(parsed_item)

                        except Exception as e:
                            logger.error(f"Error parsing post element in '{path}': {str(e)}")

                except Exception as e:
                    logger.error(f"Error parsing '{path}': {str(e)}")

    return comments

def parse_liked_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      liked_posts = helpers.find_items_bfs(data, "likes_media_likes")

      if not liked_posts:
        return []
      return [{
          'Type': 'Gelikete Posts',
          'Actie': "'Geliked': " + helpers.find_items_bfs(post, "title"),
          'URL': post.get("string_list_data", [{}])[0].get("href", ""),
          'Datum': helpers.robust_datetime_parser(post.get("string_list_data", [{}])[0].get("timestamp", 0)),
          'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Liked Posts'
          
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
                        author = 'Geen Auteur'
                        date = post.xpath('.//div[1]//text()')[1]
                        
                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Geen Auteur'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'Type': 'Gelikete Posts',
                        'Actie': "'Geliked': " + title_text,
                        'URL': 'Geen URL',
                        'Datum': date_text,
                        'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Liked Posts'
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
          'Type': 'Vind ik leuk Reacties',
          'Actie': "'Geliked': " + helpers.find_items_bfs(comment, "title"),
          'URL': comment.get("string_list_data", [{}])[0].get("href", ""),
          'Datum': helpers.robust_datetime_parser(comment.get("string_list_data", [{}])[0].get("timestamp", 0)),
          'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Liked Comments'
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
                        author = 'Geen Auteur'
                        date = comment.xpath('.//div[1]//text()')[1]
                        
                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Geen Auteur'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'Type': 'Vind ik leuk Reacties',
                        'Actie': "'Geliked': " + title_text,
                        'URL': 'Geen URL',
                        'Datum': date_text,
                        'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Liked Comments'
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
          'Type': 'Gelikete Stories',
          'Actie': "'Geliked': " + helpers.find_items_bfs(story, "title"),
          'URL': "Geen URL",
          'Datum': helpers.robust_datetime_parser(story.get("string_list_data", [{}])[0].get("timestamp", 0)),
          'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Liked Stories'
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
                        author = 'Geen Auteur'
                        date = story.xpath('.//div[1]//text()')[0]
                        
                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Geen Auteur'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'Type': 'Gelikete Stories',
                        'Actie': "'Geliked': " + title_text,
                        'URL': "Geen URL",
                        'Datum': date_text,
                        'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Liked Stories'
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
          'Type': 'Gevolgde Accounts',
          'Actie': "'Gevolgd': " + account.get("string_list_data", [{}])[0].get("value", "Unknown Account"),
          'URL': account.get("string_list_data", [{}])[0].get("href", ""),
          'Datum': helpers.robust_datetime_parser(account.get("string_list_data", [{}])[0].get("timestamp", 0)),
          'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Following'
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
                        author = 'Geen Auteur'
                        date = post.xpath('.//div[1]//text()')[0]
                        
                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Geen Auteur'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'Type': 'Gevolgde Accounts',
                        'Actie': "'Gevolgd': " + title_text,
                        'URL':  "https://www.instagram.com/" + title_text,
                        'Datum': date_text,
                        'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Following'
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
          'Type': 'Zoekopdrachten',
          'Actie': "'Gezocht naar:' " + search_word,
          'URL': "https://www.instagram.com/explore/search/keyword/?q=" + search_word,
          'Datum': helpers.robust_datetime_parser(
              search['string_map_data'].get('Time', {}).get('timestamp') or 
              search['string_map_data'].get('Tijd', {}).get('timestamp')
          ),
          'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Account Search'
      } for search in searches if (search_word := (
          search['string_map_data'].get('Search', {}).get('value', "Geen Tekst") or 
          search['string_map_data'].get('Zoekopdracht', {}).get('value', "Geen Tekst") or 
          search['string_map_data'].get('Zoeken', {}).get('value', "Geen Tekst")
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
                        author = 'Geen Auteur'

                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Geen Auteur'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'Type': 'Zoekopdrachten',
                        'Actie': "'Gezocht naar:' " + title_text,
                        'URL': "https://www.instagram.com/explore/search/keyword/?q=" + title_text,
                        'Datum': date_text,
                        'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Account Search'
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
          'Type': 'Zoekopdrachten',
          'Actie': "'Gezocht naar:' " + search_word,
          'URL': "https://www.instagram.com/explore/search/keyword/?q=" + search_word,
          'Datum': helpers.robust_datetime_parser(
              search['string_map_data'].get('Time', {}).get('timestamp') or 
              search['string_map_data'].get('Tijd', {}).get('timestamp')
          ),
          'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Keyword Search'
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
                        author = 'Geen Auteur'

                    # Ensure lists are not empty before accessing elements
                    title_text = author.strip() if author else 'Geen Auteur'
                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'Type': 'Zoekopdrachten',
                        'Actie': "'Gezocht naar:' " + title_text,
                        'URL': "https://www.instagram.com/explore/search/keyword/?q=" + title_text,
                        'Datum': date_text,
                        'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Keyword Search'
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
          'Type': 'Reels',
          'Actie': "'Geplaatst': " + reel['string_map_data'].get('Caption', {}).get('value', '') or reel['media_map_data']['Media Thumbnail'].get('title', ''),
          'URL': 'Geen URL',
          'Datum': helpers.robust_datetime_parser(reel['string_map_data']['Upload Timestamp']['timestamp']),
          'Details': json.dumps({
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
                            'Type': 'Reels',
                            'Actie': title,
                            'URL': 'Geen URL',
                            'Datum': date,
                            'Details': json.dumps({
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
          'Type': 'instagram_posted_post',
          'Actie': "'Geplaatst': " + post['media_map_data']['Media Thumbnail'].get('title', ''),
          'URL': 'Geen URL',
          'Datum': helpers.robust_datetime_parser(post['string_map_data']['Creation Timestamp']['timestamp']),
          'Details': json.dumps({
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
    posts = []

    if DATA_FORMAT == "json":
        # Loop through all paths that match the exact pattern 'posts_*.json'
        for path in validation.validated_paths:
            if path.endswith(".json") and os.path.basename(path).startswith("posts_"):
                current_posts = data.get(path, []) or helpers.find_items_bfs(data, "title")
                if isinstance(current_posts, dict):
                    current_posts = [current_posts]
                
                posts.extend([{
                    'Type': 'Posts',
                    'Actie': "'Geplaatst': " + helpers.find_items_bfs(item, "title", "Geen Tekst"),
                    'URL': 'Geen URL',
                    'Datum': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "creation_timestamp", "Geen Datum")),
                    'Bron': 'Instagram: Posts',
                    'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Posts'
                } for item in current_posts])

    elif DATA_FORMAT == "html":
        for path in validation.validated_paths:
            if path.endswith(".html") and os.path.basename(path).startswith("posts_"):
                posts_html = helpers.find_items_bfs(data, path)
                if not posts_html:
                    logger.warning(f"No content found for '{path}'.")
                    continue

                try:
                    tree = html.fromstring(posts_html)
                    posts_elements = tree.xpath('//div[@role="main"]/div')

                    for post in posts_elements:
                        try:
                            title_condition = (
                                post.xpath('div/text()') and
                                post.xpath('div[last()]/text()') and
                                post.xpath('div/text()')[0] == post.xpath('div[last()]/text()')[0]
                            )
                            title = "Geen Tekst" if title_condition else (post.xpath('div/text()')[0].strip() if post.xpath('div/text()') else '')
                            date_text = post.xpath('div[last()]/text()')[0] if post.xpath('div[last()]/text()') else ''
                            date = helpers.robust_datetime_parser(date_text)

                            parsed_item = {
                                'Type': 'Posts',
                                'Actie': "'Geplaatst': " + title,
                                'URL': 'Geen URL',
                                'Datum': date,
                                'Bron': 'Instagram: Posts',
                                'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Posts'
                            }

                            posts.append(parsed_item)

                        except Exception as e:
                            logger.error(f"Error parsing post element in '{path}': {str(e)}")

                except Exception as e:
                    logger.error(f"Error parsing '{path}': {str(e)}")

    return posts
    
def parse_reels(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      # posts = data.get("reels.json", []) or
      posts = helpers.find_items_bfs(data, "ig_reels_media")
      if not posts:
        logger.warning("No content found for 'reels.json'.")
        return []
      return [{
          'Type': 'Reels',
          'Actie': "'Geplaatst': " + helpers.find_items_bfs(item, "title", "Geen Tekst"),
          'URL': 'Geen URL',
          'Datum': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "creation_timestamp")),
          'Bron': 'Instagram Reels',
          'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Reels'
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
              'Type': 'Reels',
              'Actie': "Geen Tekst" if (post.xpath('div/text()') and post.xpath('div[last()]/text()') 
                                  and post.xpath('div/text()')[0] == post.xpath('div[last()]/text()')[0]) 
                    else (post.xpath('div/text()')[0].strip() if post.xpath('div/text()') else ''),
              'URL': 'Geen URL',
              'Datum': helpers.robust_datetime_parser(post.xpath('div[last()]/text()')[0] if post.xpath('div[last()]/text()') else ''),
              'Bron': 'Instagram Reels',
              'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Reels'
          } for post in posts]
        except Exception as e:
            logger.error(f"Error parsing 'reels.html': {str(e)}")
            return []    
          
def parse_stories(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      # posts = data.get("stories.json", []) or
      posts = helpers.find_items_bfs(data, "ig_stories")
      # if isinstance(posts, dict):
      #     posts = [posts]
      return [{
          'Type': 'Stories',
          'Actie': "'Geplaatst': " + helpers.find_items_bfs(item, "title", "Geen Tekst"),
          'URL': 'Geen URL',
          'Datum': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "creation_timestamp")),
          'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Stories'
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
              'Type': 'Stories',
              'Actie': "Geen Tekst" if (post.xpath('div/text()') and post.xpath('div[last()]/text()') 
                                  and post.xpath('div/text()')[0] == post.xpath('div[last()]/text()')[0]) 
                    else (post.xpath('div/text()')[0].strip() if post.xpath('div/text()') else ''),              'URL': 'Geen URL',
              'Datum': helpers.robust_datetime_parser(post.xpath('div[last()]/text()')[0] if post.xpath('div[last()]/text()') else ''),
              'Details': 'Geen Details',   # No additional Details
                        'Bron': 'Instagram: Stories'
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
        k: v for k, v in extracted_data.items() if not re.match(r'^\d+\.(html|json)$', k.split('/')[-1])
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
        # parse_posted_reels_insights,
        # parse_posted_posts_insights,
        parse_posts,
        parse_reels,
        parse_stories,
        parse_ads_clicked,
        parse_subscription_for_no_ads, 
        parse_advertisers_using_activity
        
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
          combined_df['Datum'] = pd.to_datetime(combined_df['Datum'], errors='coerce')
          # combined_df = combined_df.dropna(subset=['Datum'])  # Drop rows where 'Datum' is NaN
          
          # Convert all datetime objects to timezone-naive
          combined_df['Datum'] = combined_df['Datum'].dt.tz_convert(None)
          # Check for entries with dates before 2016
          pre_2000_count = (combined_df['Datum'] < pd.Timestamp('2000-01-01')).sum()
          if pre_2000_count > 0:
              logger.info(f"Found {pre_2000_count} entries with dates before 2000.")
          
              # Filter out dates before 2000
              try:
                  # Filter out dates before 2000
                  combined_df = combined_df[combined_df['Datum'] >= pd.Timestamp('2000-01-01')]  
                  # Confirm deletion
                  if pre_2000_count > 0:
                      post_filter_count = (combined_df['Datum'] < pd.Timestamp('2000-01-01')).sum()
                      if post_filter_count == 0:
                          logger.info(f"Successfully deleted {pre_2000_count} entries with dates before 2000.")
                      else:
                          logger.info(f"Failed to delete some entries with dates before 2000. Remaining: {post_filter_count}.")
                      
              except Exception as e:
                  logger.info(f"Error filtering dates before 2000: {e}")

          combined_df = combined_df.sort_values(by='Datum', ascending=False, na_position='last').reset_index(drop=True)
          combined_df['Datum'] = combined_df['Datum'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('Geen Datum')
        except Exception as e:
          logger.error(f"Error parsing or sorting date: {str(e)}")
        # combined_df['Count'] = 1
        
        # List of columns to apply the replace_email function
        columns_to_process = ['Details', 'Actie']
        
        # Loop over each column in the list
        for column in columns_to_process:
            try:
                # Ensure the column values are strings and apply the replace_email function
                combined_df[column] = combined_df[column].apply(lambda x: helpers.replace_email(str(x)))
            except Exception as e:
                logger.warning(f"Could not replace e-mail in column '{column}': {e}")

        
        try:
            combined_df = replace_username_in_dataframe(combined_df)
        except Exception as e:
            logger.warning(f"Could not replace username: {e}")
        
        table_title = props.Translatable({"en": "Instagram Activity Data", "nl": "Instagram Gegevens"})
        visses = [vis.create_chart(
            "line", 
            "Instagram Activiteit", 
            "Instagram Activiteit", 
            'Datum', 
            y_label="Aantal keren gekeken", 
            date_format="auto"
        )]

        table = props.PropsUIPromptConsentFormTable("instagram_all_data", table_title, combined_df, visualizations=visses)
        tables_to_render.append(table)
        
        logger.info(f"Successfully processed {len(combined_df)} total entries from Instagram data")
    else:
        logger.warning("No data was successfully extracted and parsed")
 
    return tables_to_render
