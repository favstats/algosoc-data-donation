import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
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
        known_files=[
            "accounts_you're_not_interested_in.json",
            "ads_viewed.json",
            "posts_viewed.json",
            "videos_watched.json",
            "your_topics.json",
            "post_comments.json",
            "liked_posts.json",
            "following.json",
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
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message="Valid DDP"),
    StatusCode(id=1, description="Not a valid DDP", message="Not a valid DDP"),
    StatusCode(id=2, description="Bad zipfile", message="Bad zip"),
]

def validate(file: Path) -> ValidateInput:
    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)
    
    try:
        paths = []
        with zipfile.ZipFile(file, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".json", ".html"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        
        if validation.ddp_category is None:
            logger.warning("Could not infer DDP category")
            validation.set_status_code(1)  # Not a valid DDP
        else:
            validation.set_status_code(0)  # Valid DDP

    except zipfile.BadZipFile:
        logger.error("Bad zip file")
        validation.set_status_code(2)  # Bad zipfile
    except Exception as e:
        logger.error(f"Unexpected error during validation: {str(e)}")
        validation.set_status_code(1)  # Not a valid DDP

    return validation

def extract_instagram_data(instagram_zip: str) -> Dict[str, Any]:
    global DATA_FORMAT
    validation = validate(Path(instagram_zip))
    if validation.status_code is None or validation.status_code.id != 0:
        logger.error(f"Invalid zip file: {validation.status_code.description if validation.status_code else 'Unknown error'}")
        return {}

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
          'Date': helpers.epoch_to_iso(post.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
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
          'Date': helpers.epoch_to_iso(video.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
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
      
      
      
def parse_post_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      comments = helpers.find_items_bfs(data, "comments_media_comments")

      if not comments:
        return []
      return [{
          'data_type': 'instagram_post_comment',
          'Action': 'Comment',
          'title': comment.get("string_map_data", {}).get("Media Owner", {}).get("value", "Unknown Media Owner"),
          'URL': '',
          'Date': helpers.epoch_to_iso(comment.get("string_map_data", {}).get("Time", {}).get("timestamp", 0)),
          'details': json.dumps({'comment': comment.get("string_map_data", {}).get("Comment", {}).get("value", "")})
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
                        author = 'Unknown Author'

                    # Ensure lists are not empty before accessing elements
                    author_text = author.strip() if author else 'Unknown Author'
                    title_text = title.strip() if title else 'Unknown Text'

                    date_text = helpers.robust_datetime_parser(date.strip()) if date else ''

                    parsed_item = {
                        'data_type': 'instagram_post_comment',
                        'Action': 'Comment',
                        'title': author_text,
                        'origin': title_text,
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
          'title': post.get("string_list_data", [{}])[0].get("value", "Unknown Post"),
          'URL': post.get("string_list_data", [{}])[0].get("href", ""),
          'Date': helpers.epoch_to_iso(post.get("string_list_data", [{}])[0].get("timestamp", 0)),
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
          'Date': helpers.epoch_to_iso(account.get("string_list_data", [{}])[0].get("timestamp", 0)),
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
            logger.error(f"Error parsing 'following.html': {str(e)}")
            return []
          
def parse_searches(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      searches = helpers.find_items_bfs(data, "searches_keyword")
      if not searches:
          return []
      
      return [{
          'data_type': 'instagram_search',
          'Action': 'Search',
          'title': search['string_map_data']['Search']['value'],
          'URL': '',
          'Date': helpers.epoch_to_iso(search['string_map_data']['Time']['timestamp']),
          'details': json.dumps({})
      } for search in searches]
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
            logger.error(f"Error parsing 'word_or_phrase_searches.html': {str(e)}")
            return []

def parse_posted_reels(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      reels = helpers.find_items_bfs(data, "organic_insights_reels")
      if not reels:
          return []
      
      return [{
          'data_type': 'instagram_posted_reel',
          'Action': 'PostedReel',
          'title': reel['string_map_data'].get('Caption', {}).get('value', '') or reel['media_map_data']['Media Thumbnail'].get('title', ''),
          'URL': '',
          'Date': helpers.epoch_to_iso(reel['string_map_data']['Upload Timestamp']['timestamp']),
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

def parse_posted_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
      posts = helpers.find_items_bfs(data, "organic_insights_posts")
      if not posts:
          return []
      
      return [{
          'data_type': 'instagram_posted_post',
          'Action': 'PostedPost',
          'title': post['media_map_data']['Media Thumbnail'].get('title', ''),
          'URL': '',
          'Date': helpers.epoch_to_iso(post['string_map_data']['Creation Timestamp']['timestamp']),
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


def process_instagram_data(instagram_zip: str) -> List[props.PropsUIPromptConsentFormTable]:
    extracted_data = extract_instagram_data(instagram_zip)
    
    
    all_data = []
    parsing_functions = [
        parse_ads_viewed, 
        parse_posts_viewed, 
        parse_videos_watched,
        parse_post_comments,
        parse_liked_posts, 
        parse_following,
        parse_searches, 
        parse_posted_reels,
        parse_posted_posts,
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
        combined_df = pd.DataFrame(all_data)
        try: 
          combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
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
