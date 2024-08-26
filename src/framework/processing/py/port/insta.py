import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
import os
import re

from port.validate import (
    DDPCategory,
    StatusCode,
    ValidateInput,
    Language,
    DDPFiletype,
)
import port.unzipddp as unzipddp
import port.helpers as helpers

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create a logs directory if it doesn't exist
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

# Generate a unique log file name based on the current timestamp
log_file_name = f'instagram_data_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
log_file_path = os.path.join(log_dir, log_file_name)
log_file_path = os.path.abspath(os.path.join(log_dir, log_file_name))

# Set up logging to both console and file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info(f"Logging to file: {log_file_path}")

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
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message=""),
    StatusCode(id=1, description="Not a valid DDP", message=""),
    StatusCode(id=2, description="Bad zipfile", message=""),
]

def validate(zfile: str) -> ValidateInput:
    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".html", ".json"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        if validation.ddp_category.id is None:
            validation.set_status_code(1)
        else:
            validation.set_status_code(0)

    except zipfile.BadZipFile:
        validation.set_status_code(2)

    return validation

def extract_instagram_data(instagram_zip: str) -> Dict[str, Any]:
    logger.info(f"Starting to extract data from {instagram_zip}")
    data = {}
    try:
        with zipfile.ZipFile(instagram_zip, 'r') as zip_ref:
            all_files = zip_ref.namelist()
            json_files = [f for f in all_files if f.endswith('.json')]
            html_files = [f for f in all_files if f.endswith('.html')]
            
            if json_files:
                logger.info(f"Found {len(json_files)} JSON files in the zip")
                for file in json_files:
                    logger.info(f"Extracting and parsing {file}")
                    buf = unzipddp.extract_file_from_zip(instagram_zip, file)
                    data[os.path.basename(file)] = unzipddp.read_json_from_bytes(buf)
                    logger.info(f"Successfully parsed {file}")
            elif html_files:
                logger.info(f"Found {len(html_files)} HTML files in the zip")
                for file in html_files:
                    logger.info(f"Extracting and parsing {file}")
                    buf = unzipddp.extract_file_from_zip(instagram_zip, file)
                    data[os.path.basename(file)] = parse_html(buf)
                    logger.info(f"Successfully parsed {file}")
            else:
                logger.warning("No JSON or HTML files found in the zip")
        
        logger.info(f"Extracted data from {len(data)} files")
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
    return data

def parse_html(html_content: bytes) -> Dict[str, Any]:
    soup = BeautifulSoup(html_content, 'html.parser')
    data = {}
    
    # Parse different sections based on their headings
    headings = soup.find_all(['h1', 'h2', 'h3'])
    for heading in headings:
        section_name = heading.text.strip().lower().replace(' ', '_')
        section_data = []
        current = heading.find_next_sibling()
        while current and current.name not in ['h1', 'h2', 'h3']:
            if current.name == 'div' and 'content' in current.get('class', []):
                item = {}
                for p in current.find_all('p'):
                    key = p.find('strong')
                    value = p.find('span')
                    if key and value:
                        item[key.text.strip()] = value.text.strip()
                if item:
                    section_data.append(item)
            current = current.find_next_sibling()
        if section_data:
            data[section_name] = section_data
    
    return data

def parse_accounts_not_interested_in(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing accounts not interested in data")
    accounts = data.get("accounts_you're_not_interested_in.json", {}).get("impressions_history_recs_hidden_authors", [])
    parsed_accounts = []
    for account in accounts:
        try:
            data = account.get("string_map_data", {})
            parsed_accounts.append({
                'data_type': 'instagram_account_not_interested',
                'Action': 'NotInterested',
                'title': data.get("Username", {}).get("value", "Unknown Account"),
                'URL': '',
                'Date': helpers.epoch_to_iso(data.get("Time", {}).get("timestamp", 0)),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing account not interested in: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_accounts)} accounts not interested in")
    return parsed_accounts

def parse_ads_viewed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing viewed ads data")
    ads_viewed = data.get("ads_viewed.json", {}).get("impressions_history_ads_seen", [])
    parsed_ads = []
    for ad in ads_viewed:
        try:
            ad_data = ad.get("string_map_data", {})
            parsed_ads.append({
                'data_type': 'instagram_ad_viewed',
                'Action': 'AdView',
                'title': ad_data.get("Author", {}).get("value", "Unknown Ad"),
                'URL': '',
                'Date': helpers.epoch_to_iso(ad_data.get("Time", {}).get("timestamp", 0)),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing viewed ad: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_ads)} viewed ads")
    return parsed_ads

def parse_posts_viewed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing viewed posts data")
    posts_viewed = data.get("posts_viewed.json", {}).get("impressions_history_posts_seen", [])
    parsed_posts = []
    for post in posts_viewed:
        try:
            post_data = post.get("string_map_data", {})
            parsed_posts.append({
                'data_type': 'instagram_post_viewed',
                'Action': 'PostView',
                'title': post_data.get("Author", {}).get("value", "Unknown Author"),
                'URL': '',
                'Date': helpers.epoch_to_iso(post_data.get("Time", {}).get("timestamp", 0)),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing viewed post: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_posts)} viewed posts")
    return parsed_posts

def parse_videos_watched(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing watched videos data")
    videos_watched = data.get("videos_watched.json", {}).get("impressions_history_videos_watched", [])
    parsed_videos = []
    for video in videos_watched:
        try:
            video_data = video.get("string_map_data", {})
            parsed_videos.append({
                'data_type': 'instagram_video_watched',
                'Action': 'VideoWatch',
                'title': video_data.get("Author", {}).get("value", "Unknown Author"),
                'URL': '',
                'Date': helpers.epoch_to_iso(video_data.get("Time", {}).get("timestamp", 0)),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing watched video: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_videos)} watched videos")
    return parsed_videos

def parse_post_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing post comments data")
    comments = data.get("post_comments.json", {}).get("comments_media_comments", [])
    parsed_comments = []
    for comment in comments:
        try:
            comment_data = comment.get("string_map_data", {})
            parsed_comments.append({
                'data_type': 'instagram_post_comment',
                'Action': 'Comment',
                'title': comment_data.get("Media Owner", {}).get("value", "Unknown Media Owner"),
                'URL': '',
                'Date': helpers.epoch_to_iso(comment_data.get("Time", {}).get("timestamp", 0)),
                'details': json.dumps({'comment': comment_data.get("Comment", {}).get("value", "")})
            })
        except Exception as e:
            logger.error(f"Error parsing post comment: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_comments)} post comments")
    return parsed_comments

def parse_liked_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing liked posts data")
    liked_posts = data.get("liked_posts.json", {}).get("likes_media_likes", [])
    parsed_liked_posts = []
    for liked_post in liked_posts:
        try:
            post_data = liked_post.get("string_list_data", [{}])[0]
            parsed_liked_posts.append({
                'data_type': 'instagram_liked_post',
                'Action': 'LikePost',
                'title': post_data.get("value", "Unknown Post"),
                'URL': post_data.get("href", ""),
                'Date': helpers.epoch_to_iso(post_data.get("timestamp", 0)),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing liked post: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_liked_posts)} liked posts")
    return parsed_liked_posts

def parse_following(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing following data")
    following = data.get("following.json", {}).get("relationships_following", [])
    parsed_following = []
    for account in following:
        try:
            account_data = account.get("string_list_data", [{}])[0]
            parsed_following.append({
                'data_type': 'instagram_following',
                'Action': 'Follow',
                'title': account_data.get("value", "Unknown Account"),
                'URL': account_data.get("href", ""),
                'Date': helpers.epoch_to_iso(account_data.get("timestamp", 0)),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing following account: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_following)} following accounts")
    return parsed_following

def process_insta_data(instagram_zip: str) -> pd.DataFrame:
    logger.info(f"Starting to process Instagram data from {instagram_zip}")
    extracted_data = extract_instagram_data(instagram_zip)
    
    all_data = []
    all_data.extend(parse_accounts_not_interested_in(extracted_data))
    all_data.extend(parse_ads_viewed(extracted_data))
    all_data.extend(parse_posts_viewed(extracted_data))
    all_data.extend(parse_videos_watched(extracted_data))
    all_data.extend(parse_post_comments(extracted_data))
    all_data.extend(parse_liked_posts(extracted_data))
    all_data.extend(parse_following(extracted_data))
    
    if all_data:
        combined_df = pd.DataFrame(all_data)
        
        logger.info("Converting 'Date' column to datetime")
        combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
        logger.info("Sorting DataFrame by 'Date'")
        combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
        combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Successfully processed {len(combined_df)} total entries from Instagram data")
        logger.info("Instagram data processing script finished")
        print(f"\nLog file saved to: {log_file_path}")
        return combined_df
    else:
        logger.warning("No data was successfully extracted and parsed")
        logger.info("Instagram data processing script finished")
        print(f"\nLog file saved to: {log_file_path}")
        return pd.DataFrame()

# Helper functions for specific data types
def posts_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_post'].drop(columns=['data_type'])

def likes_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_liked_post'].drop(columns=['data_type'])

def ads_viewed_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_ad_viewed'].drop(columns=['data_type'])

def posts_viewed_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_post_viewed'].drop(columns=['data_type'])

def videos_watched_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_video_watched'].drop(columns=['data_type'])

def following_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_following'].drop(columns=['data_type'])

def post_comments_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_post_comment'].drop(columns=['data_type'])

def accounts_not_interested_in_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_account_not_interested'].drop(columns=['data_type'])
