import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_instagram_data(instagram_zip: str) -> Dict[str, Any]:
    logger.info(f"Starting to extract data from {instagram_zip}")
    try:
        data = {}
        with zipfile.ZipFile(instagram_zip, 'r') as zip_ref:
            json_files = [f for f in zip_ref.namelist() if f.endswith('.json')]
            logger.info(f"Found {len(json_files)} JSON files in the zip: {json_files}")
            for file in json_files:
                logger.info(f"Extracting and parsing {file}")
                with zip_ref.open(file) as json_file:
                    try:
                        json_data = json.load(io.TextIOWrapper(json_file, encoding='utf-8'))
                        data[os.path.basename(file)] = json_data
                        logger.info(f"Successfully parsed {file}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse {file}: {str(e)}")
        logger.info(f"Extracted data from {len(data)} JSON files")
        return data
    except zipfile.BadZipFile:
        logger.error(f"The file {instagram_zip} is not a valid zip file")
    except Exception as e:
        logger.error(f"Unexpected error while extracting data: {str(e)}")
    return {}

def safe_get(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data
  
def parse_following(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing following data")
    following = safe_get(data, 'following.json', 'relationships_following', default=[])
    logger.info(f"Found {len(following)} accounts being followed")
    parsed_following = []
    for account in following:
        try:
            string_list_data = account.get('string_list_data', [{}])[0]
            parsed_following.append({
                'data_type': 'instagram_following',
                'Action': 'Follow',
                'title': string_list_data.get('value', 'Unknown Account'),
                'URL': string_list_data.get('href', ''),
                'Date': datetime.fromtimestamp(int(string_list_data.get('timestamp', 0))).isoformat(),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing following account: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_following)} following accounts")
    return parsed_following

def parse_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing posts data")
    posts = safe_get(data, 'posts_1.json', 'media', default=[])
    logger.info(f"Found {len(posts)} posts")
    parsed_posts = []
    for post in posts:
        try:
            parsed_posts.append({
                'data_type': 'instagram_post',
                'Action': 'Post',
                'title': post.get('title', ''),
                'URL': post.get('uri', ''),
                'Date': post.get('creation_timestamp', ''),
                'details': json.dumps({
                    'caption': post.get('caption', ''),
                    'location': post.get('location', ''),
                    'media_type': post.get('media_type', '')
                })
            })
        except Exception as e:
            logger.error(f"Error parsing post: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_posts)} posts")
    return parsed_posts

def parse_likes(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing likes data")
    likes = safe_get(data, 'likes.json', 'likes_media_likes', default=[])
    logger.info(f"Found {len(likes)} likes")
    parsed_likes = []
    for like in likes:
        try:
            parsed_likes.append({
                'data_type': 'instagram_like',
                'Action': 'Like',
                'title': like['title'],
                'URL': like['string_list_data'][0]['href'],
                'Date': datetime.fromtimestamp(like['string_list_data'][0]['timestamp']).isoformat(),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing like: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_likes)} likes")
    return parsed_likes

def parse_ads_clicked(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing clicked ads data")
    ads_clicked = safe_get(data, 'ads_clicked.json', 'impressions_history_ads_clicked', default=[])
    logger.info(f"Found {len(ads_clicked)} clicked ads")
    parsed_ads = []
    for ad in ads_clicked:
        try:
            parsed_ads.append({
                'data_type': 'instagram_ad_clicked',
                'Action': 'AdClick',
                'title': ad.get('title', 'Unknown Ad'),
                'URL': '',
                'Date': datetime.fromtimestamp(ad['string_list_data'][0]['timestamp']).isoformat(),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing clicked ad: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_ads)} clicked ads")
    return parsed_ads

def parse_ads_viewed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing viewed ads data")
    ads_viewed = safe_get(data, 'ads_viewed.json', 'impressions_history_ads_seen', default=[])
    logger.info(f"Found {len(ads_viewed)} viewed ads")
    parsed_ads = []
    for ad in ads_viewed:
        try:
            ad_data = {
                'data_type': 'instagram_ad_viewed',
                'Action': 'AdView',
                'title': 'Ad Viewed',
                'URL': '',
                'Date': '',
                'details': json.dumps({})
            }
            if 'string_map_data' in ad:
                if 'Author' in ad['string_map_data']:
                    ad_data['title'] = ad['string_map_data']['Author'].get('value', 'Ad Viewed')
                if 'Time' in ad['string_map_data']:
                    timestamp = ad['string_map_data']['Time'].get('timestamp')
                    if timestamp:
                        ad_data['Date'] = datetime.fromtimestamp(int(timestamp)).isoformat()
            parsed_ads.append(ad_data)
        except Exception as e:
            logger.error(f"Error parsing viewed ad: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_ads)} viewed ads")
    return parsed_ads

def parse_posts_viewed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing viewed posts data")
    posts_viewed = safe_get(data, 'posts_viewed.json', 'impressions_history_posts_seen', default=[])
    logger.info(f"Found {len(posts_viewed)} viewed posts")
    parsed_posts = []
    for post in posts_viewed:
        try:
            parsed_posts.append({
                'data_type': 'instagram_post_viewed',
                'Action': 'PostView',
                'title': post['string_map_data']['Author']['value'],
                'URL': '',
                'Date': datetime.fromtimestamp(int(post['string_map_data']['Time']['timestamp'])).isoformat(),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing viewed post: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_posts)} viewed posts")
    return parsed_posts

def parse_videos_watched(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logger.info("Parsing watched videos data")
    videos_watched = safe_get(data, 'videos_watched.json', 'impressions_history_videos_watched', default=[])
    logger.info(f"Found {len(videos_watched)} watched videos")
    parsed_videos = []
    for video in videos_watched:
        try:
            parsed_videos.append({
                'data_type': 'instagram_video_watched',
                'Action': 'VideoWatch',
                'title': video['string_map_data']['Author']['value'],
                'URL': '',
                'Date': datetime.fromtimestamp(int(video['string_map_data']['Time']['timestamp'])).isoformat(),
                'details': json.dumps({})
            })
        except Exception as e:
            logger.error(f"Error parsing watched video: {str(e)}")
    logger.info(f"Successfully parsed {len(parsed_videos)} watched videos")
    return parsed_videos

def parse_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    logger.info("Creating DataFrame from parsed data")
    df = pd.DataFrame(data)
    
    required_columns = ['data_type', 'Action', 'title', 'URL', 'Date', 'details']
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA
    
    logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
    return df

def process_insta_data(instagram_zip: str) -> pd.DataFrame:
    logger.info(f"Starting to process Instagram data from {instagram_zip}")
    extracted_data = extract_instagram_data(instagram_zip)
    
    all_data = []
    all_data.extend(parse_posts(extracted_data))
    all_data.extend(parse_likes(extracted_data))
    all_data.extend(parse_ads_clicked(extracted_data))
    all_data.extend(parse_ads_viewed(extracted_data))
    all_data.extend(parse_posts_viewed(extracted_data))
    all_data.extend(parse_videos_watched(extracted_data))
    all_data.extend(parse_following(extracted_data))

    
    if all_data:
        combined_df = parse_data(all_data)
        
        logger.info("Converting 'Date' column to datetime")
        combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
        logger.info("Sorting DataFrame by 'Date'")
        combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
        combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Successfully processed {len(combined_df)} total entries from Instagram data")
        return combined_df
    else:
        logger.warning("No data was successfully extracted and parsed")
        return pd.DataFrame()

# Helper functions for specific data types
def posts_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_post'].drop(columns=['data_type'])

def likes_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_like'].drop(columns=['data_type'])

def ads_clicked_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_ad_clicked'].drop(columns=['data_type'])

def ads_viewed_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_ad_viewed'].drop(columns=['data_type'])

def posts_viewed_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_post_viewed'].drop(columns=['data_type'])

def videos_watched_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_video_watched'].drop(columns['data_type'])

def following_to_df(instagram_zip: str) -> pd.DataFrame:
    df = process_insta_data(instagram_zip)
    return df[df['data_type'] == 'instagram_following'].drop(columns=['data_type'])


if __name__ == "__main__":
    logger.info("Instagram data processing script started")
    # Add any test code or main execution here if needed
    logger.info("Instagram data processing script finished")
