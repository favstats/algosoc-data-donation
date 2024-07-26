import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io

logger = logging.getLogger(__name__)

def extract_tiktok_data(tiktok_zip: str) -> Dict[str, Any]:
    try:
        with zipfile.ZipFile(tiktok_zip, 'r') as zip_ref:
            json_files = [f for f in zip_ref.namelist() if f.endswith('.json')]
            if not json_files:
                raise ValueError("No JSON file found in the zip archive")
            
            json_file = json_files[0]  # Take the first JSON file found
            with zip_ref.open(json_file) as file:
                data = json.load(io.TextIOWrapper(file, encoding='utf-8'))
        return data
    except zipfile.BadZipFile:
        logger.error(f"The file {tiktok_zip} is not a valid zip file")
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from file {json_file}")
    except Exception as e:
        logger.error(f"Error reading TikTok zip file: {e}")
    return {}
  

def safe_get(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data

def parse_hashtags(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    hashtags = safe_get(data, 'Activity', 'Hashtag', 'HashtagList', default=[])
    return [
        {
            'data_type': 'tiktok_hashtag',
            'Action': 'HashtagUse',
            'title': ht['HashtagName'],
            'URL': ht['HashtagLink'],
            'Date': '',  # No date provided in the sample data
            'details': json.dumps({})
        } for ht in hashtags
    ]

def parse_login_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    logins = safe_get(data, 'Activity', 'Login History', 'LoginHistoryList', default=[])
    return [
        {
            'data_type': 'tiktok_login',
            'Action': 'Login',
            'title': f"Login from {login['DeviceModel']} ({login['DeviceSystem']})",
            'URL': '',
            'Date': login['Date'],
            'details': json.dumps({
                'IP': login['IP'],
                'NetworkType': login['NetworkType'],
                'Carrier': login['Carrier']
            })
        } for login in logins
    ]

def parse_video_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    videos = safe_get(data, 'Activity', 'Video Browsing History', 'VideoList', default=[])
    return [
        {
            'data_type': 'tiktok_video_view',
            'Action': 'VideoView',
            'title': 'Watched video',
            'URL': video['Link'],
            'Date': video['Date'],
            'details': json.dumps({})
        } for video in videos
    ]

def parse_share_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    shares = safe_get(data, 'Activity', 'Share History', 'ShareHistoryList', default=[])
    return [
        {
            'data_type': 'tiktok_share',
            'Action': 'Share',
            'title': share['SharedContent'],
            'URL': share['Link'],
            'Date': share['Date'],
            'details': json.dumps({'Method': share['Method']})
        } for share in shares
    ]

def parse_like_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    likes = safe_get(data, 'Activity', 'Like List', 'ItemFavoriteList', default=[])
    return [
        {
            'data_type': 'tiktok_like',
            'Action': 'Like',
            'title': 'Liked video',
            'URL': like['Link'],
            'Date': like['Date'],
            'details': json.dumps({})
        } for like in likes
    ]

def parse_search_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    searches = safe_get(data, 'Activity', 'Search History', 'SearchList', default=[])
    return [
        {
            'data_type': 'tiktok_search',
            'Action': 'Search',
            'title': search['SearchTerm'],
            'URL': '',
            'Date': search['Date'],
            'details': json.dumps({})
        } for search in searches
    ]

def parse_ad_info(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    ad_activities = safe_get(data, 'Ads and data', 'Ad Interests', 'AdInterestCategories', default=[])
    return [
        {
            'data_type': 'tiktok_ad_activity',
            'Action': 'AdActivity',
            'title': activity['Event'],
            'URL': '',
            'Date': activity['TimeStamp'],
            'details': json.dumps({'Source': activity['Source']})
        } for activity in ad_activities
    ]

def parse_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    comments = safe_get(data, 'Comment', 'Comments', 'CommentsList', default=[])
    return [
        {
            'data_type': 'tiktok_comment',
            'Action': 'Comment',
            'title': comment['Comment'],
            'URL': comment['Url'],
            'Date': comment['Date'],
            'details': json.dumps({'Photo': comment['Photo']})
        } for comment in comments
    ]

def parse_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    
    required_columns = ['data_type', 'Action', 'title', 'URL', 'Date', 'details']
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA
    
    return df

def process_tiktok_data(tiktok_zip: str) -> pd.DataFrame:
    extracted_data = extract_tiktok_data(tiktok_zip)
    
    all_data = []
    all_data.extend(parse_hashtags(extracted_data))
    all_data.extend(parse_login_history(extracted_data))
    all_data.extend(parse_video_history(extracted_data))
    all_data.extend(parse_share_history(extracted_data))
    all_data.extend(parse_like_history(extracted_data))
    all_data.extend(parse_search_history(extracted_data))
    all_data.extend(parse_ad_info(extracted_data))
    all_data.extend(parse_comments(extracted_data))
    
    if all_data:
        combined_df = parse_data(all_data)
        
        combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
        combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
        combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Successfully processed {len(combined_df)} total entries from TikTok data")
        return combined_df
    else:
        logger.warning("No data was successfully extracted and parsed")
        return pd.DataFrame()

# Helper functions for specific data types
def hashtags_to_df(tiktok_zip: str) -> pd.DataFrame:
    df = process_tiktok_data(tiktok_zip)
    return df[df['data_type'] == 'tiktok_hashtag'].drop(columns=['data_type'])

def login_history_to_df(tiktok_zip: str) -> pd.DataFrame:
    df = process_tiktok_data(tiktok_zip)
    return df[df['data_type'] == 'tiktok_login'].drop(columns=['data_type'])

def video_history_to_df(tiktok_zip: str) -> pd.DataFrame:
    df = process_tiktok_data(tiktok_zip)
    return df[df['data_type'] == 'tiktok_video_view'].drop(columns=['data_type'])

def share_history_to_df(tiktok_zip: str) -> pd.DataFrame:
    df = process_tiktok_data(tiktok_zip)
    return df[df['data_type'] == 'tiktok_share'].drop(columns=['data_type'])

def like_history_to_df(tiktok_zip: str) -> pd.DataFrame:
    df = process_tiktok_data(tiktok_zip)
    return df[df['data_type'] == 'tiktok_like'].drop(columns=['data_type'])

def search_history_to_df(tiktok_zip: str) -> pd.DataFrame:
    df = process_tiktok_data(tiktok_zip)
    return df[df['data_type'] == 'tiktok_search'].drop(columns=['data_type'])

def ad_info_to_df(tiktok_zip: str) -> pd.DataFrame:
    df = process_tiktok_data(tiktok_zip)
    return df[df['data_type'] == 'tiktok_ad_activity'].drop(columns=['data_type'])

def comments_to_df(tiktok_zip: str) -> pd.DataFrame:
    df = process_tiktok_data(tiktok_zip)
    return df[df['data_type'] == 'tiktok_comment'].drop(columns=['data_type'])
