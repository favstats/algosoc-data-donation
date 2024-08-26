import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import os
import io
import re
from bs4 import UnicodeDammit
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

DATA_FORMAT = None  # Will be set to 'json' or 'txt' in extract_tiktok_data

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "user_data_tiktok.json",
            "TikTok_Data.json",
        ],
    ),
    DDPCategory(
        id="txt_en",
        ddp_filetype=DDPFiletype.TXT,
        language=Language.EN,
        known_files=[
            "Tiktok/Activity/Following.txt",
            "Tiktok/Activity/Searches.txt",
            "Tiktok/Activity/Comments.txt",
            "Tiktok/Activity/Hashtag.txt",
            "Tiktok/Activity/Like List.txt",
            "Tiktok/Activity/Share History.txt",
            "Tiktok/Activity/Login History.txt",
            "Tiktok/Activity/Browsing History.txt"
        ],
    ),
    DDPCategory(
        id="txt_en_flat",
        ddp_filetype=DDPFiletype.TXT,
        language=Language.EN,
        known_files=[
            "Searches.txt",
            "Following.txt",
            "Comments.txt",
            "Hashtag.txt",
            "Like List.txt",
            "Login History.txt",
            "Browsing History.txt"
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
                if p.suffix in (".json", ".txt"):
                    # logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        
        if validation.ddp_category is None:
            logger.warning("Could not infer DDP category")
            validation.set_status_code(1)  # Not a valid DDP
        elif validation.ddp_category.ddp_filetype in (DDPFiletype.JSON, DDPFiletype.TXT):
            validation.set_status_code(0)  # Valid DDP
            # Log the valid TikTok files found
            for p in paths:
                logger.debug("Found: %s in zip", p)
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


def extract_tiktok_data(tiktok_zip: str) -> Dict[str, Any]:
    global DATA_FORMAT
    data = {}
    try:
        with zipfile.ZipFile(tiktok_zip, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            
            json_files = [f for f in file_list if f.endswith('.json')]
            txt_files = [f for f in file_list if f.endswith('.txt')]
            
            if json_files:
                DATA_FORMAT = "json"
                files_to_process = json_files
            else:
                DATA_FORMAT = "txt"
                files_to_process = txt_files
            
            for file in files_to_process:
                with zip_ref.open(file) as f:
                    raw_data = f.read()
                    # Use UnicodeDammit to detect the encoding
                    suggestion = UnicodeDammit(raw_data)
                    encoding = suggestion.original_encoding
                    # logger.debug(f"Encountered encoding: {encoding}.")

                    try:
                        if DATA_FORMAT == "json":
                            data[os.path.basename(file)] = json.loads(raw_data.decode(encoding))
                        elif DATA_FORMAT == "txt":
                            content = raw_data.decode(encoding, errors='ignore')
                            category = os.path.basename(os.path.dirname(file))
                            file_name = os.path.basename(file).split('.')[0]
                            parsed_data = parse_txt_file(content, file_name)
                            if category not in data:
                                data[category] = {}
                            data[category][file_name] = parsed_data
                    except (UnicodeDecodeError, json.JSONDecodeError) as e:
                        logger.error(f"Error processing file {file} with encoding {encoding}: {str(e)}")
                        continue  # Skip the problematic file and continue with others

    except Exception as e:
        logger.error(f"Error reading TikTok zip file: {str(e)}")
        logger.exception("Exception details:")
    
    return data

def parse_txt_file(content: str, file_name: str) -> List[Dict[str, Any]]:
    entries = content.strip().split('\n\n')
    parsed_data = []
    for entry in entries:
        item = {}
        lines = entry.split('\n')
        for line in lines:
            parts = line.split(': ', 1)
            if len(parts) == 2:
                key, value = parts
                item[key] = value.strip()
            else:
                return []
                # item['Content'] = item.get('Content', '') + ' ' + line.strip()
        if item:
            parsed_data.append(item)
    return parsed_data

def safe_get(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data

def parse_following_list(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        following_key = "Following"
        title_key = "UserName"
    elif DATA_FORMAT == "txt":
        following_key = "Following"
        title_key = "Username"
    
    following_list = helpers.find_items_bfs(data, following_key)
    if not following_list:
      return []
    return [
        {
            'data_type': 'tiktok_following',
            'Action': 'Following',
            'title': f"Following {user.get(title_key, 'Unknown')}",
            'URL': '',
            'Date': user.get('Date', ''),
            'details': json.dumps({})
        } for user in following_list if isinstance(user, dict)
    ]

def parse_hashtags(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        hashtag_key = "HashtagList"
        name_key = "HashtagName"
        link_key = "HashtagLink"
    elif DATA_FORMAT == "txt":
        hashtag_key = "Hashtag"
        name_key = "Hashtag Name"
        link_key = "Hashtag Link"
    
    hashtags = helpers.find_items_bfs(data, hashtag_key)
    if not hashtags:
      return []
    return [
        {
            'data_type': 'tiktok_hashtag',
            'Action': 'HashtagUse',
            'title': ht.get(name_key, 'Unknown'),
            'URL': ht.get(link_key, ''),
            'Date': '',
            'details': json.dumps({})
        } for ht in hashtags if isinstance(ht, dict)
    ]

def parse_login_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        login_key = "LoginHistoryList"
        device_model_key = "DeviceModel"
        device_system_key = "DeviceSystem"
        network_type_key = "NetworkType"
    elif DATA_FORMAT == "txt":
        login_key = "Login History"
        device_model_key = "Device Model"
        device_system_key = "Device System"
        network_type_key = "Network Type"
    
    logins = helpers.find_items_bfs(data, login_key)
    # logger.info(f"Login data from {logins}")
    if not logins:
      return []
    return [
        {
            'data_type': 'tiktok_login',
            'Action': 'Login',
            'title': f"Login from {login.get(device_model_key, 'Unknown')} ({login.get(device_system_key, 'Unknown')})",
            'URL': '',
            'Date': login.get('Date', ''),
            'details': json.dumps({})
        } for login in logins if isinstance(login, dict)
    ]

def parse_video_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        video_key = "VideoList"
    elif DATA_FORMAT == "txt":
        video_key = "Browsing History"
    
    videos = helpers.find_items_bfs(data, video_key)
    if not videos:
      return []
    return [
        {
            'data_type': 'tiktok_video_view',
            'Action': 'VideoView',
            'title': 'Watched video',
            'URL': video.get('Link', ''),
            'Date': video.get('Date', ''),
            'details': json.dumps({})
        } for video in videos if isinstance(video, dict)
    ]

def parse_share_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        share_key = "ShareHistoryList"
        content_key = "SharedContent"
    elif DATA_FORMAT == "txt":
        share_key = "Share History"
        content_key = "Shared Content"
    
    shares = helpers.find_items_bfs(data, share_key)
    if not shares:
      return []
    return [
        {
            'data_type': 'tiktok_share',
            'Action': 'Share',
            'title': share.get(content_key, 'Unknown'),
            'URL': share.get('Link', ''),
            'Date': share.get('Date', ''),
            'details': json.dumps({'Method': share.get('Method', '')})
        } for share in shares if isinstance(share, dict)
    ]

def parse_like_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        like_key = "ItemFavoriteList"
    elif DATA_FORMAT == "txt":
        like_key = "Like List"
    
    likes = helpers.find_items_bfs(data, like_key)
    if not likes:
      return []
    return [
        {
            'data_type': 'tiktok_like',
            'Action': 'Like',
            'title': 'Liked video',
            'URL': like.get('Link', ''),
            'Date': like.get('Date', ''),
            'details': json.dumps({})
        } for like in likes if isinstance(like, dict)
    ]
    
def parse_fav_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        like_key = "FavoriteVideoList"
    elif DATA_FORMAT == "txt":
        like_key = "Favorite Videos"
    
    likes = helpers.find_items_bfs(data, like_key)
    if not likes:
      return []
    return [
        {
            'data_type': 'tiktok_favorite',
            'Action': 'Favorite',
            'title': 'Favorited video',
            'URL': like.get('Link', ''),
            'Date': like.get('Date', ''),
            'details': json.dumps({})
        } for like in likes if isinstance(like, dict)
    ]
    
def parse_fav_hashtag(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        like_key = "FavoriteHashtagList"
    elif DATA_FORMAT == "txt":
        like_key = "Favorite HashTags"
    
    likes = helpers.find_items_bfs(data, like_key)
    if not likes:
      return []
    return [
        {
            'data_type': 'tiktok_favorite_hashtag',
            'Action': 'Favorite',
            'title': 'Favorited Hashtag',
            'URL': like.get('Link', like.get('HashTag Link', like.get('HashTag Link:', ''))),
            'Date': like.get('Date', ''),
            'details': json.dumps({})
        } for like in likes if isinstance(like, dict)
    ]

def parse_search_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        search_key = "SearchList"
        term_key = "SearchTerm"
    elif DATA_FORMAT == "txt":
        search_key = "Searches"
        term_key = "Search Term"
    
    searches = helpers.find_items_bfs(data, search_key)
    if not searches:
      return []
    return [
        {
            'data_type': 'tiktok_search',
            'Action': 'Search',
            'title': search.get(term_key, 'Unknown search'),
            'URL': '',
            'Date': search.get('Date', ''),
            'details': json.dumps({})
        } for search in searches if isinstance(search, dict)
    ]

def parse_ad_info(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        ad_key = "AdInterestCategories"
    elif DATA_FORMAT == "txt":
        ad_key = "Ad Interests"

    ad_interests = helpers.find_items_bfs(data, ad_key)
    if not ad_interests:
      return []
    return [
        {
            'data_type': 'tiktok_ad_interest',
            'Action': 'AdInterest',
            'title': interest,
            'URL': '',
            'Date': '',
            'details': json.dumps({})
        } for interest in ad_interests if isinstance(interest, str)
    ]
    
def parse_ad_ca(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        ad_key = "OffTikTokActivityDataList"
    elif DATA_FORMAT == "txt":
        ad_key = "Off TikTok Activity"
    
    ad_interests = helpers.find_items_bfs(data, ad_key)
    if not ad_interests:
      return []
    return [
        {
            'data_type': 'tiktok_custom_audiences',
            'Action': interest.get("Event", 'Unknown action'),
            'title': interest.get("Source", 'Unknown uploader'),
            'URL': '',
            'Date': interest.get("TimeStamp", interest.get("Date", '')),
            'details': json.dumps({})
        } for interest in ad_interests if isinstance(interest, dict) and interest.get("Event") == "Customer file upload"
    ]


def parse_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        comments_key = "CommentsList"
    elif DATA_FORMAT == "txt":
        comments_key = "Comments"
    
    comments = helpers.find_items_bfs(data, comments_key)
    if not comments:
      return []
    return [
        {
            'data_type': 'tiktok_comment',
            'Action': 'Comment',
            'title': comment.get('Comment', ''),
            'URL': comment.get('Url', ''),
            'Date': comment.get('Date', ''),
            'details': json.dumps({'Photo': comment.get('Photo', '')})
        } for comment in comments if isinstance(comment, dict)
    ]

def parse_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    
    required_columns = ['data_type', 'Action', 'title', 'URL', 'Date', 'details']
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA
    
    return df

def process_tiktok_data(tiktok_file: str) -> List[props.PropsUIPromptConsentFormTable]:
    logger = logging.getLogger("process_tiktok_data")
    logger.info("Starting to extract TikTok data from {tiktok_file}.")   

    
    extracted_data = extract_tiktok_data(tiktok_file)
    # Assuming `extracted_data` is a dictionary where keys are the file paths or names.
    filtered_extracted_data = {
        k: v for k, v in extracted_data.items() if not re.match(r'^\d+\.html$', k.split('/')[-1])
    }
    
    # Logging only the filtered keys
    logger.info(f"Extracted data keys: {helpers.get_json_keys(filtered_extracted_data) if filtered_extracted_data else 'None'}")   
    
    all_data = []
    parsing_functions = [
        # parse_hashtags, 
        parse_login_history, 
        parse_video_history, 
        parse_share_history, 
        parse_like_history, 
        parse_fav_hashtag,
        parse_fav_history,
        parse_ad_ca,
        parse_search_history,   
        parse_ad_info,
        parse_comments, 
        parse_following_list
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
        logger.info(f"Combined data frame shape: {combined_df.shape}")
        
        if not combined_df.empty:
            combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
            
             # Check for entries with dates before 2016
            pre_2016_count = (combined_df['Date'] < pd.Timestamp('2016-01-01')).sum()
            if pre_2016_count > 0:
                logger.info(f"Found {pre_2016_count} entries with dates before 2016.")
           
                # Filter out dates before 2016
                try:
                    # Filter out dates before 2016
                    combined_df = combined_df[combined_df['Date'] >= pd.Timestamp('2016-01-01')]

                    # Confirm deletion
                    if pre_2016_count > 0:
                        post_filter_count = (combined_df['Date'] < pd.Timestamp('2016-01-01')).sum()
                        if post_filter_count == 0:
                            logger.info(f"Successfully deleted {pre_2016_count} entries with dates before 2016.")
                        else:
                            logger.info(f"Failed to delete some entries with dates before 2016. Remaining: {post_filter_count}.")

                        
                except Exception as e:
                    logger.info(f"Error filtering dates before 2016: {e}")
            
            combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
            combined_df['Count'] = 1  # Add a Count column to the original data

            try: 
              # Apply the helpers.replace_email function to each of the specified columns
              combined_df['title'] = combined_df['title'].apply(helpers.replace_email)
              combined_df['details'] = combined_df['details'].apply(helpers.replace_email)
              combined_df['Action'] = combined_df['Action'].apply(helpers.replace_email)
            except Exception as e:
               logger.warning(f"Could not replace e-mail: {e}")

            combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            

            # Create a single table with all data
            table_title = props.Translatable({"en": "TikTok Activity Data", "nl": "TikTok Gegevens"})
            visses = [vis.create_chart(
              "line", 
              "TikTok Video Browsing Over Time", 
              "TikTok-activiteit", 
              "Date", 
              y_label="Aantal observaties", 
              date_format="auto"#, 
              # group_by="Action", 
              # df=combined_df.groupby('Action')['Count'].sum().reset_index()
            )]

            logger.info(f"Visualizations created: {len(visses)}")
            
            # Pass the ungrouped data for the table and grouped data for the chart
            table = props.PropsUIPromptConsentFormTable("tiktok_all_data", table_title, combined_df, visualizations=visses)
            tables_to_render.append(table)
            
            logger.info(f"Successfully processed First {len(combined_df)} total entries from TikTok data")

        else:
            logger.warning("First Combined DataFrame is empty")  
    else:
        logger.warning("First Combined DataFrame: No data with dates was successfully extracted and parsed")
                
    ### this is for all things without dates
    all_data = []
    parsing_functions = [
        parse_hashtags#, parse_ad_info
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
       
            if 'details' in combined_df.columns:
                combined_df = combined_df.drop(columns=['details'])
            # Create a single table with all data
            table_title = props.Translatable({"en": "TikTok Hashtag Use and Ad Info", "nl": "TikTok Gegevens"})


            # Pass the ungrouped data for the table and grouped data for the chart
            table = props.PropsUIPromptConsentFormTable("tiktok_hashtag_ads", table_title, combined_df)
            tables_to_render.append(table)
            
            logger.info(f"Successfully processed Second {len(combined_df)} total entries from TikTok data")
        else:
            logger.warning("Second Combined DataFrame is empty")
    else:
        logger.warning("Second Combined DataFrame: No data without dates was successfully extracted and parsed")
    
    return tables_to_render

# Helper functions for specific data types
def video_browsing_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    tables = process_tiktok_data(tiktok_zip)
    if tables:
        df = tables[0].df
        return df[df['data_type'] == 'tiktok_video_view'].drop(columns=['data_type'])
    return pd.DataFrame()

def favorite_videos_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    tables = process_tiktok_data(tiktok_zip)
    if tables:
        df = tables[0].df
        return df[df['data_type'] == 'tiktok_like'].drop(columns=['data_type'])
    return pd.DataFrame()

def following_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    tables = process_tiktok_data(tiktok_zip)
    if tables:
        df = tables[0].df
        return df[df['data_type'] == 'tiktok_following'].drop(columns=['data_type'])
    return pd.DataFrame()

def like_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    tables = process_tiktok_data(tiktok_zip)
    if tables:
        df = tables[0].df
        return df[df['data_type'] == 'tiktok_like'].drop(columns=['data_type'])
    return pd.DataFrame()

def search_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    tables = process_tiktok_data(tiktok_zip)
    if tables:
        df = tables[0].df
        return df[df['data_type'] == 'tiktok_search'].drop(columns=['data_type'])
    return pd.DataFrame()

def share_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    tables = process_tiktok_data(tiktok_zip)
    if tables:
        df = tables[0].df
        return df[df['data_type'] == 'tiktok_share'].drop(columns=['data_type'])
    return pd.DataFrame()

def comment_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    tables = process_tiktok_data(tiktok_zip)
    if tables:
        df = tables[0].df
        return df[df['data_type'] == 'tiktok_comment'].drop(columns=['data_type'])
    return pd.DataFrame()

def hashtags_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    tables = process_tiktok_data(tiktok_zip)
    if tables:
        df = tables[0].df
        return df[df['data_type'] == 'tiktok_hashtag'].drop(columns=['data_type'])
    return pd.DataFrame()

def login_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    tables = process_tiktok_data(tiktok_zip)
    if tables:
        df = tables[0].df
        return df[df['data_type'] == 'tiktok_login'].drop(columns=['data_type'])
    return pd.DataFrame()

def ad_interests_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    tables = process_tiktok_data(tiktok_zip)
    if tables:
        df = tables[0].df
        return df[df['data_type'] == 'tiktok_ad_interest'].drop(columns=['data_type'])
    return pd.DataFrame()
