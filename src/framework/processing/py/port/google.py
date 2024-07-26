"""
DDP extract Chrome
"""
from pathlib import Path
import logging
import zipfile
from typing import Any, Dict, List
import pandas as pd
from tqdm.auto import tqdm
from datetime import datetime
import csv
import json

import port.unzipddp as unzipddp
import port.helpers as helpers
from port.validate import (
    DDPCategory,
    Language,
    DDPFiletype,
    ValidateInput,
    StatusCode,
)


# import os
# os.getcwd()
# os.setcw
# os.chdir('src/framework/processing/py/port')

import port.unzipddp as unzipddp
import port.helpers as helpers
from port.validate import (
    DDPCategory,
    Language,
    DDPFiletype,
    ValidateInput,
    StatusCode,
)
# local = True
# import unzipddp
# import helpers
# from validate import (
#     DDPCategory,
#     Language,
#     DDPFiletype,
#     ValidateInput,
#     StatusCode,
# )



logger = logging.getLogger(__name__)


DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "Autofill.json",
            "Bookmarks.html",
            "BrowserHistory.json",
            "Device Information.json",
            "Dictionary.csv",
            "Extensions.json",
            "Omnibox.json",
            "OS Settings.json",
            "ReadingList.html",
            "SearchEngines.json",
            "SyncSettings.json",
            "My Activity.json",
            "My Activities.json",
            "comments.csv"
        ],
    ),
    DDPCategory(
        id="json_nl",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.NL,
        known_files=[
            "Adressen en meer.json",
            "Bookmarks.html",
            "Geschiedenis.json",
            "Leeslijst.html",
            "Woordenboek.csv",
            "Apparaatgegevens.json",
            "Extensies.json",
            "Instellingen.json",
            "OS-instellingen.json",
            "My Activity.json"
        ],
    ),
]



STATUS_CODES = [
    StatusCode(id=0, description="Valid zip", message=""),
    StatusCode(id=1, description="Not a valid DDP", message=""),
    StatusCode(id=2, description="Bad zipfile", message=""),
]


def validate(zfile: Path) -> ValidateInput:
    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".json", ".csv", ".html"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        if validation.infer_ddp_category(paths):
            validation.set_status_code(0)  # Valid zip
        else:
            validation.set_status_code(1)  # Not a valid DDP

    except zipfile.BadZipFile:
        validation.set_status_code(2)  # Bad zipfile

    return validation
  
  
def extract_zip_content(google_zip: str) -> Dict[str, Any]:
    validation = validate(Path(google_zip))
    if validation.status_code is None or validation.status_code.id != 0:
        logger.error(f"Invalid zip file: {validation.status_code.description if validation.status_code else 'Unknown error'}")
        return {}

    file_paths = {
        "ads": ["Geschiedenis.json", "Takeout/My Activity/Ads/My Activity.json"],
        "searches": ["Geschiedenis.json", "Takeout/My Activity/Search/My Activity.json"],
        "browser_history": ["Geschiedenis.json", "Takeout/My Activity/Chrome/My Activity.json"],
        "google_news": ["Geschiedenis.json", "Takeout/My Activity/Google News/My Activity.json"],
        "news": ["Geschiedenis.json", "Takeout/My Activity/News/My Activity.json"],
        "video_search": ["Geschiedenis.json", "Takeout/My Activity/Video Search/My Activity.json"],
        "youtube": ["Geschiedenis.json", "Takeout/My Activity/YouTube/My Activity.json"],
        "youtube_comment": ["Takeout/YouTube and YouTube Music/comments/comments.csv"]
    }
    
    extracted_data = {}
    
    try:
        with zipfile.ZipFile(google_zip, "r") as zf:
            for data_type, paths in file_paths.items():
                for path in paths:
                    try:
                        if data_type == "youtube_comment":
                            with zf.open(path) as file:
                                csv_content = file.read()
                                data = parse_youtube_comments(csv_content)
                        else:
                            with zf.open(path) as file:
                                data = unzipddp.read_json_from_bytes(file)
                        if data:
                            extracted_data[data_type] = data
                            logger.info(f"Successfully extracted {data_type} data from {path}")
                            break
                    except KeyError:
                        continue
                    except Exception as e:
                        logger.error(f"Error extracting {path} for {data_type}: {e}")
                        continue
                
                if data_type not in extracted_data:
                    logger.info(f"No data found for {data_type}")
    
    except zipfile.BadZipFile:
        logger.error(f"The file {google_zip} is not a valid zip file")
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing {google_zip}: {e}")
    
    return extracted_data

def parse_youtube_comments(csv_content: bytes) -> List[Dict[str, Any]]:
    comments = []
    csv_content_str = csv_content.decode('utf-8-sig')
    reader = csv.DictReader(csv_content_str.splitlines())
    for row in reader:
        try:
            comment_text = json.loads(row['Comment text'])['text']
        except json.JSONDecodeError:
            comment_text = row['Comment text']  # Fallback if JSON parsing fails
        comments.append({
            'data_type': 'youtube_comment',
            'Action': 'Comment',
            'title': comment_text,
            'URL': f"https://www.youtube.com/watch?v={row['Video ID']}",
            'Date': pd.to_datetime(row['Comment create timestamp']),
            'details': json.dumps({
                'comment_id': row['Comment ID'],
                'parent_comment_id': row['Parent comment ID'],
                'video_id': row['Video ID']
            })
        })
    return comments

def parse_data(data: List[Dict[str, Any]], data_type: str) -> pd.DataFrame:
    parsed_data = []
    
    for item in data:
        parsed_item = {'data_type': data_type}
        for key, value in item.items():
            if key == 'time' or key == 'Date':
                # Convert timestamp to datetime
                try:
                    if isinstance(value, (int, float)):
                        # Check if the timestamp is in seconds or milliseconds
                        if value > 1e11:  # Likely milliseconds
                            parsed_item['Date'] = pd.to_datetime(value, unit='ms')
                        else:  # Likely seconds
                            parsed_item['Date'] = pd.to_datetime(value, unit='s')
                    else:
                        parsed_item['Date'] = pd.to_datetime(value)
                except:
                    parsed_item['Date'] = pd.NaT
            elif key == 'details' and data_type == 'youtube_comment':
                parsed_item['details'] = value  # Keep as JSON string for YouTube comments
            elif isinstance(value, list):
                parsed_item[key] = ', '.join(str(v) for v in value)
            elif isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    parsed_item[f"{key}_{sub_key}"] = sub_value
            else:
                parsed_item[key] = value
        
        parsed_data.append(parsed_item)
    
    df = pd.DataFrame(parsed_data)
    
    # Rename some common columns if they exist
    column_mapping = {
        'header': 'Action',
        'titleUrl': 'URL',
        'details_name': 'Ad_Type'
    }
    df.rename(columns={old: new for old, new in column_mapping.items() if old in df.columns}, inplace=True)
    
    return df

def process_google_data(google_zip: str) -> pd.DataFrame:
   extracted_data = extract_zip_content(google_zip)
    
   all_data = []
   total_items = sum(len(data) for data in extracted_data.values())
   
   with tqdm(total=total_items, desc="Processing Google data") as pbar:
       for data_type, data in extracted_data.items():
           if data:
               logger.debug(f"Processing {len(data)} items for {data_type}")
               chunk_size = 1000  # Adjust this value based on your typical data size
               for i in range(0, len(data), chunk_size):
                   chunk = data[i:i+chunk_size]
                   df = parse_data(chunk, data_type)
                   all_data.append(df)
                   pbar.update(len(chunk))
   
   if all_data:
       combined_df = pd.concat(all_data, ignore_index=True)
       
       
       # Ensure all required columns are present
       required_columns = ['data_type', 'Action', 'title', 'URL', 'Date', 'products', 'details']
       for col in required_columns:
           if col not in combined_df.columns:
               combined_df[col] = pd.NA
       
       # Ensure 'Date' is datetime
       # combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
       
       # Sort by 'Date' in descending order, putting NaT values at the bottom
       combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
       
       # Format the date as a string in a readable format
       combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
       
       logger.info(f"Successfully processed {len(combined_df)} total entries from Google data")
       return combined_df
   else:
       logger.warning("No data was successfully extracted and parsed")
       return pd.DataFrame()

# The following functions can be kept for backwards compatibility
def ads_to_df(google_zip: str) -> pd.DataFrame:
    df = process_google_data(google_zip)
    return df[df['data_type'] == 'ads'].drop(columns=['data_type'])

def searches_to_df(google_zip: str) -> pd.DataFrame:
    df = process_google_data(google_zip)
    return df[df['data_type'] == 'searches'].drop(columns=['data_type'])

def browser_history_to_df(google_zip: str) -> pd.DataFrame:
    df = process_google_data(google_zip)
    return df[df['data_type'] == 'browser_history'].drop(columns=['data_type'])



