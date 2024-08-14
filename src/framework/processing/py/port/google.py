import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
import lxml
import csv
import port.api.props as props
import port.helpers as helpers
import port.vis as vis
from pathlib import Path

from port.validate import (
    DDPCategory,
    StatusCode,
    ValidateInput,
    Language,
    DDPFiletype,
)

logger = logging.getLogger(__name__)

DATA_FORMAT = None  # Will be set to 'json' or 'html'

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "comments.csv",
            "My Activity.json",
            "MyActivity.json",
            "MyActivities.json",
            "My Activities.json",
        ],
    ),
    DDPCategory(
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
            "comments.csv",
            "MyActivity.html",
            "MyActivities.html",
            "My Activities.html",
            "My Activity.html"
        ],
    ),
    DDPCategory(
        id="json_nl",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.NL,
        known_files=[
            "reacties.csv",
            "MyActivity.json",
            "MyActivities.json",
            "My Activities.json",
            "My Activity.json"
        ],
    ),
    DDPCategory(
        id="html_nl",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.NL,
        known_files=[
            "reacties.csv",
            "MyActivity.html",
            "MyActivities.html",
            "My Activities.html",
            "My Activity.html"
        ],
    ),
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
                if p.suffix in (".json", ".html", ".csv"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        
        if validation.ddp_category is None:
            logger.warning("Could not infer DDP category")
            validation.set_status_code(1)  # Not a valid DDP
        elif validation.ddp_category.ddp_filetype in (DDPFiletype.JSON, DDPFiletype.HTML):
            validation.set_status_code(0)  # Valid DDP
        else:
            validation.set_status_code(1)  # Not a valid DDP

    except zipfile.BadZipFile:
        logger.error("Bad zip file")
        validation.set_status_code(2)  # Bad zipfile
    except Exception as e:
        logger.error(f"Unexpected error during validation: {str(e)}")
        validation.set_status_code(1)  # Not a valid DDP

    return validation

def extract_zip_content(google_zip: str) -> Dict[str, Any]:
    global DATA_FORMAT
    validation = validate(Path(google_zip))
    if validation.status_code is None or validation.status_code.id != 0:
        logger.error(f"Invalid zip file: {validation.status_code.description if validation.status_code else 'Unknown error'}")
        return {}
    
    file_paths = {
        "ads": ["Takeout/Mijn activiteit/Advertenties/My Activity.json", "Takeout/Mijn activiteit/Advertenties/My Activities.json", "Takeout/Mijn activiteit/Advertenties/MyActivities.json", "Takeout/Mijn activiteit/Advertenties/MyActivity.json", "Takeout/My Activity/Ads/My Activity.json", "Takeout/My Activity/Ads/My Activities.json", "Takeout/My Activity/Ads/MyActivities.json", "Takeout/My Activity/Ads/MyActivity.json"],
        "searches": ["Takeout/Mijn activiteit/Zoeken/My Activity.json", "Takeout/Mijn activiteit/Zoeken/My Activities.json", "Takeout/Mijn activiteit/Zoeken/MyActivities.json", "Takeout/Mijn activiteit/Zoeken/MyActivity.json", "Takeout/My Activity/Search/My Activity.json", "Takeout/My Activity/Search/My Activities.json", "Takeout/My Activity/Search/MyActivities.json", "Takeout/My Activity/Search/MyActivity.json"],
        "browser_history": ["Takeout/Mijn activiteit/Chrome/My Activity.json", "Takeout/Mijn activiteit/Chrome/My Activities.json", "Takeout/Mijn activiteit/Chrome/MyActivities.json", "Takeout/Mijn activiteit/Chrome/MyActivity.json", "Takeout/My Activity/Chrome/My Activity.json", "Takeout/My Activity/Chrome/My Activities.json", "Takeout/My Activity/Chrome/MyActivities.json", "Takeout/My Activity/Chrome/MyActivity.json"],
        "google_news": ["Takeout/Mijn activiteit/Google Nieuws/My Activity.json", "Takeout/Mijn activiteit/Google Nieuws/My Activities.json", "Takeout/Mijn activiteit/Google Nieuws/MyActivities.json", "Takeout/Mijn activiteit/Google Nieuws/MyActivity.json", "Takeout/My Activity/Google News/My Activity.json", "Takeout/My Activity/Google News/My Activities.json", "Takeout/My Activity/Google News/MyActivities.json", "Takeout/My Activity/Google News/MyActivity.json"],
        "news": ["Takeout/Mijn activiteit/Nieuws/My Activity.json", "Takeout/Mijn activiteit/Nieuws/My Activities.json", "Takeout/Mijn activiteit/Nieuws/MyActivities.json", "Takeout/Mijn activiteit/Nieuws/MyActivity.json", "Takeout/My Activity/News/My Activity.json", "Takeout/My Activity/News/My Activities.json", "Takeout/My Activity/News/MyActivities.json", "Takeout/My Activity/News/MyActivity.json"],
        "video_search": ["Takeout/Mijn activiteit/Video_s zoeken/My Activity.json", "Takeout/Mijn activiteit/Video_s zoeken/My Activities.json", "Takeout/Mijn activiteit/Video_s zoeken/MyActivities.json", "Takeout/Mijn activiteit/Video_s zoeken/MyActivity.json", "Takeout/My Activity/Video Search/My Activity.json", "Takeout/My Activity/Video Search/My Activities.json", "Takeout/My Activity/Video Search/MyActivities.json", "Takeout/My Activity/Video Search/MyActivity.json"],
        "youtube": ["Takeout/Mijn activiteit/YouTube/My Activity.json", "Takeout/Mijn activiteit/YouTube/My Activities.json", "Takeout/Mijn activiteit/YouTube/MyActivities.json", "Takeout/Mijn activiteit/YouTube/MyActivity.json", "Takeout/My Activity/YouTube/My Activity.json", "Takeout/My Activity/YouTube/My Activities.json", "Takeout/My Activity/YouTube/MyActivities.json", "Takeout/My Activity/YouTube/MyActivity.json"],
        "youtube_comment": ["Takeout/YouTube en YouTube Music/reacties/reacties.csv", "Takeout/YouTube and YouTube Music/comments/comments.csv"],
        "bookmarks": ["Bookmarks.html", "Takeout/Chrome/Bookmarks.html"],
        "reading_list": ["Leeslijst.html", "Takeout/Chrome/ReadingList.html"]
    }

    file_counts = {"json": 0, "html": 0}

    # First pass: determine file type counts
    with zipfile.ZipFile(google_zip, "r") as zf:
        for info in zf.infolist():
            if info.filename.endswith('.json'):
                file_counts["json"] += 1
            # elif info.filename.endswith('.csv'):
            #     file_counts["csv"] += 1
            elif info.filename.endswith('.html'):
                file_counts["html"] += 1

    # Determine majority file type
    DATA_FORMAT = max(file_counts, key=file_counts.get)
    logger.info(f"Determined majority file format: {DATA_FORMAT}")

    extracted_data = {}

    # Second pass: extract data
    try:
        with zipfile.ZipFile(google_zip, "r") as zf:
            for data_type, paths in file_paths.items():
                for path in paths:
                    try:
                        if path.endswith(f'.{DATA_FORMAT}'):
                            with zf.open(path) as file:
                                if DATA_FORMAT == "json":
                                    data = json.load(io.TextIOWrapper(file, encoding='utf-8'))
                                # elif DATA_FORMAT == "csv":
                                #     csv_content = file.read()
                                #     data = parse_csv_content(csv_content)
                                elif DATA_FORMAT == "html":
                                    html_content = file.read().decode('utf-8')
                                    data = parse_html_content(html_content, data_type)
                                else:
                                    continue
                                
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
        logger.error("Bad zip file")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    return extracted_data
def parse_csv_content(csv_content: bytes) -> List[Dict[str, Any]]:
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

def parse_html_content(html_content: str, data_type: str) -> List[Dict[str, Any]]:
    parsed_data = []
    doc = lxml.html.fromstring(html_content)
    
    for item in doc.xpath('//div[@class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"]'):
        title = item.xpath('.//a/text()')
        url = item.xpath('.//a/@href')
        date = item.xpath('.//div/text()')
        
        parsed_data.append({
            'data_type': data_type,
            'Action': 'View',
            'title': title[0] if title else None,
            'URL': url[0] if url else None,
            'Date': pd.to_datetime(date[0] if date else None, errors='coerce'),
            'details': json.dumps({'content_length': len(html_content)})
        })
    
def parse_data(data: List[Dict[str, Any]], data_type: str) -> pd.DataFrame:
    parsed_data = []
    
    for item in data:
        parsed_item = {'data_type': data_type}
        for key, value in item.items():
            if key == 'time' or key == 'Date':
                try:
                    if isinstance(value, (int, float)):
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
    
    column_mapping = {
        'header': 'Action',
        'titleUrl': 'URL',
        'details_name': 'Ad_Type'
    }
    df.rename(columns={old: new for old, new in column_mapping.items() if old in df.columns}, inplace=True)
    
    return df

def make_timestamps_consistent(df: pd.DataFrame) -> pd.DataFrame:
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Ensure all dates are converted to datetime
        df['Date'] = df['Date'].apply(lambda x: x.tz_localize(None) if x is not pd.NaT else x)  # Make all timestamps tz-naive
    return df

def process_google_data(google_zip: str) -> List[props.PropsUIPromptConsentFormTable]:
    extracted_data = extract_zip_content(google_zip)
    
    all_data = []
    total_items = sum(len(data) for data in extracted_data.values())
    
    for data_type, data in extracted_data.items():
        if data:
            logger.debug(f"Processing {len(data)} items for {data_type}")
            chunk_size = 1000  # Adjust this value based on your typical data size
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i+chunk_size]
                df = parse_data(chunk, data_type)
                df = make_timestamps_consistent(df)  # Ensure timestamps are consistent
                all_data.append(df)
    
    tables_to_render = []
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        required_columns = ['data_type', 'Action', 'title', 'URL', 'Date', 'details']
        for col in required_columns:
            if col not in combined_df.columns:
                combined_df[col] = pd.NA
        
        combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
        
        combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        combined_df['Count'] = 1
        
        table_title = props.Translatable({"en": "Google Activity Data", "nl": "Google Gegevens"})
        visses = [vis.create_chart(
            "line", 
            "Google Activity Over Time", 
            "Google Activity Over Time", 
            "Date", 
            y_label="Number of Observations", 
            date_format="auto"
        )]
        
        table = props.PropsUIPromptConsentFormTable("google_all_data", table_title, combined_df, visualizations=visses)
        tables_to_render.append(table)
        
        logger.info(f"Successfully processed {len(combined_df)} total entries from Google data")
    else:
        logger.warning("No data was successfully extracted and parsed")
    
    return tables_to_render


# Helper functions for specific data types
def ads_to_df(google_zip: str) -> pd.DataFrame:
    df = process_google_data(google_zip)
    return df[df['data_type'] == 'ads'].drop(columns=['data_type'])

def searches_to_df(google_zip: str) -> pd.DataFrame:
    df = process_google_data(google_zip)
    return df[df['data_type'] == 'searches'].drop(columns=['data_type'])

def browser_history_to_df(google_zip: str) -> pd.DataFrame:
    df = process_google_data(google_zip)
    return df[df['data_type'] == 'browser_history'].drop(columns=['data_type'])

def youtube_comments_to_df(google_zip: str) -> pd.DataFrame:
    df = process_google_data(google_zip)
    return df[df['data_type'] == 'youtube_comment'].drop(columns=['data_type'])

def youtube_history_to_df(google_zip: str) -> pd.DataFrame:
    df = process_google_data(google_zip)
    return df[df['data_type'] == 'youtube'].drop(columns=['data_type'])
