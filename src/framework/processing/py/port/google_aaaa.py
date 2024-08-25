import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
from pathlib import Path
from lxml import html  # Make sure this import is present
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

def parse_json_content(file) -> list[dict]:
    """Improved JSON parser function with better error handling"""
    try:
        data = json.load(io.TextIOWrapper(file, encoding='utf-8'))
        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            logger.debug(f"Parsed {len(data)} items from JSON")
            return data
        else:
            logger.warning("Unexpected JSON structure: Expected a list of dictionaries.")
            return []
    except UnicodeDecodeError as e:
        logger.error(f"JSON parsing error due to encoding: {e}")
        return []
    except Exception as e:
        logger.error(f"JSON parsing error: {e}")
        return []


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
                    paths.append(p.name)
        
        validation.infer_ddp_category(paths)
        
        if validation.ddp_category is None:
            logger.warning("Could not infer DDP category")
            validation.set_status_code(1)  # Not a valid DDP
        elif validation.ddp_category.ddp_filetype in (DDPFiletype.JSON, DDPFiletype.HTML):
            validation.set_status_code(0)  # Valid DDP
            # Log the valid Google files found
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


def extract_zip_content(google_zip: str) -> Dict[str, Any]:
    global DATA_FORMAT
    
    # Base paths up to the directory containing the JSON/HTML/CSV files (Dutch and English versions)
    base_paths = {
        "ads": [
            "Takeout/Mijn activiteit/Advertenties/",
            "Takeout/My Activity/Ads/"
        ],
        "searches": [
            "Takeout/Mijn activiteit/Zoeken/",
            "Takeout/My Activity/Search/"
        ],
        "browser_history": [
            "Takeout/Mijn activiteit/Chrome/",
            "Takeout/My Activity/Chrome/"
        ],
        "google_news": [
            "Takeout/Mijn activiteit/Google Nieuws/",
            "Takeout/My Activity/Google News/"
        ],
        "news": [
            "Takeout/Mijn activiteit/Nieuws/",
            "Takeout/My Activity/News/"
        ],
        "video_search": [
            "Takeout/Mijn activiteit/Video_s zoeken/",
            "Takeout/My Activity/Video Search/"
        ],
        "youtube": [
            "Takeout/Mijn activiteit/YouTube/",
            "Takeout/My Activity/YouTube/"
        ],
        "youtube_comment": [
            "Takeout/YouTube en YouTube Music/reacties/",
            "Takeout/YouTube and YouTube Music/comments/"
        ],
        "bookmarks": [
            "Takeout/Chrome/",
        ],
        "reading_list": [
            "Takeout/Chrome/",
        ]
    }

    file_counts = {"json": 0, "html": 0, "csv": 0}

    # First pass: determine file type counts and identify files
    with zipfile.ZipFile(google_zip, "r") as zf:
        for info in zf.infolist():
            if info.filename.endswith('.json'):
                file_counts["json"] += 1
            elif info.filename.endswith('.html'):
                file_counts["html"] += 1

    # Determine majority file type
    DATA_FORMAT = max(file_counts, key=file_counts.get)
    logger.info(f"Determined majority file format: {DATA_FORMAT}")

    extracted_data = {}

    # Extract data based on file type
    try:
        with zipfile.ZipFile(google_zip, "r") as zf:
            for data_type, paths in base_paths.items():
                found = False
                for base_path in paths:
                    for info in zf.infolist():
                        if info.filename.startswith(base_path) and info.filename.endswith(f'.{DATA_FORMAT}') or info.filename.endswith('.csv'):
                            if info.filename.startswith('__MACOSX/') or info.filename.startswith('._'):
                                logger.info(f"Skipping macOS metadata file: {info.filename}")
                                continue
                            try:
                                with zf.open(info.filename) as file:
                                    if info.filename.endswith('.json'):
                                        data = parse_json_content(file)
                                    elif info.filename.endswith('.html'):
                                        html_content = file.read().decode('utf-8')
                                        data = parse_html_content(html_content, data_type)
                                    elif info.filename.endswith('.csv'):
                                        csv_content = file.read()
                                        data = parse_csv_content(csv_content)
                                    else:
                                        continue
                                    
                                    if data:
                                        extracted_data[data_type] = data
                                        logger.info(f"Successfully extracted {data_type} data from {info.filename}")
                                        found = True
                                        break
                            except Exception as e:
                                logger.error(f"Error extracting {info.filename} for {data_type}: {e}")
                                continue
                    if found:
                        break
                
                if data_type not in extracted_data:
                    logger.info(f"No data found for {data_type}")

    except zipfile.BadZipFile:
        logger.error("Bad zip file")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    return extracted_data
  
  

def parse_csv_content(csv_content: bytes) -> List[Dict[str, Any]]:
    """
    Parse the content of a CSV file and return a list of dictionaries for each row.
    Tries to parse by header names first; if headers are not found, uses column positions as fallback.
    """
    comments = []
    csv_content_str = csv_content.decode('utf-8-sig')
    reader = csv.reader(csv_content_str.splitlines())

    # Try to read the header row to determine the structure
    headers = next(reader, None)

    if headers:
        try:
            # Attempt to find the column indices based on header names
            comment_text_index = headers.index('Comment text') if 'Comment text' in headers else headers.index('Reactietekst')
            video_id_index = headers.index('Video ID') if 'Video ID' in headers else headers.index('Video-id')
            comment_id_index = headers.index('Comment ID') if 'Comment ID' in headers else headers.index('Reactie-ID')
            parent_comment_id_index = headers.index('Parent comment ID') if 'Parent comment ID' in headers else headers.index('Bovenliggende reactie-ID')
            timestamp_index = headers.index('Comment create timestamp') if 'Comment create timestamp' in headers else headers.index('Reactiecreatietijdstempel')
        except ValueError as e:
            # Fallback to predefined column positions if headers are not found
            logger.warning(f"Expected header not found, falling back to predefined column positions: {e}")
            comment_text_index = 0
            video_id_index = 1
            comment_id_index = 2
            parent_comment_id_index = 3
            timestamp_index = 4

        # Read each row using the determined indices
        for row in reader:
            try:
                # Use column positions to extract data if headers aren't matched
                comment_text = row[comment_text_index]
                video_id = row[video_id_index]
                comment_id = row[comment_id_index]
                parent_comment_id = row[parent_comment_id_index]
                timestamp = row[timestamp_index]

                # Attempt to parse comment text as JSON
                try:
                    comment_text = json.loads(comment_text)['text'] if comment_text else ""
                except json.JSONDecodeError:
                    # Fallback to raw text if JSON parsing fails
                    pass

                # Construct the dictionary for this comment
                comments.append({
                    'data_type': 'youtube_comment',
                    'Action': 'Comment',
                    'title': comment_text,
                    'URL': f"https://www.youtube.com/watch?v={video_id}",
                    'Date': helpers.robust_datetime_parser(timestamp),
                    'details': json.dumps({
                        'comment_id': comment_id,
                        'parent_comment_id': parent_comment_id,
                        'video_id': video_id
                    })
                })
            except IndexError as e:
                logger.error(f"IndexError when processing row: {e}")
            except Exception as e:
                logger.error(f"Unexpected error when processing row: {e}")
    else:
        logger.error("No headers found in CSV file")

    return comments

def remove_google_url_prefix(input_string: str) -> str:
    prefix = "https://www.google.com/url?q="
    if input_string.startswith(prefix):
        return input_string[len(prefix):]
    return input_string


def parse_html_content(html_content: str, data_type: str) -> List[Dict[str, Any]]:
    parsed_data = []
    logger.debug(f"Starting HTML parsing for {data_type}")
    
    try:
        # logger.debug(f"Length of HTML content: {len(html_content)}")
        
        # Attempt to parse the HTML content
        try:
            doc = html.fromstring(html_content)
        except Exception as e:
            logger.error(f"Failed to parse HTML content: {e}")
            raise
        
        items = doc.xpath('//div[contains(@class, "outer-cell")]')
        logger.debug(f"Found {len(items)} items in HTML content for {data_type}")

        for item in items:
            try:
                content_div = item.xpath('.//div[@class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"]')[0]
                title_parts = content_div.xpath('text() | a/text()')
                title = " ".join(part.strip() for part in title_parts if part.strip())
                
                url_list = item.xpath('.//a/@href')
                url = remove_google_url_prefix(url_list[0].strip()) if url_list else None
                
                date_text_list = item.xpath('.//div[contains(@class, "content-cell")]/text()[last()]')
                date_text = date_text_list[0].strip() if date_text_list else None
                
                details_list = item.xpath('.//div[contains(@class, "mdl-typography--caption")]/text()')
                details = details_list[2].strip() if len(details_list) > 2 else None
                
                product = item.xpath('.//div[@class="content-cell mdl-cell mdl-cell--12-col mdl-typography--caption"]/text()[1]')
                product = product[0].strip() if product else None
                
                parsed_item = {
                    'data_type': data_type,
                    'Action': product,
                    'title': title,
                    'URL': url,
                    'Date': helpers.robust_datetime_parser(date_text) if date_text else None,
                    'details': details
                }
                parsed_data.append(parsed_item)
                
            except IndexError as e:
                logger.error(f"IndexError when parsing item: {e}")
            except Exception as e:
                logger.error(f"Unexpected error when parsing item: {e}")

    except Exception as e:
        logger.error(f"Error parsing HTML content of {data_type}. Error: {e}")
        raise  # Consider raising the exception to halt further execution during debugging

    logger.debug(f"Finished HTML parsing for {data_type}, parsed {len(parsed_data)} items")
    return parsed_data

    # parsed_data = []
    # 
    # try:
    #     doc = html.fromstring(html_content)
    #     # Extract content cells that contain the relevant activity information
    #     items = doc.xpath('//div[@class="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp"]')
    #     if len(items) == 1:
    #         items = items[0].getchildren()
    #         
    #     for item in items:
    #         content_div = item.xpath('.//div[@class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"]')[0]
    #         title_parts = content_div.xpath('text() | a/text()')
    # 
    #         if len(title_parts) >= 2:
    #             title = title_parts[0].strip() + " " + title_parts[1].strip() + " " + title_parts[2].strip()
    #         else:
    #             title = None
    # 
    #         url_elements = content_div.xpath('.//a/@href')
    #         date_text = content_div.xpath('text()[last()]')[0].strip() if content_div.xpath('text()[last()]') else None
    # 
    #         product = item.xpath('.//div[@class="content-cell mdl-cell mdl-cell--12-col mdl-typography--caption"]/text()[1]')
    #         product = product[0].strip() if product else None
    # 
    #         parsed_item = {
    #             'data_type': product,
    #             'Action': 'View',
    #             'title': title if title else None,
    #             'URL': remove_google_url_prefix(url_elements[0].strip()) if url_elements else None,
    #             'Date': helpers.robust_datetime_parser(date_text) if date_text else None,
    #             'details': json.dumps({})
    #         }
    # 
    #         parsed_data.append(parsed_item)
    # 
    # except Exception as e:
    #     logger.error(f"Error parsing HTML content for {data_type}: {e}")
    # 
    # return parsed_data
    
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
        df['Date'] = helpers.robust_datetime_parser(df['Date'])
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Ensure all dates are converted to datetime
        df['Date'] = df['Date'].apply(lambda x: x.tz_localize(None) if x is not pd.NaT else x)  # Make all timestamps tz-naive
    return df

def process_google_data(google_zip: str) -> List[props.PropsUIPromptConsentFormTable]:
    extracted_data = extract_zip_content(google_zip)
    logger.info(f"Extracted data keys: {extracted_data.keys() if extracted_data else 'None'}")
    all_data = []

    for data_type, data in extracted_data.items():
        if data:
            df = pd.DataFrame(data)
            df = make_timestamps_consistent(df)
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
