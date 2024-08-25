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
import re
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

def parse_json_content(file, data_type: str) -> List[Dict[str, Any]]:
    """Improved JSON parser function with better error handling"""
    try:
        data = json.load(io.TextIOWrapper(file, encoding='utf-8'))
        parsed_data = []

        # Convert data to desired format
        for item in data:
            # logger.debug(f"Parsing item: {item}")
            raw_time = helpers.find_items_bfs(item, 'time')
            parsed_time = helpers.robust_datetime_parser(raw_time)
            product = helpers.find_items_bfs(item, 'product')
            if product == "":
                product = helpers.find_items_bfs(item, 'products')
            
            details = helpers.find_items_bfs(item, 'details')
            if not details:
                details_json = ""  # or use None if you want it explicitly as a null in the dataframe
            else:
                try:
                    # Convert details to a JSON string, assuming details is a dictionary or a similar structure
                    details_json = json.dumps(details)
                except TypeError as e:
                    logger.error(f"Error serializing details: {e}")
                    details_json = ""  # Fallback to empty string if serialization fails
                    
                    
            parsed_item = {
                'data_type': data_type,
                'Action': product,  # Renamed from 'header'
                'title': item.get('title', ''),
                'URL': remove_google_url_prefix(item.get('titleUrl', '')),  # Renamed from 'titleUrl'
                'Date': parsed_time,  # Renamed from 'time'
                'details': details_json
            }
            parsed_data.append(parsed_item)

        return parsed_data
    except UnicodeDecodeError as e:
        logger.error(f"JSON parsing error due to encoding: {e}")
        return []
    except Exception as e:
        logger.error(f"JSON parsing error: {e}")
        return []

DDP_CATEGORIES = [
    # English JSON
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "comments.csv",
            "subscriptions.csv",
            "My Activity.json",
            "MyActivity.json",
            "MyActivities.json",
            "My Activities.json",
        ],
    ),
    # English HTML
    DDPCategory(
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
            "comments.csv",
            "subscriptions.csv",
            "MyActivity.html",
            "MyActivities.html",
            "My Activities.html",
            "My Activity.html"
        ],
    ),
    # Dutch JSON
    DDPCategory(
        id="json_nl",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.NL,
        known_files=[
            "reacties.csv",
            "abonnementen.csv",
            "MyActivity.json",
            "MyActivities.json",
            "My Activities.json",
            "My Activity.json",
            "Mijn activiteit.json",
            "Mijnactiviteit.json",
        ],
    ),
    # Dutch HTML
    DDPCategory(
        id="html_nl",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.NL,
        known_files=[
            "reacties.csv",
            "abonnementen.csv",
            "MyActivity.html",
            "MyActivities.html",
            "My Activities.html",
            "My Activity.html",
            "Mijn activiteit.html",
            "Mijnactiviteit.html",
        ],
    ),
    # Spanish JSON
    DDPCategory(
        id="json_es",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.ES,
        known_files=[
            "comentarios.csv",
            "suscripciones.csv",
            "Mi actividad.json",
            "MiActividad.json",
            "MisActividades.json",
            "Mis Actividades.json"
        ],
    ),
    # Spanish HTML
    DDPCategory(
        id="html_es",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.ES,
        known_files=[
            "comentarios.csv",
            "suscripciones.csv",
            "MiActividad.html",
            "MisActividades.html",
            "Mis Actividades.html",
            "Mi actividad.html"
        ],
    ),
    # German JSON
    DDPCategory(
        id="json_de",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.DE,
        known_files=[
            "kommentare.csv",
            "abonnements.csv",
            "Meine Aktivität.json",
            "MeineAktivität.json",
            "MeineAktivitäten.json",
            "Meine Aktivitäten.json"
        ],
    ),
    # German HTML
    DDPCategory(
        id="html_de",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.DE,
        known_files=[
            "kommentare.csv",
            "abonnements.csv",
            "MeineAktivität.html",
            "MeineAktivitäten.html",
            "Meine Aktivitäten.html",
            "Meine Aktivität.html"
        ],
    ),
    # Arabic JSON
    DDPCategory(
        id="json_ar",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.AR,
        known_files=[
            "التعليقات.csv",
            "الاشتراكات.csv",
            "نشاطي.json",
            "نشاطاتي.json",
            "أنشطتي.json",
        ],
    ),
    # Arabic HTML
    DDPCategory(
        id="html_ar",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.AR,
        known_files=[
            "التعليقات.csv",
            "الاشتراكات.csv",
            "نشاطي.html",
            "نشاطاتي.html",
            "أنشطتي.html",
        ],
    ),
    # Turkish JSON
    DDPCategory(
        id="json_tr",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.TR,
        known_files=[
            "yorumlar.csv",
            "abonelikler.csv",
            "Etkinliğim.json",
            "Etkinliklerim.json",
        ],
    ),
    # Turkish HTML
    DDPCategory(
        id="html_tr",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.TR,
        known_files=[
            "yorumlar.csv",
            "abonelikler.csv",
            "Etkinliğim.html",
            "Etkinliklerim.html",
        ],
    ),
    # Chinese JSON
    DDPCategory(
        id="json_zh",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.ZH,
        known_files=[
            "评论.csv",
            "订阅内容.csv",
            "我的活动.json",
        ],
    ),
    # Chinese HTML
    DDPCategory(
        id="html_zh",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.ZH,
        known_files=[
            "评论.csv",
            "订阅内容.csv",
            "我的活动.html",
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


def extract_zip_content(google_zip: str) -> Dict[str, Any]:


    base_paths = {
        "ads": [
            "Takeout/Mijn activiteit/Advertenties/",
            "Takeout/My Activity/Ads/",
            "Takeout/Mi actividad/Anuncios/",
            "Takeout/Meine Aktivitäten/Anzeigen/",
            "Takeout/أنشطتي/الإعلانات/",
            "Takeout/Etkinliğim/Reklamlar/",
            "Takeout/我的活动/广告/"
        ],
        "searches": [
            "Takeout/Mijn activiteit/Zoeken/",
            "Takeout/My Activity/Search/",
            "Takeout/Mi actividad/Búsqueda/",
            "Takeout/Meine Aktivitäten/Suchen/",
            "Takeout/أنشطتي/بحث/",
            "Takeout/Etkinliğim/Arama/",
            "Takeout/我的活动/搜索/"
        ],
        "discover": [
            "Takeout/Mijn activiteit/Discover/",
            "Takeout/My Activity/Discover/",
            "Takeout/Mi actividad/Descubrir/",
            "Takeout/Meine Aktivitäten/Entdecken/",
            "Takeout/أنشطتي/اكتشف/",
            "Takeout/Etkinliğim/Keşfet/",
            "Takeout/我的活动/发现/"
        ],
        "browser_history": [
            "Takeout/Mijn activiteit/Chrome/",
            "Takeout/My Activity/Chrome/",
            "Takeout/Mi actividad/Chrome/",
            "Takeout/Meine Aktivitäten/Chrome/",
            "Takeout/أنشطتي/Chrome/",
            "Takeout/Etkinliğim/Chrome/",
            "Takeout/我的活动/Chrome/"
        ],
        "google_news": [
            "Takeout/Mijn activiteit/Google Nieuws/",
            "Takeout/My Activity/Google News/",
            "Takeout/Mi actividad/Google Noticias/",
            "Takeout/Meine Aktivitäten/Google News/",
            "Takeout/أنشطتي/أخبار جوجل/",
            "Takeout/Etkinliğim/Google Haberler/",
            "Takeout/我的活动/Google 新闻/"
        ],
        "news": [
            "Takeout/Mijn activiteit/Nieuws/",
            "Takeout/My Activity/News/",
            "Takeout/Mi actividad/Noticias/",
            "Takeout/Meine Aktivitäten/Nachrichten/",
            "Takeout/أنشطتي/الأخبار/",
            "Takeout/Etkinliğim/Haberler/",
            "Takeout/我的活动/新闻/"
        ],
        "video_search": [
            "Takeout/Mijn activiteit/Video_s zoeken/",
            "Takeout/My Activity/Video Search/",
            "Takeout/Mi actividad/Búsqueda de videos/",
            "Takeout/Meine Aktivitäten/Videosuche/",
            "Takeout/أنشطتي/البحث عن الفيديو/",
            "Takeout/Etkinliğim/Video Arama/",
            "Takeout/我的活动/视频搜索/"
        ],
        "youtube": [
            "Takeout/Mijn activiteit/YouTube/",
            "Takeout/My Activity/YouTube/",
            "Takeout/Mi actividad/YouTube/",
            "Takeout/Meine Aktivitäten/YouTube/",
            "Takeout/أنشطتي/YouTube/",
            "Takeout/Etkinliğim/YouTube/",
            "Takeout/我的活动/YouTube/"
        ],
        "youtube_comment": [
            "Takeout/YouTube en YouTube Music/reacties/",
            "Takeout/YouTube and YouTube Music/comments/",
            "Takeout/YouTube y YouTube Music/comentarios/",
            "Takeout/YouTube und YouTube Music/Kommentare/",
            "Takeout/يوتيوب و يوتيوب ميوزيك/تعليقات/",
            "Takeout/YouTube ve YouTube Music/yorumlar/",
            "Takeout/YouTube 和 YouTube Music/评论/"
        ],
        "youtube_subscription": [
            "Takeout/YouTube en YouTube Music/abonnementen/",
            "Takeout/YouTube and YouTube Music/subscriptions/",
            "Takeout/YouTube y YouTube Music/suscripciones/",
            "Takeout/YouTube und YouTube Music/Abonnements/",
            "Takeout/يوتيوب و يوتيوب ميوزيك/الاشتراكات/",
            "Takeout/YouTube ve YouTube Music/abonelikler/",
            "Takeout/YouTube 和 YouTube Music/订阅内容/"
        ]
    }


    file_counts = {"json": 0, "html": 0, "csv": 0}
    
    # First pass: determine file type counts and identify files
    ## there are always csvs, so that should not be checked
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
    try:
        with zipfile.ZipFile(google_zip, "r") as zf:
            for info in zf.infolist():
                file_path = info.filename.lower()  # Convert the file path to lowercase

                for data_type, paths in base_paths.items():
                    for base_path in paths:
                        if file_path.startswith(base_path.lower()):  # Convert base path to lowercase
                            if info.filename.startswith('__MACOSX/') or info.filename.startswith('._'):
                                continue  # Skip macOS metadata files
                            
                            try:
                                with zf.open(info.filename) as file:
                                    if info.filename.endswith('.json'):
                                        data = parse_json_content(file, data_type)
                                    elif info.filename.endswith('.html'):
                                        html_content = file.read().decode('utf-8')
                                        data = parse_html_content(html_content, data_type)
                                    elif info.filename.endswith('.csv'):
                                        csv_content = file.read()
                                        data = parse_csv_content(csv_content, data_type)
                                    else:
                                        continue
                                    
                                    if data:
                                        if data_type not in extracted_data:
                                            extracted_data[data_type] = []
                                        extracted_data[data_type].extend(data)
                                        logger.info(f"Successfully extracted {data_type} data from {info.filename}")
                                    break  # Break the base_path loop if file is processed
                            except Exception as e:
                                logger.error(f"Error extracting {info.filename} for {data_type}: {e}")
                            break  # Break the base_path loop after processing a file
                    else:
                        continue
                    break  # Break the data_type loop if a matching file is found
            else:
                logger.info(f"No data found for {data_type}")

    except zipfile.BadZipFile:
        logger.error("Bad zip file")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    return extracted_data
  
  
def parse_csv_content(csv_content: bytes, data_type: str) -> List[Dict[str, Any]]:
    """
    Parse the content of a CSV file and return a list of dictionaries for each row.
    Tries to parse by header names first; if headers are not found, uses column positions as fallback.
    """
    csv_content_str = csv_content.decode('utf-8-sig')
    reader = csv.reader(csv_content_str.splitlines())
    records = []
    # Try to read the header row to determine the structure
    headers = next(reader, None)
    
    if headers:
        # Convert headers to lowercase for case-insensitive matching
        headers = [header.lower() for header in headers]
        
        # Logic for YouTube comments
        if data_type == 'youtube_comment':
            
            try:
                # Attempt to find the column indices based on header names
                comment_text_index = headers.index('comment text'.lower()) if 'comment text'.lower() in headers else headers.index('reactietekst'.lower())
                video_id_index = headers.index('video id'.lower()) if 'video id'.lower() in headers else headers.index('video-id'.lower())
                channel_id_index = headers.index('channel id'.lower()) if 'channel id'.lower() in headers else headers.index('video-id'.lower())
                comment_id_index = headers.index('comment id'.lower()) if 'comment id'.lower() in headers else headers.index('reactie-id'.lower())
                parent_comment_id_index = headers.index('parent comment id'.lower()) if 'parent comment id'.lower() in headers else headers.index('bovenliggende reactie-id'.lower())
                timestamp_index = headers.index('comment create timestamp'.lower()) if 'comment create timestamp'.lower() in headers else headers.index('reactiecreatietijdstempel'.lower())
            except ValueError as e:
                # Fallback to predefined column positions if headers are not found
                logger.warning(f"Expected header not found, falling back to predefined column positions: {e}")
                comment_text_index = 6
                video_id_index = 5
                channel_id_index = 0
                parent_comment_id_index = 4
                comment_id_index = 3
                timestamp_index = 1
    
            # Read each row using the determined indices
            for row in reader:
                try:
                    # Use column positions to extract data if headers aren't matched
                    comment_text = row[comment_text_index]
                    video_id = row[video_id_index]
                    channel_id = row[channel_id_index]
                    parent_comment_id = row[parent_comment_id_index]
                    comment_id = row[comment_id_index]
                    timestamp = row[timestamp_index]
    
                    # Attempt to parse comment text as JSON
                    if comment_text:
                        try:
                            parsed_comment = json.loads(comment_text)
                            # Check if parsed_comment contains exactly one text entry
                            if isinstance(parsed_comment, dict) and 'text' in parsed_comment and len(parsed_comment) == 1:
                                comment_text = parsed_comment['text']
                        except json.JSONDecodeError:
                            # Leave comment_text as is if JSON parsing fails
                            pass
    
                    # Construct the dictionary for this comment
                    records.append({
                        'data_type': data_type,
                        'Action': 'Comment',
                        'title': comment_text,
                        'URL': f"https://www.youtube.com/watch?v={video_id}",
                        'Date': helpers.robust_datetime_parser(timestamp),
                        'details': json.dumps({
                            'comment_id': comment_id,
                            'parent_comment_id': parent_comment_id,
                            'video_id': video_id,
                            'channel_id': channel_id
                        })
                    })
                # except IndexError as e:
                #     logger.error(f"IndexError when processing row: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error when processing row: {e}")

        # Logic for YouTube subscriptions
        elif data_type == 'youtube_subscription':
            try:
                # Define column positions or names based on expected headers for subscriptions
                channel_name_index = headers.index('channel title') if 'channel title' in headers else headers.index('kanaalnaam')
                channel_url_index = headers.index('channel url') if 'channel url' in headers else headers.index('kanaal url')
                # timestamp_index = headers.index('subscription timestamp') if 'subscription timestamp' in headers else headers.index('abonnementtijdstempel')
            except ValueError as e:
                # Fallback to predefined column positions if headers are not found
                logger.warning(f"Expected header not found, falling back to predefined column positions: {e}")
                channel_name_index = 2
                channel_url_index = 1
                # timestamp_index = 2

            # Read each row using the determined indices
            for row in reader:
                try:
                    # Extract data from row
                    channel_name = row[channel_name_index]
                    channel_url = row[channel_url_index]
                    # timestamp = row[timestamp_index]

                    # Construct the dictionary for this subscription
                    records.append({
                        'data_type': data_type,
                        'Action': 'Subscribtion',
                        'title': channel_name,
                        'URL': channel_url,
                        'Date': '',
                        'details': json.dumps({})
                    })
                # except IndexError as e:
                #     logger.error(f"IndexError when processing row: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error when processing row: {e}")

    else:
        logger.error("No headers found in CSV file")

    return records


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
                
            # except IndexError as e:
            #     logger.error(f"IndexError when parsing item: {e}")
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
        # df['Date'] = helpers.robust_datetime_parser(df['Date'])
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Ensure all dates are converted to datetime
        df['Date'] = df['Date'].apply(lambda x: x.tz_localize(None) if x is not pd.NaT else x)  # Make all timestamps tz-naive
    return df
  
# Function to check if a URL should be excluded
def should_exclude_url(url: str) -> bool:
    try:
        # List of URLs to exclude
        exclude_prefixes = [
            "https://mail.google.com/mail"
        ]
    
        # List of popular porn websites (only domains, without "www." or "https://")
        porn_websites = [
            "pornhub.com", "xvideos.com", "xnxx.com", "redtube.com", "xhamster.com",
            "youporn.com", "tube8.com", "spankbang.com", "youjizz.com", "fapdu.com",
            "brazzers.com", "mofos.com", "naughtyamerica.com", "bangbros.com", 
            "pornmd.com", "clips4sale.com", "camsoda.com", "chaturbate.com",
            "myfreecams.com", "livejasmin.com", "streamate.com", "bongacams.com",
            "onlyfans.com", "adultfriendfinder.com", "sextube.com", "beeg.com", 
            "porn.com", "xtube.com", "slutload.com", "tnaflix.com", "pornhubpremium.com",
            "javhd.com", "realitykings.com", "metart.com", "eroprofile.com", "nudelive.com",
            "fantasti.cc", "hclips.com", "alphaporno.com", "ashemaletube.com", "hdpornvideo.xxx",
            "playvid.com", "4tube.com", "javfinder.com", "pornbb.org", "sex.com", "hentaigasm.com",
            "hentaistream.com", "adulttime.com", "wicked.com", "dogfartnetwork.com",
            "keezmovies.com", "xempire.com", "alotporn.com", "familyporn.tv", "pornrips.com",
            "thumzilla.com", "madthumbs.com", "drtuber.com", "pornhd.com", "upornia.com",
            "fapdu.com", "freeones.com", "twistys.com", "3movs.com", "vporn.com", 
            "porndoe.com", "pornhd.com", "hdtube.porn", "recurbate.com", "tubegalore.com",
            "porndig.com", "h2porn.com", "lobstertube.com", "nuvid.com", "sexvid.xxx",
            "xhamsterlive.com", "playboy.tv", "cams.com", "badoinkvr.com", "vrporn.com",
            "vrcosplayx.com", "metartx.com", "hegre-art.com", "joymii.com", "goodporn.to",
            "spankwire.com", "homepornking.com", "pornrabbit.com", "megapornx.com",
            "jizzbunker.com", "eporner.com", "cam4.com", "sexier.com", "adultempire.com",
            "joysporn.com", "slutroulette.com", "bigxvideos.com", "hotmovs.com", "milfporn.xxx",
            "kinky.nl"
        ]
    
      
        # Check if URL starts with any excluded prefixes
        if any(url.startswith(prefix) for prefix in exclude_prefixes):
            return True
        # Check if URL refers to a porn website
        domain = re.findall(r'://(?:www\.)?([^/]+)', url)
        if domain and any(porn_site in domain[0] for porn_site in porn_websites):
            return True
        return False
    except Exception as e:
        return False

def process_google_data(google_zip: str) -> List[props.PropsUIPromptConsentFormTable]:
    logger.info("Starting to extract Google data from {google_zip}.")   

    extracted_data = extract_zip_content(google_zip)
    # Assuming `extracted_data` is a dictionary where keys are the file paths or names.
    filtered_extracted_data = {
        k: v for k, v in extracted_data.items() if not re.match(r'^\d+\.html$', k.split('/')[-1])
    }
    
    # Logging only the filtered keys
    logger.info(f"Extracted data keys: {helpers.get_json_keys(filtered_extracted_data) if filtered_extracted_data else 'None'}")   
    
    all_data = []
    subscription_data = []
    


    # Separate data based on the presence of dates
    for data_type, data in extracted_data.items():
        if data:
            df = pd.DataFrame(data)
            # Filter out unwanted URLs
            df = df[~df['URL'].apply(should_exclude_url)]

            if data_type == 'youtube_subscription':
                subscription_data.append(df)
            else:
                df = make_timestamps_consistent(df)
                all_data.append(df)

    tables_to_render = []

    # Process data that has dates
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
        logger.warning("No data with dates was successfully extracted and parsed")

    ### Process data without dates (YouTube subscriptions)
    if subscription_data:
        combined_subscription_df = pd.concat(subscription_data, ignore_index=True)
        
        if not combined_subscription_df.empty:
            # Remove unnecessary columns if they exist
            if 'Date' in combined_subscription_df.columns:
                combined_subscription_df = combined_subscription_df.drop(columns=['Date'])
            if 'details' in combined_subscription_df.columns:
                combined_subscription_df = combined_subscription_df.drop(columns=['details'])
            
            table_title = props.Translatable({"en": "YouTube Subscriptions", "nl": "YouTube Abonnementen"})
            
            # Create table for YouTube subscription data
            table = props.PropsUIPromptConsentFormTable("youtube_subscriptions", table_title, combined_subscription_df)
            tables_to_render.append(table)
            
            logger.info(f"Successfully processed {len(combined_subscription_df)} total entries for YouTube subscriptions")
        else:
            logger.warning("Subscription DataFrame is empty")
    else:
        logger.warning("No YouTube subscription data was successfully extracted and parsed")
    
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
