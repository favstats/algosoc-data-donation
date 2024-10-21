import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
import os
from bs4 import UnicodeDammit
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

def parse_json_content(data, Type: str) -> List[Dict[str, Any]]:
    """Improved JSON parser function with better error handling"""
    try:
        parsed_data = []

        # Convert data to desired format
        for item in data:
            # logger.debug(f"Parsing item: {item}")
            raw_time = helpers.find_items_bfs(item, 'time')
            parsed_time = helpers.robust_datetime_parser(raw_time)
            product = helpers.find_items_bfs(item, 'product')
            if product == "":
                product = helpers.find_items_bfs(item, 'products')
            
            if len(product) == 1:
                product = product[0]
            
            details = helpers.find_items_bfs(item, 'details')
            if not details:
                details_json = "Geen Details"  # or use None if you want it explicitly as a null in the dataframe
            else:
                try:
                    # Convert details to a JSON string, assuming details is a dictionary or a similar structure
                    details_json = json.dumps(details)
                except TypeError as e:
                    logger.error(f"Error serializing details: {e}")
                    details_json = ""  # Fallback to empty string if serialization fails
                    
                    
            parsed_item = {
                'Type': Type,
                'Actie': item.get('title', ''),  # Renamed from 'header'
                'URL': remove_google_url_prefix(item.get('titleUrl', 'Geen URL')),  # Renamed from 'titleUrl'
                'Datum': parsed_time,  # Renamed from 'time'
                'Details': details_json,
                'Bron': "Google Gegevens"
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
            "Abos.csv",
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
            "Abos.csv",
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
            logger.info(f"Valid DDP inferred")
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
        "Advertentie Info": [
            "Takeout/Mijn activiteit/Advertenties/",
            "Takeout/My Activity/Ads/",
            "Takeout/Mi actividad/Publicidad/",
            "Takeout/Meine Aktivitäten/Anzeigen/",
            "Takeout/أنشطتي/الإعلانات/",
            "Takeout/Etkinliğim/Reklamlar/",
            "Takeout/我的活动/广告/"
        ],
        "Zoekopdrachten": [
            "Takeout/Mijn activiteit/Zoeken/",
            "Takeout/My Activity/Search/",
            "Takeout/Mi actividad/Búsqueda/",
            "Takeout/Meine Aktivitäten/Google Suche/",
            "Takeout/أنشطتي/بحث/",
            "Takeout/Etkinliğim/Arama/",
            "Takeout/我的活动/搜索/"
        ],
        "Google Discover": [
            "Takeout/Mijn activiteit/Discover/",
            "Takeout/My Activity/Discover/",
            "Takeout/Mi actividad/Discover/",
            "Takeout/Meine Aktivitäten/Discover/",
            "Takeout/أنشطتي/اكتشف/",
            "Takeout/Etkinliğim/Keşfet/",
            "Takeout/我的活动/发现/"
        ],
        "Browsergeschiedenis": [
            "Takeout/Mijn activiteit/Chrome/",
            "Takeout/My Activity/Chrome/",
            "Takeout/Mi actividad/Chrome/",
            "Takeout/Meine Aktivitäten/Chrome/",
            "Takeout/أنشطتي/Chrome/",
            "Takeout/Etkinliğim/Chrome/",
            "Takeout/我的活动/Chrome/"
        ],
        "Google News": [
            "Takeout/Mijn activiteit/Google Nieuws/",
            "Takeout/My Activity/Google News/",
            "Takeout/Mi actividad/Google Noticias/",
            "Takeout/Meine Aktivitäten/Google News/",
            "Takeout/أنشطتي/أخبار جوجل/",
            "Takeout/Etkinliğim/Google Haberler/",
            "Takeout/我的活动/Google 新闻/"
        ],
        "Nieuwsbetrokkenheid": [
            "Takeout/Mijn activiteit/Nieuws/",
            "Takeout/My Activity/News/",
            "Takeout/Mi actividad/Noticias/",
            "Takeout/Meine Aktivitäten/Nachrichten/",
            "Takeout/أنشطتي/الأخبار/",
            "Takeout/Etkinliğim/Haberler/",
            "Takeout/我的活动/新闻/"
        ],
        "Video Zoekopdrachten": [
            "Takeout/Mijn activiteit/Video_s zoeken/",
            "Takeout/My Activity/Video Search/",
            "Takeout/Mi actividad/Búsqueda de videos/",
            "Takeout/Meine Aktivitäten/Videosuche/",
            "Takeout/أنشطتي/البحث عن الفيديو/",
            "Takeout/Etkinliğim/Video Arama/",
            "Takeout/我的活动/视频搜索/"
        ],
        "YouTube Kijkgeschiedenis": [
            "Takeout/Mijn activiteit/YouTube/",
            "Takeout/My Activity/YouTube/",
            "Takeout/Mi actividad/YouTube/",
            "Takeout/Meine Aktivitäten/YouTube/",
            "Takeout/أنشطتي/YouTube/",
            "Takeout/Etkinliğim/YouTube/",
            "Takeout/我的活动/YouTube/"
        ],
        "YouTube Reacties": [
            "Takeout/YouTube en YouTube Music/reacties/",
            "Takeout/YouTube and YouTube Music/comments/",
            "Takeout/YouTube y YouTube Music/comentarios/",
            "Takeout/YouTube und YouTube Music/Kommentare/",
            "Takeout/يوتيوب و يوتيوب ميوزيك/تعليقات/",
            "Takeout/YouTube ve YouTube Music/yorumlar/",
            "Takeout/YouTube 和 YouTube Music/评论/"
        ],
        "YouTube Abonnementen": [
            "Takeout/YouTube en YouTube Music/abonnementen/",
            "Takeout/YouTube and YouTube Music/subscriptions/",
            "Takeout/YouTube y YouTube Music/suscripciones/",
            "Takeout/YouTube und YouTube Music/Abos/",
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
                
                for Type, paths in base_paths.items():
                    for base_path in paths:
                        if file_path.startswith(base_path.lower()):  # Convert base path to lowercase
                            if info.filename.startswith('__MACOSX/') or info.filename.startswith('._'):
                                continue  # Skip macOS metadata files
    
                            try:
                                with zf.open(info.filename) as file:
                                    raw_data = file.read()
                                    file_size_gb = info.file_size / (1024 ** 2)  # Convert bytes to MB
                                    # Attempt to decode using UTF-8 first
                                    try:
                                        decoded_data = raw_data.decode('utf-8')
                                        encoding = 'utf-8'
                                    except UnicodeDecodeError:
                                        # If UTF-8 decoding fails, use UnicodeDammit to guess the encoding
                                        suggestion = UnicodeDammit(raw_data)
                                        decoded_data = suggestion.unicode_markup
                                        encoding = suggestion.original_encoding
    
                                    # Handle JSON files
                                    if info.filename.endswith('.json'):
                                        try:
                                            json_data = json.loads(decoded_data)
                                            data = parse_json_content(json_data, Type)
                                        except (UnicodeDecodeError, json.JSONDecodeError) as e:
                                            logger.error(f"Error processing JSON file {info.filename} with encoding {encoding}: {e}. File size: {file_size_gb} MB.")
                                            continue
    
                                    # Handle HTML files
                                    elif info.filename.endswith('.html'):
                                        try:
                                            data = parse_html_content(decoded_data, Type)
                                        except UnicodeDecodeError as e:
                                            logger.error(f"Error processing HTML file {info.filename} with encoding {encoding}: {e}. File size: {file_size_gb} MB.")
                                            continue
    
                                    # Handle CSV files
                                    elif info.filename.endswith('.csv'):
                                        try:
                                            data = parse_csv_content(decoded_data, Type)
                                        except UnicodeDecodeError as e:
                                            logger.error(f"Error processing CSV file {info.filename} with encoding {encoding}: {e}. File size: {file_size_gb} MB.")
                                            continue
    
                                    else:
                                        continue
    
                                    if data:
                                        if Type not in extracted_data:
                                            extracted_data[Type] = []
                                        extracted_data[Type].extend(data)
                                        logger.info(f"Successfully extracted {Type} data from {info.filename}. File size: {file_size_gb} MB.")
                                    break  # Break the base_path loop if file is processed
                            except Exception as e:
                                logger.error(f"Error extracting {info.filename} for {Type}: {e}")
                            break  # Break the base_path loop after processing a file
                    else:
                        continue
                    break  # Break the Type loop if a matching file is found
            # else:
                # logger.info(f"No data found for {Type}")
    except zipfile.BadZipFile:
        logger.error("The provided file is not a valid zip file.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

    return extracted_data
  
  
def parse_csv_content(csv_content: bytes, Type: str) -> List[Dict[str, Any]]:
    """
    Parse the content of a CSV file and return a list of dictionaries for each row.
    Tries to parse by header names first; if headers are not found, uses column positions as fallback.
    """
    # csv_content_str = csv_content.decode('utf-8-sig')
    reader = csv.reader(csv_content.splitlines())
    records = []
    # Try to read the header row to determine the structure
    headers = next(reader, None)
    
    if headers:
        # Convert headers to lowercase for case-insensitive matching
        headers = [header.lower() for header in headers]
        
        # Logic for YouTube comments
        if Type == 'YouTube Reacties':
            
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
                        'Type': Type,
                        'Actie': comment_text,
                        'URL': f"https://www.youtube.com/watch?v={video_id}",
                        'Datum': helpers.robust_datetime_parser(timestamp),
                        'Details': json.dumps({
                            'comment_id': comment_id,
                            'parent_comment_id': parent_comment_id,
                            'video_id': video_id,
                            'channel_id': channel_id
                        }),
                'Bron': "Google Gegevens"
                    })
                # except IndexError as e:
                #     logger.error(f"IndexError when processing row: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in {Type} when processing row: {e}")

        # Logic for YouTube subscriptions
        elif Type == 'YouTube Abonnementen':
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
                        'Type': Type,
                        'Actie': 'Geabonneerd op ' + channel_name,
                        'URL': channel_url,
                        'Datum': 'Geen Datum',
                        'Details': "Geen Details",
                'Bron': "Google Gegevens"
                    })
                # except IndexError as e:
                #     logger.error(f"IndexError when processing row: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in {Type} when processing row: {e}")

    else:
        logger.error("No headers found in CSV file")

    return records


def remove_google_url_prefix(input_string: str) -> str:
    prefix = "https://www.google.com/url?q="
    if input_string.startswith(prefix):
        return input_string[len(prefix):]
    return input_string


def parse_html_content(html_content: str, Type: str) -> List[Dict[str, Any]]:
    parsed_data = []
    logger.debug(f"Starting HTML parsing for {Type}")
    
    try:
        # logger.debug(f"Length of HTML content: {len(html_content)}")
        
        # Attempt to parse the HTML content
        try:
            doc = html.fromstring(html_content)
        except Exception as e:
            logger.error(f"Failed to parse HTML content: {e}")
            raise
        
        items = doc.xpath('//div[contains(@class, "outer-cell")]')
        logger.debug(f"Found {len(items)} items in HTML content for {Type}")

        for item in items:
            try:
                content_div = item.xpath('.//div[@class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"]')[0]
                title_parts = content_div.xpath('text() | a/text()')
                title = " ".join(part.strip() for part in title_parts if part.strip())
                
                url_list = item.xpath('.//a/@href')
                url = remove_google_url_prefix(url_list[0].strip()) if url_list else None
                if not url:
                  url = "Geen URL"
                
                
                date_text_list = item.xpath('.//div[contains(@class, "content-cell")]/text()[last()]')
                date_text = date_text_list[0].strip() if date_text_list else None
                
                details_list = item.xpath('.//div[contains(@class, "mdl-typography--caption")]/text()')
                details = details_list[2].strip() if len(details_list) > 2 else None
                
                product = item.xpath('.//div[@class="content-cell mdl-cell mdl-cell--12-col mdl-typography--caption"]/text()[1]')
                product = product[0].strip() if product else None
                
                parsed_item = {
                    'Type': Type,
                    'Actie': title,
                    'URL': url,
                    'Datum': helpers.robust_datetime_parser(date_text) if date_text else None,
                    'Details': details if details else "Geen Details",
                    'Bron': "Google Gegevens"
                }
                parsed_data.append(parsed_item)
                
            # except IndexError as e:
            #     logger.error(f"IndexError when parsing item: {e}")
            except Exception as e:
                logger.error(f"Unexpected error when parsing item: {e}")

    except Exception as e:
        logger.error(f"Error parsing HTML content of {Type}. Error: {e}")
        raise  # Consider raising the exception to halt further execution during debugging

    logger.debug(f"Finished HTML parsing for {Type}, parsed {len(parsed_data)} items")
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
    #             'Type': product,
    #             'Action': 'View',
    #             'title': title if title else None,
    #             'URL': remove_google_url_prefix(url_elements[0].strip()) if url_elements else None,
    #             'Datum': helpers.robust_datetime_parser(date_text) if date_text else None,
    #             'Details': "Geen Details"
    #         }
    # 
    #         parsed_data.append(parsed_item)
    # 
    # except Exception as e:
    #     logger.error(f"Error parsing HTML content for {Type}: {e}")
    # 
    # return parsed_data

def make_timestamps_consistent(df: pd.DataFrame) -> pd.DataFrame:
    if 'Datum' in df.columns:
        # df['Datum'] = helpers.robust_datetime_parser(df['Datum'])
        df['Datum'] = pd.to_datetime(df['Datum'], errors='coerce')  # Ensure all dates are converted to datetime
        df['Datum'] = df['Datum'].apply(lambda x: x.tz_localize(None) if x is not pd.NaT else x)  # Make all timestamps tz-naive
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
            "pornhub.com", "xvideos.com", "xnxx.com", "redtube.com", "xhamster.com", "deloris-ai",
            "youporn.com", "tube8.com", "spankbang.com", "youjizz.com", "fapdu.com", "9xbuddy.xyz",
            "brazzers.com", "mofos.com", "naughtyamerica.com", "bangbros.com", "deepmode.ai",
            "pornmd.com", "clips4sale.com", "camsoda.com", "chaturbate.com", "casualdating1.com",
            "myfreecams.com", "livejasmin.com", "streamate.com", "bongacams.com", "deepmode.ai",
            "onlyfans.com", "adultfriendfinder.com", "sextube.com", "beeg.com", "akg01.com",
            "porn.com", "xtube.com", "slutload.com", "tnaflix.com", "pornhubpremium.com",
            "javhd.com", "realitykings.com", "metart.com", "eroprofile.com", "nudelive.com",
            "fantasti.cc", "hclips.com", "alphaporno.com", "ashemaletube.com", "hdpornvideo.xxx",
            "playvid.com", "4tube.com", "javfinder.com", "pornbb.org", "sex.com", "hentaigasm.com",
            "hentaistream.com", "adulttime.com", "wicked.com", "dogfartnetwork.com",
            "keezmovies.com", "xempire.com", "alotporn.com", "familyporn.tv", "pornrips.com",
            "thumzilla.com", "madthumbs.com", "drtuber.com", "pornhd.com", "upornia.com",
            "fapdu.com", "freeones.com", "twistys.com", "3movs.com", "vporn.com", "candy.ai",
            "porndoe.com", "pornhd.com", "hdtube.porn", "recurbate.com", "tubegalore.com",
            "porndig.com", "h2porn.com", "lobstertube.com", "nuvid.com", "sexvid.xxx",
            "xhamsterlive.com", "playboy.tv", "cams.com", "badoinkvr.com", "vrporn.com",
            "vrcosplayx.com", "metartx.com", "hegre-art.com", "joymii.com", "goodporn.to",
            "spankwire.com", "homepornking.com", "pornrabbit.com", "megapornx.com", "tingo.ai",
            
            "jizzbunker.com", "eporner.com", "cam4.com", "sexier.com", "adultempire.com", "basedlabs.ai",
            "joysporn.com", "slutroulette.com", "bigxvideos.com", "hotmovs.com", "milfporn.xxx",
            
            # Dutch Porn Websites
            "kinky.nl", "geilevrouwen.nl", "sexfilms.nl", "nlporno.com", 
            "echtneuken.nl", "viva.nl", "sexjobs.nl", "vagina.nl", "binkdate.nl", "chatgirl.nl",
        
            # Gay Porn Websites
            "men.com", "gaytube.com", "justusboys.com", "gaymaletube.com", "dudetube.com",
            "nextdoorstudios.com", "cockyboys.com", "helixstudios.net", "hothouse.com", "corbinfisher.com",
        
            # Lesbian Porn Websites
            "girlsway.com", "naughtylady.com", "bellesa.co", "sweetsinner.com", "transangelsnetwork.com",
            "girlfriendsfilms.com", "thelesbianexperience.com", "wifelovers.com", "wearehairy.com", "lucasentertainment.com",
        
            # Trans Porn Websites
            "shemale.xxx", "groobygirls.com", "ts-dating.com", "tgirls.com", "trannytube.tv",
            "transgenderpornstar.com", "trans500.com", "pure-ts.com", "transangels.com", "tgirlporn.tv"
        ]
    
      
        # Check if URL starts with any excluded prefixes
        if any(url.startswith(prefix) for prefix in exclude_prefixes):
            return True
        # Check if URL refers to a porn website
        domain = re.findall(r'://(?:www\.)?([^/]+)', url)
        if domain:
            domain_name = domain[0].lower()  # Lowercase the domain only after extraction
            if any(porn_site in domain_name for porn_site in porn_websites) or "porn" in domain_name or "xxx" in domain_name:
                return True
        return False
    except Exception as e:
        return False

def detect_explicit_content(text: str) -> bool:
    try:

        # List of common keywords associated with explicit content
        explicit_keywords = [
        "porn", "pornhub", "xhamster", "xnxx", "xxx", "sex", "erotic", "kinky", "fetish",
        "camgirl", "webcam", "cam site", "cam show",
        "onlyfans", "fansly", "premium snapchat",
        "lingerie", "sexy lingerie", "boudoir",
        "adult video", "adult movie", "adult industry",
        "escort", "prostitute", "escort service", "sex worker",
        "stripper", "strip club", "exotic dancer",
        "nsfw", "not safe for work", 
        "nudes", "sexting", "snapchat nudes",
        "adult chat", "sex chat", "dirty chat",
        "bukkake", "gangbang", "threesome",
        "fetlife", "bdsm", "dominatrix", 
        "masturbation", "gooning", "fap", "jerk off", "nudity",
        "hentai", "anime porn", "doujin",
        "webcam model", "pornstar", "adult actress",
         "hotwife", 
        "pegging", "rimming", 
         "incest",
        "cheating wife", "cheating husband",
        "fetish videos", "foot fetish",
        # Adult Subreddits
        "gonewild",
        "r/boobs", "r/ass", "r/thick", "r/bigtits",
        "r/altgonewild", "r/gonewild30plus", "r/milf",
        "r/legalteens", "r/collegesluts", "r/bdsm",
        "r/cumsluts", "r/dirtypenpals", "r/cocktits",
        "r/dirtysnapchat", "r/snapchatnudes", "r/horny",
        "r/camwhores", "r/onlyfansgirls", "r/feetpics",
        "r/swingersgw", "r/hotWife", "r/cuckoldcommunity",
        "r/brothel", "r/sexworkers", "r/strippers",
        "r/fetish", "r/Femdom", "r/chubby",
        "r/sissy", "r/cuckold", "r/amateurporn",
        "r/threesomes", "r/hotwife", "r/ladybonersgw",
        "r/pornvids", "r/freefapvideos", "r/unitedporn",
        "r/dirtyr4r", "r/nsfwhumor", "r/hentai",
        "r/hentai_gif", "r/hentai_videos", "r/hentai_porn",
        "r/celebnsfw", "r/celeb_gonewild",
        "r/wifesharing", "r/cuckquean", "r/cuckoldplace",
        "r/hotwives", "xxx", "sex", "erotic",  "kinky", "fetish", "asianporn",
        "hot girls", "theporndude", "nsfw", "camgirl", "gooncaves",
        "onlyfans", "lingerie", "adult video", "Comic Book Girl 19",
        "adult industry", "exotic dancer", "15 seconds to fap",
        # Added popular names
        "taylor sands", "porno travel tv", "naughty celeste", 
        "esperanza del horno", "kalisi ink", "romy indy", "ellie leen", 
        "kate haven", "chelsey lanette", "sarah calanthe", "esadora", 
        "delicious liv", "verona van de leur", "saskia steele", "cyberly crush", 
        "nayomi sharp", "sofia valentine", "zara whites", "helen duval", 
        "mandy slim", "gammabia", "sarah fonteyna", "mistress kym", "esluna love", 
        "chrystal sinn", "terri summers", "linda lush", "cherryflowerxxx", 
        "jentina small", "julie mandrews", "deidre holland", "susi star", 
        "leayummy", "nude chrissy", "zoe davis", "ivey passion", "bibi bugatti", 
        "melody pleasure", "jenna joy", "leona queen", "assbandida", 
        "melizza more", "venus et vulcanus", "bobbi eden", "emily crystal", 
        "tiffany roxx", "milena star", "kyla king", "britt angel", "himiwako", 
        "spermanneke", "liddy", "kate more", "ownedasian", "ilse de rooij", 
        "jolee jordan", "debbie van gils", "swing babe", "stella michaells", 
        "britney dutch", "nathalie kitten", "foxxy angel", "tina starr", 
        "bibi diamond", "kayla crawford", "mona summers", "vickiluv", 
        "marianne moist", "wendy somer", "suraya jamal", "curly chloe", 
        "scarlett hope", "lilimissarab", "chica la roxxx", "marlenadi", 
        "sintia stone", "angela ts sissy", "dutch dirty games", "silver forrest", 
        "myella", "chamo 1972", "wildestkitten", "kim holland", "charisma gold", 
        "samanthakiss", "debby pleasure", "angelina e", "wet denise", 
        "teresa dumore", "mature kim", "ivana hyde", "zeeuwskoppel1972", 
        "joy latoya", "laura lust", "anna lynx", "jane von deffa", "joy draiki", 
        "sofia britt putalocura", "bunny jane", "pixie pink", "noortje", 
        "veronica doll", "daphne laat", "ana maria221", "lynn lynx", 
        "pauline teutscher", "romee strijd", "trixi heinenn", "passiekoppel", 
        "raven", "meesteresnoir", "slutty granny", "lisa brunet", "suraya stars", 
        "lemon haze", "teresa du more", "wet_denise", "diana van laar", 
        "sweet rebel", "natasja less", "vivanica", "jenny joy", "mrs amsterdam", 
        "carmen joy", "little ductapegirl lilly", "vera delightfull", 
        "creampie sophie", "saphira m", "mistress tirza", "natalie visser", 
        "didi devil", "nina van dick", "sasha fears", "danique", "arienh", 
        "hailey haze", "dirty lee", "goldykim", "juliette squirt", "elvira princess", 
        "victoria fox", "oma von lizzy", "miss blazy", "mishi", "nora sparkle", 
        "linastarr", "sophieheels", "niffy noon", "shirly wild", "morgan", 
        "dutch jewel", "slave mila", "candy35", "sylvia milf", "chayenne shea", 
        "footfetisdom", "guilbert by orm", "denise star", "alyx star", 
        "princess lili", "natasha nice", "penny barber", "angela white", 
        "mia khalifa", "coco lovelock", "abigaiil morris", "dani daniels", 
        "eliza ibarra", "brandi love", "siri", "abella danger", "cory chase", 
        "leana lovings", "cherie deville", "jordi el nino polla", "emily willis", 
        "sunny leone", "ava addams", "blake blossom", "dee williams", 
        "valentina nappi", "kenzie reeves", "sophia leone", "lauren phillips", 
        "riley reid", "syren demer", "reagan foxx", "camilla creampie", 
        "lena paul", "danny d", "alexis fawx", "krissy lynn", "julia ann", 
        "dirty tina", "sheena ryder", "hazel moore", "kate rich", "gina gerson", 
        "alex adams", "tim deen", "adriana chechik", "veronica leal", 
        "gali diva", "carmela clutch", "violet myers", "chanel preston", 
        "mia malkova", "lexi lore", "alexis crystal", "ariella ferrera", 
        "lana rhoades", "brianna beach", "aubree valentine", "lilly hall", 
        "lila lovely", "anissa kate", "ryan keely", "lexi luna", "johnny sins", 
        "daynia xxx", "cathy heaven", "rachael cavalli", "anny aurora", 
        "india summer", "bella gray", "julia north", "chloe surreal", 
        "rae lil black", "juan el caballo loco", "cherry kiss", "lulu chu", 
        "melody marks", "angel wicky", "sofia lee", "casey calvert", 
        "miho ichiki", "jodi west", "veronica avluv", "nicole aniston", 
        "phoenix marie", "j-mac", "ricky spanish", "jasmine jae", "kay parker", 
        "codi vore", "tiffany tatum", "mariska x", "brittany bardot", 
        "leah gotti", "nina hartley", "xxlayna marie", "jane wilde", 
        "lisa ann", "karlee grey", "andi james", "olivia sparkle", "christie stevens", 
        "ella knox", "jax slayher", "whitney wright", "manuel ferrara", 
        "alina lopez", "alura jenson", "amirah adara", "gabbie carter", 
        "shane diesel", "elsa jean", "bunny colby", "mandy muse", "jia lissa", 
        "kendra lust", "anna claire clouds", "steve holmes", "stacy cruz", 
        "leo ahsoka", "sharon white", "fae love", "shalina devine", "elena koshka", 
        "eva elfie", "sexy susi", "molly little", "alex magni", "london river", 
        "bella rolland", "sara jay", "ryan madison", "diana douglas", 
        "dana vespoli", "piper perri", "nadia ali", "montse swinger", 
        "gia derza", "hailey rose", "lia louise", "lilian black", "bea dumas", 
        "eveline dellai", "melanie hicks", "katty west", "eva notty", 
        "jennifer white", "nickey huntsman", "nuria millan", "egon kowalski", 
        "maria wars", "salome gil", "sheila ortega", "xander corvus", 
        "kylie rocket", "kyler quinn", "tejashwini", "nicole dupapillon", 
        "kira noir", "adria rae", "kenzie taylor", "crystal rush", "savannah bond", 
        "blaire ivory", "may thai", "sally d'angelo", "eveline magic", 
        "reiko kobayakawa", "jaye summers", "ryan conner", "mona wales", 
        "nicole murkovski", "jenna starr", "gal ritchie", "chloe temple", 
        "gia paige", "payton preslee", "james deen", "octavia red", "bridgette b", 
        "shooting star", "violet starr", "romi rain", "pristine edge", 
        "vanessa vega", "vanna bardot", "brigitte lahaie", "molly maracas", 
        "kristen scott", "ginger mi", "kenna james", "nikky thorne", 
        "charlie forde", "charles dera", "janet mason", "shione cooper", 
        "rocco siffredi", "alex coal", "ellie nova", "emma hix", "aletta ocean", 
        "anna polina", "carolina sweets", "jessica ryan", "kendra sunderland", 
        "kathia nobili", "mick blue", "yui hatano", "anna de ville", 
        "karla kush", "aften opal", "haley spades", "luna star", "sucharita", 
        "seth gamble", "armani black", "britney amber", "sophie dee", 
        "stella cox", "molly jane", "jimmy michaels", "kagney linn karter", 
        "jasmine black", "jessica bangkok", "tina kay", "laney grey", 
        "isiah maxwell", "sybil a kailena", "sensual jane", "koko blond", 
        "clea gaultier", "angie faith", "luca ferrero", "margot von teese", 
        "tyler nixon", "liv revamped", "maitland ward", "vanessa cage", 
        "jenny stella", "shay sights", "chanel camryn", "alyssa hart", 
        "alex legend", "austin young", "joey mills", "legrand wolf", 
        "viktor rom", "malik delgaty", "roman todd", "dakota lovell", 
        "dante colle", "devin franco", "jax thirio", "jack bailey", "drew sebastian", 
        "spikey dee", "rocco steele", "drake von", "manuel skye", "felix fox", 
        "chris damned", "joel someone", "sergeant miles", "brody kayman", 
        "tim kruger", "armond rizzo", "jayden marcos", "killian knox", 
        "skylar finchh", "arad winwin", "dakota payne", "allen king", 
        "bastian karim", "jack valor", "jake preston", "johnny rapid", 
        "reece scott", "sam ledger", "trevor brooks", "adam snow", "lance charger", 
        "zayne bright", "heathen halo", "dirk caber", "shae reynolds", 
        "sean xavier", "michael boston", "foxy alex", "jay magnus", "diego sans", 
        "dale savage", "jordan starr", "carter woods", "beau butler", 
        "sir peter", "joaquin santana", "bruce jones", "william seed", 
        "lawson james", "tony keit", "markus kage", "santi noguera", 
        "tomas brand", "dallas steele", "trevor harris", "cole church", 
        "nick capra", "jonah wheeler", "rafael alencar", "pierce paris", 
        "ashton summers", "reese rideout", "romeo davis", "sebastian kane", 
        "andy star", "alex mecum", "troye dean", "drew dixon", "jack hunter", 
        "teddy torres", "johnny ford", "calvin banks", "mateo tomas", 
        "dillon diaz", "greg dixxon", "eddie patrick", "ray diesel", 
        "justin matthews", "ty roderick", "bo sinn", "alex madriz", "marco napoli", 
        "sean ford", "derek kage", "cade maddox", "kyle michaels", "mason lear", 
        "gabriel clark", "aaron trainer", "bishop angus", "cain marko", 
        "nick floyd", "rob quin", "antonio biaggi", "jeremy bilding", 
        "elliot finn", "sam narcis", "draven navarro", "ruslan angelo", 
        "juven", "brian bonds", "cole blue", "adam russo", "adrian hart", 
        "allen silver", "john thomas", "dani robles", "andre donovan", 
        "hugo antonin", "jack dixon", "dylan hayes", "tommy defendi", 
        "michael lucas", "jake nicola", "michael del ray", "skyy knox", 
        "max sargent", "will angell", "carter dane", "dylan james", "kyler moss", 
        "dalton riley", "jack waters", "alex roman", "riley mitchel", 
        "kenzo alvarez", "colby keller", "sharok", "solomon aspen", "oliver carter", 
        "colby jansen", "rico marlon", "marcus mcneil", "matt hughes", 
        "cutler x", "dean young", "connor maguire", "leon giok", "ricky larkin", 
        "angel rivers", "paul wagner", "brad kalvo", "leo louis", "roxas caelum", 
        "cyrus stark", "jordan lake", "krave melanin", "tony genius", 
        "vincent o'reilly", "tannor reed", "rafael carreras", "sean duran", 
        "timmy cole", "jake olsen", "adam killian", "brock banks", "ryan bones", 
        "wesley woods", "kane fox", "miguel rey", "gabriel cross", "damien crosse", 
        "jace reed", "angel rivera", "adam veller", "archi gold", "blake mitchell", 
        "daniel hausser", "muscled madison", "ryder owens", "ben huller", 
        "devin trez", "jeffrey lloyd", "jake morgan", "jake cruise", 
        "jj knight", "beno eker", "jay taylor", "alpha wolfe", "gabe bradshaw", 
        "rodrigo amor", "rick kelson", "matthew figata", "damian night", 
        "maverick sun", "scott carter", "marco paris", "billy santoro", 
        "paul canon", "joel tamir", "lito cruz", "kyle fletcher", "johnny b", 
        "jaxton wheeler", "donovin rece", "will braun", "atlas grant", 
        "bad boi benvi", "koldo goran", "harrison todd", "donte thick", 
        "dom king", "tyler tanner", "christian wilde", "luke hudson", 
        "amone bane", "archie paige", "ryan rose", "vander pulaski", 
        "thiagui twink", "masyn thorne", "colton reece", "richard lennox", 
        "hans berlin", "sage roux", "adam ramzi", "curtis cameron", "leo grand", 
        "rocky vallarta", "diego mattos", "bryan slater", "nate grimes", 
        "stas landon", "micah martinez", "bobby blake", "elijah zayne", 
        "dolf dietrich", "ethan tate", "max konnor", "edward terrant", 
        "gerasim spartak", "kam stone", "antony carter", "daisy taylor", 
        "emma rose", "jade venus", "jessy dubai", "ariel demure", "ella hollywood", 
        "eva maxim", "natalie mars", "izzy wilde", "korra del rio", "chanel santini", 
        "khloe kay", "erica cherry", "aubrey kate", "christian xxx", 
        "brittney kade", "yasmin dornelles", "casey kisses", "kim wagner", 
        "kasey kei", "mariana cordoba", "zariah aura", "ts foxxxy", "crystal thayer", 
        "tori easton", "venus lux", "gabrielly ferraz", "shiri allwood", 
        "gracie jane", "aspen brooks", "bella trix", "domino presley", 
        "lola morena", "bella salvatore", "ember fiera", "ramon monstercock", 
        "angellica good", "lena kelly", "gia itzel", "kapri sun", "raphaella ferrari", 
        "kimber lee", "grazyeli silva", "bianca hills", "jenna jaden", 
        "cherry mavrik", "autumn rain", "nina lawless", "miran", "kellie shaw", 
        "melanie brooks", "mariana lins", "kayleigh coxx", "sabina steele", 
        "delia delions", "chanel noir", "lianna lawson", "marcelle herrera", 
        "alexa scout", "carol penelope", "stacy lynn", "mia isabella", 
        "lludy fortune", "ivory mayhem", "sabrina suzuki", "carrie emberlyn", 
        "camilla jolie", "natassia dreams", "nicolly pantoja", "katie fox", 
        "sarina valentina", "sydney summers", "barbara perez", "marissa minx", 
        "annabelle lane", "joanna jet", "alice marques", "jamie french", 
        "melissa pozzi", "janelle fennec", "alisia rae", "jessica fox", 
        "jhoany wilker", "andylynn payne", "keyla marques", "joey michaels", 
        "janie blade", "sofia bun", "vanniall", "melissa leal", "asia belle", 
        "angeles cid", "sara salazar", "bianka nascimento", "nikki north", 
        "kendall penny", "magaly vaz", "evelin frazao", "bailey paris", 
        "adriana rodrigues", "jenna creed", "jane marie", "jenna gargles", 
        "yasmin lee", "fernanda cristine", "haven rose", "bailey jay", 
        "nataly souza", "chelsea marie", "nikki vicious", "jenny flowers", 
        "andrea montoya", "isa lawrence", "jessica blake", "valkyria domina", 
        "zoe fuckpuppet", "jonelle brooks", "lizzy laynez", "juliana leal", 
        "maira dimov", "leilani li", "beatriz andrade", "allison dale", 
        "jessy bells", "vaniity", "kalliny nomura", "chris epic", "walleska sargentely", 
        "walkiria drumond", "luna love", "dana delvey", "sapphire young", 
        "rosy pinheiro", "claire tenebrarum", "kate zoha", "sabrina prezotte", 
        "carla novaes", "neci archer", "roberta cortes", "avery angel", 
        "kendra sinclaire", "sasha de sade", "vitoria neves", "paloma veiga", 
        "isabella fontaleni", "kimber haven", "bruna butterfly", "mia maffia", 
        "celine dijjon", "lia dotada", "luana alves", "mos b", "suzanna holmes", 
        "aylla gattina", "nadia love", "paula long", "graziela cinturinha", 
        "andressa paiva", "olivia would", "thea daze", "laura ferraz", 
        "kai bailey", "victor hugo", "danni daniels", "summer hart", 
        "kalli grace", "pietra radi", "bella de la fuente", "gloria voguel", 
        "livi doll", "gabi ink", "hanna rios", "amanda fialho", "carla cardille", 
        "eva lin", "deborah mastronelly", "bruna castro", "luana pacheco", 
        "veronika havenna", "kelly klaymour", "isabelly ferreira", "morgan bailey", 
        "janny costa", "emma indica", "hime marie", "ts madison", "renata davila", 
        "tyra scott", "adrieli pinheiro", "beatrix doll", "honey foxxx", 
        "india taina", "kawaii fiona", "agata lopes", "chelsea poe", 
        "ava holt", "estella duarte", "juliana souza", "genesis green", 
        "pixi lust", "taryn elizabeth", "rachel nova", "thaysa carvalho", 
        "giovana portylla", "juliette stray", "sasha strokes", "foxy angel", 
        "lizbeth kyo", "arianna vogue", "aimee fawx", "bianca meirelles", 
        "dani peterson", "vanessa jhons", "yago ribeiro", "danielle foxxx", 
        "mireya rinaldi", "thaysa lopes", "penelope jolie", "roxxie moth", 
        "naomi chi", "jordyn starstruck", "rachael belle", "ciboulette", 
        "jasmine lotus", "liberty harkness", "mandy mitchell", "luna rose", 
        "sunshyne monroe", "natalia coxxx", "eva paradis", "julia alves", 
        "variety itsol", "yuria misaki", "clara ludovice", "anastasia kessler", 
        "cheffie", "nathalie hardcore", "alexis fox", "curved marvin", 
        "heidi besk", "dominatrix dinah", "sasha", "stella maas", "cataleya solvage", 
        "noa livia", "georgina verbaan", "beau hesling", "trixie fox", 
        "esther heart", "ancilla tilia", "ariana angel", "emy george", 
        "yasie lee", "celine maxima", "tracy oba", "sebriena star", "tanya de vries", 
        "rose de jong", "sandy cage", "sasha xiphias", "logan moore", 
        "peto coast", "kris blent", "diego summers", "nathan devos", 
        "braxton boyd", "toby dutch", "rick van sant", "johan kane", 
        "scott miller", "dominique hansson", "jp philips", "dylan greene", 
        "tommy skylar", "abella danger", "adriana chechik", "aimi yoshikawa", 
        "amarna miller", "angela white", "anna polina", "anri okita", 
        "arabelle raphael", "honey_sunshine", "ariana marie", "august ames", 
        "ayu sakurai", "belle knox", "bonnie rotten", "brett rossi", 
        "carter cruise", "casey calvert", "chanel preston", "charlotte sartre", 
        "chloe cherry", "christy mack", "dakota skye", "eve sweet", "ebony mystique", 
        "ela darling", "emily willis", "eva elfie", "gianna dior", "ginger banks", 
        "iori kogawa", "jia lissa", "jessie andrews", "jessie rogers", 
        "julia alexandratou", "kaho shibuya", "kendra sunderland", "lana rhoades", 
        "lasirena69", "lauren phillips", "lizz tayler", "maitland ward", 
        "mana sakura", "megan barton-hanson", "melissa bulanhagui", "mercedes carrera", 
        "mia khalifa", "mia magma", "mia malkova", "nadia ali", "rebecca more", 
        "remy lacroix", "renee gracie", "reya sunshine", "rika hoshimi", 
        "riley reid", "saki hatsumi", "samantha bentley", "sara tommasi", 
        "scarlet young", "siew pui yi", "siouxsie q", "sophie anderson", 
        "tasha reign", "tsusaka aoi", "valentina nappi", "whitney wright", 
        "alvin tan", "arad winwin", "armond rizzo", "austin wolf", "billy santoro", 
        "brendon miller", "griffin barrows", "jordi el niño polla", 
        "matthew camp", "rocco steele", "ty mitchell", "amouranth", "belle delphine", 
        "cara cunningham", "nang mwe san", "projekt melody"
        ]
        
        # Create a regex pattern that looks for any of these keywords in a case-insensitive way
        explicit_pattern = re.compile(r'(?:' + '|'.join(map(re.escape, explicit_keywords)) + r')', re.IGNORECASE)
        
        # Search for the pattern in the text
        if explicit_pattern.search(text):
            return True  # Explicit content detected
        else:
            return False  # No explicit content detected
    except Exception as e:
        return False
      
def process_google_data(google_zip: str) -> List[props.PropsUIPromptConsentFormTable]:
    logger.info("Starting to extract Google data.")   
    try:
        extracted_data = extract_zip_content(google_zip)
        # Assuming `extracted_data` is a dictionary where keys are the file paths or names.
        filtered_extracted_data = {
        k: v for k, v in extracted_data.items() if not re.match(r'^\d+\.(html|json)$', k.split('/')[-1])
        }
        
        # Logging only the filtered keys
        logger.info(f"Extracted data keys: {helpers.get_json_keys(filtered_extracted_data) if filtered_extracted_data else 'None'}")   
        
        all_data = []
        subscription_data = []
        
    
    
        # Separate data based on the presence of dates
        for Type, data in extracted_data.items():
            if data:
                df = pd.DataFrame(data)
                # Filter out unwanted URLs
                # Combine URL and Actie checks into a single boolean mask
                mask = ~(df['URL'].apply(should_exclude_url) | df['Actie'].apply(detect_explicit_content))
                
                # Apply the mask to filter the DataFrame
                df = df[mask]
    
                if Type == 'youtube_subscription':
                    subscription_data.append(df)
                else:
                    df = make_timestamps_consistent(df)
                    all_data.append(df)
    
        tables_to_render = []
    
        # Process data that has dates
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            required_columns = ['Type', 'Actie', 'URL', 'Datum', 'Details']
            for col in required_columns:
                if col not in combined_df.columns:
                    combined_df[col] = "Geen " + col
            
            combined_df = combined_df.sort_values(by='Datum', ascending=False, na_position='last').reset_index(drop=True)
            
            combined_df['Datum'] = combined_df['Datum'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('Geen Datum')
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

            
            table_title = props.Translatable({"en": "Google Activity Data", "nl": "Google Gegevens"})
            visses = [vis.create_chart(
                "line", 
                "Google Activiteit", 
                "Google Activity-activiteit", 
                "Datum", 
                y_label="Aantal keren gekeken", 
                date_format="auto"
            )]
            table = props.PropsUIPromptConsentFormTable("google_all_data", table_title, combined_df, visualizations=visses)
            tables_to_render.append(table)
            
            logger.info(f"Successfully processed {len(combined_df)} total entries from Google data")
        else:
            logger.warning("No data was successfully extracted and parsed")
    
        return tables_to_render
    except Exception as e:
       logger.warning(f"Could not parse Google data: {e}")
