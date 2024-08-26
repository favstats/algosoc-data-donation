import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
import re
import os
from bs4 import UnicodeDammit
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
            "ad_preferences.json",
            "people_we_think_you_should_follow.json",
            "advertisers_using_your_activity_or_information.json",
            "advertisers_you've_interacted_with.json",
            "comments.json",
            "likes_and_reactions_1.json",
            "other_categories_used_to_reach_you.json",
            "recently_viewed.json",
            "recently_visited.json",
            "story_views_in_past_7_days.json",
            "subscription_for_no_ads.json",
            "who_you've_followed.json",
            "your_posts__check_ins__photos_and_videos_1.json",
            "your_search_history.json",
            "your_group_membership_activity.json",
            "group_posts_and_comments.json",
            "your_comments_in_groups.json",
            "ads_interests.json"
        ],
    ),
    DDPCategory(
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
            "ad_preferences.html",
            "people_we_think_you_should_follow.html",
            "advertisers_using_your_activity_or_information.html",
            "advertisers_you've_interacted_with.html",
            "comments.html",
            "likes_and_reactions_1.html",
            "other_categories_used_to_reach_you.html",
            "recently_viewed.html",
            "recently_visited.html",
            "story_views_in_past_7_days.html",
            "subscription_for_no_ads.html",
            "who_you've_followed.html",
            "your_posts__check_ins__photos_and_videos_1.html",
            "your_search_history.html",
            "your_group_membership_activity.json",
            "group_posts_and_comments.json",
            "your_comments_in_groups.json"
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message="Valid DDP"),
    StatusCode(id=1, description="Not a valid DDP", message="Not a valid DDP"),
    StatusCode(id=2, description="Bad zipfile", message="Bad zip"),
]



def is_valid_zipfile(file_path: Path) -> bool:
    """
    Checks if the file is a valid zip by reading its signature.
    """
    try:
        with open(file_path, 'rb') as file:
            signature = file.read(4)
        return signature == b'PK\x03\x04'  # ZIP file signature
    except Exception as e:
        logger.error(f"Error reading file signature: {e}", exc_info=True)
        return False
      
      
def validate(file: Path) -> ValidateInput:
    global validation
    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)
    # file_size = helpers.log_file_size(file)

    # Check file size and set a different status code if it's larger than 1.75GB
    # file_size_gb = file_size/ (1024 ** 3)  # Convert bytes to GB
    # if file_size_gb > 1.75:
    #     logger.error(f"File size is {file_size_gb:.2f}GB, which exceeds the 1.75GB limit.")
    #     validation.set_status_code(3)  # Set a different status code for large file size
    #     return validation

    try:
        paths = []
        file_name = file.lower()  # Convert file name to lowercase for consistent checks

        # logger.info(f"Opening zip file: {file}")

        with zipfile.ZipFile(file, "r", allowZip64=True) as zf:
            # logger.info(f"Successfully opened zip file: {file}")
            for f in zf.namelist():
                try:
                    p = Path(f)
                    # logger.debug(f"Found file in zip: {p.name}")

                    if p.suffix in (".json", ".html"):
                        # logger.debug(f"Valid file found: {p.name} with suffix {p.suffix}")
                        paths.append(p.name.lower())  # Convert to lowercase for consistent checks
                    # else:
                    #     logger.debug(f"Skipping file: {p.name} with unsupported suffix {p.suffix}")

                except Exception as e:
                    logger.error(f"There was an error processing file {f} in zip: {e}", exc_info=True)

        logger.info(f"Total valid files found in zip: {len(paths)}")
        validation.infer_ddp_category(paths)

        if validation.ddp_category is None:
            logger.warning("Could not infer DDP category")
            validation.set_status_code(1)  # Not a valid DDP
        elif validation.ddp_category.ddp_filetype in (DDPFiletype.JSON, DDPFiletype.HTML):
            if "instagram" in file_name and "facebook" not in file_name:
                validation.set_status_code(1)  # Not a valid DDP for facebook
                logger.warning("Found 'instagram' in zip file so it can't be Facebook!")
            else:
                validation.set_status_code(0)  # Assume valid DDP
                logger.info(f"Valid DDP inferred")
        else:
            logger.warning("Could not infer DDP category")
            validation.set_status_code(1)  # Not a valid DDP

    except zipfile.BadZipFile as e:
        logger.error(f"Bad zip file: {file}. Error: {e}", exc_info=True)
        validation.set_status_code(2)  # Bad zipfile
    except OSError as e:
        logger.error(f"OSError: likely due to file size. Error: {e}", exc_info=True)
        validation.set_status_code(1)  # General error, not valid DDP
    except Exception as e:
        logger.error(f"Unexpected error during validation of file: {e}", exc_info=True)
        validation.set_status_code(1)  # Not a valid DDP

    validation.validated_paths = paths  # Store the validated paths
    return validation



def parse_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    
    required_columns = ['data_type', 'Action', 'title', 'URL', 'Date', 'details']
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA
    
    return df

def extract_facebook_data(facebook_zip: str) -> Dict[str, Any]:
    global DATA_FORMAT
    global the_user

    # validation = validate(Path(facebook_zip))
    # if validation.status_code is None or validation.status_code.id != 0:
    #     logger.error(f"Invalid zip file: {validation.status_code.description if validation.status_code else 'Unknown error'}")
    #     return {}

    data = {}
    try:
        with zipfile.ZipFile(facebook_zip, "r") as zf:
            json_files = [f for f in zf.namelist() if f.endswith('.json')]
            html_files = [f for f in zf.namelist() if f.endswith('.html')]
            
            # Determine data format based on majority file type
            DATA_FORMAT = "json" if len(json_files) > len(html_files) else "html"
            
            files_to_process = json_files if DATA_FORMAT == "json" else html_files
            for file in files_to_process:
                with zf.open(file) as f:
                    raw_data = f.read()
                    # Use UnicodeDammit to detect the encoding
                    suggestion = UnicodeDammit(raw_data)
                    encoding = suggestion.original_encoding
                    # logger.debug(f"Encountered encoding: {encoding}.")

                    try:
                        if DATA_FORMAT == "json":
                            data[Path(file).name] = json.loads(raw_data.decode(encoding))
                        elif DATA_FORMAT == "html":
                            data[Path(file).name] = raw_data.decode(encoding)
                    except (UnicodeDecodeError, json.JSONDecodeError) as e:
                        logger.error(f"Error processing file {file} with encoding {encoding}: {str(e)}")
                        continue  # Skip the problematic file and continue with othersr(e)}")

        the_user = helpers.find_items_bfs(data, "author")
        if not the_user:
          the_user = helpers.find_items_bfs(data, "actor")
          
        logger.info(f"Extracted data from {len(data)} files. Data format: {DATA_FORMAT}")
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
    return data
  
def parse_advertisers_using_activity(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        advertisers = helpers.find_items_bfs(data, "custom_audiences_all_types_v2")
        if not advertisers:
          return []
        return [{
            'data_type': 'facebook_advertiser_activity',
            'Action': 'AdvertiserActivity',
            'title': helpers.find_items_bfs(advertiser,"advertiser_name"),
            'URL': '',
            'Date': '',
            'details': json.dumps({
                'has_data_file_custom_audience': advertiser.get("has_data_file_custom_audience", False),
                'has_remarketing_custom_audience': advertiser.get("has_remarketing_custom_audience", False),
                'has_in_person_store_visit': advertiser.get("has_in_person_store_visit", False)
            })
        } for advertiser in advertisers]
    elif DATA_FORMAT == "html":
        html_content = data.get("advertisers_using_your_activity_or_information.html", "")
        if not html_content:
            logger.info("'advertisers_using_your_activity_or_information.html' not found.")
            return []

        try:
            tree = html.fromstring(html_content)
            rows = tree.xpath('//table/tbody/tr')
            results = []

            for row in rows:
                title = row.xpath('./td[1]/strong/text()')[0]
                columns = row.xpath('./td/text()')

                # Implementing the logic for checking the presence of 'x' in each column
                has_data_file_custom_audience = columns[1].strip() == 'x' if len(columns) > 1 else False
                has_remarketing_custom_audience = columns[2].strip() == 'x' if len(columns) > 2 else False
                has_in_person_store_visit = columns[3].strip() == 'x' if len(columns) > 3 else False

                results.append({
                    'data_type': 'facebook_advertiser_activity',
                    'Action': 'AdvertiserActivity',
                    'title': title,
                    'URL': '',
                    'Date': '',
                    'details': json.dumps({
                        'has_data_file_custom_audience': has_data_file_custom_audience,
                        'has_remarketing_custom_audience': has_remarketing_custom_audience,
                        'has_in_person_store_visit': has_in_person_store_visit
                    })
                })

            return results

        except Exception as e:
            logger.error(f"Error parsing 'advertisers_using_your_activity_or_information.html': {str(e)}")
            return []


def parse_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        # comments =  helpers.find_items_bfs(data,"comments.json")
        comments = helpers.find_items_bfs(data,"comments_v2")

        if not comments:
          return []
        
        def replace_author(text: str, author: str) -> str:
            if author in text:
                return text.replace(author, "the_user").strip()
            return text

        the_author = helpers.find_items_bfs(comments, "author")
        result = []
        for comment in comments:
            title = helpers.find_items_bfs(comment, "title")
            details = json.dumps({"comment": helpers.find_items_bfs(helpers.find_items_bfs(comment, "comment"), "comment")})
            
            # Replace the_author with "the_user" in title and details
            title = replace_author(title, the_author)
            details = replace_author(details, the_author)
            
            result.append({
                'data_type': 'facebook_comment',
                'Action': 'Comment',
                'title': title,
                'URL': helpers.find_items_bfs(comment, "external_context"),
                'Date': helpers.robust_datetime_parser(helpers.find_items_bfs(comment, "timestamp")),
                'details': details
            })
        
        return result
    elif DATA_FORMAT == "html":
        html_content = data.get("comments.html", "")
        if not html_content:
            logger.info("'comments.html' not found.")
            return []
    
        results = []
        
        try:
            tree = html.fromstring(html_content)
            comment_items = tree.xpath('//div[@role="main"]/div')
            
            for item in comment_items:
                try:
                    # Extracting the comment term - locate divs with text content directly
                    term_element = item.xpath('.//div[normalize-space(text())]')
                    # logger.debug(f"{term_element}")
                    action = term_element[0].text_content().strip().replace('"', '') if term_element else ""
                    term = term_element[1].text_content().strip().replace('"', '') if term_element else ""
                    date = term_element[2].text_content().strip().replace('"', '') if term_element else ""
   
                    date_iso = helpers.robust_datetime_parser(date)
                    if term and date_iso:
                        results.append({
                            'data_type': 'facebook_comment',
                            'Action': action,
                            'title': term,
                            'URL': '',
                            'Date': date_iso,
                            'details': ''
                        })
                except Exception as inner_e:
                    continue
                    logger.error(f"Failed to parse an item in comments.html: {inner_e}")
                    
        except Exception as e:
            # continue
            logger.error(f"Error parsing 'comments.html': {str(e)}")
        # logger.error(f"Failesdadasd {results}")

        return results
    


def parse_likes_and_reactions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    reactions = []
    
    if DATA_FORMAT == "json":
        # Loop through all paths that match the pattern 'likes_and_reactions_*.json'
        for path in validation.validated_paths:
            if path.endswith(".json") and "likes_and_reactions_" in path:
                current_reactions = helpers.find_items_bfs(data, path)

                reactions.extend([{
                    'data_type': 'facebook_reaction',
                    'Action': 'Reaction',
                    'title': remove_the_user_from_title(item.get("title", "")),
                    'URL': '',
                    'Date': helpers.robust_datetime_parser(item.get("timestamp", 0)),
                    'details': json.dumps({"reaction": item["data"][0].get("reaction", {}).get("reaction", "")})
                } for item in current_reactions])
    
    if DATA_FORMAT == "html":
        reactions = []
        # Loop through all paths that match the pattern 'likes_and_reactions_*.html'
        for path in validation.validated_paths:
            if path.endswith(".html") and "likes_and_reactions_" in path:
                html_content = data.get(path, "")
                if not html_content:
                    # logger.error(f"HTML content for '{path}' not found.")
                    continue

                try:
                    tree = html.fromstring(html_content)
                    reaction_items = tree.xpath('//div[@role="main"]/div')

                    for item in reaction_items:
                        try:
                            # Extract the title
                            title = item[0].text_content().strip().replace('"', '') if item is not None else ""

                            # Extracting the date
                            date_element = item.xpath('.//a//div[contains(text(), ":")]/text()')
                            date_text = date_element[0].strip() if date_element else ""
                            date_iso = helpers.robust_datetime_parser(date_text)

                            # Extracting the reaction type from the image src attribute
                            reaction_img_element = item.xpath('.//img[contains(@src, "icons")]/@src')
                            reaction_type = reaction_img_element[0].split('/')[-1].replace('.png', '') if reaction_img_element else ""

                            # Append the parsed data with the reaction type included in details
                            if title and date_iso:
                                reactions.append({
                                    'data_type': 'facebook_reaction',
                                    'Action': 'Reaction',
                                    'title': remove_the_user_from_title(title),
                                    'URL': '',  # URL parsing not required in this structure
                                    'Date': date_iso,
                                    'details': json.dumps({"reaction": reaction_type})
                                })
                        except Exception as inner_e:
                            logger.error(f"Failed to parse an item in {path}: {inner_e}")

                except Exception as e:
                    logger.error(f"Error parsing '{path}': {str(e)}")

    return reactions

  
  
## todo: this isnt working for the large html
def parse_your_search_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        # searches = data.get("your_search_history.json", {}).get("searches_v2", [])
        
        searches = helpers.find_items_bfs(data,"searches_v2")

        if not searches:
          return []
        
        return [{
            'data_type': 'facebook_search',
            'Action': helpers.find_items_bfs(item, "title"),
            'title': item["data"][0].get("text", ""),
            'URL': '',
            'Date': helpers.robust_datetime_parser(item.get("timestamp", 0)),
            'details': json.dumps({})
        } for item in searches]
        
    elif DATA_FORMAT == "html":
        html_content = data.get("your_search_history.html", "")
        if not html_content:
            logger.info("'your_search_history.html' not found.")
            return []
    
        results = []
        
        try:
            tree = html.fromstring(html_content)
            search_items = tree.xpath('//div[@role="main"]/div')
            
            for item in search_items:
                try:
                    # Extracting the search term - locate divs with text content directly
                    term_element = remove_the_user_from_title(item.xpath('.//div[normalize-space(text())]'))
                    action = term_element[0].text_content().strip().replace('"', '') if term_element else ""
                    term = term_element[1].text_content().strip().replace('"', '') if term_element else ""
                    date = term_element[2].text_content().strip().replace('"', '') if term_element else ""
   
                    date_iso = helpers.robust_datetime_parser(date)
                    # logger.error(f"Failesdadasd {date_iso}")
                    if term and date_iso:
                        results.append({
                            'data_type': 'facebook_search',
                            'Action': action,
                            'title': term,
                            'URL': '',
                            'Date': date_iso,
                            'details': ''
                        })
                except Exception as inner_e:
                    logger.error(f"Failed to parse an item in your_search_history.html: {inner_e}")
                    
        except Exception as e:
            logger.error(f"Error parsing 'your_search_history.html': {str(e)}")
        # logger.error(f"Failesdadasd {results}")

        return results
    
def find_structure(json_data):
    """
    Recursively search the JSON data for structures where 'dict' is a key with a list
    containing dictionaries that have keys like 'ent_field_name', 'label', and 'value'.
    """
    matches = []

    def recursive_search(data):
        # Check if data is a dictionary
        if isinstance(data, dict):
            # Look for the 'dict' key that contains a list of dictionaries
            if "dict" in data and isinstance(data["dict"], list):
                if all(isinstance(item, dict) for item in data["dict"]):
                    # Check if the dictionaries contain the specific structure
                    for item in data["dict"]:
                        if "ent_field_name" in item and "label" in item and "value" in item:
                            matches.append(data)
                            break
            # Recursively search in dictionary values
            for key, value in data.items():
                recursive_search(value)
        
        # If data is a list, recursively search each element
        elif isinstance(data, list):
            for item in data:
                recursive_search(item)

    # Start the recursive search from the root of the JSON data
    recursive_search(json_data)
    return matches

    
## also doesnt work for large html
def parse_ad_preferences(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    # List of possible translations for "Name"
    name_keys = ["Name", "Naam", "اسم", "İsim", "ⴰⵣⴳⵣⴰⵏ", "Imię", "Nom", "Nome"]
    
    if DATA_FORMAT == "json":
        preferences_dat = data.get("ad_preferences.json", {}).get("label_values", [])
        
        if not preferences_dat:
            logger.info("''ad_preferences.json' not found.")
            return []       
          
        preferences = []
        
        for pref in preferences_dat:
            left_value = pref.get("label", "")
            right_value = pref.get("value", "")
            action_type = 'AdPreference'
            data_type = 'facebook_ad_preference'
            title = left_value
            
            if action_type == 'AdPreference':
                if title and right_value is not "":
                  preferences.append({
                      'data_type': data_type,
                      'Action': action_type,
                      'title': title,
                      'URL': '',
                      'Date': '',
                      'details': right_value
                  })
                  
        ad_interests_dat = find_structure(preferences_dat)
        for pref in ad_interests_dat:
            action_type = 'Info Used to Target You'
            data_type = 'facebook_other_interests'
            title = helpers.find_items_bfs(pref, "value")
            if title:
                  preferences.append({
                      'data_type': data_type,
                      'Action': action_type,
                      'title': title,
                      'URL': '',
                      'Date': '',
                      'details': ''
                  })           
            
        return preferences
        # return [{
        #     'data_type': 'facebook_ad_preference',
        #     'Action': 'AdPreference',
        #     'title': pref.get("label", ""),
        #     'URL': '',
        #     'Date': '',
        #     'details': json.dumps(pref.get("value", ""))
        # } for pref in preferences]
    elif DATA_FORMAT == "html":
        html_content = data.get("ad_preferences.html", "")
        if not html_content:
            logger.info("'ad_preferences.html' not found.")
            return []


        try:
            tree = html.fromstring(html_content)
            rows = tree.xpath('//table/tr')
            preferences = []
    
            for row in rows:
                try: 
                  left_value = row.xpath('./td[1]//text()')[0].strip() if row.xpath('./td[1]//text()') else ""
                  right_value = row.xpath('./td[2]//text()')[0].strip() if row.xpath('./td[2]//text()') else ""
                  action_type = 'AdPreference'
                  data_type = 'facebook_ad_preference'
                  title = left_value
                  if left_value in name_keys:
                      action_type = 'Info Used to Target You'
                      data_type = 'facebook_other_interests'
                      title = right_value
                      right_value = ""
                  
                  if action_type == 'AdPreference':
                      if title and right_value is not "":
                        preferences.append({
                            'data_type': data_type,
                            'Action': action_type,
                            'title': title,
                            'URL': '',
                            'Date': '',
                            'details': right_value
                        })
                  else:
                      if title:
                        preferences.append({
                            'data_type': data_type,
                            'Action': action_type,
                            'title': title,
                            'URL': '',
                            'Date': '',
                            'details': right_value
                        })
                except Exception as e:
                  pass
    
            return preferences
      

        except Exception as e:
            logger.error(f"Error parsing 'ad_preferences.html': {str(e)}")
            return []
      
## todo: havent found a valid html
def parse_ads_personalization_consent(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        preferences = data.get("ads_personalization_consent.json", {}).get("label_values", [])
        
        if not preferences:
          return []
        
        
        return [{
            'data_type': 'facebook_ads_personalization',
            'Action': 'AdPreference',
            'title': pref.get("label", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps(pref.get("value", ""))
        } for pref in preferences]
    elif DATA_FORMAT == "html":
        html_content = data.get("ads_personalization_consent.html", "")
        if not html_content:
          return []
        else:
          return []
        # tree = html.fromstring(html_content)
        # items = tree.xpath('//div/div/div')
        # preferences = []
        # for item in items:
        #     title = item.xpath('./text()')[0]
        #     details = item.xpath('./following-sibling::div//text()')
        #     preferences.append({
        #         'data_type': 'facebook_ads_personalization',
        #         'Action': 'AdPreference',
        #         'title': title,
        #         'URL': '',
        #         'Date': '',
        #         'details': json.dumps(details)
        #     })
        # return preferences

def parse_advertisers_interacted_with(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        interactions = data.get("advertisers_you've_interacted_with.json", {}).get("history_v2", [])
        
        if not interactions:
          return []
        
        
        return [{
            'data_type': 'facebook_ad_interaction',
            'Action': 'AdInteraction',
            'title': item.get("title", "No Text") if not item.get("title", "").startswith("http") else "No Text",
            'URL': item.get("title", "") if item.get("title", "").startswith("http") else '',
            'Date': helpers.robust_datetime_parser(item.get("timestamp", 0)),
            'details': json.dumps({"action": item.get("action", "")})
        } for item in interactions]
    elif DATA_FORMAT == "html":
        html_content = data.get("advertisers_you've_interacted_with.html", "")
        if not html_content:
            # logger.info("'advertisers_you've_interacted_with.html' not found.")
            return []
    
        try:
            tree = html.fromstring(html_content)
            ads = tree.xpath('//div[contains(text(), "Clicked ad") or contains(text(), "Op advertentie geklikt")]/parent::div')

            interactions = []
    
            for ad in ads:
                title_element = ad.xpath('./div[2]')
                title = title_element[0].text_content().strip() if title_element else ""
    
                date_element = ad.xpath('.//div[contains(text(), "am") or contains(text(), "pm")]/text()')
                date = date_element[0].strip() if date_element else ""
    
                interactions.append({
                    'data_type': 'facebook_ad_interaction',
                    'Action': 'AdClick',
                    'title': title if not title.startswith("http") else "No Text",
                    'URL': title if title.startswith("http") else '',
                    'Date': helpers.robust_datetime_parser(date),
                    'details': '' 
                })
    
            return interactions
    
        except Exception as e:
            logger.error(f"Error parsing 'advertisers_you've_interacted_with.html': {str(e)}")
            return []
          
          
def parse_ads_interests(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        categories = data.get("ads_interests.json", {})
        categories = categories.get("topics_v2", [])
        
        if not categories:
            return []

        
        return [{
            'data_type': 'facebook_ads_interests',
            'Action': 'Info Used to Target You',
            'title': category,
            'URL': '',
            'Date': '',
            'details': json.dumps({})
        } for category in categories]
    elif DATA_FORMAT == "html":
        html_content = data.get("ads_interests.html", "")
        if not html_content:
            logger.info("'ads_interests.html' not found.")
            return []

        try:
            tree = html.fromstring(html_content)
            # Refine the XPath to better target interest titles using structure
            interests = tree.xpath('//div[@role="main"]//div[not(@style)]/text()')

            results = []

            for title in interests:
                title = title.strip() if title else ""

                # Only add entries with non-empty titles
                if title:
                    results.append({
                        'data_type': 'facebook_ads_interests',
                        'Action': 'Info Used to Target You',
                        'title': title,
                        'URL': '',  # No URL is present in this structure
                        'Date': '',  # No Date information is provided
                        'details': ''  # No additional details available
                    })

            return results

        except Exception as e:
            logger.error(f"Error parsing 'ads_interests.html': {str(e)}")
            return []





## todo: review the data type
def parse_other_categories_used(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        categories = data.get("other_categories_used_to_reach_you.json", {})
        categories = categories.get("bcts", [])
        
        if not categories:
            return []
        
        return [{
            'data_type': 'facebook_ad_categories',
            'Action': 'Info Used to Target You',
            'title': category,
            'URL': '',
            'Date': '',
            'details': json.dumps({})
        } for category in categories]
    elif DATA_FORMAT == "html":
        html_content = data.get("other_categories_used_to_reach_you.html", "")
        if not html_content:
            logger.info("'other_categories_used_to_reach_you.html' not found.")
            return []

        try:
            tree = html.fromstring(html_content)
            results = []

            # Updated XPath to directly access each category title
            categories = tree.xpath('//div[@role="main"]//div//div[normalize-space(text())]')

            for category in categories:
                # Extract the text content directly from the targeted div
                title = category.text_content().strip()

                if title:  # Only add non-empty titles
                    results.append({
                        'data_type': 'facebook_ad_categories',
                        'Action': 'Info Used to Target You',
                        'title': title,
                        'URL': '',  # No URL is present in this structure
                        'Date': '',  # No Date information is provided
                        'details': ''  # No additional details are provided
                    })

            return results

        except Exception as e:
            logger.error(f"Error parsing 'other_categories_used_to_reach_you.html': {str(e)}")
            return []

def parse_recently_viewed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        viewed = data.get("recently_viewed.json", {}).get("recently_viewed", [])
        result = []
        
        if not viewed:
            return []
        
        for category in viewed:
            action = category.get('description', 'Viewed')
            if 'entries' in category:
                for entry in category['entries']:
                    result.append({
                        'data_type': 'facebook_recently_viewed',
                        'Action': action,
                        'title': entry['data'].get('name', ''),
                        'URL': entry['data'].get('uri', ''),
                        'Date': helpers.robust_datetime_parser(entry.get('timestamp', 0)),
                        'details': json.dumps({})
                    })
        return result
    elif DATA_FORMAT == "html":
        html_content = data.get("recently_viewed.html", "")
        
        if not html_content:
          return []
        
        try: 
          # Parse the HTML content
          tree = html.fromstring(html_content)
          
          # Prepare a list to collect the parsed data
          parsed_data = []
          
          # Extract sections by looking for divs containing text indicating actions
          sections = tree.xpath('//div[div[contains(text(), "Berichten") or contains(text(), "Video") or contains(text(), "Advertentie") or contains(text(), "Posts that have been") or contains(text(), "Videos you have") or contains(text(), "Ads")]]')
          
          for section in sections:
              try:
                  # Extract the action text, which is in a div with specific text
                  action = section.xpath('.//div[contains(text(), "Berichten") or contains(text(), "Video") or contains(text(), "Advertentie") or contains(text(), "Posts that have been") or contains(text(), "Videos you have") or contains(text(), "Ads")]/text()')
                  action = action[0].strip() if action else "Unknown Action"
      
                  # Extract the individual entries under this action by looking for divs that have an <a> tag
                  entries = section.xpath('.//div[div/a]')  # This assumes each entry has an <a> tag
                  
                  for entry in entries:
                      try:
                        # Extract title by looking for the divs that contain the title text
                        title = entry.xpath('.//ancestor::div[1]//div[1]/div/div[1]/text()')
                        title = title[0].strip() if title else "No Title"
                        
                        # # Extract URL from the <a> tag
                        # url = entry.xpath('.//a/@href')
                        # url = url[0].strip() if url else "No URL"
                        
                        # Extract date from the div inside the <a> tag
                        date_text = entry.xpath('.//a/div/text()')
                        date_text = date_text[0].strip() if date_text else "No Date"
        
                        # Attempt to parse the date using robust_datetime_parser
                        date = helpers.robust_datetime_parser(date_text)
                        
                        # Append the data to the parsed_data list
                        parsed_data.append({
                            'data_type': 'facebook_recently_viewed',
                            'Action': action,
                            'title': title,
                            'URL': '',
                            'Date': date,
                            'details': json.dumps({})
                        })
                      except Exception as e:
                        return []
              
              except Exception as e:
                  return []
        except Exception as e:
            logger.error(f"Error parsing 'recently_viewed.html': {str(e)}")
            return []              
        return parsed_data
      
      
## todo: events parsing not working for html
def parse_recently_visited(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        visited = data.get("recently_visited.json", {}).get("visited_things_v2", [])
        result = []
        
        if not visited:
            return []
        
        for category in visited:
            action = category.get('description', 'visited')
            if 'entries' in category:
                for entry in category['entries']:
                    result.append({
                        'data_type': 'facebook_recently_visited',
                        'Action': action,
                        'title': entry['data'].get('name', ''),
                        'URL': entry['data'].get('uri', ''),
                        'Date': helpers.robust_datetime_parser(entry.get('timestamp', 0)),
                        'details': json.dumps({})
                    })
        return result
    elif DATA_FORMAT == "html":
        html_content = data.get("recently_visited.html", "")
        
        if not html_content:
          return []
        
        try: 
          # Parse the HTML content
          tree = html.fromstring(html_content)
          
          # Prepare a list to collect the parsed data
          parsed_data = []
          
          # Extract sections by looking for divs containing text indicating actions
          sections = tree.xpath('//div[div[contains(text(), "Profielbezoeken") or contains(text(), "Paginabezoeken") or contains(text(), "Bezochte evenementen") or contains(text(), "Bezochte groepen") or contains(text(), "Profile visits") or contains(text(), "Page visits") or contains(text(), "Events visited") or contains(text(), "Groups visited")]]')
          
          for section in sections:
              try:
                  # Extract the action text, which is in a div with specific text
                  action = section.xpath('.//div[contains(text(), "Mensen") or contains(text(), "Pagina") or contains(text(), "Groepen") or contains(text(), "People") or contains(text(), "Pages") or contains(text(), "Groups")]/text()')
                  action = action[0].strip() if action else "Unknown Action"
      
                  # Extract the individual entries under this action by looking for divs that have an <a> tag
                  entries = section.xpath('.//div[div/a]')  # This assumes each entry has an <a> tag
                  
                  for entry in entries:
                      try:
                        # Extract title by looking for the divs that contain the title text
                        title = entry.xpath('.//ancestor::div[1]//div[1]/div/div[1]/text()')
                        title = title[0].strip() if title else "No Title"
                        
                        # # Extract URL from the <a> tag
                        # url = entry.xpath('.//a/@href')
                        # url = url[0].strip() if url else "No URL"
                        
                        # Extract date from the div inside the <a> tag
                        date_text = entry.xpath('.//a/div/text()')
                        date_text = date_text[0].strip() if date_text else "No Date"
        
                        # Attempt to parse the date using robust_datetime_parser
                        date = helpers.robust_datetime_parser(date_text)
                        
                        # Append the data to the parsed_data list
                        parsed_data.append({
                            'data_type': 'facebook_recently_visited',
                            'Action': action,
                            'title': title,
                            'URL': '',
                            'Date': date,
                            'details': json.dumps({})
                        })
                      except Exception as e:
                        return []
              
              except Exception as e:
                  return []
        except Exception as e:
            logger.error(f"Error parsing 'recently_viewed.html': {str(e)}")
            return []              
        return parsed_data


def parse_subscription_for_no_ads(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        subscriptions = data.get("subscription_for_no_ads.json", {}).get("label_values", [])
        
        if not subscriptions:
            return []
        
        return [{
            'data_type': 'facebook_subscription',
            'Action': 'SubscriptionStatus',
            'title': sub.get("label", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps(sub.get("value", ""))
        } for sub in subscriptions]
    elif DATA_FORMAT == "html":
        html_content = helpers.find_items_bfs(data, "subscription_for_no_ads.html")
        if not html_content:
          return []
        
        try: 
        
          tree = html.fromstring(html_content)
          subscriptions = []
          
          # Find all table rows in the main content
          subscription_rows = tree.xpath('//div[@role="main"]//table//tr')
          
          for row in subscription_rows:
              label = row.xpath('.//td[1]/text()')[0].strip() if row.xpath('.//td[1]/text()') else ""
              value = row.xpath('.//td[2]/text()')[0].strip() if row.xpath('.//td[2]/text()') else ""
              
              subscriptions.append({
                  'data_type': 'facebook_subscription',
                  'Action': 'SubscriptionStatus',
                  'title': label + ": " + value,
                  'URL': '',
                  'Date': '',
                  'details': json.dumps({})
              })
        
          return subscriptions
        
        except Exception as e:
            logger.error(f"Error parsing 'subscription_for_no_ads.html': {str(e)}")
            return []      

### todo: no html
def parse_events(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        # follows = data.get("who_you've_followed.jsoeventsn", {}).get("events_joined", [])
        events = helpers.find_items_bfs(data, "event_responses_v2")
        if not events:
            return []
        
        return [{
            'data_type': 'facebook_event',
            'Action': 'EventParticipation',
            'title': event.get("name", ""),
            'URL': '',
            'Date': helpers.robust_datetime_parser(event.get("start_timestamp", 0)),
            'details': json.dumps({})
        } for event in events]
    if DATA_FORMAT == "json":
        return []

def parse_who_you_followed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        follows = data.get("who_you've_followed.json", {}).get("following_v3", [])
        
        if not follows:
            return []
        
        return [{
            'data_type': 'facebook_follow',
            'Action': 'Follow',
            'title': follow.get("name", ""),
            'URL': '',
            'Date': helpers.robust_datetime_parser(follow.get("timestamp", 0)),
            'details': json.dumps({})
        } for follow in follows]
        
    elif DATA_FORMAT == "html":
        html_content = data.get("who_you've_followed.html", "")
        if not html_content:
            logger.info("'who_you've_followed.html' not found.")
            return []

        try:
            tree = html.fromstring(html_content)
            results = []

            # Find all main divs that might contain the followed information
            followed_entries = tree.xpath('//div[@role="main"]/div')

            for entry in followed_entries:
                # Extract the title by finding the first div that contains text
                title_element = entry.xpath('.//div[normalize-space(text())]')
                title = title_element[0].text_content().strip() if title_element else ""

                # Extract the date by finding the first div that contains a date format text
                date_element = entry.xpath('.//div[contains(text(), ",") and contains(text(), ":")]')
                date_text = date_element[0].text_content().strip() if date_element else ""
                date = helpers.robust_datetime_parser(date_text)

                results.append({
                    'data_type': 'facebook_followed',
                    'Action': 'Followed',
                    'title': title,
                    'URL': '',  # No URL is present in this structure
                    'Date': date,
                    'details': ''  # No additional details available
                })

            return results

        except Exception as e:
            logger.error(f"Error parsing 'who_you've_followed.html': {str(e)}")
            return []




def remove_the_user_from_title(title: str) -> str:
    if 'the_user' in globals() and the_user:  # Check if the_user exists and is not empty
        return title.replace(the_user, "the_user").strip()
    return title


## this sometimes includes where you checked in
def parse_your_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    posts = []

    if DATA_FORMAT == "json":
        # Loop through all paths that match the exact pattern 'your_posts__check_ins__photos_and_videos_*.json'
        for path in validation.validated_paths:
            if path.endswith(".json") and os.path.basename(path).startswith("your_posts__check_ins__photos_and_videos_"):
                current_posts = data.get(path, {})

                if not current_posts:
                    continue

                posts.extend([{
                    'data_type': 'facebook_post',
                    'Action': 'Post',
                    'title': remove_the_user_from_title(helpers.find_items_bfs(item, "title")) if helpers.find_items_bfs(item, "title") else "Posted",
                    'URL': helpers.find_items_bfs(item, "url"),
                    'Date': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "timestamp")),
                    'details': json.dumps({"post_content": helpers.find_items_bfs(item, "post")})
                } for item in current_posts])

        return posts
    
    elif DATA_FORMAT == "html":
        # posts = []
        # try:
        #     tree = html.fromstring(data['your_posts__check_ins__photos_and_videos_1.html'])
        #     post_items = tree.xpath('//div[@role="main"]/div')
        # 
        #     for item in post_items:
        #         try:
        #             # Extract the title: We will capture the first significant text node as the title
        #             title_element = item.xpath('.//div[string-length(normalize-space(text())) > 0][1]')
        #             title = title_element[0].text_content().strip().replace('"', '') if title_element else "Posted"
        # 
        #             # Extracting the URL: Look for any link within the post
        #             # url_element = item.xpath('.//a[contains(@href, "facebook.com")]/@href')
        #             # url = url_element[0] if url_element else ""
        # 
        #             # Extracting the date: Look for a div that contains time-related text
        #             date_element = item.xpath('.//div[contains(text(), ":")]/text()')
        #             date_text = date_element[0].strip() if date_element else ""
        #             date_iso = helpers.robust_datetime_parser(date_text)
        # 
        #             # Extracting the post content: Collect all text nodes within a deeper div
        #             post_content_element = item.xpath('.//div[string-length(normalize-space(text())) > 0]')
        #             post_content = " ".join(elem.text_content().strip() for elem in post_content_element[1:] if elem.text_content().strip())
        # 
        #             # Append the parsed data
        #             if title and date_iso:
        #                 posts.append({
        #                     'data_type': 'facebook_post',
        #                     'Action': 'Post',
        #                     'title': title,
        #                     'URL': '',
        #                     'Date': date_iso,
        #                     'details': json.dumps({"post_content": post_content})
        #                 })
        #         except Exception as inner_e:
        #             logger.error(f"Failed to parse an item in your_posts__check_ins__photos_and_videos_1.html: {inner_e}")
        # 
        # except Exception as e:
        #     logger.error(f"Error parsing 'your_posts__check_ins__photos_and_videos_1.html': {str(e)}")
        # 
        # return posts
        return []


def parse_facebook_account_suggestions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        categories = helpers.find_items_bfs(data, "people_we_think_you_should_follow.json")
        categories = helpers.find_items_bfs(categories, "vec")

        if not categories:
          return []

        return [{
            'data_type': 'facebook_account_suggestions',
            'Action': 'People You Should Follow',
            'title': category.get("value", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps({})
        } for category in categories]
    elif DATA_FORMAT == "html":
        html_content = data.get("people_we_think_you_should_follow.html", "")
        
        if not html_content:
          return []
        
        items = html.fromstring(html_content)
        
        # Extract all <div> elements that contain the names
        names_elements = items.xpath('.//td/div/div/div/div')
        
        # Extract the text content from each <div> element
        names = [name.text_content().strip() for name in names_elements]
        categories = []
        for item in names:
            title = item
            categories.append({
                'data_type': 'facebook_account_suggestions',
                'Action': 'People You Should Follow',
                'title': title,
                'URL': '',
                'Date': '',
                'details': json.dumps({})
            })
        return categories


def parse_group_posts_and_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":

        posts = data.get("group_posts_and_comments.json", {}).get("group_posts_v2", [])
        
        if not posts:
          return []
        
        return [{
            'data_type': 'facebook_group_post',
            'Action': 'Group Post',
            'title': remove_the_user_from_title(helpers.find_items_bfs(item, "title")),
            'URL': '',
            'Date': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "timestamp")),
            'details': json.dumps({"post_content": helpers.find_items_bfs(item, "post")})
        } for item in posts]
    elif DATA_FORMAT == "html":
        reactions = []
        try:
            posts = helpers.find_items_bfs(data, 'group_posts_and_comments.html')
            if not posts:
              logger.info("'group_posts_and_comments.html' not found.")
              return []
            
            tree = html.fromstring(posts)
            reaction_items = tree.xpath('//div[@role="main"]/div')
    
            for item in reaction_items:
                try:
                    # Extract the title based on the structure, assuming it's the first significant text node
                    title = item.xpath('.//div[normalize-space(text())][1]/text()')
                    title = title[0].strip().replace('"', '') if title else ""
    
                    # Extracting the date based on structure
                    date_element = item.xpath('.//a//div[contains(text(), ":")]/text()')
                    date_text = date_element[0].strip() if date_element else ""
                    date_iso = helpers.robust_datetime_parser(date_text)
    
                    # Extracting the post content without using classes
                    post_content_element = item.xpath('.//div[div/div]//text()')
                    post_content = post_content_element[0].strip() if post_content_element else ""
                    # Append the parsed data with post content in details
                    if title and date_iso:
                        reactions.append({
                            'data_type': 'facebook_group_post',            
                            'Action': 'Group Post',
                            'title': remove_the_user_from_title(title),
                            'URL': '',  # URL not required
                            'Date': date_iso,
                            'details': json.dumps({"post_content": post_content})
                        })
                except Exception as inner_e:
                    logger.error(f"Failed to parse an item in group_posts_and_comments.html: {inner_e}")
    
        except Exception as e:
            logger.error(f"Error parsing 'group_posts_and_comments.html': {str(e)}")
    
        return reactions    
    


def parse_your_comments_in_groups(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":

        comments = data.get("your_comments_in_groups.json", {}).get("group_comments_v2", [])
        
        if not comments:
          return []
        
        return [{
            'data_type': 'facebook_group_comment',
            'Action': 'Comment',
            'title': remove_the_user_from_title(item.get("title", "Comment in Group")),
            'URL': '',
            'Date': helpers.robust_datetime_parser(item.get("timestamp", 0)),
            'details': json.dumps({
                "comment": item.get("data", [{}])[0].get("comment", {}).get("comment", ""),
                "group": item.get("data", [{}])[0].get("comment", {}).get("group", "")
            })
        } for item in comments]
        
    elif DATA_FORMAT == "html":
        comments = []
        try:
            posts = helpers.find_items_bfs(data, 'your_comments_in_groups.html')
            if not posts:
              logger.info("'your_comments_in_groups.html' not found.")
              return []
            
            tree = html.fromstring(posts)
        
            comment_items = tree.xpath('//div[@role="main"]/div')
    
            for item in comment_items:
                try:
                    # Extract the title (comment context)
                    title = item.xpath('.//div[normalize-space(text())][1]/text()')
                    title = title[0].strip().replace('"', '') if title else "Comment in Group"
    
                    # Extracting the date
                    date_element = item.xpath('.//a//div[contains(text(), ":")]/text()')
                    date_text = date_element[0].strip() if date_element else ""
                    date_iso = helpers.robust_datetime_parser(date_text)
    
                    # Extracting the comment text and group name
                    comment_text = item.xpath('.//div[div/text()][last()]/text()')
                    comment_text = comment_text[0].strip() if comment_text else ""
    
                    group_name = item.xpath(
                        './/span[contains(text(), "Groep") or contains(text(), "Grup") or contains(text(), "مجموعة") or '
                        'contains(text(), "Gruppo") or contains(text(), "Gruppe") or contains(text(), "Group")]/following-sibling::text()'
                    )                    
                    group_name = group_name[0].strip() if group_name else ""
    
                    # Append the parsed data
                    if title and date_iso:
                        comments.append({
                            'data_type': 'facebook_group_comment',
                            'Action': 'Comment',
                            'title': title,
                            'URL': '',  # URL not required
                            'Date': date_iso,
                            'details': json.dumps({
                                "comment": comment_text,
                                "group": group_name
                            })
                        })
                except Exception as inner_e:
                    logger.error(f"Failed to parse an item in your_comments_in_groups.html: {inner_e}")
    
        except Exception as e:
            logger.error(f"Error parsing 'your_comments_in_groups.html': {str(e)}")
    
        return comments


def parse_your_group_membership_activity(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":

        activities = data.get("your_group_membership_activity.json", {}).get("groups_joined_v2", [])
        
        if not activities:
          return []
        
        return [{
            'data_type': 'facebook_group_membership',
            'Action': 'Group Membership',
            'title': item.get("title", "Group Membership Activity"),
            'URL': '',
            'Date': helpers.robust_datetime_parser(item.get("timestamp", 0)),
            'details': json.dumps({
                "group": item.get("data", [{}])[0].get("name", "")
            })
        } for item in activities]
    elif DATA_FORMAT == "html":

        activities = []
        try:
          
            posts = helpers.find_items_bfs(data, 'your_group_membership_activity.html')
            if not posts:
              logger.info("'your_group_membership_activity.html' not found.")
              return []
            
            tree = html.fromstring(posts)
          
            activity_items = tree.xpath('//div[@role="main"]/div')
    
            for item in activity_items:
                try:
                    # Extract the title (e.g., "Je bent lid geworden van We Pretend It’s Medieval Internet.")
                    title = item.xpath('.//div[normalize-space(text())][1]/text()')
                    title = title[0].strip().replace('"', '') if title else "Group Membership Activity"
    
                    # Extracting the date
                    date_element = item.xpath('.//div[contains(text(), ":")]/text()')
                    date_text = date_element[0].strip() if date_element else ""
                    date_iso = helpers.robust_datetime_parser(date_text)
    
                    # Extracting the group name (from the title)
                    group_name = title.split("van")[-1].strip() if "van" in title else ""
    
                    # Append the parsed data
                    if title and date_iso:
                        activities.append({
                            'data_type': 'facebook_group_membership',
                            'Action': 'Group Membership',
                            'title': title,
                            'URL': '',  # URL not required
                            'Date': date_iso,
                            'details': json.dumps({
                                "group": group_name
                            })
                        })
                except Exception as inner_e:
                    logger.error(f"Failed to parse an item in your_group_membership_activity.html: {inner_e}")
    
        except Exception as e:
            logger.error(f"Error parsing 'your_group_membership_activity.html': {str(e)}")
    
        return activities
      



def process_facebook_data(facebook_zip: str) -> List[props.PropsUIPromptConsentFormTable]:
    logger.info("Starting to extract Facebook data.")   

    extracted_data = extract_facebook_data(facebook_zip)
    # Assuming `extracted_data` is a dictionary where keys are the file paths or names.
    filtered_extracted_data = {
        k: v for k, v in extracted_data.items() if not re.match(r'^\d+\.(html|json)$', k.split('/')[-1])
    }
    
    # Logging only the filtered keys
    logger.info(f"Extracted data keys: {helpers.get_json_keys(filtered_extracted_data) if filtered_extracted_data else 'None'}")   
    
    all_data = []
    parsing_functions = [
        parse_your_search_history,
        parse_who_you_followed,
        parse_advertisers_interacted_with, 
        parse_comments,
        parse_likes_and_reactions, 
        parse_recently_viewed,
        parse_recently_visited,
        parse_your_posts,
        parse_events,
        parse_group_posts_and_comments,
        parse_your_comments_in_groups,
        parse_your_group_membership_activity
    ]
    
    for parse_function in parsing_functions:
        try:
            parsed_data = parse_function(extracted_data)
            if parsed_data:
                logger.info(f"{parse_function.__name__} returned {len(parsed_data)} items")
                all_data.extend(parsed_data)
        except Exception as e:
            logger.error(f"Error in {parse_function.__name__}: {str(e)}")
        
    tables_to_render = []
    
    if all_data:
        combined_df = pd.DataFrame(all_data)
        
        combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
        # logger.warning(f"{print(combined_df)}")

        try:
          # Localize naive timestamps before converting
          combined_df['Date'] = combined_df['Date'].apply(lambda x: x.tz_localize('UTC') if x.tzinfo is None else x)
        except Exception as e:
          logger.info(f"Error localizing dates: {e}")
                
        try:
          # Convert all datetime objects to timezone-naive
          combined_df['Date'] = combined_df['Date'].dt.tz_convert(None)
        except Exception as e:
          logger.info(f"Error converting dates: {e}")
        
        # Check for entries with dates before 2016
        pre_2000_count = (combined_df['Date'] < pd.Timestamp('2000-01-01')).sum()
        if pre_2000_count > 0:
            logger.info(f"Found {pre_2000_count} entries with dates before 2000.")
        
            # Filter out dates before 2000
            try:
                # Filter out dates before 2000
                combined_df = combined_df[combined_df['Date'] >= pd.Timestamp('2000-01-01')] 
                # Confirm deletion
                if pre_2000_count > 0:
                    post_filter_count = (combined_df['Date'] < pd.Timestamp('2000-01-01')).sum()
                    if post_filter_count == 0:
                        logger.info(f"Successfully deleted {pre_2000_count} entries with dates before 2000.")
                    else:
                          logger.info(f"Failed to delete some entries with dates before 2000. Remaining: {post_filter_count}.")
                    
            except Exception as e:
                logger.info(f"Error filtering dates before 2000: {e}")
    
        
        combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
        combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        combined_df['Count'] = 1
        
        # List of columns to apply the replace_email function
        columns_to_process = ['title', 'details', 'Action']
        
        # Loop over each column in the list
        for column in columns_to_process:
            try:
                # Ensure the column values are strings and apply the replace_email function
                combined_df[column] = combined_df[column].apply(lambda x: helpers.replace_email(str(x)))
            except Exception as e:
                logger.warning(f"Could not replace e-mail in column '{column}': {e}")

        
        table_title = props.Translatable({"en": "Facebook Activity Data", "nl": "Facebook Gegevens"})
        visses = [vis.create_chart(
            "line", 
            "Facebook Activity Over Time", 
            "Facebook-activiteit", 
            "Date", 
            y_label="Aantal observaties", 
            date_format="auto"
        )]
        
        table = props.PropsUIPromptConsentFormTable("facebook_all_data", table_title, combined_df, visualizations=visses)
        tables_to_render.append(table)
        
        logger.info(f"Successfully processed {len(combined_df)} total entries from Facebook data")
    else:
        logger.warning("No data with dates was successfully extracted and parsed.")
    
    ### this is for all things without dates
    all_data = []
    parsing_functions = [
        parse_subscription_for_no_ads, 
        parse_ad_preferences,
        parse_ads_personalization_consent,
        parse_ads_interests,
        parse_other_categories_used,
        parse_facebook_account_suggestions,
        parse_advertisers_using_activity
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
            if 'URL' in combined_df.columns:
                combined_df = combined_df.drop(columns=['URL'])
            # combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
            # combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
            # combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
       
            # if 'details' in combined_df.columns:
            #     combined_df = combined_df.drop(columns=['details'])
            # Create a single table with all data
            table_title = props.Translatable({"en": "Facebook Ad Info & Suggestions", "nl": "Facebook Gegevens"})


            # Pass the ungrouped data for the table and grouped data for the chart
            table = props.PropsUIPromptConsentFormTable("facebook_ad_info", table_title, combined_df)
            tables_to_render.append(table)
            
            logger.info(f"Successfully processed Second {len(combined_df)} total entries from Facebook data")
        else:
            logger.warning("Second Combined DataFrame is empty")
    else:
        logger.warning("Second Combined DataFrame: No data without dates was successfully extracted and parsed")
    
    return tables_to_render

# Helper functions for specific data types
def group_interactions_to_df(facebook_zip: str) -> pd.DataFrame:
    tables = process_facebook_data(facebook_zip)
    df = tables[0].data if tables else pd.DataFrame()
    return df[df['data_type'] == 'facebook_group_interaction'].drop(columns=['data_type'])

def comments_to_df(facebook_zip: str) -> pd.DataFrame:
    tables = process_facebook_data(facebook_zip)
    df = tables[0].data if tables else pd.DataFrame()
    return df[df['data_type'] == 'facebook_comment'].drop(columns=['data_type'])

def likes_and_reactions_to_df(facebook_zip: str) -> pd.DataFrame:
    tables = process_facebook_data(facebook_zip)
    df = tables[0].data if tables else pd.DataFrame()
    return df[df['data_type'] == 'facebook_reaction'].drop(columns=['data_type'])

def your_posts_to_df(facebook_zip: str) -> pd.DataFrame:
    tables = process_facebook_data(facebook_zip)
    df = tables[0].data if tables else pd.DataFrame()
    return df[df['data_type'] == 'facebook_post'].drop(columns=['data_type'])

def your_search_history_to_df(facebook_zip: str) -> pd.DataFrame:
    tables = process_facebook_data(facebook_zip)
    df = tables[0].data if tables else pd.DataFrame()
    return df[df['data_type'] == 'facebook_search'].drop(columns=['data_type'])
