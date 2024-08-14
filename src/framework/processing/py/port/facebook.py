import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
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
            "group_interactions.json",
            "ad_preferences.json",
            "your_posts_1.json",
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
            "your_search_history.json"
        ],
    ),
    DDPCategory(
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
            "group_interactions.html",
            "ad_preferences.html",
            "your_posts_1.json",
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
            "your_search_history.html"
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
                if p.suffix in (".json", ".html"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        
        if validation.ddp_category is None:
            logger.warning("Could not infer DDP category")
            validation.set_status_code(1)  # Not a valid DDP
        else:
            validation.set_status_code(0)  # Valid DDP

    except zipfile.BadZipFile:
        logger.error("Bad zip file")
        validation.set_status_code(2)  # Bad zipfile
    except Exception as e:
        logger.error(f"Unexpected error during validation: {str(e)}")
        validation.set_status_code(1)  # Not a valid DDP

    return validation

def extract_facebook_data(facebook_zip: str) -> Dict[str, Any]:
    global DATA_FORMAT
    validation = validate(Path(facebook_zip))
    if validation.status_code is None or validation.status_code.id != 0:
        logger.error(f"Invalid zip file: {validation.status_code.description if validation.status_code else 'Unknown error'}")
        return {}

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
                    if DATA_FORMAT == "json":
                        data[Path(file).name] = json.load(io.TextIOWrapper(f, encoding='utf-8'))
                    elif DATA_FORMAT == "html":
                        data[Path(file).name] = f.read().decode('utf-8')
        logger.info(f"Extracted data from {len(data)} files. Data format: {DATA_FORMAT}")
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
    return data
  
def parse_advertisers_using_activity(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        advertisers = data.get("advertisers_using_your_activity_or_information.json", {}).get("custom_audiences_all_types_v2", [])
        return [{
            'data_type': 'facebook_advertiser_activity',
            'Action': 'AdvertiserActivity',
            'title': advertiser.get("advertiser_name", ""),
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
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        advertisers = []
        for item in items:
            title = item.xpath('./text()')[0]
            details = item.xpath('./following-sibling::div//text()')
            advertisers.append({
                'data_type': 'facebook_advertiser_activity',
                'Action': 'AdvertiserActivity',
                'title': title,
                'URL': '',
                'Date': '',
                'details': json.dumps(details)
            })
        return advertisers


def parse_group_interactions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    interactions = data.get("group_interactions.json", {}).get("group_interactions_v2", [{}])[0].get("entries", [])
    return [{
        'data_type': 'facebook_group_interaction',
        'Action': 'GroupInteraction',
        'title': item.get("data", {}).get("name", "Unknown Group"),
        'URL': item.get("data", {}).get("uri", ""),
        'Date': '',  # No date information available in this data
        'details': json.dumps({"times_interacted": item.get("data", {}).get("value", '').split(" ")[0]})
    } for item in interactions]

def parse_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        comments = data.get("comments.json", {}).get("comments_v2", [])
        return [{
            'data_type': 'facebook_comment',
            'Action': 'Comment',
            'title': item.get("title", ""),
            'URL': '',
            'Date': helpers.epoch_to_iso(item.get("timestamp", 0)),
            'details': json.dumps({"comment": item["data"][0].get("comment", {}).get("comment", "")})
        } for item in comments]
    elif DATA_FORMAT == "html":
        html_content = data.get("comments.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        comments = []
        for item in items:
            title = item.xpath('./text()')[0]
            date = item.xpath('./following-sibling::div//text()')
            comments.append({
                'data_type': 'facebook_comment',
                'Action': 'Comment',
                'title': title,
                'URL': '',
                'Date': helpers.robust_datetime_parser(date[0]),
                'details': ''
            })
        return comments

def parse_likes_and_reactions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        reactions = data.get("likes_and_reactions_1.json", [])
        return [{
            'data_type': 'facebook_reaction',
            'Action': 'Reaction',
            'title': item.get("title", ""),
            'URL': '',
            'Date': helpers.epoch_to_iso(item.get("timestamp", 0)),
            'details': json.dumps({"reaction": item["data"][0].get("reaction", {}).get("reaction", "")})
        } for item in reactions]
    elif DATA_FORMAT == "html":
        html_content = data.get("likes_and_reactions_1.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        reactions = []
        for item in items:
            title = item.xpath('./text()')[0]
            date = item.xpath('./following-sibling::div//text()')
            reactions.append({
                'data_type': 'facebook_reaction',
                'Action': 'Reaction',
                'title': title,
                'URL': '',
                'Date': helpers.robust_datetime_parser(date[0]),
                'details': ''
            })
        return reactions


def parse_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    posts = data.get("your_posts_1.json", [])
    if isinstance(posts, dict):
        posts = [posts]
    return [{
        'data_type': 'facebook_post',
        'Action': 'Post',
        'title': helpers.find_items_bfs(item, "title"),
        'URL': helpers.find_items_bfs(item, "url"),
        'Date': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "timestamp")),
        'details': json.dumps({"post_content": helpers.find_items_bfs(item, "post")})
    } for item in posts]

def parse_your_search_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        searches = data.get("your_search_history.json", {}).get("searches_v2", [])
        return [{
            'data_type': 'facebook_search',
            'Action': 'Search',
            'title': item["data"][0].get("text", ""),
            'URL': '',
            'Date': helpers.epoch_to_iso(item.get("timestamp", 0)),
            'details': json.dumps({})
        } for item in searches]
    elif DATA_FORMAT == "html":
        html_content = data.get("your_search_history.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        searches = []
        for item in items:
            title = item.xpath('./text()')[0]
            date = item.xpath('./following-sibling::div//text()')
            searches.append({
                'data_type': 'facebook_search',
                'Action': 'Search',
                'title': title,
                'URL': '',
                'Date': helpers.robust_datetime_parser(date[0]),
                'details': ''
            })
        return searches
    
    
    
def parse_ad_preferences(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        preferences = data.get("ad_preferences.json", {}).get("label_values", [])
        return [{
            'data_type': 'facebook_ad_preference',
            'Action': 'AdPreference',
            'title': pref.get("label", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps(pref.get("value", ""))
        } for pref in preferences]
    elif DATA_FORMAT == "html":
        html_content = data.get("ad_preferences.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        preferences = []
        for item in items:
            title = item.xpath('./text()')[0]
            details = item.xpath('./following-sibling::div//text()')
            preferences.append({
                'data_type': 'facebook_ad_preference',
                'Action': 'AdPreference',
                'title': title,
                'URL': '',
                'Date': '',
                'details': json.dumps(details)
            })
        return preferences

def parse_advertisers_interacted_with(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        interactions = data.get("advertisers_you've_interacted_with.json", {}).get("history_v2", [])
        return [{
            'data_type': 'facebook_ad_interaction',
            'Action': 'AdInteraction',
            'title': item.get("title", ""),
            'URL': '',
            'Date': helpers.epoch_to_iso(item.get("timestamp", 0)),
            'details': json.dumps({"action": item.get("action", "")})
        } for item in interactions]
    elif DATA_FORMAT == "html":
        html_content = data.get("advertisers_you've_interacted_with.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        interactions = []
        for item in items:
            title = item.xpath('./text()')[0]
            date = item.xpath('./following-sibling::div//text()')
            interactions.append({
                'data_type': 'facebook_ad_interaction',
                'Action': 'AdInteraction',
                'title': title,
                'URL': '',
                'Date': helpers.robust_datetime_parser(date[0]),
                'details': ''
            })
        return interactions

def parse_story_views(data):
    views = data.get("story_views_in_past_7_days.json", {}).get("label_values", [])
    return [{
        'data_type': 'facebook_story_view',
        'Action': 'StoryView',
        'title': view.get("label", ""),
        'URL': '',
        'Date': '',
        'details': json.dumps({"value": view.get("value", "")})
    } for view in views]
    
    
def parse_other_categories_used(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        categories = data.get("other_categories_used_to_reach_you.json", {}).get("label_values", [])
        return [{
            'data_type': 'facebook_other_category',
            'Action': 'OtherCategoryUsed',
            'title': category.get("label", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps(category.get("value", ""))
        } for category in categories]
    elif DATA_FORMAT == "html":
        html_content = data.get("other_categories_used_to_reach_you.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        categories = []
        for item in items:
            title = item.xpath('./text()')[0]
            details = item.xpath('./following-sibling::div//text()')
            categories.append({
                'data_type': 'facebook_other_category',
                'Action': 'OtherCategoryUsed',
                'title': title,
                'URL': '',
                'Date': '',
                'details': json.dumps(details)
            })
        return categories

def parse_recently_viewed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        viewed = data.get("recently_viewed.json", {}).get("label_values", [])
        return [{
            'data_type': 'facebook_recently_viewed',
            'Action': 'RecentlyViewed',
            'title': view.get("label", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps(view.get("value", ""))
        } for view in viewed]
    elif DATA_FORMAT == "html":
        html_content = data.get("recently_viewed.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        viewed = []
        for item in items:
            title = item.xpath('./text()')[0]
            details = item.xpath('./following-sibling::div//text()')
            viewed.append({
                'data_type': 'facebook_recently_viewed',
                'Action': 'RecentlyViewed',
                'title': title,
                'URL': '',
                'Date': '',
                'details': json.dumps(details)
            })
        return viewed

def parse_recently_visited(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        visited = data.get("recently_visited.json", {}).get("label_values", [])
        return [{
            'data_type': 'facebook_recently_visited',
            'Action': 'RecentlyVisited',
            'title': visit.get("label", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps(visit.get("value", ""))
        } for visit in visited]
    elif DATA_FORMAT == "html":
        html_content = data.get("recently_visited.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        visited = []
        for item in items:
            title = item.xpath('./text()')[0]
            details = item.xpath('./following-sibling::div//text()')
            visited.append({
                'data_type': 'facebook_recently_visited',
                'Action': 'RecentlyVisited',
                'title': title,
                'URL': '',
                'Date': '',
                'details': json.dumps(details)
            })
        return visited


def parse_subscription_for_no_ads(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        subscriptions = data.get("subscription_for_no_ads.json", {}).get("label_values", [])
        return [{
            'data_type': 'facebook_subscription',
            'Action': 'SubscriptionStatus',
            'title': sub.get("label", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps(sub.get("value", ""))
        } for sub in subscriptions]
    elif DATA_FORMAT == "html":
        html_content = data.get("subscription_for_no_ads.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        subscriptions = []
        for item in items:
            title = item.xpath('./text()')[0]
            details = item.xpath('./following-sibling::div//text()')
            subscriptions.append({
                'data_type': 'facebook_subscription',
                'Action': 'SubscriptionStatus',
                'title': title,
                'URL': '',
                'Date': '',
                'details': json.dumps(details)
            })
        return subscriptions

def parse_who_you_followed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        follows = data.get("who_you've_followed.json", {}).get("following_v3", [])
        return [{
            'data_type': 'facebook_follow',
            'Action': 'Follow',
            'title': follow.get("name", ""),
            'URL': '',
            'Date': helpers.epoch_to_iso(follow.get("timestamp", 0)),
            'details': json.dumps({})
        } for follow in follows]
    elif DATA_FORMAT == "html":
        html_content = data.get("who_you've_followed.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        follows = []
        for item in items:
            title = item.xpath('./text()')[0]
            date = item.xpath('./following-sibling::div//text()')
            follows.append({
                'data_type': 'facebook_follow',
                'Action': 'Follow',
                'title': title,
                'URL': '',
                'Date': helpers.robust_datetime_parser(date[0]),
                'details': ''
            })
        return follows



def parse_your_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        posts = data.get("your_posts__check_ins__photos_and_videos_1.json", {})
        if isinstance(posts, dict):
            posts = [posts]
        return [{
            'data_type': 'facebook_post',
            'Action': 'Post',
            'title': helpers.find_items(helpers.dict_denester(item), "title"),
            'URL': helpers.find_items(helpers.dict_denester(item), "url"),
            'Date': helpers.epoch_to_iso(helpers.find_items(helpers.dict_denester(item), "timestamp")),
            'details': json.dumps({"post_content": helpers.find_items(helpers.dict_denester(item), "post")})
        } for item in posts]
    elif DATA_FORMAT == "html":
        html_content = data.get("your_posts__check_ins__photos_and_videos_1.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        posts = []
        for item in items:
            title = item.xpath('./text()')[0]
            date = item.xpath('./following-sibling::div//text()')
            posts.append({
                'data_type': 'facebook_post',
                'Action': 'Post',
                'title': title,
                'URL': '',
                'Date': helpers.robust_datetime_parser(date[0]),
                'details': ''
            })
        return posts


def process_facebook_data(facebook_zip: str) -> List[props.PropsUIPromptConsentFormTable]:
    extracted_data = extract_facebook_data(facebook_zip)
    
    
    all_data = []
    parsing_functions = [
        parse_ad_preferences, 
        parse_advertisers_interacted_with, 
        parse_group_interactions,
        parse_comments,
        parse_likes_and_reactions, 
        parse_posts,
        parse_other_categories_used, 
        parse_recently_viewed,
        parse_recently_visited,
        parse_subscription_for_no_ads,
        parse_your_posts,
        parse_advertisers_using_activity
        
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
        combined_df = pd.DataFrame(all_data)
        
        combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
        combined_df = combined_df.sort_values(by='Date', ascending=False, na_position='last').reset_index(drop=True)
        combined_df['Date'] = combined_df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        combined_df['Count'] = 1
        
        table_title = props.Translatable({"en": "Facebook Activity Data", "nl": "Facebook Gegevens"})
        visses = [vis.create_chart(
            "line", 
            "Facebook Activity Over Time", 
            "Facebook Activity Over Time", 
            "Date", 
            y_label="Number of Observations", 
            date_format="auto"
        )]
        
        table = props.PropsUIPromptConsentFormTable("facebook_all_data", table_title, combined_df, visualizations=visses)
        tables_to_render.append(table)
        
        logger.info(f"Successfully processed {len(combined_df)} total entries from Facebook data")
    else:
        logger.warning("No data was successfully extracted and parsed")
    
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
