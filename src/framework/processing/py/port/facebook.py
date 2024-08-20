import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import logging
import zipfile
import io
import re
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
            "your_comments_in_groups.json"
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

def validate(file: Path) -> ValidateInput:
    global validation
    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)
    
    try:
        paths = []
        file_name = file.lower()  # Convert file name to lowercase for consistent checks
        
        with zipfile.ZipFile(file, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".json", ".html"):
                    paths.append(p.name.lower())  # Convert to lowercase for consistent checks

        validation.infer_ddp_category(paths)
        
        if validation.ddp_category is None:
            logger.warning("Could not infer DDP category")
            validation.set_status_code(1)  # Not a valid DDP
        elif validation.ddp_category.ddp_filetype in (DDPFiletype.JSON, DDPFiletype.HTML):
            # Check if the file name indicates Facebook or facebook
            if "facebook" in file_name and "facebook" not in file_name:
                validation.set_status_code(1)  # Not a valid DDP for facebook
                logger.warning("Found facebook in zip file so can't be Facebook!")
            # elif "facebook" in file_name and "facebook" not in file_name:
            #     validation.set_status_code(0)  # Valid DDP for facebook
            #     
            #     # Log the valid facebook files found
            #     for p in paths:
            #         logger.debug("Found: %s in zip", p)
            # 
            # If file name does not indicate, fallback to checking paths
            elif any("facebook" in path for path in paths) and not any("facebook" in path for path in paths):
                validation.set_status_code(1)  # Not a valid DDP for facebook
                logger.warning("Found facebook in file names so can't be Facebook!")
            # elif any("facebook" in path for path in paths) and not any("facebook" in path for path in paths):
            #     validation.set_status_code(0)  # Valid DDP for facebook
            #     
            else:
                validation.set_status_code(0)  # Assume valid DDP
                # Log the valid Facebook files found
                for p in paths:
                    # Check if the path matches the pattern of purely digits followed by .html
                    if not re.match(r'^\d+\.html$', p.split('/')[-1]):
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
                    if DATA_FORMAT == "json":
                        data[Path(file).name] = json.load(io.TextIOWrapper(f, encoding='utf-8'))
                    elif DATA_FORMAT == "html":
                        data[Path(file).name] = f.read().decode('utf-8')
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
        if not html_content:
            logger.error("HTML content for 'advertisers_using_your_activity_or_information.html' not found.")
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


# def parse_group_interactions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
#     interactions = data.get("group_interactions.json", {}).get("group_interactions_v2", [{}])[0].get("entries", [])
#     return [{
#         'data_type': 'facebook_group_interaction',
#         'Action': 'GroupInteraction',
#         'title': item.get("data", {}).get("name", "Unknown Group"),
#         'URL': item.get("data", {}).get("uri", ""),
#         'Date': '',  # No date information available in this data
#         'details': json.dumps({"times_interacted": item.get("data", {}).get("value", '').split(" ")[0]})
#     } for item in interactions]

def parse_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        comments = data.get("comments.json", {}).get("comments_v2", [])
        
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
    reactions = []
    
    if DATA_FORMAT == "json":
        # Loop through all paths that match the pattern 'likes_and_reactions_*.json'
        for path in validation.validated_paths:
            if path.endswith(".json") and "likes_and_reactions_" in path:
                current_reactions = helpers.find_items_bfs(data, path)
                the_author = helpers.find_items_bfs(current_reactions , "actor")

                def replace_author(text: str, author: str) -> str:
                    if author in text:
                        return text.replace(author, "the_user").strip()
                    return text

                reactions.extend([{
                    'data_type': 'facebook_reaction',
                    'Action': 'Reaction',
                    'title': replace_author(item.get("title", ""), the_author),
                    'URL': '',
                    'Date': helpers.robust_datetime_parser(item.get("timestamp", 0)),
                    'details': json.dumps({"reaction": item["data"][0].get("reaction", {}).get("reaction", "")})
                } for item in current_reactions])
    
    elif DATA_FORMAT == "html":
        # Assuming all HTML files are parsed similarly
        for path in validation.validated_paths:
            if path.endswith(".html") and "likes_and_reactions_" in path:
                html_content = helpers.load_html_file(path)  # Assuming helpers has a method to load HTML
                tree = html.fromstring(html_content)
                items = tree.xpath('//div/div/div')
                for item in items:
                    title = item.xpath('./text()')[0]
                    date = item.xpath('./following-sibling::div//text()')

                    # Replace the_author with "the_user" in title
                    title = replace_author(title, the_author)

                    reactions.append({
                        'data_type': 'facebook_reaction',
                        'Action': 'Reaction',
                        'title': title,
                        'URL': '',
                        'Date': helpers.robust_datetime_parser(date[0]),
                        'details': ''
                    })
    
    return reactions


def parse_your_search_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        searches = data.get("your_search_history.json", {}).get("searches_v2", [])
        return [{
            'data_type': 'facebook_search',
            'Action': 'Search',
            'title': item["data"][0].get("text", ""),
            'URL': '',
            'Date': helpers.robust_datetime_parser(item.get("timestamp", 0)),
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
        if not html_content:
            logger.error("HTML content for 'ad_preferences.html' not found.")
            return []

        try:
            tree = html.fromstring(html_content)
            rows = tree.xpath('//table/tbody/tr')
            preferences = []

            for row in rows:
                # Extract the value on the left
                left_value = row.xpath('./td[1]/text()')[0].strip() if row.xpath('./td[1]/text()') else ""
                
                # Extract the value on the right
                right_value = row.xpath('./td[2]/text()')[0].strip() if row.xpath('./td[2]/text()') else ""
                
                preferences.append({
                    'data_type': 'facebook_ad_preference',
                    'Action': 'AdPreference',
                    'title': left_value,
                    'URL': '',
                    'Date': '',
                    'details': right_value
                })

            return preferences

        except Exception as e:
            logger.error(f"Error parsing 'ad_preferences.html': {str(e)}")
            return []
      
      
def parse_ads_personalization_consent(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        preferences = data.get("ads_personalization_consent.json", {}).get("label_values", [])
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
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        preferences = []
        for item in items:
            title = item.xpath('./text()')[0]
            details = item.xpath('./following-sibling::div//text()')
            preferences.append({
                'data_type': 'facebook_ads_personalization',
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
            'Date': helpers.robust_datetime_parser(item.get("timestamp", 0)),
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
        categories = data.get("other_categories_used_to_reach_you.json", {})
        categories = categories.get("bcts", [])
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
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        categories = []
        for item in items:
            title = item.xpath('./text()')[0]
            details = item.xpath('./following-sibling::div//text()')
            categories.append({
                'data_type': 'facebook_ad_categories',
                'Action': 'Info Used to Target You',
                'title': title,
                'URL': '',
                'Date': '',
                'details': json.dumps(details)
            })
        return categories


def parse_recently_viewed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        viewed = data.get("recently_viewed.json", {}).get("recently_viewed", [])
        result = []
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
        visited = data.get("recently_visited.json", {}).get("visited_things_v2", [])
        result = []
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
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        visited = []
        for item in items:
            title = item.xpath('./text()')[0]
            details = item.xpath('./following-sibling::div//text()')
            visited.append({
                'data_type': 'facebook_recently_visited',
                'Action': 'Recentlyvisited',
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

def parse_who_you_followed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        follows = data.get("who_you've_followed.json", {}).get("following_v3", [])
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


def remove_the_user_from_title(title: str) -> str:
    if 'the_user' in globals() and the_user:  # Check if the_user exists and is not empty
        return title.replace(the_user, "the_user").strip()
    return title

def parse_your_posts(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        posts = data.get("your_posts__check_ins__photos_and_videos_1.json", {})
        if isinstance(posts, dict):
            posts = [posts]


        return [{
            'data_type': 'facebook_post',
            'Action': 'Post',
            'title': remove_the_user_from_title(helpers.find_items_bfs(item, "title")) if helpers.find_items_bfs(item, "title") else "Posted",
            'URL': helpers.find_items_bfs(item, "url"),
            'Date': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "timestamp")),
            'details': json.dumps({"post_content": helpers.find_items_bfs(item, "post")})
        } for item in posts]
    
    elif DATA_FORMAT == "html":
        html_content = data.get("your_posts__check_ins__photos_and_videos_1.html", "")
        tree = html.fromstring(html_content)
        items = tree.xpath('//div/div/div')
        posts = []



        for item in items:
            title = item.xpath('./text()')[0]
            title = remove_the_user_from_title(title)  # Apply the function to remove the_user
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


def parse_facebook_account_suggestions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if DATA_FORMAT == "json":
        categories = helpers.find_items_bfs(data, "people_we_think_you_should_follow.json")
        categories = helpers.find_items_bfs(categories, "vec")

        return [{
            'data_type': 'facebook_account_suggestions',
            'Action': 'People You Should Follow',
            'title': category.get("value", ""),
            'URL': '',
            'Date': '',
            'details': json.dumps({})
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
                'data_type': 'facebook_account_suggestions',
                'Action': 'People You Should Follow',
                'title': title,
                'URL': '',
                'Date': '',
                'details': json.dumps(details)
            })
        return categories


def parse_group_posts_and_comments(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    posts = data.get("group_posts_and_comments.json", {}).get("group_posts_v2", [])
    return [{
        'data_type': 'facebook_group_post',
        'Action': 'Group Post',
        'title': remove_the_user_from_title(helpers.find_items_bfs(item, "title")),
        'URL': '',
        'Date': helpers.robust_datetime_parser(helpers.find_items_bfs(item, "timestamp")),
        'details': json.dumps({"post_content": helpers.find_items_bfs(item, "post")})
    } for item in posts]

def parse_your_comments_in_groups(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    comments = data.get("your_comments_in_groups.json", {}).get("group_comments_v2", [])
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


def parse_your_group_membership_activity(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    activities = data.get("your_group_membership_activity.json", {}).get("groups_joined_v2", [])
    return [{
        'data_type': 'facebook_group_membership',
        'Action': 'Group Membership',
        'title': item.get("title", "Group Membership Activity"),
        'URL': '',
        'Date': helpers.robust_datetime_parser(item.get("timestamp", 0)),
        'details': json.dumps({
            "group_name": item.get("data", [{}])[0].get("name", "")
        })
    } for item in activities]



def process_facebook_data(facebook_zip: str) -> List[props.PropsUIPromptConsentFormTable]:
    extracted_data = extract_facebook_data(facebook_zip)
    # Assuming `extracted_data` is a dictionary where keys are the file paths or names.
    filtered_extracted_data = {
        k: v for k, v in extracted_data.items() if not re.match(r'^\d+\.html$', k.split('/')[-1])
    }
    
    # Logging only the filtered keys
    logger.info(f"Extracted data keys: {filtered_extracted_data.keys() if filtered_extracted_data else 'None'}")   
    
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
        parse_group_posts_and_comments,
        parse_your_comments_in_groups,
        parse_your_group_membership_activity
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
        # Convert all datetime objects to timezone-naive
        combined_df['Date'] = combined_df['Date'].dt.tz_convert(None)
        
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
    
    ### this is for all things without dates
    all_data = []
    parsing_functions = [
        parse_subscription_for_no_ads, 
        parse_ad_preferences,
        parse_ads_personalization_consent,
        parse_other_categories_used, 
        parse_advertisers_using_activity,
        parse_facebook_account_suggestions
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
       
            # if 'details' in combined_df.columns:
            #     combined_df = combined_df.drop(columns=['details'])
            # Create a single table with all data
            table_title = props.Translatable({"en": "Facebook Ad Info & Suggestions", "nl": "Facebook Gegevens"})


            # Pass the ungrouped data for the table and grouped data for the chart
            table = props.PropsUIPromptConsentFormTable("Facebook_all_data2", table_title, combined_df)
            tables_to_render.append(table)
            
            logger.info(f"Successfully processed Second {len(combined_df)} total entries from Facebook data")
        else:
            logger.warning("Second Combined DataFrame is empty")
    else:
        logger.warning("Second Combined DataFrame: No data was successfully extracted and parsed")
    
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
