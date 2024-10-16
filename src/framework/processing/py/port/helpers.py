from datetime import datetime, timezone
from typing import Any
import warnings
import math
import logging
import re
from collections import deque
from dateutil import parser
from zoneinfo import ZoneInfo
from lxml import html  # Make sure this import is present
from pathlib import Path


from dateutil.parser import parse
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

REGEX_ISO8601_FULL = r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$"
REGEX_ISO8601_DATE = r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])$"


def split_dataframe(df: pd.DataFrame, row_count: int) -> list[pd.DataFrame]:
    """
    Port has trouble putting large tables in memory. 
    Has to be expected. Solution split tables into smaller tables.
    I have tried non-bespoke table soluions they did not perform any better

    I hope you have an idea to make tables faster! Would be nice
    """
    # Calculate the number of splits needed.
    num_splits = int(len(df) / row_count) + (len(df) % row_count > 0)

    # Split the DataFrame into chunks of size row_count.
    df_splits = [df[i*row_count:(i+1)*row_count].reset_index(drop=True) for i in range(num_splits)]

    return df_splits


class CannotConvertEpochTimestamp(Exception):
    """"Raise when epoch timestamp cannot be converted to isoformat"""


def is_timestamp(input_string: str) -> bool:
    """
    Detects if string is a timestamp
    relies on pandas.to_datetime() to detect the time format
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("error")  # temporary behaviour

        try:
            assert isinstance(input_string, str)
            assert input_string != ""
            assert input_string.isdigit() is False

            pd.to_datetime(input_string)

            logger.debug("timestamp FOUND in: '%s'", input_string)
            return True

        except (ValueError, AssertionError) as e:
            logger.debug("Timestamp NOT found in: '%s', %s", input_string, e)
            return False

        except Warning as e:
            logger.warning(
                "WARNING was raised as exception "
                "probably NO timestamp in: "
                "'%s', %s",
                input_string,
                e,
            )
            return False

        except Exception as e:
            logger.error(e)
            return False



def is_isoformat(
    datetime_str: list[str] | list[int], check_minimum: int, date_only: bool = False
) -> bool:
    """
    Check if list like object containing datetime stamps are ISO 8601 strings
    date_only = True, checks if only the date part is ISO 8601
    """

    regex = (
        REGEX_ISO8601_FULL
        if date_only is False
        else REGEX_ISO8601_DATE
    )

    try:
        for i in range(min(len(datetime_str), check_minimum)):
            if isinstance(datetime_str[i], int):
                logger.debug(
                    "Could not detect ISO 8601 timestamp (date_only=%s): %s",
                    date_only,
                    datetime_str[i],
                )
                return False

            if re.fullmatch(regex, datetime_str[i]) is None:  # type: ignore
                logger.debug(
                    "Could not detect ISO 8601 timestamp (date_only=%s): %s",
                    date_only,
                    datetime_str[i],
                )
                return False

    except Exception as e:
        logger.debug(
            "Could not detect ISO 8601 timestamp (date_only=%s): %s, error: %s",
            date_only,
            datetime_str[i],
            e
        )
        return False

    logger.debug("ISO 8601 timestamp detected (date_only=%s)", date_only)
    return True


def is_epoch(datetime_int: list[int] | list[str], check_minimum: int) -> bool:
    """
    Check if list-like object with ints or str that can be interpreted as ints
    epoch time (unit seconds) fall between the start of year 2000 and the year 2040
    """

    year2000 = 946684800
    year2040 = 2208988800

    try:
        for i in range(min(len(datetime_int), check_minimum)):
            check_time = int(datetime_int[i])
            if not year2000 <= check_time <= year2040:
                logger.debug("Could not detect epoch time timestamp: %s", check_time)
                return False

    except Exception as e:
        logger.debug("Could not detect epoch time timestamp, %s", e)
        return False

    logger.debug("Epoch timestamp detected")
    return True


def epoch_to_iso(epoch_timestamp: str | int) -> str:
    """
    Convert epoch timestamp to an ISO 8601 string. Assumes UTC.
    """

    out = str(epoch_timestamp)
    try:
        epoch_timestamp = int(epoch_timestamp)
        out = datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc).isoformat()
        # disabled print, because this prints every row to console (slow)
        #print(f"TIMESTAMP: {out}")
    except (OverflowError, OSError, ValueError, TypeError) as e:
        logger.error("Could not convert epoch time timestamp, %s", e)

    return out

def dict_denester(
    inp: dict[Any, Any] | list[Any],
    new: dict[Any, Any] | None = None,
    name: str = "",
    run_first: bool = True,
) -> dict[Any, Any]:
    """
    Denest a dict or list, returns a new denested dict
    """

    if run_first:
        new = {}

    if isinstance(inp, dict):
        for k, v in inp.items():
            if isinstance(v, (dict, list)):
                dict_denester(v, new, f"{name}-{str(k)}", run_first=False)
            else:
                newname = f"{name}-{k}"
                new.update({newname[1:]: v})  # type: ignore

    elif isinstance(inp, list):
        for i, item in enumerate(inp):
            dict_denester(item, new, f"{name}-{i}", run_first=False)

    else:
        new.update({name[1:]: inp})  # type: ignore

    return new  # type: ignore


# Function using breadth-first search
def find_items_bfs(d: dict, key_to_match: str, replacement_value: str = '') -> str:
    try:
        
        queue = deque([d])
        
        while queue:
            current = queue.popleft()
            
            if isinstance(current, dict):
                if key_to_match in current:
                    value = current[key_to_match]
                    # Return replacement_value if value is None or empty string
                    return value if value else replacement_value  
                queue.extend(current.values())
            elif isinstance(current, list):
                queue.extend(current)
        
        return replacement_value
    except Exception as e:
        logger.error("bork bork: %s", e)
        return replacement_value
      


def find_items(d: dict[Any, Any],  key_to_match: str) -> str:
    """
    d is a denested dict
    match all keys in d that contain key_to_match

    return the value beloning to that key that is the least nested
    In case of no match return empty string

    example:
    key_to_match = asd

    asd-asd-asd-asd-asd-asd: 1
    asd-asd: 2
    qwe: 3

    returns 2

    This function is needed because your_posts_1.json contains a wide variety of nestedness per post
    """
    out = ""
    pattern = r"{}".format(f"^.*{key_to_match}.*$")
    depth = math.inf

    try:
        for k, v in d.items():
            if re.match(pattern, k):
                depth_current_match = k.count("-")
                if depth_current_match < depth:
                    depth = depth_current_match
                    out = str(v)
    except Exception as e:
        logger.error("bork bork: %s", e)

    return out



def sort_isotimestamp_empty_timestamp_last(timestamp_series: pd.Series) -> pd.Series:
    """
    Can be used as follows:

    df = df.sort_values(by="Date", key=sort_isotimestamp_empty_timestamp_last)
    """

    def convert_timestamp(timestamp):
        out = np.inf
        try:
            if isinstance(timestamp, str) and len(timestamp) > 0:
                dt = datetime.fromisoformat(timestamp)
                out = -dt.timestamp()
        except Exception as e:
            logger.debug("Cannot convert timestamp: %s", e)

        return out

    return timestamp_series.apply(convert_timestamp)



def fix_latin1_string(input: str) -> str:
    """
    Fixes the string encoding by attempting to encode it using the 'latin1' encoding and then decoding it.

    Args:
        input (str): The input string that needs to be fixed.

    Returns:
        str: The fixed string after encoding and decoding, or the original string if an exception occurs.
    """
    try:
        fixed_string = input.encode("latin1").decode()
        return fixed_string
    except Exception:
        return input


def try_to_convert_any_timestamp_to_iso8601(timestamp: str) -> str:
    """
    WARNING 

    Use this function with caution and only as a last resort
    Conversion can go wrong when datetime formats are ambiguous
    When ambiguity occurs it chooses MM/DD instead of DD/MM

    Checkout: dateutil.parsers parse
    """
    timestamp = replace_months(timestamp)
    try:
       timestamp = parse(timestamp, dayfirst=False).isoformat()
    except Exception as e:
        timestamp = ""
    return timestamp


def replace_months(input_string):

    month_mapping = {
        'mrt': 'mar',
        'mei': 'may',
        'okt': 'oct',
    }

    for dutch_month, english_month in month_mapping.items():
        if dutch_month in input_string:
            replaced_string = input_string.replace(dutch_month, english_month, 1)
            return replaced_string

    return input_string


from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dateutil import parser
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

def epoch_to_iso(epoch_timestamp: str | int) -> str:
    """
    Convert epoch timestamp to an ISO 8601 string. Assumes UTC.
    """
    out = str(epoch_timestamp)
    try:
        epoch_timestamp = int(float(epoch_timestamp))
        out = datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError, TypeError) as e:
        logger.error("Could not convert epoch time timestamp, %s", e)
    return out


def log_file_size(file_path):
    file_size = Path(file_path).stat().st_size
    logger.info(f"File is being uploaded. Size: {file_size} bytes.")
    return file_size

def robust_datetime_parser(timestamp: Any) -> str:
    """
    A robust function to convert various timestamp formats to ISO 8601.
    
    Args:
    timestamp (Any): The input timestamp (can be string, int, or float).
    
    Returns:
    str: The timestamp converted to ISO 8601 format, or an empty string if conversion fails.
    """
    # Convert input to string if it's not already
    timestamp = str(timestamp).strip().lower()
    
    if not timestamp:
        # logger.warning("Received an empty or invalid timestamp.")
        return ""

    # Handle Unix timestamps (seconds since epoch)
    if timestamp.isdigit() or (timestamp.replace('.', '', 1).isdigit() and timestamp.count('.') < 2):
        try:
            result = datetime.fromtimestamp(int(float(timestamp)), tz=timezone.utc).isoformat()
            # logger.debug(f"Parsed Unix timestamp: {result}")
            return result
        except ValueError as e:
            # logger.error(f"Failed to parse Unix timestamp: {timestamp} - {e}")
            pass

    # Replace Dutch month abbreviations with English ones
    month_mapping = {
        'jan': 'jan', 'feb': 'feb', 'mrt': 'mar', 'apr': 'apr', 
        'mei': 'may', 'jun': 'jun', 'jul': 'jul', 'aug': 'aug', 
        'sep': 'sep', 'okt': 'oct', 'nov': 'nov', 'dec': 'dec'
    }
    for dutch, english in month_mapping.items():
        timestamp = re.sub(r'\b' + dutch + r'\b', english, timestamp)

    # Try parsing with dateutil
    try:
        dt = parser.parse(timestamp, dayfirst=False, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        result = dt.isoformat()
        # logger.debug(f"Parsed with dateutil: {result}")
        return result
    except Exception as e:
        pass

    # Custom parsing for specific formats
    formats_to_try = [
        "%b %d, %Y, %I:%M %p",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%b %d %Y %H:%M:%S",
        "%b %d %Y",
        "%d %b %Y %H:%M:%S",
        "%d %b %Y",
        "%B %d, %Y %H:%M:%S",
        "%B %d, %Y"
    ]

    for fmt in formats_to_try:
        try:
            dt = datetime.strptime(timestamp, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            result = dt.isoformat()
            # logger.debug(f"Parsed with custom format {fmt}: {result}")
            return result
        except ValueError:
            continue

    # logger.warning(f"Failed to parse timestamp: {timestamp}")
    return ""


# robust_datetime_parser("Jul 24, 2024, 11:54 PM")
# robust_datetime_parser("1721418966")
# try_to_convert_any_timestamp_to_iso8601("1721418966")
# epoch_to_iso("1721418966")
# robust_datetime_parser("jul 19, 2024, 9:32 am")



def html_tables(lxml_html):
    # Parse the HTML content
    tree = html.fromstring(lxml_html)
    
    # Find all tables in the HTML
    tables = tree.xpath('//table')
    
    # List to store DataFrames
    dfs = []
    
    # Loop through each table found
    for table in tables:
        # Extract the rows
        rows = table.xpath('.//tr')
        
        # Extract the headers (if any)
        headers = [header.text_content().strip() for header in rows[0].xpath('.//th')]
        
        # Extract the table rows
        table_data = []
        for row in rows[1:]:
            row_data = [cell.text_content().strip() for cell in row.xpath('.//td')]
            table_data.append(row_data)
        
        # Create the DataFrame
        if headers:
            df = pd.DataFrame(table_data, columns=headers)
        else:
            df = pd.DataFrame(table_data)
        
        # Append the DataFrame to the list
        dfs.append(df)
    
    return dfs
  
  
def str_squish(text: str) -> str:
    """
    Mimics the behavior of str_squish from R, which trims leading and trailing spaces 
    and reduces all multiple spaces to a single space.
    """
    return re.sub(r'\s+', ' ', text).strip()
  
  
def get_json_keys(data, prefix=''):
    """
    Retrieve all unique keys and subkeys from a JSON object, combining them with '__' for each sub-level.
    
    Parameters:
    - data (dict): The JSON object (Python dictionary) to traverse.
    - prefix (str): The prefix to use for nested keys (used in recursive calls).
    
    Returns:
    - keys (set): A set of unique key paths.
    """
    keys = set()
    
    if isinstance(data, dict):
        for key, value in data.items():
            # Skip keys containing 'badges', 'messages', or 'cookie'
            if 'badges' in key.lower() or 'messages' in key.lower() or 'cookie' in key.lower():
                continue
            
            # Form the new prefix by appending the current key with '__'
            new_prefix = f"{prefix}__{key}" if prefix else key
            keys.add(new_prefix)
            
            # Recursively get keys for nested dictionaries
            keys.update(get_json_keys(value, new_prefix))
    elif isinstance(data, list):
        for item in data:
            # For lists, we don't append any index to the prefix, just proceed with the elements
            keys.update(get_json_keys(item, prefix))
    
    return keys

  
  
  
def replace_email(text: str) -> str:
  # Regular expression pattern for matching email addresses
  email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
  
  # Use re.sub to replace all email addresses with 'this_is_an_email'
  replaced_text = re.sub(email_pattern, 'this_is_an_email', text)
  
  return replaced_text
