
"""
DDP Twitter module

This module contains functions to handle *.js files contained within a twitter ddp
"""

from pathlib import Path
from typing import Any
import logging
import zipfile
import json
import io
import re

import pandas as pd

import port.unzipddp as unzipddp
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
        id="js_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "manifest.js",
            "account-creation-ip.js",
            "account-label.js",
            "account-suspension.js",
            "account-timezone.js",
            "account.js",
            "ad-engagements.js",
            "ad-free-article-visits.js",
            "ad-impressions.js",
            "ad-mobile-conversions-attributed.js",
            "ad-mobile-conversions-unattributed.js",
            "ad-online-conversions-attributed.js",
            "ad-online-conversions-unattributed.js",
            "ageinfo.js",
            "app.js",
            "birdwatch-note-rating.js",
            "birdwatch-note-tombstone.js",
            "birdwatch-note.js",
            "block.js",
            "branch-links.js",
            "catalog-item.js",
            "commerce-catalog.js",
            "community-tweet.js",
            "connected-application.js",
            "contact.js",
            "deleted-tweet.js",
            "device-token.js",
            "direct-message-group-headers.js",
            "direct-message-headers.js",
            "direct-message-mute.js",
            "direct-messages-group.js",
            "direct-messages.js",
            "email-address-change.js",
            "follower.js",
            "following.js",
            "ip-audit.js",
            "like.js",
            "lists-created.js",
            "lists-member.js",
            "lists-subscribed.js",
            "moment.js",
            "mute.js",
            "ni-devices.js",
            "periscope-account-information.js",
            "periscope-ban-information.js",
            "periscope-broadcast-metadata.js",
            "periscope-comments-made-by-user.js",
            "periscope-expired-broadcasts.js",
            "periscope-followers.js",
            "periscope-profile-description.js",
            "personalization.js",
            "phone-number.js",
            "product-drop.js",
            "product-set.js",
            "professional-data.js",
            "profile.js",
            "protected-history.js",
            "reply-prompt.js",
            "saved-search.js",
            "screen-name-change.js",
            "shop-module.js",
            "shopify-account.js",
            "smartblock.js",
            "spaces-metadata.js",
            "sso.js",
            "tweet.js",
            "tweetdeck.js",
            "twitter-circle-member.js",
            "twitter-circle-tweet.js",
            "twitter-circle.js",
            "twitter-shop.js",
            "user-link-clicks.js",
            "verified.js",
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message=""),
    StatusCode(id=1, description="Not valid DDP", message=""),
    StatusCode(id=2, description="Bad zipfile", message=""),
]


def validate(zfile: Path) -> ValidateInput:
    """
    Validates the input of a Youtube zipfile
    """

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".js"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        if validation.ddp_category.id is None:
            validation.set_status_code(1)
        else:
            validation.set_status_code(0)

    except zipfile.BadZipFile:
        validation.set_status_code(2)

    return validation


def bytesio_to_listdict(bytes_to_read: io.BytesIO) -> list[dict[Any, Any]]:
    """
    Converts a io.BytesIO buffer containing a twitter.js file, to a list of dicts

    A list of dicts is the current structure of twitter.js files
    """

    out = []
    lines = []

    try:
        with io.TextIOWrapper(bytes_to_read, encoding="utf8") as f:
            lines = f.readlines()

        # change first line so its a valid json
        lines[0] = re.sub("^.*? = ", "", lines[0])

        # convert to a list of dicts
        out = json.loads("".join(lines))

    except json.decoder.JSONDecodeError as e:
        logger.error("The input buffer did not contain a valid JSON: %s", e)
    except IndexError as e:
        logger.error("No lines were read, could be empty input buffer: %s", e)
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    finally:
        return out



def following_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    following.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "following.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("following", {}).get("userLink", None)
            ))
        out = pd.DataFrame(datapoints, columns=["Link to user"])
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out



def like_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    like.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "like.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("like", {}).get("tweetId", None),
                item.get("like", {}).get("fullText", None)
            ))
        out = pd.DataFrame(datapoints, columns=["Tweet Id", "Tweet"])
        out["Tweet Id"] = "https://twitter.com/a/status/" + out["Tweet Id"]
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def tweets_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    tweets.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "tweets.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("tweet", {}).get("created_at", None),
                item.get("tweet", {}).get("full_text", None),
                str(item.get("tweet", {}).get("retweeted", ""))
            ))
        out = pd.DataFrame(datapoints, columns=["Date", "Tweet", "Retweeted"])
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def user_link_clicks_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    user-link-clicks.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "user-link-clicks.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("userInteractionsData", {}).get("linkClick", {}).get("timeStampOfInteraction", None),
                item.get("userInteractionsData", {}).get("linkClick", {}).get("finalUrl", None),
                item.get("userInteractionsData", {}).get("linkClick", {}).get("tweetId", None),
            ))
        out = pd.DataFrame(datapoints, columns=["Date", "Url", "Tweet Id"])
        out["Tweet Id"] = "https://twitter.com/a/status/" + out["Tweet Id"]
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def block_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    block.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "block.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("blocking", {}).get("userLink", "")
            ))
        out = pd.DataFrame(datapoints, columns=["Blocked users"])
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def mute_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    mute.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "mute.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("muting", {}).get("userLink", "")
            ))
        out = pd.DataFrame(datapoints, columns=["Muted users"])
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out
