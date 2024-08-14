"""
DDP extract Linkedin
"""
from pathlib import Path
import logging
import zipfile
import re
import io

import pandas as pd

import port.unzipddp as unzipddp

from port.validate import (
    DDPCategory,
    Language,
    DDPFiletype,
    ValidateInput,
    StatusCode,
)

logger = logging.getLogger(__name__)


DDP_CATEGORIES = [
    DDPCategory(
        id="csv_en",
        ddp_filetype=DDPFiletype.CSV,
        language=Language.EN,
        known_files=[
            "Ad_Targeting.csv",
            "Endorsement_Given_Info.csv",
            "Member_Follows.csv",
            "Recommendations_Given.csv",
            "Company Follows.csv",
            "Endorsement_Received_Info.csv",
            "messages.csv",
            "Registration.csv",
            "Connections.csv",
            "Inferences_about_you.csv",
            "PhoneNumbers.csv",
            "Rich Media.csv",
            "Contacts.csv",
            "Invitations.csv",
            "Positions.csv",
            "Skills.csv",
            "Education.csv",
            "Profile.csv",
            "Votes.csv",
            "Email Addresses.csv",
            "Learning.csv",
            "Reactions.csv"
        ]
    ),
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message=""),
    StatusCode(id=1, description="Not a valid DDP", message=""),
    StatusCode(id=2, description="Bad zipfile", message=""),
]


def validate(zfile: Path) -> ValidateInput:
    """
    Validates the input of an LinkedIn zipfile

    This function sets a validation object generated with ValidateInput
    This validation object can be read later on to infer possible problems with the zipfile
    """

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".csv"):
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


def company_follows_to_df(linkedin_zip: str) -> pd.DataFrame:
    """
    'Company Follows.csv'
    """
    filename = "Company Follows.csv"

    b = unzipddp.extract_file_from_zip(linkedin_zip, filename)
    df = unzipddp.read_csv_from_bytes_to_df(b)

    return df


def member_follows_to_df(linkedin_zip: str) -> pd.DataFrame:
    """
    'Member_Follows.csv'
    """
    filename = "Member_Follows.csv"
    b = unzipddp.extract_file_from_zip(linkedin_zip, filename)
    df = pd.DataFrame()
    try:
        # remove zero or more any chars (including linebreaks) non greedy up to and including 2 consequetive line breaks
        b = io.BytesIO(re.sub(b'^((?s).)*?\n\n', b'', b.read()))
        df = unzipddp.read_csv_from_bytes_to_df(b)
    except:
        pass

    return df


def connections_to_df(linkedin_zip: str) -> pd.DataFrame:
    """
    'Connections.csv'
    """
    filename = "Connections.csv"
    b = unzipddp.extract_file_from_zip(linkedin_zip, filename)

    # remove zero or more any chars (including linebreaks) non greedy up to and including 2 consequetive line breaks
    b = io.BytesIO(re.sub(b'^((?s).)*?\n\n', b'', b.read()))

    df = unzipddp.read_csv_from_bytes_to_df(b)

    return df


def reactions_to_df(linkedin_zip: str) -> pd.DataFrame:
    """
    'Reactions.csv'
    """
    filename = "Reactions.csv"
    b = unzipddp.extract_file_from_zip(linkedin_zip, filename)
    df = unzipddp.read_csv_from_bytes_to_df(b)

    return df


def ads_clicked_to_df(linkedin_zip: str) -> pd.DataFrame:
    """
    'Ads Clicked.csv'
    """
    filename = "Ads Clicked.csv"
    b = unzipddp.extract_file_from_zip(linkedin_zip, filename)
    df = unzipddp.read_csv_from_bytes_to_df(b)

    return df


def search_queries_to_df(linkedin_zip: str) -> pd.DataFrame:
    """
    'SearchQueries.csv'
    """
    filename = "SearchQueries.csv"
    b = unzipddp.extract_file_from_zip(linkedin_zip, filename)
    df = unzipddp.read_csv_from_bytes_to_df(b)

    return df


def shares_to_df(linkedin_zip: str) -> pd.DataFrame:
    """
    'Shares.csv'
    """
    filename = "Shares.csv"
    b = unzipddp.extract_file_from_zip(linkedin_zip, filename)
    df = unzipddp.read_csv_from_bytes_to_df(b)

    return df


def comments_to_df(linkedin_zip: str) -> pd.DataFrame:
    """
    'Comments.csv'
    """
    filename = "Comments.csv"
    b = unzipddp.extract_file_from_zip(linkedin_zip, filename)
    df = unzipddp.read_csv_from_bytes_to_df(b)

    return df

