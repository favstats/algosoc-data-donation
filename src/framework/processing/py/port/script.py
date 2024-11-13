import logging
import json
import io
import random
import re
from typing import Optional, Literal
import pandas as pd
from pathlib import Path

import port.api.props as props
import port.helpers as helpers
import port.validate as validate
import port.tiktok as tiktok
import port.facebook as facebook
import port.google as google
import port.instagram as instagram

from port.api.commands import (CommandSystemDonate, CommandUIRender, CommandSystemExit)

LOG_STREAM = io.StringIO()

logging.basicConfig(
    ## todo: enable when submitting
    stream=LOG_STREAM,
    level=logging.DEBUG,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")


def process(session_id):
    global validation

    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platforms = [
        ("Facebook", facebook.process_facebook_data, facebook.validate),
        ("Google", google.process_google_data, google.validate),
        ("Instagram", instagram.process_instagram_data, instagram.validate),
        ("TikTok", tiktok.process_tiktok_data, tiktok.validate)
    ]

    ### SET THE PLATFORM TO BE DONATED:
    # "Google", "Facebook", "Instagram", "TikTok"
    current_platform = "Google"
    # Sort platforms to ensure current_platform is checked first
    platforms = [platform for platform in platforms if platform[0] == current_platform]

    subflows = len(platforms)
    steps = 2
    step_percentage = (100 / subflows) / steps
    progress = 0
    
    while True:
        LOGGER.info("Prompt for file")
        yield donate_logs(f"{session_id}-tracking")
    
        promptFile = prompt_file("application/zip", current_platform)
        file_result = yield render_donation_page("Bestand kiezen", promptFile, progress)
    
        LOGGER.info("Uploaded a file")
        if file_result.__type__ == "PayloadString":
          
              # Perform file size check here
            file_path = Path(file_result.value)  # Get the file path
            file_size_mb = file_path.stat().st_size / (1024 ** 2)  # Convert bytes to MB
            file_size_gb = file_path.stat().st_size / (1024 ** 3)  # Convert bytes to GB

            LOGGER.info(f"Uploaded file size: {file_size_mb} MB")
            if file_size_gb > 1.9:
                LOGGER.warning("File too large; prompt retry_confirmation")
                # Check if it's the final platform or if it's the last successful validatio
                yield donate_logs(f"{session_id}-tracking")
                retry_result = yield render_donation_page("Uw bestand is te groot", render_large_file_message("Social Media"), progress)
    
                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    LOGGER.info("Skipped during retry")
                    yield donate_logs(f"{session_id}-tracking")
                    table_list = None
                    break    
                  
            table_list = None  # Initialize table_list before the loop
            for idx, (platform_name, extraction_fun, validation_fun) in enumerate(platforms):

                LOGGER.info(f"Attempting to process as {platform_name} data")
                validation = validation_fun(file_result.value)
                
                if validation.status_code.id == 0:
                  
                    yield donate_logs(f"{session_id}-tracking")
    
                    table_list = extraction_fun(file_result.value)
                    
                    if table_list and len(table_list) > 0:
                        LOGGER.info(f"Successfully processed as {platform_name} data")
                        break
                else:
                    LOGGER.info(f"Not a valid {platform_name} zip")
 
            ### this part would trigger if the right files are found but parsing fails for some reason
            if (not table_list or len(table_list) == 0) and validation.status_code.id == 0:
                LOGGER.warning("Valid data found but no data inside; creating empty table")
                table_list = [create_empty_table(platform_name)]
                yield donate_logs(f"{session_id}-tracking")   
                break
               
            if table_list and len(table_list) > 0:
                break
            else:
                LOGGER.info("No valid data found; prompt retry_confirmation")
                # Check if it's the final platform or if it's the last successful validation
                LOGGER.error("All platforms failed. Showing file names now.")
                validation = validation_fun(file_result.value)
                
                try: 
                  filtered_paths = [
                      p for p in validation.validated_paths 
                      if re.search(r'\.(json|html|csv)$', p) and not re.search(r'^\d+\.(json|html|csv)$', p.split('/')[-1])
                  ]
                  
                  # Concatenate and log the filtered paths
                  if filtered_paths:
                      LOGGER.debug(f"Paths: {', '.join(filtered_paths[:30])}")
                  else:
                      LOGGER.debug("No valid file paths found")
                except Exception as e:
                  LOGGER.error(f"error {e}")
        
                yield donate_logs(f"{session_id}-tracking")
                retry_result = yield render_donation_page("Onverwacht bestand", retry_confirmation("Social Media"), progress)
    
                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    LOGGER.info("Skipped during retry")
                    yield donate_logs(f"{session_id}-tracking")
                    break
        else:
            LOGGER.info("Skipped file upload")
            yield donate_logs(f"{session_id}-tracking")
            break


    progress += step_percentage

    if table_list and len(table_list) > 0:
        LOGGER.info("Prompt consent")
        yield donate_logs(f"{session_id}-tracking")
        # try:
        #   LOGGER.info(f"Number of rows before: {table_list[0].shape[0]}")
        # except Exception as e:
        #   LOGGER.error(f"error {e}")
        prompt = assemble_tables_into_form(table_list)
        consent_result = yield render_donation_page("Bekijk uw gegevens", prompt, progress)

        if consent_result.__type__ == "PayloadJSON":
            LOGGER.info("Data donated")
            # try:
            #   LOGGER.info(f"Number of rows after: {table_list[0].shape[0]}")
            # except Exception:
            #   pass
            yield donate_logs(f"{session_id}-tracking")
            yield donate(f"{session_id}-{platform_name}-donation", consent_result.value)
        else:
            LOGGER.info("Skipped after reviewing consent")
            yield donate_logs(f"{session_id}-tracking")

    yield exit(0, "Success")
    yield render_end_page()



##################################################################

def assemble_tables_into_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    return props.PropsUIPromptConsentForm(table_list, [])


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def create_empty_table(platform_name: str) -> props.PropsUIPromptConsentFormTable:
    """
    Show something in case no data was extracted
    """
    title = props.Translatable({
       "en": "Er zijn geen gegevens gevonden in uw pakket. Dat is niet erg. U kunt op ‘Ja, doneer’ klikken om het lege pakketje alsnog te doneren. Dan weten we dat dit gelukt is.",
       "nl": "Er zijn geen gegevens gevonden in uw pakket. Dat is niet erg. U kunt op ‘Ja, doneer’ klikken om het lege pakketje alsnog te doneren. Dan weten we dat dit gelukt is."
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table


##########################################
# Functions provided by Eyra did not change

def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_donation_page(platform, body, progress):
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform}))

    footer = props.PropsUIFooter(progress)
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)
  
  

def render_large_file_message(platform):
    text = props.Translatable({
        "en": "Sorry, the file you submitted was too large. \n\n\n Please download this software to reduce the file size of your zip file and submit again:",
        "nl": "We kunnen uw bestand op dit moment niet verwerken. \n\n\n Hier is een oplossing voor. Neem contact op met de helpdesk van Centerdata op het gratis nummer 0800-0231415, of stuur een e-mail naar info@centerpanel.nl. We helpen u dan graag verder."
    })
    link_text = props.Translatable({
        "en": "Click here to download the CleanZIP software",
        "nl": "Stuur een e-mail naar info@centerpanel.nl"
    })
    link_url = "mailto:info@centerpanel.nl"
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue without donating", "nl": "Verder zonder te doneren"})
    # optional_text = props.Translatable({
    #     "en": "(Optional) You can also try to manually remove large files (video, audio, images) from your zip file and repackage it.",
    #     "nl": "(Optioneel) U kunt ook proberen grote bestanden (video, audio, afbeeldingen) handmatig uit uw zip-bestand te verwijderen en het vervolgens opnieuw in te pakken."
    # 
    # })
    return props.PropsUIPromptConfirmWithLink(text, link_text, link_url, ok, cancel)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"We kunnen uw bestand op dit moment niet verwerken. Mogelijk hebt u een ander soort bestand gebruikt. Weet u zeker dat u het juiste bestand hebt gekozen? Ga dan verder. Klik op ‘Probeer opnieuw’ als u een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(extensions, platform):
    if platform == "Google":
        description = props.Translatable(
            {
                "nl": "Kies het Google-bestand dat u in de vorige stappen hebt aangevraagd en opgeslagen op uw computer. Het verwerken van uw bestand kan even duren. Dit is afhankelijk van hoe groot het is. [em]Heb dan even geduld.[/em]\n\n[b]Tip:[/b] de naam van het Google bestand begint met '[b]takeout-[/b]' en staat in de map op uw computer waar u het hebt opgeslagen.",
                "en": "Choose the Google file that you requested and saved on your computer in the previous steps. Processing your file may take a while depending on the size. Please be patient. Tip: The name of the Google file starts with 'takeout-' and can be found in the folder where you saved it."
            }
        )
    elif platform == "Facebook":
        description = props.Translatable(
            {
                "nl": "Kies het Facebook-bestand dat u in de vorige stappen hebt aangevraagd en opgeslagen op uw computer. Het verwerken van uw bestand kan even duren. Dit is afhankelijk van hoe groot het is. [em]Heb dan even geduld.[/em]\n\n[b]Tip:[/b] de naam van het Facebook bestand begint met '[b]facebook-[/b]' en staat in de map op uw computer waar u het hebt opgeslagen.",
                "en": "Choose the Facebook file that you requested and saved on your computer in the previous steps. Processing your file may take a while depending on the size. Please be patient. Tip: The name of the Facebook file starts with 'facebook-' and can be found in the folder where you saved it."
            }
        )
    elif platform == "Instagram":
        description = props.Translatable(
            {
                "nl": "Kies het Instagram-bestand dat u in de vorige stappen hebt aangevraagd en opgeslagen op uw computer. Het verwerken van uw bestand kan even duren. Dit is afhankelijk van hoe groot het is. [em]Heb dan even geduld.[/em]\n\n[b]Tip:[/b] de naam van het Instagram bestand begint met '[b]instagram-[/b]' en staat in de map op uw computer waar u het hebt opgeslagen.",
                "en": "Choose the Instagram file that you requested and saved on your computer in the previous steps. Processing your file may take a while depending on the size. Please be patient. Tip: The name of the Instagram file starts with 'instagram-' and can be found in the folder where you saved it."
            }
        )
    elif platform == "TikTok":
        description = props.Translatable(
            {
                "nl": "Kies het TikTok-bestand dat u in de vorige stappen hebt aangevraagd en opgeslagen op uw computer. Het verwerken van uw bestand kan even duren. Dit is afhankelijk van hoe groot het is. [em]Heb dan even geduld.[/em]\n\n[b]Tip:[/b] de naam van het TikTok bestand begint met '[b]TikTok_Data_[/b]' en staat in de map op uw computer waar u het hebt opgeslagen.",
                "en": "Choose the TikTok file that you requested and saved on your computer in the previous steps. Processing your file may take a while depending on the size. Please be patient. Tip: The name of the TikTok file starts with 'TikTok_Data_' and can be found in the folder where you saved it."
            }
        )
    else:
        # Fallback if platform is not recognized
        description = props.Translatable(
            {
                "nl": "Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Dit kan even duren, afhankelijk van de grootte van uw inzending. Gelieve geduldig te zijn.",
                "en": "Please follow the download instructions and choose the file that you stored on your device. This might take a while depending on the size of your submission, please be patient."
            }
        )

    return props.PropsUIPromptFileInput(description, extensions)



def donate(key, json_string):
    return CommandSystemDonate(key, json_string)

def exit(code, info):
    return CommandSystemExit(code, info)
