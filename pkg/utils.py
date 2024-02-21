import logging
import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from pkg.params import *


def send_slack_notification(channel, message):
    """
    Sends a Slack notification to a specified channel using the provided Slack API token.

    Args:
        token (str): The Slack API token required for authentication.
        channel (str): The ID or name of the channel to send the notification to.
        message (str): The message to be sent in the Slack notification.

    Raises:
        SlackApiError: If there is an error while sending the Slack notification.

    """

    client = WebClient(token=slack_token)

    try:
        response = client.chat_postMessage(channel=channel, text=message)
        success = response.data["ok"]

        if success:
            log.info("Slack notification sent successfully!")
        else:
            log.info("Failed to send Slack notification.")

    except SlackApiError as e:
        logging.error(f"Error sending Slack notification: {e.response['error']}")


def create_logger(log_file_name):
    log = logging.getLogger(log_file_name)
    log.setLevel(logging.INFO)
    file_handler = logging.FileHandler("logs/" + log_file_name)
    # stream_handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s:%(funcName)s:%(message)s")
    file_handler.setFormatter(formatter)

    log.addHandler(file_handler)
    # log.addHandler(stream_handler)

    return log


log = create_logger(
    "main_logs_" + datetime.datetime.now().strftime(r"%Y%m%d_%H%M%S") + " .log"
)
