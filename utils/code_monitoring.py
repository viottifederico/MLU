# FUNCTIONS/CLASSES TO MONITOR CODE AND SEND ALARM TO SLACK
import logging
import splunklib.client as client
import splunklib.results as results

SPLUNK_HOST = "your_splunk_host"
SPLUNK_PORT = 8089
SPLUNK_USERNAME = "your_splunk_username"
SPLUNK_PASSWORD = "your_splunk_password"

def configure_logging():
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger().setLevel(logging.ERROR)

def send_to_splunk(message, severity="error"):
    service = client.connect(
        host=SPLUNK_HOST,
        port=SPLUNK_PORT,
        username=SPLUNK_USERNAME,
        password=SPLUNK_PASSWORD,
    )

    logging.error(message)  # Log the error locally

    # Send the error message to Splunk
    event = {
        "event": message,
        "severity": severity,
    }

    index = "your_splunk_index"
    service.indexes[index].submit(event)
    logging.info(f"Error sent to Splunk with severity: {severity}")
