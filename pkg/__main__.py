from .utils import log
from .main import ClickstreamLoad

# Main entry point to use case pipeline
try:
    log.info("Executing pipeline..\n")
    clickstream_etl = ClickstreamLoad()
    clickstream_etl.execute()
    log.info("Pipeline execution completed.\n")

except Exception as e:
    log.exception(f"Error Ocuured: {e}\n")
