import io
import json
import logging
from dataclasses import asdict

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from fastavro import json_reader, parse_schema

from streaming.setup import (
    BQ_TABLE_STREAMING_RAW,
    FIRMS_NRT_SCHEMA,
    SCHEMA_PATH,
    SUBSCRIPTION_PATH,
)
from streaming.subscriber.transformation import DetectionTransformer
from utils.constant import BQ_DATASET_STREAMING, PROJECT_ID

logger = logging.getLogger(__name__)

def load_schema() -> dict:
    with SCHEMA_PATH.open("r", encoding="utf-8") as file:
        return parse_schema(
            json.load(file)
        )

SCHEMA = load_schema()

def decode_message(
    message: bytes
) -> dict:
    
    """
    Decode event Json
    """
    
    buffer = io.StringIO(message.decode("utf-8"))
    
    event = next(json_reader(buffer, SCHEMA))
    logger.info("Received event_id=%s", event["event_id"])
    
    return event
    
def run():
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logger.info("Starting Beam subscriber: %s", SUBSCRIPTION_PATH)
    
    options = PipelineOptions()
    
    standard_opt = options.view_as(StandardOptions)
    standard_opt.streaming = True
    
    transformer = DetectionTransformer()
    
    try:
        with beam.Pipeline(options=options) as pipeline:
            
            messages = (
                pipeline
                
                | "Read from PubSub"
                >> ReadFromPubSub(
                    subscription=SUBSCRIPTION_PATH
                )
                
                | "Decode Avro JSON"
                >> beam.Map(
                    decode_message
                )
            )
            
            transformed = (
                
                messages
                
                | "Transform Detection"
                >> beam.Map(
                    transformer.transform
                )
            )

            (
                transformed
                            
                | "Detection to dict"
                >> beam.Map(
                    asdict
                )
                
                | "Write to BigQuery"
                >> WriteToBigQuery(
                    table=BQ_TABLE_STREAMING_RAW,
                    dataset=BQ_DATASET_STREAMING,
                    project=PROJECT_ID,
                    schema=FIRMS_NRT_SCHEMA,
                    write_disposition=(beam.io.BigQueryDisposition.WRITE_APPEND),
                    create_disposition=(beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                    method=WriteToBigQuery.Method.STREAMING_INSERTS,
                    insert_retry_strategy="RETRY_ON_TRANSIENT_ERROR",
                    batch_size=1000
                )
            )
            
    except KeyboardInterrupt:
        logger.info("Beam subscriber interrupted by user.")

    finally:
        logger.info("Beam subscriber stopped.")
        
if __name__ == "__main__":
    run()