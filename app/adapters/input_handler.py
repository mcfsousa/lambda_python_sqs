import json
import cachetools.func

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.validation import validate
from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, batch_processor
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from domain.invoicedata import InvoiceData
from domain.lambda_domain import process

logger = Logger(service="InvoiceUpdate")
processor = BatchProcessor(event_type=EventType.SQS)


def update_invoice(record: SQSRecord):
    logger.set_correlation_id(record.message_id)
    
    input_schema = get_schema("adapters/input_schema.json")
    validate(
        event=json.loads(record.body),
        schema=input_schema)
    invoice = parse(
        event=record.body,
        model=InvoiceData)
    process(
        logger,
        invoice
    )


@batch_processor(record_handler=update_invoice, processor=processor)
def update_invoices(event, context):
    return_value = processor.response()
    logger.info(return_value)
    return return_value


@cachetools.func.ttl_cache(maxsize=10240, ttl=3600)
def get_schema(schema_name: str) -> dict:
    with open(schema_name, 'r') as file:
        return json.load(file)
