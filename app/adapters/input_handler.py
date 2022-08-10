import json
import cachetools.func

import aws_lambda_powertools as PowerToolsLog
import aws_lambda_powertools.utilities.validation as PowerToolsValidation
import aws_lambda_powertools.utilities.parser as PowerToolsParser
import aws_lambda_powertools.utilities.batch as PowerToolsBatch
import aws_lambda_powertools.utilities.data_classes.sqs_event as PowerToolsSqs

import domain.invoicedata as DomainTypes
import domain.lambda_domain as DomainRules

logger = PowerToolsLog.Logger(service="InvoiceUpdate")
processor = PowerToolsBatch.BatchProcessor(event_type=PowerToolsBatch.EventType.SQS)


def update_invoice(record: PowerToolsSqs.SQSRecord):
    logger.set_correlation_id(record.message_id)

    input_schema = get_schema("adapters/input_schema.json")
    PowerToolsValidation.validate(event=json.loads(record.body), schema=input_schema)
    invoice = PowerToolsParser.parse(event=record.body, model=DomainTypes.InvoiceData)
    DomainRules.process(logger, invoice)


@PowerToolsBatch.batch_processor(record_handler=update_invoice, processor=processor)
def update_invoices(event, context):
    return_value = processor.response()
    logger.info(return_value)
    return return_value


@cachetools.func.ttl_cache(maxsize=10240, ttl=3600)
def get_schema(schema_name: str) -> dict:
    with open(schema_name, "r") as file:
        return json.load(file)
