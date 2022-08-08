from aws_lambda_powertools import Tracer
from adapters import sqs_handler

tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    return sqs_handler.update_invoices(event, context)
