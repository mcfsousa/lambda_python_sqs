from aws_lambda_powertools import Tracer
from adapters import sqs_handler

tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    return sqs_handler.update_invoices(event, context)


import json


body1 = {
    "invoice_id": 1,
    "customer_id": 2,
    "invoice_quantity": 10,
    "invoice_unit_price": 1.542348,
    "invoice_comment": "test"
}

body2 = {
    "invoice_id": 2,
    "customer_id": 3,
    "invoice_quantity": 10,
    "invoice_unit_price": 1.542348,
    "invoice_comment": "test"
}

event = {
  "Records": [
    {
      "messageId": "f3a3c25a-f244-44ab-8f54-27e28e261ed9",
      "body": json.dumps(body1),
      "eventSource": "aws:sqs"
    },
    {
      "messageId": "f3a3c25a-f244-44ab-8f54-27e28e261ed8",
      "body": json.dumps(body2),
      "eventSource": "aws:sqs"
    }
  ]
}
lambda_handler(event = event,context=None)