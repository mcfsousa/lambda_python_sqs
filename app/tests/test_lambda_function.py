import json
import unittest
import boto3
import moto
import lambda_function
import aws_lambda_powertools.utilities.batch as PowerToolsBatch

MESSAGE_ID_1="f3a3c25a-f244-44ab-8f54-27e28e261ed9"
MESSAGE_ID_2="f3a3c25a-f244-44ab-8f54-27e28e261ed8"

class InvoiceTest(unittest.TestCase):

    def create_table(self):
        table_key_schema=[
                {
                    "AttributeName": "invoice_id",
                    "KeyType": "HASH"
                }
            ]
        table_attribute_definitions=[
                {
                    "AttributeName": "invoice_id",
                    "AttributeType": "N"
                }
            ]
        boto3.setup_default_session()
        client = boto3.client("dynamodb", region_name='sa-east-1')
        client.create_table(
            TableName="invoice",
            KeySchema=table_key_schema,
            AttributeDefinitions=table_attribute_definitions,
            BillingMode='PAY_PER_REQUEST'
        )

    def create_payload(self, body1: dict, body2: dict):
        return {
            "Records": [
                {
                    "messageId": MESSAGE_ID_1,
                    "body": json.dumps(body1),
                    "eventSource": "aws:sqs"
                },
                {
                    "messageId": MESSAGE_ID_2,
                    "body": json.dumps(body2),
                    "eventSource": "aws:sqs"
                }
            ]
        }
                
    @moto.mock_dynamodb
    def test_lambda_function_success(self):
        self.create_table()
        
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

        payload=self.create_payload(body1,body2)

        response = lambda_function.lambda_handler(event=payload,context=None)
        assert response == { "batchItemFailures": [] }
        
    @moto.mock_dynamodb
    def test_lambda_function_partial_success(self):
        self.create_table()
        body1 = {
            "invoice_id": "1X",
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

        payload=self.create_payload(body1,body2)
        response = lambda_function.lambda_handler(event=payload,context=None)
        assert response == { "batchItemFailures": [{'itemIdentifier': MESSAGE_ID_1}] }
              
    @moto.mock_dynamodb
    def test_lambda_function_fail(self):
        self.create_table()
        
        body1 = {
            "invoice_id": "1x",
            "customer_id": 2,
            "invoice_quantity": 10,
            "invoice_unit_price": 1.542348,
            "invoice_comment": "test"
        }

        body2 = {
            "invoice_id": "2x",
            "customer_id": 3,
            "invoice_quantity": 10,
            "invoice_unit_price": 1.542348,
            "invoice_comment": "test"
        }
        payload=self.create_payload(body1,body2)
        self.assertRaises(
            PowerToolsBatch.exceptions.BatchProcessingError,
            lambda_function.lambda_handler,
            event=payload,
            context=None)
