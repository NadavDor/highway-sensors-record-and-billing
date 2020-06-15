import boto3
from datetime import datetime

from boto3.dynamodb.conditions import Key

'''
    This script is a basic structure of a simple billing program integrated with Mongo DB.
    * We assume all the used table are already created in the DB.
'''
dynamodb = boto3.resource('dynamodb')

costumers_table = dynamodb.Table('costumers')
sensors_table = dynamodb.Table('sensors')
records_table = dynamodb.Table('records')
billing_table = dynamodb.Table('billing')


def calc_billing(start_time_range: str, end_time_range: str):
    costumers = get_costumers()

    for costumer in costumers:
        license_plates = costumer.get('licensePlates')

        payment = 0
        for license_plate in license_plates:
            # get all the records for that license plate that were given in a specific time range.
            response = records_table.query(
                KeyConditionExpression=Key('licensePlate').eq(license_plate) & Key('time').between(start_time_range,
                                                                                                   end_time_range)
            )
            records = response.get('Items')

            for record in records:
                if not record.get('isBilled'):
                    # mark the record as paid
                    add_to_records_table(record.get('licensePlate'), record.get('sensorId'), record.get('time'), True)

                    # add the cost of the record to the total billing sum.
                    sensor_id = record.get('sensorId')
                    payment += sensors_table.get_item(Key={'sensorId': sensor_id}).get('Item').get('cost')

        if payment > 0:
            add_to_billing_table(costumer.get('costumerId'), costumer.get('billingUrl'), payment)


def deliver_bills():
    bills = billing_table.scan()
    if bills:
        bills = bills.get('Items')

    for bill in bills:
        costumer_id = bill.get('costumerId')
        billing_url = bill.get('billingUrl')
        cost = bill.get('cost')

        '''
            here we can add a function that sends to bill forward
        '''
        print("Billing sent successfully."
              "\nCostumer ID: ", costumer_id,
              "\nBilling Url: ", billing_url,
              "\nCost: ", cost, '$')
        print('--------------------------------')


def get_costumers():
    costumers = costumers_table.scan()
    if costumers:
        return costumers.get('Items')
    return []


def add_to_records_table(license_plate: str, sensor_id: str, time: str, is_billed: bool):
    records_table.put_item(
        Item={
            'licensePlate': license_plate,
            'sensorId': sensor_id,
            'time': time,
            'isBilled': is_billed
        })


def add_to_billing_table(costumer_id: str, billing_url: str, cost: int):
    billing_table.put_item(
        Item={
            'costumerId': costumer_id,
            'billingUrl': billing_url,
            'cost': cost,
            'creationDate': datetime.now().strftime("%d/%m/%y-%H:%M")
        })


def main():
    """
     As an input to the we can give the program a start and end time sta,ps from which it will calculate the billing
     (we hard coded some values in order to demonstrate functionality).
    """

    start_time_range = '0000'
    end_time_range = '9999'

    calc_billing(start_time_range, end_time_range)
    deliver_bills()

    add_to_records_table('2222', '3344', '2345', False)


if __name__ in ('__main__', '__builtin__', 'builtins'):
    main()
