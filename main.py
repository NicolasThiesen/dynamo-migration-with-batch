from email.policy import default
from json import load
from boto3 import client
import click


def handle_conversion(data):
    new_data = []
    for item in range(0,len(data["Items"])):
        new_data.append({"PutRequest":{"Item": data["Items"][item]}})
    return new_data

def handle_batch(data,table_name,region):
    insert_data = {table_name: []}
    dynamodb = client("dynamodb", region)
    for item in range(0,len(data)):
        if item%25 == 0 and item!=0:
            response = dynamodb.batch_write_item(RequestItems=insert_data)
            print(response)
            insert_data[table_name] = []
        else:
            insert_data[table_name].append(data[item])

@click.command()
@click.option("--table-name", help="The table name that you want to insert the data", required=True)
@click.option("--region", help="The region where the table are", default="us-eat-1", show_default=True)
@click.option("--data-file", help="The name of the file where is the data", required=True)
def main(table_name,region,data_file):
    file = open(data_file)
    json_data = load(file)
    data = handle_conversion(json_data, region)
    print("Starting to inserting the items")
    handle_batch(data, table_name)


if __name__ == "__main__":
    main()