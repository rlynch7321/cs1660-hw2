import boto3
import csv

s3 = boto3.resource('s3', aws_access_key_id='abc',aws_secret_access_key='123')

try:
    s3.create_bucket(Bucket='ral94-bucket-1', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print("Bucket may already exist.")

bucket = s3.Bucket("ral94-bucket-1")
bucket.Acl().put(ACL='public-read')

body = open('exp1.csv', 'rb')

o = s3.Object('ral94-bucket-1', 'test').put(Body=body)

s3.Object('ral94-bucket-1', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id='AKIAUDTQ7BXXALPSDEWB',aws_secret_access_key='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')

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except: #if there is an exception, the table may already exist.   if so...
    table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

with open('experiments.csv', 'r', encoding="utf-8-sig") as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    first_read = True
    for item in csvf:
        if first_read:
            first_read = False
            continue
        #print(item)
        body = open(item[4], 'rb')
        s3.Object('ral94-bucket-1', item[4]).put(Body=body)
        md = s3.Object('ral94-bucket-1', item[4]).Acl().put(ACL='public-read')
        url =" https://s3-us-west-2.amazonaws.com/ral94-bucket-1/"+item[4]
        metadata_item = {
            'PartitionKey': item[0],
            'RowKey': item[1],
            'description': item[3],
            'date': item[2],
            'url':url
        }
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")


response = table.get_item(
    Key={
        'PartitionKey': 'experiment1',
        'RowKey': 'data1'
    }
)
item = response['Item']
print(item)