import boto3
from botocore.exceptions import ClientError
 
# AWS credentials are automatically sourced from IAM role
s3 = boto3.client('s3')
 
def list_objects(sb):
    try:
        objects = s3.list_objects(Bucket=sb)['Contents']
    except Exception as e:
        print("Error while fetching:", e)
        return None
 
    return objects
 
def copy_objects(ob, sb, db):
    files = []
    try:
        for obj in ob:
            objkey = obj['Key']
            files.append(objkey)
            response = s3.get_object(Bucket=sb, Key=objkey)
            data = response['Body'].read()
            response = s3.put_object(Body=data, Bucket=db, Key=objkey)
            print("file successfully uploaded at {}".format(db))
    except Exception as e:
        print(e)
        return None
 
    return files
 
def delete_objects(sb, files):
    for file in files:
        response = s3.delete_objects(Bucket=sb, Delete={'Objects': [{'Key': file}]})
        print("file {} deleted from {} ".format(file, sb))
 
def main():
    sourcebucket = 'source-s3-step-bucket'
    destinationbucket = 'destination-s3-step-bucket'
    objs = list_objects(sourcebucket)
    if objs is not None:
        files = copy_objects(objs, sourcebucket, destinationbucket)
        if files is not None:
            print("All files successfully copied into Destination bucket")
            delete_objects(sourcebucket, files)
            print("All files Deleted from Source Bucket")
 
if __name__ == '__main__':
    main()