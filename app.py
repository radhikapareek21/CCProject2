from flask import Flask, request
import boto3
import os
import json
import time
import threading

# Initialize the Flask application
app = Flask(__name__)

# AWS region and ASU ID
REGION = 'us-east-1'
ASU_ID = '1231545642'

# SQS Queue URLs based on the ASU ID
REQUEST_QUEUE_URL = f'https://sqs.{REGION}.amazonaws.com/147997137167/{ASU_ID}-req-queue'
RESPONSE_QUEUE_URL = f'https://sqs.{REGION}.amazonaws.com/147997137167/{ASU_ID}-resp-queue'

# AWS S3 bucket names based on ASU ID
INPUT_BUCKET = f'{ASU_ID}-in-bucket'
OUTPUT_BUCKET = f'{ASU_ID}-out-bucket'

# Initialize AWS clients
sqs = boto3.client('sqs', region_name=REGION)
ec2 = boto3.resource('ec2', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

# App Tier AMI and Instance configuration
APP_TIER_AMI_ID = 'ami-05b9307aa795111f9'  # Replace with your AMI ID
APP_TIER_INSTANCE_TYPE = 't2.micro'
MAX_INSTANCES = 20
MIN_INSTANCES = 0

# Thresholds for scaling
SCALE_UP_THRESHOLD = 5  # Scale up if more than 5 messages in the queue
SCALE_DOWN_THRESHOLD = 1  # Scale down if fewer than 1 message in the queue

# Function to get the number of messages in the SQS request queue
def get_queue_size():
    response = sqs.get_queue_attributes(
        QueueUrl=REQUEST_QUEUE_URL,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    return int(response['Attributes']['ApproximateNumberOfMessages'])

# Function to scale up the App Tier (launch EC2 instances with sequential names)
'''
def scale_up(current_instance_count):
    if current_instance_count < MAX_INSTANCES:
        # Launch up to 5 instances at a time, but don't exceed MAX_INSTANCES
        instances_to_add = min(MAX_INSTANCES - current_instance_count, 1)

        # Get existing instance numbers
        instances = ec2.instances.filter(
            Filters=[{'Name': 'tag:Name', 'Values': ['app-tier-instance-*']},
                     {'Name': 'instance-state-name', 'Values': ['running']}]
        )
        instance_numbers = [int(instance.tags[0]['Value'].split('-')[-1]) for instance in instances if instance.tags]
        next_instance_number = max(instance_numbers) + 1 if instance_numbers else 1

        for i in range(instances_to_add):
            instance_name = f"app-tier-instance-{next_instance_number + i}"
            ec2.create_instances(
                ImageId=APP_TIER_AMI_ID,
                InstanceType=APP_TIER_INSTANCE_TYPE,
                MinCount=1,
                MaxCount=1,
                TagSpecifications=[{
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Name', 'Value': instance_name}]
                }]
            )
        print(f"Scaling up: {instances_to_add} instances added, named app-tier-instance-{next_instance_number} to app-tier-instance-{next_instance_number + instances_to_add - 1}.")

# Function to scale down the App Tier (terminate EC2 instances)
def scale_down(current_instance_count):
    if current_instance_count > MIN_INSTANCES:
        instances_to_remove = min(current_instance_count - MIN_INSTANCES, 1)  # Terminate up to 5 instances at a time
        instances = ec2.instances.filter(
            Filters=[{'Name': 'tag:Name', 'Values': ['app-tier-instance-*']},
                     {'Name': 'instance-state-name', 'Values': ['running']}]
        )
        for instance in instances.limit(instances_to_remove):
            print(f"Terminating instance {instance.id} ({instance.tags[0]['Value']})")
            instance.terminate()
        print(f"Scaling down: {instances_to_remove} instances removed.")
    else:
        print("No instances to terminate.")


# Autoscaling controller to monitor queue and adjust App Tier instances
def autoscaling_controller():
    while True:
        queue_size = get_queue_size()
        current_instances = list(ec2.instances.filter(
            Filters=[{'Name': 'tag:Name', 'Values': ['app-tier-instance-*']},
                     {'Name': 'instance-state-name', 'Values': ['running']}]
        ))
        current_instance_count = len(current_instances)

        print(f"Queue size: {queue_size}, Current App Tier instances: {current_instance_count}")

        if queue_size > SCALE_UP_THRESHOLD:
            scale_up(current_instance_count)
        elif queue_size < SCALE_DOWN_THRESHOLD:
            scale_down(current_instance_count)

        # Autoscaling check every 10 seconds
        time.sleep(10)
        '''

@app.route("/", methods=["POST"])
def handle_image():
    if 'inputFile' not in request.files:
        return "No file part", 400
    
    file = request.files['inputFile']
    filename = file.filename
    image_data = file.read()
    print("Sending to request queue")
    print(filename)
    # Store the image in the S3 input bucket
    s3.put_object(Bucket=INPUT_BUCKET, Key=filename, Body=image_data)

    # Send the image filename and data to the App Tier via SQS
    message = {
        "filename": filename,
        "image_data": image_data.decode('ISO-8859-1')  # Encoding for sending binary data
    }
    try:
        response = sqs.send_message(
            QueueUrl=REQUEST_QUEUE_URL,
            MessageBody=json.dumps(message)
        )
        # Log successful message send
        print(f"Message sent to request queue. MessageId: {response['MessageId']}")
    except Exception as e:
        # Log error if sending message fails
        print(f"Failed to send message to request queue: {str(e)}")
        return "Error submitting image for processing", 500

    # Poll the response queue for the result
    while True:
        response = sqs.receive_message(
            QueueUrl=RESPONSE_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5  # Poll for 5 seconds
        )
        if 'Messages' in response:
            print("Got message from response queue")
            for msg in response['Messages']:
                body = json.loads(msg['Body'])
                print(f"Expected filename: {filename}, Received filename: {body['filename']}")
                print(body)
                if body['filename'] == filename:
                    # Store the result in the S3 output bucket
                    # s3.put_object(Bucket=OUTPUT_BUCKET, Key=filename, Body=body['result'])
                    print(body['result'])
                    # Delete the message from the queue
                    sqs.delete_message(
                        QueueUrl=RESPONSE_QUEUE_URL,
                        ReceiptHandle=msg['ReceiptHandle']
                    )
                    return f"{filename}:{body['result']}", 200
        else:
            # Continue polling until we get a response
            continue

if __name__ == "__main__":
    # Run the autoscaling controller in a separate thread
    # autoscaling_thread = threading.Thread(target=autoscaling_controller)
    # autoscaling_thread.daemon = True
    # autoscaling_thread.start()

    # Run the Flask server on port 8000
    app.run(host="0.0.0.0", port=8000,debug=True)

