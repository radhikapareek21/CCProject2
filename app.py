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

# EC2 instance details
APP_TIER_AMI_ID = 'ami-05b9307aa795111f9'  # Replace with your AMI ID
APP_TIER_INSTANCE_TYPE = 't2.micro'
MAX_INSTANCES = 20

# Scaling thresholds
SCALE_UP_THRESHOLD = 1  # Scale up if there is at least 1 message in the queue
SCALE_DOWN_THRESHOLD = 0  # Scale down if there are no messages

# Function to get the number of messages in the SQS request queue
def get_queue_size():
    response = sqs.get_queue_attributes(
        QueueUrl=REQUEST_QUEUE_URL,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    return int(response['Attributes'].get('ApproximateNumberOfMessages', 0))

# Function to scale up the App Tier (launch EC2 instances)
def scale_up(current_instance_count):
    if current_instance_count < MAX_INSTANCES:
        instances_to_add = min(MAX_INSTANCES - current_instance_count, 1)
        
        # Launch new instances
        for i in range(instances_to_add):
            instance_name = f"app-tier-instance-{current_instance_count + i + 1}"
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
        print(f"Scaling up: {instances_to_add} instance(s) added.")

# Function to scale down the App Tier (terminate EC2 instances)
def scale_down(current_instance_count):
    if current_instance_count > 0:
        instances_to_remove = current_instance_count  # Scale down all instances when no messages
        instances = ec2.instances.filter(
            Filters=[{'Name': 'tag:Name', 'Values': ['app-tier-instance-*']},
                     {'Name': 'instance-state-name', 'Values': ['running']}]
        )
        for instance in instances.limit(instances_to_remove):
            instance.terminate()
            print(f"Terminating instance {instance.id}")
        print(f"Scaling down: {instances_to_remove} instance(s) removed.")
    else:
        print("No instances to terminate.")

# Autoscaling controller to monitor queue and adjust App Tier instances
def autoscaling_controller():
    while True:
        # Get the current message count in the SQS queue
        queue_size = get_queue_size()
        current_instances = list(ec2.instances.filter(
            Filters=[{'Name': 'tag:Name', 'Values': ['app-tier-instance-*']},
                     {'Name': 'instance-state-name', 'Values': ['running']}]
        ))
        current_instance_count = len(current_instances)

        print(f"Queue size: {queue_size}, Current App Tier instances: {current_instance_count}")

        # SCALE UP if there are messages in the queue and we have not reached the maximum number of instances
        if queue_size >= SCALE_UP_THRESHOLD:
            scale_up(current_instance_count)
        
        # SCALE DOWN if there are no messages in the queue and there are running instances
        elif queue_size == SCALE_DOWN_THRESHOLD:
            scale_down(current_instance_count)

        # Autoscaling check every 10 seconds
        time.sleep(10)

@app.route("/", methods=["POST"])
def handle_image():
    if 'inputFile' not in request.files:
        return "No file part", 400
    
    file = request.files['inputFile']
    filename = file.filename
    image_data = file.read()

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
        print(f"Message sent to request queue. MessageId: {response['MessageId']}")
    except Exception as e:
        print(f"Failed to send message to request queue: {str(e)}")
        return "Error submitting image for processing", 500

    # Poll the response queue for the result
    while True:
        response = sqs.receive_message(
            QueueUrl=RESPONSE_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )
        if 'Messages' in response:
            for msg in response['Messages']:
                body = json.loads(msg['Body'])
                if body['filename'] == filename:
                    # Process the result from the App Tier
                    result = body['result']
                    print(f"Received result for {filename}: {result}")
                    
                    # Delete the message from the queue
                    sqs.delete_message(
                        QueueUrl=RESPONSE_QUEUE_URL,
                        ReceiptHandle=msg['ReceiptHandle']
                    )
                    return f"{filename}: {result}", 200
        else:
            time.sleep(2)

if __name__ == "__main__":
    # Run the autoscaling controller in a separate thread
    autoscaling_thread = threading.Thread(target=autoscaling_controller)
    autoscaling_thread.daemon = True
    autoscaling_thread.start()

    # Run the Flask server on port 8000
    app.run(host="0.0.0.0", port=8000, debug=True)
