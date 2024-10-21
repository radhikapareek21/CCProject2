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
APP_TIER_AMI_ID = 'ami-05297a07b94165351'  # Replace with your AMI ID
APP_TIER_INSTANCE_TYPE = 't2.micro'
MAX_INSTANCES = 20  # Limit the max instances to 20
MIN_INSTANCES = 0

# Function to get the number of messages in the SQS request queue
def get_queue_size():
    response = sqs.get_queue_attributes(
        QueueUrl=REQUEST_QUEUE_URL,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    return int(response['Attributes']['ApproximateNumberOfMessages'])

# Function to get the current number of running instances
def get_running_instance_count():
    instances = list(ec2.instances.filter(
        Filters=[{'Name': 'tag:Name', 'Values': ['app-tier-instance-*']},
                 {'Name': 'instance-state-name', 'Values': ['running']}]
    ))
    return len(instances)

# Function to scale up the App Tier based on queue size
def scale_up(queue_size, current_instance_count):
    # Determine how many more instances we can add based on the vCPU limit
    available_vcpus = 16  # Replace with your actual vCPU limit
    vcpus_per_instance = 1  # t2.micro has 1 vCPU
    max_instances = available_vcpus // vcpus_per_instance
    
    # Calculate the number of instances to add, ensuring we don't exceed the limit
    instances_to_add = min(max_instances - current_instance_count, queue_size - current_instance_count, 5)

    if instances_to_add > 0:
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
        print(f"Scaling up: {instances_to_add} instances added.")
    else:
        print("No additional instances needed or vCPU limit reached.")

# Function to scale down the App Tier based on queue size
def scale_down(queue_size, current_instance_count):
    # Calculate how many instances need to be terminated
    instances_to_terminate = current_instance_count - queue_size

    # Ensure we don't try to terminate more instances than are running
    if instances_to_terminate > 0:
        instances = ec2.instances.filter(
            Filters=[{'Name': 'tag:Name', 'Values': ['app-tier-instance-*']},
                     {'Name': 'instance-state-name', 'Values': ['running']}]
        )

        running_instances = list(instances)  # Fetch the list of running instances
        if len(running_instances) < instances_to_terminate:
            instances_to_terminate = len(running_instances)

        for instance in running_instances[:instances_to_terminate]:
            print(f"Terminating instance {instance.id}")
            instance.terminate()

        print(f"Scaling down: {instances_to_terminate} instances removed, total running: {current_instance_count - instances_to_terminate}.")
    else:
        print("No instances to terminate.")

# Autoscaling controller to monitor queue and adjust App Tier instances
def autoscaling_controller():
    while True:
        queue_size = get_queue_size()
        current_instance_count = get_running_instance_count()
        
        print(f"Queue size: {queue_size}, Current App Tier instances: {current_instance_count}")

        if queue_size > current_instance_count:
            scale_up(queue_size, current_instance_count)
        elif queue_size < current_instance_count:
            scale_down(queue_size, current_instance_count)
        
        # Autoscaling check every 10 seconds
        time.sleep(10)

@app.route("/", methods=["POST"])
def handle_image():
    if 'inputFile' not in request.files:
        return "No file part", 400
    
    file = request.files['inputFile']
    filename = file.filename
    image_data = file.read()

    # Send the image filename and data to the App Tier via SQS
    message = {
        "filename": filename,
        "image_data": image_data.decode('ISO-8859-1')  # Encoding for sending binary data
    }
    sqs.send_message(
        QueueUrl=REQUEST_QUEUE_URL,
        MessageBody=json.dumps(message)
    )

    # Store the image in the S3 input bucket
    s3.put_object(Bucket=INPUT_BUCKET, Key=filename, Body=image_data)

    return f"Image {filename} has been submitted for processing.", 200

@app.route("/result", methods=["GET"])
def get_result():
    filename = request.args.get('filename')

    # Poll the response queue for the result
    while True:
        response = sqs.receive_message(
            QueueUrl=RESPONSE_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5  # Poll for 5 seconds
        )
        if 'Messages' in response:
            for msg in response['Messages']:
                body = json.loads(msg['Body'])
                if body['filename'] == filename:
                    sqs.delete_message(
                        QueueUrl=RESPONSE_QUEUE_URL,
                        ReceiptHandle=msg['ReceiptHandle']
                    )
                    return f"{filename}: {body['result']}", 200
        else:
            # Continue polling until we get a response
            continue

if __name__ == "__main__":
    # Run the autoscaling controller in a separate thread
    autoscaling_thread = threading.Thread(target=autoscaling_controller)
    autoscaling_thread.daemon = True
    autoscaling_thread.start()

    # Run the Flask server on port 8000
    app.run(host="0.0.0.0", port=8000, debug=True)
