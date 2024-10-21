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

# EC2 details
AMI_ID = 'ami-05b9307aa795111f9'  # Replace with your AMI ID
INSTANCE_TYPE = 't2.micro'  # Smaller instances to reduce vCPU usage
KEY_NAME = 'your-key-pair'  # Replace with your key pair
MAX_INSTANCES = 20  # Maximum number of App Tier instances
USER_DATA_SCRIPT = '''#!/bin/bash
# Add your user data script here to start app_tier.py on the instance
''' 

# Initialize AWS clients
sqs = boto3.client('sqs', region_name=REGION)
ec2 = boto3.client('ec2', region_name=REGION)
service_quotas = boto3.client('service-quotas', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

running_app_instances = []  # Track running App Tier instances

# Function to launch new EC2 instances (App Tier)
def launch_app_instance():
    try:
        available_vcpus = get_available_vcpu_limit()  # Recheck vCPUs before each launch
        used_vcpus = count_used_vcpus()

        # Check if we have enough available vCPUs before launching the instance
        if used_vcpus < available_vcpus:
            instance = ec2.run_instances(
                ImageId=AMI_ID,
                InstanceType=INSTANCE_TYPE,
                MinCount=1,
                MaxCount=1,
                KeyName=KEY_NAME,
                UserData=USER_DATA_SCRIPT,
                TagSpecifications=[{
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Name', 'Value': 'app-tier-instance'}]
                }]
            )
            instance_id = instance['Instances'][0]['InstanceId']
            print(f"Launched EC2 app instance {instance_id}")
            return instance_id
        else:
            print(f"Insufficient vCPUs available. Used: {used_vcpus}, Available: {available_vcpus}")
            return None
    except Exception as e:
        print(f"Error occurred while launching EC2 instance: {str(e)}")
        return None

# Function to terminate EC2 instances (App Tier)
def terminate_app_instances(instance_ids):
    try:
        ec2.terminate_instances(InstanceIds=instance_ids)
        print(f"Terminated EC2 app instances: {instance_ids}")
        time.sleep(10)  # Add a delay to allow AWS to register the termination
    except Exception as e:
        print(f"Error occurred while terminating EC2 instances: {str(e)}")

# Function to get the size of the request queue
def get_request_queue_size():
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=REQUEST_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        return int(response['Attributes']['ApproximateNumberOfMessages'])
    except Exception as e:
        print(f"Error fetching queue length: {str(e)}")
        return 0

# Function to count running instances
def count_running_instances():
    response = ec2.describe_instances(
        Filters=[{
            'Name': 'instance-state-name',
            'Values': ['pending', 'running']
        }]
    )
    running_instances = 0
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            running_instances += 1
    return running_instances

# Function to check available vCPU limit
def get_available_vcpu_limit():
    try:
        response = service_quotas.get_service_quota(
            ServiceCode='ec2',
            QuotaCode='L-1216C47A'  # Quota for Running On-Demand Standard Instances
        )
        vcpu_limit = response['Quota']['Value']
        return vcpu_limit
    except Exception as e:
        print(f"Error fetching vCPU limit: {str(e)}")
        return 0

# Function to count the currently used vCPUs
def count_used_vcpus():
    try:
        response = ec2.describe_instances(
            Filters=[{
                'Name': 'instance-state-name',
                'Values': ['pending', 'running']
            }]
        )
        vcpus_used = 0
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instance_type = instance['InstanceType']
                # Get vCPU count based on the instance type
                if instance_type == 't2.micro':
                    vcpus_used += 1
                elif instance_type == 't2.nano':
                    vcpus_used += 1
                # Add similar checks for other instance types
        return vcpus_used
    except Exception as e:
        print(f"Error counting used vCPUs: {str(e)}")
        return 0

# Autoscaling logic to scale up or down the App Tier
def autoscaling_controller():
    global counter
    counter = 0
    while True:
        request_queue_size = get_request_queue_size()
        total_running_app_instances = count_running_instances() - 1  # Subtract 1 for Web Tier instance

        # Get vCPU limits and usage
        available_vcpu_limit = get_available_vcpu_limit()
        used_vcpus = count_used_vcpus()

        remaining_vcpus = available_vcpu_limit - used_vcpus
        if remaining_vcpus <= 0:
            print("No available vCPUs to launch new instances.")
            time.sleep(3)
            continue

        # Scale up if there are more messages than running instances and vCPUs are available
        if request_queue_size > total_running_app_instances and total_running_app_instances < MAX_INSTANCES:
            additional_app_instances_needed = min(remaining_vcpus, request_queue_size - total_running_app_instances)
            for i in range(additional_app_instances_needed):
                instance_id = launch_app_instance()
                if instance_id and instance_id not in running_app_instances:
                    running_app_instances.append(instance_id)

        # Increment counter if queue is empty
        if request_queue_size == 0:
            counter += 1
        else:
            counter = 0

        # Scale down after 5 consecutive empty queue checks
        if counter >= 5 and total_running_app_instances > 0:
            terminate_app_instances(running_app_instances)
            running_app_instances.clear()
            counter = 0

        time.sleep(3)

# Flask route to handle image uploads
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

                    # Once the result is received, return the result to the client
                    return f"{filename}: {result}", 200
        else:
            time.sleep(2)

if __name__ == "___main__":
    # Run the autoscaling controller in a separate thread
    autoscaling_thread = threading.Thread(target=autoscaling_controller)
    autoscaling_thread.daemon = True
    autoscaling_thread.start()

    # Run the Flask server on port 8000
    app.run(host="0.0.0.0", port=8000, debug=True)
