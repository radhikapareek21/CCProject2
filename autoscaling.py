import boto3
import time

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
INSTANCE_TYPE = 't2.micro'
MAX_INSTANCES = 15  # Maximum number of App Tier instances
# Add your user data script here to start app_tier.py on the instance


# Initialize AWS clients
sqs = boto3.client('sqs', region_name=REGION)
ec2 = boto3.client('ec2', region_name=REGION)

running_app_instances = []  # Track running App Tier instances

user_data_script = """#!/bin/bash
LOG_FILE="/home/ubuntu/startup.log"

# Start logging
echo "Starting user data script execution..." > $LOG_FILE

# Navigate to the CCProject2AppTier directory
echo "Navigating to the project directory..." >> $LOG_FILE
cd /home/ubuntu/CCProject2AppTier || {
    echo "Failed to navigate to CCProject2AppTier directory." >> $LOG_FILE
    exit 1
}

# Log current directory
echo "Current directory: $(pwd)" >> $LOG_FILE

# List directory contents
echo "Listing directory contents:" >> $LOG_FILE
ls -lah >> $LOG_FILE

# Activate the virtual environment
echo "Activating the virtual environment..." >> $LOG_FILE
source venv/bin/activate || {
    echo "Failed to activate the virtual environment." >> $LOG_FILE
    exit 1
}

# Run the App_Tier.py script using nohup and send it to the background
echo "Starting the App_Tier.py script..." >> $LOG_FILE
nohup python3 App_Tier.py >> $LOG_FILE 2>&1 &

echo "User data script execution completed." >> $LOG_FILE


"""


# user_data_encoded = base64.b64encode(user_data_script.encode('utf-8')).decode('utf-8')


# Function to launch new EC2 instances (App Tier)
def launch_app_instance():
    try:
        instance = ec2.run_instances(
            ImageId=AMI_ID,
            InstanceType=INSTANCE_TYPE,
            MinCount=1,
            MaxCount=1,
            KeyName = 'RadhikaKeyPair',
            UserData=user_data_script,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': 'app-tier-instance'}]
            }],
            IamInstanceProfile={
                 'Name': 'EC2-S3Access-Role'  # The name of your IAM role's instance profile
             }
        )
        instance_id = instance['Instances'][0]['InstanceId']
        print(f"Launched EC2 app instance {instance_id}")
        return instance_id
    except Exception as e:
        print(f"Error occurred while launching EC2 instance: {str(e)}")
        return None

# Function to terminate EC2 instances (App Tier)
def terminate_app_instances(instance_ids):
    try:
        ec2.terminate_instances(InstanceIds=instance_ids)
        print(f"Terminated EC2 app instances: {instance_ids}")
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

# Autoscaling logic to scale up or down the App Tier
if __name__ == '__main__':
    global counter
    counter = 0
    while True:
        request_queue_size = get_request_queue_size()
        total_running_app_instances = count_running_instances() - 1  # Subtract 1 for Web Tier instance
        print("Request queue size " + str(request_queue_size) + " total running instances " + str(total_running_app_instances))
        # Scale up if there are more messages than running instances
        if request_queue_size > total_running_app_instances and total_running_app_instances < MAX_INSTANCES:
            additional_app_instances_needed = min(MAX_INSTANCES - total_running_app_instances, request_queue_size - total_running_app_instances)
            print("Taking min of " + str(MAX_INSTANCES - total_running_app_instances) + " and "+ str(request_queue_size - total_running_app_instances))
            print("Additional instances needed " + str(additional_app_instances_needed))
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
        if counter >= 20 and total_running_app_instances > 0:
            terminate_app_instances(running_app_instances)
            running_app_instances.clear()
            counter = 0

        time.sleep(1)