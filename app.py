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
        time.sleep(5)
    except Exception as e:
        # Log error if sending message fails
        print(f"Failed to send message to request queue: {str(e)}")
        return "Error submitting image for processing", 500

    # Poll the response queue for the result with two checks
    attempts = 0
    while attempts < 2:
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
                    # Wait for 2 seconds before polling again to give time for the message to be deleted
                    time.sleep(2)
                    continue
        else:
            # Increment the number of attempts
            attempts += 1
            print(f"No messages found, attempt {attempts} of 2")
            time.sleep(2)  # Wait before trying again
    
    # If no message is received after two attempts
    return "No result found after polling twice", 404

if __name__ == "__main__":
    # Run the Flask server on port 8000
    app.run(host="0.0.0.0", port=8000,debug=True)
