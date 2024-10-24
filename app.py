from flask import Flask, request
import boto3
import os
import json
import time
import threading
import base64

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
ec2 = boto3.client('ec2', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)
image_mapper = dict()

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
        # print(f"Message sent to request queue. MessageId: {response['MessageId']}")
    except Exception as e:
        print(f"Failed to send message to request queue: {str(e)}")
        return "Error submitting image for processing", 500

    imagename = filename.split('.')[0]
    result = poll_for_response(imagename)
    result = result.split(':')[0]
    print("Result:",result)
    return f"{filename}:{result}", 200
    
def poll_for_response(imagename):
    # Poll the response queue for the result
    while True:
        if imagename in image_mapper.keys():
            print("Found data for ",imagename, " in ",image_mapper)
            res = image_mapper[imagename]
            del image_mapper[imagename]
            return res
        
        response = sqs.receive_message(
            QueueUrl=RESPONSE_QUEUE_URL,
            MaxNumberOfMessages=10
        )
        if 'Messages' in response:
            for msg in response['Messages']:
                body = json.loads(msg['Body'])
                print('Body:', body)
                resimage = body['filename'].split('.')[0]
                result = body['result'].split(':')[0]
                print(f"Received result for {resimage}:{result}")
                image_mapper[resimage] = result
                print("Added data for ",resimage, " in ",image_mapper, " as ",result)
                # Delete the message from the queue
                sqs.delete_message(
                    QueueUrl=RESPONSE_QUEUE_URL,
                    ReceiptHandle=msg['ReceiptHandle']
                )
                if resimage == imagename:
                    res = image_mapper[imagename]
                    del image_mapper[imagename]
                    return res

if __name__ == "__main__":
    # Run the autoscaling controller in a separate thread
    # autoscaling_thread = threading.Thread(target=autoscaling_controller)
    # autoscaling_thread.daemon = True
    # autoscaling_thread.start()

    # Run the Flask server on port 8000
    app.run(host="0.0.0.0", port=8000, debug=True)
