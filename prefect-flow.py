import boto3
import json
from prefect import flow, task
from pathlib import Path
import time

session = boto3.Session(profile_name="dsproject2")
sqs = session.client("sqs", region_name="us-east-1")

sqs_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/ehe9vz"
num_messages = 21

@task
def wait_for_messages(url):
    sqs = boto3.client("sqs", region_name="us-east-1")
    waited = 0
    while waited < 900:  # wait up to 15 minutes total
        attributes = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )["Attributes"]

        visible = int(attributes["ApproximateNumberOfMessages"])
        delayed = int(attributes["ApproximateNumberOfMessagesDelayed"])
        total = visible + delayed

        print(f"Visible: {visible}, Delayed: {delayed}, Total: {total}")

        if visible > 0:  # as soon as some are available, start fetching
            print("messages available — starting fetch!")
            return
        
        print("waiting for messages...")
        time.sleep(60)
        waited += 60

    print("timeout reached, continuing anyway...")

@task
def fetch_messages(url):
    sqs = boto3.client("sqs", region_name="us-east-1")
    messages = []

    while len(messages) < 21:  # keep polling until all messages received
        response = sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
            MessageAttributeNames=["All"],
        )

        if "Messages" not in response:
            print(f"Fetched {len(messages)} so far... waiting for more.")
            time.sleep(60)
            continue

        for msg in response["Messages"]:
            attrs = msg["MessageAttributes"]
            order_num = int(attrs["order_no"]["StringValue"])
            word = attrs["word"]["StringValue"]
            messages.append((order_num, word))

            sqs.delete_message(
                QueueUrl=url,
                ReceiptHandle=msg["ReceiptHandle"]
            )

        print(f"number of messages fetched so far: {len(messages)}")

    with open("project2messages.json", "w") as f:
        json.dump(messages, f, indent=2)

    return messages

# sort messages by order number and reassemble the full message
@task
def reassemble_message(messages):
    
    sorted_messages = sorted(messages, key=lambda x: x[0]) # sort messages by order number
    
    # get full message
    full_message = ''
    for _, word in sorted_messages:
        full_message += word + ' '
    full_message = full_message.strip()
    
    # Save to file
    with open("final_message.txt", 'w') as f:
        f.write(full_message)
    
    return full_message

# Prefect flow
@flow
def sqs_pipeline():

    print("Starting SQS Message Processing Pipeline")
    
    wait_for_messages(sqs_queue_url)

    messages = fetch_messages(url=sqs_queue_url)  # fetch messages from SQS
    
    if not messages:
        print("no messages found in queue")
        return
        
    full_message = reassemble_message(messages) # reassemble
    
    print("ipeline completed successfully!")
    
    return full_message


if __name__ == "__main__":
    sqs_pipeline()