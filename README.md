# Prefect SQS Message Pipeline

This project implements a batch pipeline using **Prefect** to fetch messages from an AWS SQS queue, assemble them in order, and output a final message. It demonstrates workflow orchestration for processing queued messages in a reliable and maintainable way.

---

## Installation and Setup

1. **Clone the repository**
2. Set up a Python virtual environment

python3 -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

3. Install dependencies
pip install -r requirements.txt

4. Configure AWS credentials
Ensure you have an AWS profile set up with SQS access:
aws configure --profile dsproject2

5. Run the prefect flow: python sqs_pipeline.py

The flow consists of three tasks:

1. wait_for_messages: waits up to 15 minutes for messages to appear in the SQS queue.

2. fetch_messages: retrieves messages from the queue, deletes them, and saves them in project2messages.json.

3. reassemble_message: sorts the messages by order number and writes the final assembled message to final_message.txt.

For this project, Prefect was chosen for orchestration because it allows clear task definition, automatic logging, and easy management of dependencies. The pipeline is structured to poll the SQS queue, ensuring it only starts processing once messages are available. Tasks are separated into modular units—waiting for messages, fetching messages, and reassembling them—to make debugging and maintenance simpler. Intermediate messages are saved in a JSON file for traceability, while the final assembled message is output as plain text for readability. This design ensures reliability, clarity, and maintainability throughout the workflow.

An example output is:
- <img width="912" height="79" alt="Screenshot 2025-11-11 at 9 54 47 PM" src="https://github.com/user-attachments/assets/a462a229-ce9f-4257-9910-22e11035695c" />


