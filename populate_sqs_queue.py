import requests

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/ehe9vz"

response = requests.post(url) #get response

if response.status_code != 200: # check for errors
    print(f"Error: Received status code {response.status_code}")
    print(response.text)
    exit(1)

payload = response.json() # returns SQS URL
print("API response:", payload)

sqs_url = payload.get("sqs_url") # save URL
print("URL: ", sqs_url)
