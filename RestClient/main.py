import requests
url="http://127.0.0.1:8005/courses"

response = requests.get(url)
print(response)
print(response.text)