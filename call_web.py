import requests

url = "https://siamintershop.com/"

res = requests.get(url)
with open('res.html', 'w') as f:
    f.write(res.text)