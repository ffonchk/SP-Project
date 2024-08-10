import requests

url = "https://siamintershop.com/category/bongkoch/838"

res = requests.get(url)
with open('res.html', 'w') as f:
    f.write(res.text)