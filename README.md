# โปรเจคกลุ่ม วิชา  265472-Special Topics in Data Science 

 

## กลุ่มที่ 1 : Power Ranger 4 หน่อ
## Git repo.: https://github.com/ffonchk/MidTerm-Project.git

## สมาชิกกลุ่ม 

หัวหน้าทีม : 	นางสาว ชลกานต์ เกตุเพ็ชร 		64311256 

สมาชิก #1: 	นาย จิตติพัฒน์ แก่นจันทร์ 		64310822 

สมาชิก #2:	นางสาว ธนัชพร มระกูล 		64312260 

สมาชิก #3:	นางสาว ธรรมยา เครือเขื่อนเพ็ชร 	64312369 

 

## ปัญหาที่สนใจ 
 หน้าเว็บไซต์จัดเรียงหนังสือไม่เป็นระเบียบ ทำให้เมื่อเลื่อนดูจะรู้สึกไม่สบายตา เนื่องจากหนังสือถูกจัดเรียงแบบสุ่ม

## ให้เขียนอธิบายเกี่ยวกับปัญหาที่สนใจ แหล่งข้อมูลของปัญหา และวิธีการเก็บข้อมูลเพื่อตอบปัญหา 
 ### ปัญหาที่เราสนใจ
 ต้องการจัดเรียงชื่อหนังสือให้เป็นระเบียบ หาได้ง่าย โดยต้องการให้แสดงตามตัวเลข ตัวหนังสือภาษาอังกฤษ ไปจนถึงตัวหน้งสือภาษาไทย 
 ### แหล่งข้อมูล
 https://siamintershop.com/category/bongkoch/838
 ### วิธิเก็บข้อมูล
 ```python
import requests
import json
import pandas as pd

# API ที่ได้จากการ Search (products) ใน Network จนได้ของทุกหน้ามา 9 URL
api_urls = [
    "https://siamintershop.com/api/v1/products/search?filter=%7B%22limit%22:60,%22offset%22:0,%22category_id%22:%22838%22,%22category_with_child%22:true%7D&include=shop_mini,dropship",
    "https://siamintershop.com/api/v1/products/search?filter=%7B%22limit%22:60,%22offset%22:60,%22category_id%22:%22838%22,%22category_with_child%22:true%7D&include=shop_mini,dropship",
    "https://siamintershop.com/api/v1/products/search?filter=%7B%22limit%22:60,%22offset%22:120,%22category_id%22:%22838%22,%22category_with_child%22:true%7D&include=shop_mini,dropship",
    "https://siamintershop.com/api/v1/products/search?filter=%7B%22limit%22:60,%22offset%22:180,%22category_id%22:%22838%22,%22category_with_child%22:true%7D&include=shop_mini,dropship",
    "https://siamintershop.com/api/v1/products/search?filter=%7B%22limit%22:60,%22offset%22:240,%22category_id%22:%22838%22,%22category_with_child%22:true%7D&include=shop_mini,dropship",
    "https://siamintershop.com/api/v1/products/search?filter=%7B%22limit%22:60,%22offset%22:300,%22category_id%22:%22838%22,%22category_with_child%22:true%7D&include=shop_mini,dropship",
    "https://siamintershop.com/api/v1/products/search?filter=%7B%22limit%22:60,%22offset%22:360,%22category_id%22:%22838%22,%22category_with_child%22:true%7D&include=shop_mini,dropship",
    "https://siamintershop.com/api/v1/products/search?filter=%7B%22limit%22:60,%22offset%22:420,%22category_id%22:%22838%22,%22category_with_child%22:true%7D&include=shop_mini,dropship",
    "https://siamintershop.com/api/v1/products/search?filter=%7B%22limit%22:60,%22offset%22:480,%22category_id%22:%22838%22,%22category_with_child%22:true%7D&include=shop_mini,dropship"
]

#สร้างเพื่อเอาไว้รับค่า Products
all_products = []

#เป็นฟังก์ชั่นที่เราต้องสร้างขึ้นเพื่อให้ URL ลูปกัน
for api_url in api_urls:
    res = requests.get(api_url)
    if res.status_code == 200: # ถ้าสเตตัสเป็น 200 จะไปต่อ ถ้าไม่จะแสดงบรรทัด else
          data = res.json()
          products = data.get('products', []) #product มาจากการการค้นหาโดยเข้า Inspace หน้า Elements และหาว่าชื่อหนังสือเก็บในตัวแปรไหน แล้วเราเอาชื่อตัวแปร(products)ไปค้นหา API ของแต่ละหน้า 
          all_products.extend(products)
    else:
        print(f'Error: {res.status_code} for URL: {api_url}')

#เลือกเก็บแต่ชื่อหนังสือมาใช้
product_names = [product['product_name'] for product in all_products] #สร้างฟังก์ชั่น โดยกำหนดตัวแปรด้วย เพื่อจะเอามาใช้

#แปลงเป็น DataFrame เพื่อจะได้ง่ายต่อการนำมาใช้
df = pd.DataFrame(product_names, columns=['product_name'])

#ลบโดยให้เหลือไว้แค่ประโยคก่อนหน้าคำว่าเล่ม  เพราะ เราต้องการแค่ชื่อของหนังสือ
df['product_name'] = df['product_name'].str.split(' เล่ม').str[0]
```
ตัวอย่างข้อมูลที่ได้ก่อนจะนำมาแก้ปัญหา
product_name|
Coffee&Vanilla หนุ่มกาแฟกับสาววานิลลา|
หัวใจในกรงทอง|
คำสาบานของหนูจนมุม -ลวงรักโอเมก้า-|
อลวนรักก๊วนหนุ่มแต่งหญิง|


 
## Project Workflow/Data Pipeline 

## อ้างอิง 

