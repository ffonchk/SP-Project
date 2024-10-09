import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


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

def fetch_and_process_data():
    all_products = []
    
    for api_url in api_urls:
        res = requests.get(api_url)
        if res.status_code == 200:
            data = res.json()
            products = data.get('products', [])
            all_products.extend(products)
        else:
            print(f'Error: {res.status_code} for URL: {api_url}')

    # รับ product_names
    product_names = [product['product_name'] for product in all_products]
    print(f'Total products collected: {len(product_names)}')

    # สร้าง DataFrame และจัดการกับข้อมูล
    df = pd.DataFrame(product_names, columns=['product_name'])
    df_split = df['product_name'].str.split(' เล่ม').str[0]
    df_drop = df_split.drop_duplicates().reset_index(drop=True)

    # ส่งคืน DataFrame ที่มีคอลัมน์ 'product_name'
    df_final = pd.DataFrame(df_drop, columns=['product_name'])
    return df_final  # ส่งคืน DataFrame

def categorize_product_names():
    df = fetch_and_process_data()  # เรียกใช้งานฟังก์ชันและเก็บ DataFrame

    # ฟังก์ชัน categorize
    def categorize(product_name):
        first_char = product_name[0]  # เข้าถึงค่าใน product_name
        if first_char.isdigit():
            return 'ตัวเลข'
        elif first_char.isalpha() and first_char.isascii():
            return 'อักษรภาษาอังกฤษ'
        elif '\u0E00' <= first_char <= '\u0E7F':
            return 'อักษรภาษาไทย'
        else:
            return 'อักขระพิเศษ'

    # ใช้ฟังก์ชัน categorize กับคอลัมน์ product_name
    df['category'] = df['product_name'].apply(categorize)

    # สร้างคอลัมน์ใหม่แยกแต่ละหมวดหมู่
    df['ตัวเลข'] = df['product_name'].where(df['category'] == 'ตัวเลข')
    df['อักษรภาษาอังกฤษ'] = df['product_name'].where(df['category'] == 'อักษรภาษาอังกฤษ')
    df['อักษรภาษาไทย'] = df['product_name'].where(df['category'] == 'อักษรภาษาไทย')
    df['อักขระพิเศษ'] = df['product_name'].where(df['category'] == 'อักขระพิเศษ')

    # ลบคอลัมน์ที่ไม่ต้องการและจัดเรียงใหม่
    df_final = df[['ตัวเลข', 'อักษรภาษาอังกฤษ', 'อักษรภาษาไทย', 'อักขระพิเศษ']]
    df_final.to_csv('/home/airflow/data/final_output.csv', index=False)
    


# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 10, 7),
    'email': ['chollakarnk@gmail.com'],  # กำหนดอีเมลสำหรับการแจ้งเตือน
    'email_on_failure': True
}

with DAG(
    'catflow',
    default_args=default_args,
    description='DAG to fetch and categorize product names',
    schedule=timedelta(hours=8),  # ใช้สตริงเพื่อความชัดเจน
    catchup=False,
    tags=['example'],  # ใช้แท็กเพื่อจัดกลุ่ม
) as dag:

    fetch_and_process_task = PythonOperator(
        task_id='fetch_and_process_data_task',
        python_callable=fetch_and_process_data
    )

    categorize_task = PythonOperator(
        task_id='categorize_product_name_task',
        python_callable=categorize_product_names
    )

    fetch_and_process_task >> categorize_task  # กำหนดลำดับการทำงานของ task
