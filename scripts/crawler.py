import csv
import json
import os
import time
import requests

# API endpoints for Tiki.vn
PRODUCTS_CATEGORY_ENDPOINT = "https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&sort=top_seller&page={}&category={}"
PRODUCT_DETAIL_ENDPOINT = "https://tiki.vn/api/v2/products/{}"
REVIEWS_ENDPOINT = (
    "https://tiki.vn/api/v2/reviews?product_id={}&include=comments&page={}"
)

# Category IDs for Tiki.vn
CATEGORY_ID_MAP = {
    "sach-van-hoc": 839,
    "sach-kinh-te": 846,
    "sach-ky-nang-song": 870,
    "nuoi-day-con": 2527,
    "sach-kien-thuc-tong-hop": 873,
    "lich-su-dia-ly": 880,
}

# Headers for requests
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    "AppleWebKit/537.36 (KHTML, like Gecko)"
    "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://tiki.vn/",
    "Origin": "https://tiki.vn",
}


def ensure_dir(path):
    """Ensure the directory exists."""
    if not os.path.exists(path):
        os.makedirs(path)


def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_products_per_category(category_id):
    page = 1
    products = []
    while True:
        url = PRODUCTS_CATEGORY_ENDPOINT.format(page, category_id)
        for _ in range(3):
            try:
                response = requests.get(url, headers=headers)
                break
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}. Retrying...")
                time.sleep(3)
        time.sleep(1)
        if response.status_code == 200:
            data = response.json().get("data", [])
            last_page = response.json().get("paging", {}).get("last_page", 0)
            if not data:
                break

            products.extend(data)

            if page >= last_page or page > 3:
                break
            page += 1
        else:
            print(f"Failed to fetch products: {response.status_code}")
            break

    return products


def get_reviews_per_product(product_id):
    page = 1
    reviews = []

    while True:
        url = REVIEWS_ENDPOINT.format(product_id, page)
        for _ in range(3):
            try:
                response = requests.get(url, headers=headers)
                break
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}. Retrying...")
                time.sleep(3)
        time.sleep(1)
        if response.status_code == 200:
            data = response.json().get("data", [])
            last_page = response.json().get("paging", {}).get("last_page", 0)
            if not data:
                break
            reviews.extend(data)
            if page >= last_page:
                break
            page += 1
        else:
            print(f"Failed to fetch reviews: {response.status_code}")
            break

    return reviews


if __name__ == "__main__":
    for category_name, category_id in CATEGORY_ID_MAP.items():
        print(f">>> Crawling category: {category_name}")
        products = get_products_per_category(category_id)

        product_dir = f"data/raw/products/{category_name}"
        review_dir = f"data/raw/reviews/{category_name}"
        ensure_dir(product_dir)
        ensure_dir(review_dir)

        for product in products:
            product_id = product["id"]

            # Save product detail
            product_detail_url = PRODUCT_DETAIL_ENDPOINT.format(product_id)
            for i in range(3):
                try:
                    product_response = requests.get(product_detail_url, headers=headers)
                    break
                except requests.exceptions.RequestException as e:
                    print(f"Request failed: {e}. Retrying...")
                    time.sleep(3)
            if product_response.status_code == 200:
                product_data = product_response.json()
                if os.path.exists(f"{product_dir}/{product_id}.json"):
                    print(f"Product {product_id} already exists. Skipping...")
                    continue
                save_json(product_data, f"{product_dir}/{product_id}.json")

                # Save reviews
                reviews = get_reviews_per_product(product_id)
                save_json(reviews, f"{review_dir}/{product_id}.json")
            else:
                print(f"Failed to fetch product detail: {product_response.status_code}")
