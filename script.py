#import pandas as pd
from datetime import date
import requests
from bs4 import BeautifulSoup as bs
import os
from supabase import Client, create_client


enseignes = ["darty",
"boulanger",
"but",
"cdiscount",
"conforama",
"electrodepot",
"fnac",
"ikea",
"ldlc"]

trustpilot = ["darty.com",
"boulanger.com",
"but.fr",
"cdiscount.com",
"conforama.fr",
"electrodepot.fr",
"fnac.com",
"ikea.com",
"ldlc.com"]

all_data_for_supabase = []
try:
    for enseigne, url in zip(enseignes, trustpilot):
        page = 0
        enseigne_rows = []
        date_scraped = date.today().isoformat()

        while True:
            
            page += 1
            res = requests.get(f'https://fr.trustpilot.com/review/www.{url}?page={page}')
            soup = bs(res.content, "lxml")

            reviews_container = soup.find('div', attrs={'data-reviews-list-start': 'true'})
            if not reviews_container:
                break
                reviews = reviews_container.find_all('article', attrs={'data-service-review-card-paper': 'true'})

                for review in reviews:
                    author = review.find('span', attrs={'data-consumer-name-typography': 'true'}).text.strip()

                    rating_tag = review.select_one('img[alt^="Noté"]')
                    rating = rating_tag['alt'].split(' ')[1] if rating_tag else None
                    if rating is not None:
                        rating = int(rating)

                        title_tag = review.find('h2', attrs={'data-service-review-title-typography': 'true'})
                        title = title_tag.text.strip() if title_tag else None

                        content_tag = review.find('p', attrs={'data-service-review-text-typography': 'true'})
                        content = content_tag.text.strip() if content_tag else None

                        time_tag = review.find('time')
                        date_pub = time_tag['datetime'] if time_tag else None

                        exp_tag = review.find('div', attrs={'data-testid': 'review-badge-date'})
                        date_exp = exp_tag.text.strip() if exp_tag else None

                        date_scraped = datetime.today().strftime('%Y-%m-%d') 

                        new_review = {
                        "author": author,
                        "rating": rating,
                        "title": title,
                        "date_pub": date_pub,
                        "date_exp": date_exp,
                        "content": content,
                        "date_scraped": date_scraped,
                        "company": enseigne
                        }
                        enseigne_rows.append(new_review)
                        all_data_for_supabase.append(new_review)
except Exception as e:
   print(f"Erreur lors du scraping: {e}")

api_url = os.environ.get("SUPABASE_URL")
secret_key = os.environ.get("SUPABASE_KEY")

try:
    supabase_client: Client = create_client(api_url, secret_key)
    if all_data_for_supabase:
        response = (
            supabase_client.from_("Trustpilot_scraping")
            .insert(all_data_for_supabase)
            .execute()
        )
except Exception as e:
   print(f"Erreur en tentant d'insérer les données: {e}")