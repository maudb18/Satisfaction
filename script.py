import asyncio
import sys
import time
import random
import os
from bs4 import BeautifulSoup as bs
import google_colab_selenium as gs
from fake_useragent import UserAgent
from supabase import Client, create_client

# --- CONFIGURATION ---
api_url = os.environ.get("SUPABASE_URL")
secret_key = os.environ.get("SUPABASE_KEY")

enseignes = ["darty", "boulanger", "but", "cdiscount", "conforama", "electrodepot", "fnac", "ikea", "ldlc"]
trustpilot = ["darty.com", "boulanger.com", "but.fr", "cdiscount.com", "conforama.fr", "electrodepot.fr", "fnac.com", "ikea.com", "ldlc.com"]

all_data_for_supabase = []

def main():
    global all_data_for_supabase

    ua = UserAgent(browsers=['chrome'], os=['windows', 'macos'])
    random_user_agent = ua.random

    print("⏳ Démarrage du navigateur Colab (Mode Furtif)...")
    try:
        driver = gs.UndetectedChrome()
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": random_user_agent})
    except Exception as e:
        print(f"💥 Impossible de lancer le navigateur : {e}")
        sys.exit(1)

    try:
        for enseigne, url in zip(enseignes, trustpilot):
            page = 0
            enseigne_rows = []

            while True:
                page += 1
                target_url = f'https://fr.trustpilot.com/review/www.{url}?page={page}'

                print(f"🚀 {enseigne} : Chargement Page {page}...")
                driver.get(target_url)

                time.sleep(random.uniform(4.0, 7.0))
                content_html = driver.page_source

                if "Verifying your connection" in content_html or "cf-challenge" in content_html:
                    print(f"❌ Bloqué par Cloudflare pour {enseigne}.")
                    break

                soup = bs(content_html, "lxml")
                reviews_container = soup.find('div', attrs={'data-reviews-list-start': 'true'})
                
                if not reviews_container:
                    print(f"🏁 Fin des pages pour {enseigne}.")
                    break

                reviews = reviews_container.find_all('article', attrs={'data-service-review-card-paper': 'true'})
                if not reviews:
                    break

                for review in reviews:
                    author_tag = review.find('span', attrs={'data-consumer-name-typography': 'true'})
                    author = author_tag.text.strip() if author_tag else "Anonyme"

                    rating_tag = review.select_one('img[alt^="Noté"]')
                    rating = int(rating_tag['alt'].split(' ')[1]) if rating_tag else None

                    if rating is not None:
                        title_tag = review.find('h2', attrs={'data-service-review-title-typography': 'true'})
                        content_tag = review.find('p', attrs={'data-service-review-text-typography': 'true'})
                        time_tag = review.find('time')

                        new_review = {
                            "author": author,
                            "rating": rating,
                            "title": title_tag.text.strip() if title_tag else None,
                            "date_pub": time_tag['datetime'] if time_tag else None,
                            "content": content_tag.text.strip() if content_tag else None,
                            "company": enseigne
                        }
                        all_data_for_supabase.append(new_review)
                        enseigne_rows.append(new_review)

            print(f"📈 Total {enseigne} : {len(enseigne_rows)} avis")

    except Exception as e:
        print(f"⚠️ Erreur pendant le scraping : {e}")
    finally:
        driver.quit()

    # --- NETTOYAGE DES DOUBLONS AVANT INSERTION ---
    seen_reviews = set()
    cleaned_data_for_supabase = []
    for review in all_data_for_supabase:
        review_signature = (review["author"], review["date_pub"], review["company"])
        if review_signature not in seen_reviews:
            seen_reviews.add(review_signature)
            cleaned_data_for_supabase.append(review)

    # --- INSERTION SUPABASE ---
    try:
        print(f"📤 Connexion Supabase... ({len(cleaned_data_for_supabase)} avis uniques)")
        supabase_client: Client = create_client(api_url, secret_key)
        
        if cleaned_data_for_supabase:
            response = (
                supabase_client.from_("trustpilot_scraping")
                .upsert(cleaned_data_for_supabase, on_conflict="author, date_pub, company")
                .execute()
            )
            print("✅ Données insérées avec succès !")
    except Exception as e:
        print(f"❌ Erreur Supabase : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()