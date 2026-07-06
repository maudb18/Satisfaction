import os
import sys
import time
import random
from datetime import datetime
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
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

    print("⏳ Configuration du navigateur pour GitHub Actions...")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    ua = UserAgent(browsers=['chrome'], os=['windows', 'macos'])
    chrome_options.add_argument(f"user-agent={ua.random}")

    try:
        # Utilisation de webdriver-manager pour installer le bon driver automatiquement
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Désactivation du flag webdriver pour la discrétion
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
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
                        rating = int(rating)
                        title_tag = review.find('h2', attrs={'data-service-review-title-typography': 'true'})
                        title = title_tag.text.strip() if title_tag else None
                        content_tag = review.find('p', attrs={'data-service-review-text-typography': 'true'})
                        content = content_tag.text.strip() if content_tag else None
                        time_tag = review.find('time')
                        date_pub = time_tag['datetime'] if time_tag else None
                        exp_tag = review.find('div', attrs={'data-testid': 'review-badge-date'})
                        date_exp = exp_tag.text.strip() if exp_tag else None

                        mois = {
                            "janvier": "01", "février": "02", "mars": "03", "avril": "04",
                            "mai": "05", "juin": "06", "juillet": "07", "août": "08",
                            "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12"
                        }

                        jour, nom_mois, annee = date_exp.split()
                        num_mois = mois.get(nom_mois.lower())
                        date_exp = f"{annee}-{num_mois}-{jour}"
                        #date_exp = datetime.strptime(f"{jour}-{num_mois}-{annee}", "%d-%m-%Y").date()

                        new_review = {
                            "author": author,
                            "rating": rating,
                            "title": title,
                            "date_pub": date_pub,
                            "date_exp": date_exp,
                            "content": content,
                            "company": enseigne
                        }
                        all_data_for_supabase.append(new_review)
                        enseigne_rows.append(new_review)

            print(f"📈 Total {enseigne} : {len(enseigne_rows)} avis")

    except Exception as e:
        print(f"⚠️ Erreur pendant le scraping : {e}")
        raise
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
                .upsert(cleaned_data_for_supabase, on_conflict="author, title, date_pub, company")
                .execute()
            )
            print("✅ Données insérées avec succès !")
    except Exception as e:
        print(f"❌ Erreur Supabase : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()