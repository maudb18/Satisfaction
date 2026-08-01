import os
import sys
import time
import random
from datetime import datetime
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from supabase import Client, create_client
from dotenv import load_dotenv # new
load_dotenv() # new 

# --- CONFIGURATION ---
api_url = os.environ.get("SUPABASE_URL")
secret_key = os.environ.get("SUPABASE_KEY")

enseignes = [
    "darty",
    "boulanger",
    "but",
    "cdiscount",
    "conforama",
    "electrodepot",
    "fnac",
    "ikea",
    "ldlc"
]

poulpeo = [
    "https://www.poulpeo.com/avis/darty.htm",
    "https://www.poulpeo.com/avis/boulanger.htm",
    "https://www.poulpeo.com/avis/but.htm",
    "https://www.poulpeo.com/avis/cdiscount.htm",
    "https://www.poulpeo.com/avis/conforama.htm",
    "https://www.poulpeo.com/avis/electrodepot.htm",
    "https://www.poulpeo.com/avis/fnac.htm",
    "https://www.poulpeo.com/avis/ikea.htm",
    "https://www.poulpeo.com/avis/ldlc.htm"
]

all_data_for_supabase = []

stats = {
    enseigne: {"tentees": 0, "reussies": 0}  # pour avoir le ratio de pages scrapées réussies/tentées 
    for enseigne in enseignes
}

def main():
    global all_data_for_supabase, stats
    print("Configuration du navigateur pour GitHub Actions...")

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    except Exception as e:
        print(f"Impossible de lancer le navigateur : {e}")
        sys.exit(1)

    try:
        
        for enseigne, url in zip(enseignes, poulpeo):

            enseigne_rows = []

            print(f"\n===== {enseigne.upper()} =====")

            driver.get(url)

            time.sleep(5)

            clicks = 0

            while True:

                try:

                    bouton = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, "button.plptable-more")
                        )
                    )

                    driver.execute_script(
                        "arguments[0].scrollIntoView({block:'center'});",
                        bouton
                    )

                    time.sleep(1)

                    driver.execute_script(
                        "arguments[0].click();",
                        bouton
                    )

                    clicks += 1


                    print(f"Voir plus ({clicks})")

                    time.sleep(random.uniform(2,4))

                except TimeoutException:

                    print("Tous les avis sont chargés.")

                    break

        print("=" * 50)
        print(f"Enseigne : {enseigne}")
        print(f"{clicks} clic(s) sur Voir plus")
        print("=" * 50)

        

        html = driver.page_source

        soup = bs(html, "lxml")

        reviews = soup.select("div.review")
        stats[enseigne]["tentees"] += 1
        if len(reviews) > 0:
            stats[enseigne]["reussies"] += 1

        print(f"{len(reviews)} avis trouvés")


        for review in reviews:
            try:
                  author = review.select_one("span.reviewer").get_text(strip=True)
            except:
                author = "Anonyme"

            try:
                rating = int(
                    review.select_one(
                'meta[itemprop="ratingValue"]'
            )["content"]
        )
            except:
                rating = None

            dates = review.find_all("time")

            date_pub = None
            date_exp = None

            if len(dates) >= 1:
                date_pub = dates[0]["datetime"]

            if len(dates) >= 2:
                date_exp = dates[1]["datetime"]

            content = ""

            contenu = review.select_one("div.review-content")

            if contenu:
                content = contenu.get_text(" ", strip=True)

            new_review = {
                "author": author,

                "rating": rating,

                "title": "Sans titre",

                "date_pub": date_pub,

                "date_exp": date_exp,

                "content": content,

                "company": enseigne,

                "source": "Poulpeo"
            }

            all_data_for_supabase.append(new_review)

            enseigne_rows.append(new_review)

            print(f"Total {enseigne} : {len(enseigne_rows)} avis")

            t, r = stats[enseigne]["tentees"], stats[enseigne]["reussies"]

            taux = round((r / t) * 100, 2) if t > 0 else 0

            print(f" {enseigne} : {r}/{t} pages réussies ({taux}%)")

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
        print(f"Connexion Supabase... ({len(cleaned_data_for_supabase)} avis uniques)")
        supabase_client: Client = create_client(api_url, secret_key)

        if cleaned_data_for_supabase:
            response = (
                supabase_client.from_("scraping_test")
                .upsert(cleaned_data_for_supabase, on_conflict="author, title, date_pub, company")
                .execute()
            )
            print("Données insérées avec succès !")

        # enregistrement des KPI de scraping
        stats_rows = []
        for enseigne, s in stats.items():
            t, r = s["tentees"], s["reussies"]
            taux = round((r / t) * 100, 2) if t > 0 else 0
            stats_rows.append({
                "source": "Poulpeo",
                "attempted_pages": t,
                "success_pages": r,
                "success_rate": taux
                })
        if stats_rows:
            supabase_client.from_("scraping_logs").insert(stats_rows).execute()
            print(" Statistiques de scraping enregistrées !")

    except Exception as e:
        print(f"Erreur Supabase : {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()