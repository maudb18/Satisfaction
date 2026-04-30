#import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup as bs
import os
import sys
from supabase import Client, create_client
import asyncio
import json
from playwright.async_api import async_playwright
import playwright_stealth

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
async def main():
    global all_data_for_supabase
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page_browser = await context.new_page()
        
        try:
            await playwright_stealth.stealth_async(page_browser)
        except:
            pass

        try:
            for enseigne, url in zip(enseignes, trustpilot):
                page = 0
                enseigne_rows = []

                while True:
                    page += 1
                    target_url = f'https://fr.trustpilot.com/review/www.{url}?page={page}'
                    
                    # 1. Navigation et récupération immédiate du statut
                    response = await page_browser.goto(target_url, wait_until="domcontentloaded")
                    status_code = response.status # On définit la variable ICI
                    
                    print(f"{enseigne}: Page {page} - Status: {status_code}")
                    
                    # 2. Vérification du statut AVANT de continuer
                    if status_code == 403:
                        print(f"Bloqué par Cloudflare sur {enseigne}. Arrêt critique.")
                        await browser.close()
                        sys.exit(1)
                        
                    if status_code != 200:
                        print(f"Fin ou erreur {status_code} pour {enseigne}. Suivant.")
                        break
                    
                    # 3. Attente du rendu et parsing
                    await asyncio.sleep(2) 
                    content_html = await page_browser.content()
                    soup = bs(content_html, "lxml")

                    reviews_container = soup.find('div', attrs={'data-reviews-list-start': 'true'})
                    # Si Trustpilot renvoie une page 200 mais vide (cas rare mais possible)
                    if not reviews_container:
                        break

                    reviews = reviews_container.find_all('article', attrs={'data-service-review-card-paper': 'true'})
                    if not reviews:
                        break

                    for review in reviews:
                        # --- TON PARSING (Inchangé et correct) ---
                        author_tag = review.find('span', attrs={'data-consumer-name-typography': 'true'})
                        author = author_tag.text.strip() if author_tag else "Anonyme"

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

                            new_review = {
                                "author": author,
                                "rating": rating,
                                "title": title,
                                "date_pub": date_pub,
                                "date_exp": date_exp,
                                "content": content,
                                "company": enseigne
                            }
                            enseigne_rows.append(new_review)
                            all_data_for_supabase.append(new_review)
                    
                    print(f"{enseigne}: {len(enseigne_rows)} avis récupérés")

        except Exception as e:
            print(f"Erreur lors du scraping: {e}")
            await browser.close()
            sys.exit(1)

        await browser.close()
        print(f"Total récupéré: {len(all_data_for_supabase)}")

        api_url = os.environ.get("SUPABASE_URL")
        secret_key = os.environ.get("SUPABASE_KEY")

        try:
            print("Supabase connection in progress")
            supabase_client: Client = create_client(api_url, secret_key)
            if all_data_for_supabase:
                response = (
                    supabase_client.from_("trustpilot_scraping")
                    .upsert(all_data_for_supabase, on_conflict="author, rating, title, date_pub, company")
                    .execute()
                )
        except Exception as e:
            print(f"Erreur en tentant d'insérer les données: {e}")
            sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())