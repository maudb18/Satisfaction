import re
import scrapy
import os
import sys
from supabase import Client, create_client
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from scrapy.selector import Selector


def _safe_text(el):
    if not el:
        return None
    try:
        t = (el.text or "").strip()
        if t:
            return t
        tc = (el.get_attribute("textContent") or "").strip()
        return tc if tc else None
    except Exception:
        return None


def _first(parent, css):
    try:
        return parent.find_element(By.CSS_SELECTOR, css)
    except Exception:
        return None


def _attr(el, name: str):
    try:
        return el.get_attribute(name)
    except Exception:
        return None


def _parse_author_date(s: str | None):
    if not s:
        return (None, None, None)
    
    # Nettoie les espaces multiples et sauts de ligne
    s = " ".join(s.split())
    
    # Expression régulière mise à jour :
    # 1. (.+?) capture l'auteur
    # 2. (\d{2}/\d{2}/\d{4}) capture la date de publication
    # 3. (?:suite à une expérience du\s+(\d{2}/\d{2}/\d{4}))? capture facultativement la date d'expérience
    pattern = r"Avis publié par\s+(.+?)\s+le\s+(\d{2}/\d{2}/\d{4})(?:\s+suite à une expérience du\s+(\d{2}/\d{2}/\d{4}))?"
    
    m = re.search(pattern, s, flags=re.I)
    if not m:
        return (None, None, None)
        
    author = m.group(1).strip()
    date_fr = m.group(2).strip()
    exp_date = m.group(3).strip() if m.group(3) else None
    
    return (author, date_fr, exp_date)


def _parse_rating(review_el):
    hidden = _first(review_el, 'div.hidden[itemprop="reviewRating"]')
    if not hidden:
        return None

    meta_val = _first(hidden, 'meta[itemprop="ratingValue"]')
    if meta_val:
        v = _attr(meta_val, "content")
        try:
            return int(float(v))
        except Exception:
            return None

    txt = _safe_text(hidden)
    if txt:
        m = re.search(r"(\d+)", txt)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    return None


class PoulpeoAvisSpider(scrapy.Spider):
    name = "poulpeo_avis"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "LOG_LEVEL": "INFO",
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def __init__(self, urls=None, load_more=10, *args, **kwargs):
        """
        load_more:
          - 0 => clique jusqu’à disparition du bouton (avec max_clicks interne)
          - n>0 => clique au max n fois
        """
        super().__init__(*args, **kwargs)
        self.collected_data = []
        self.load_more = int(load_more)

        default_urls = ['https://www.poulpeo.com/avis/darty.htm',
        'https://www.poulpeo.com/avis/boulanger.htm',
        'https://www.poulpeo.com/avis/but.htm',
        'https://www.poulpeo.com/avis/cdiscount.htm',
        'https://www.poulpeo.com/avis/conforama.htm',
        'https://www.poulpeo.com/avis/electrodepot.htm',
        'https://www.poulpeo.com/avis/fnac.htm',
        'https://www.poulpeo.com/avis/ikea.htm',
        'https://www.poulpeo.com/avis/ldlc.htm']

        # On traite la chaîne reçue depuis run.py ou on prend la valeur par défaut
        if urls:
            print("urls received", urls)
            self.start_urls = [u.strip() for u in urls.split(",") if u.strip()]
            print("start_urls", self.start_urls)
        else:
            self.start_urls = default_urls

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")  # enlève si tu veux voir Chrome
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        self.seen = set()

    def parse(self, response):
        self.driver.get(response.url)

        wait = WebDriverWait(self.driver, 25)

        # attendre 1er avis
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.review[itemprop="review"]')))

        max_clicks = self.load_more if self.load_more > 0 else 200  # sécurité
        clicks = 0

        def count_reviews():
            return len(self.driver.find_elements(By.CSS_SELECTOR, 'div.review[itemprop="review"]'))

        # cliquer "Voir plus..." tant que ça ajoute des avis
        while clicks < max_clicks:
            before = count_reviews()

            try:
                btn = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "div.plpLazyDataload-more button.plptable-more"))
                )
            except Exception:
                self.logger.info("Plus de bouton 'Voir plus...' (stop).")
                break

            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                self.driver.execute_script("arguments[0].click();", btn)
                clicks += 1
            except Exception:
                self.logger.info("Bouton trouvé mais click impossible (stop).")
                break

            # attendre que le nombre d’avis augmente (ou timeout => stop)
            try:
                wait.until(lambda d: count_reviews() > before)
            except Exception:
                after = count_reviews()
                self.logger.info(f"Clic #{clicks} mais aucun nouvel avis (before={before}, after={after}) => stop.")
                break

            after = count_reviews()
            self.logger.info(f"Clic #{clicks} OK: avis {before} -> {after}")

        # extraction
        reviews = self.driver.find_elements(By.CSS_SELECTOR, 'div.review[itemprop="review"]')
        self.logger.info(f"Total avis chargés: {len(reviews)}")

        for r in reviews:
            info = _safe_text(_first(r, "div.review-infos"))
            author, date_fr, exp_date = _parse_author_date(info)
            content = _safe_text(_first(r, 'div.review-content[itemprop="reviewBody"]'))
            rating = _parse_rating(r)

            rid = f"{author}|{date_fr}|{content}"
            rid = f"poulpeo:{hash(rid)}"
            if rid in self.seen:
                continue
            self.seen.add(rid)

            regex = r"https://www\.poulpeo\.com/avis/(.*?)\.htm"
            page_url = response.url
            company = Selector(text=page_url).re_first(regex)

            if date_fr is not None:
                jour, mois, annee = date_fr.split("/")
                date_pub = f"{annee}-{mois}-{jour}" + "T00:00:00.000Z"

            if exp_date is not None:
                jour, mois, annee = exp_date.split("/")
                date_exp = f"{annee}-{mois}-{jour}" + "T00:00:00.000Z"
                
            item = {
                "source": "poulpeo",
                "company": company,
                "author": author,
                "date_pub": date_pub,
                "date_exp": date_exp,
                "rating": rating,
                "content": content,
                "title": ""
            }

            self.collected_data.append(item)
            yield item

    def closed(self, reason):
            try:
                if self.driver:
                    self.driver.quit()
            except Exception:
                pass
                
            self.insertion_donnees()

    def insertion_donnees(self):
            if not hasattr(self, 'collected_data') or not self.collected_data:
                self.logger.warning("Aucune donnée n'a été collectée. Impossible de procéder à l'insertion.")
                return

            cleaned_data_for_supabase = []
            seen_reviews = set()

            api_url = os.environ.get("SUPABASE_URL")
            secret_key = os.environ.get("SUPABASE_KEY")

            for review in self.collected_data:
                review_signature = (review.get("author"), review.get("date_pub"), review.get("company"))
                
                if review_signature not in seen_reviews:
                    seen_reviews.add(review_signature)
                    cleaned_data_for_supabase.append(review)

            print("cleaned_data_for_supabase", len(cleaned_data_for_supabase))

            self.logger.info(f"📤 Connexion à Supabase... Préparation de {len(cleaned_data_for_supabase)} avis uniques.")
            
            try:
                supabase_client: Client = create_client(api_url, secret_key)
            except Exception as e:
                self.logger.error(f"❌ Échec de l'initialisation du client Supabase : {e}")
                return

            if cleaned_data_for_supabase:
                try:
                    response = (
                        supabase_client.from_("trustpilot_scraping")
                        .upsert(cleaned_data_for_supabase, on_conflict="author, title, date_pub, company")
                        .execute()
                    )
                    
                    self.logger.info("✅ Données insérées/mises à jour avec succès dans Supabase !")
                    
                except Exception as e:
                    self.logger.error(f"❌ Erreur lors de l'envoi des données vers Supabase : {e}")
            else:
                self.logger.warning("⚠️ Aucun avis unique à insérer.")