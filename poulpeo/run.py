import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

enseignes = ["darty",
"boulanger",
"but",
"cdiscount",
"conforama",
"electro-depot",
"fnac",
"ikea-fr",
"ldlc"]

urls = [f"https://www.poulpeo.com/avis/{enseigne}.htm" for enseigne in enseignes]

url = ",".join(urls)

def run_spider():
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'scrapoulpeo.settings')
    settings = get_project_settings()
    
    process = CrawlerProcess(settings)
    
    spider_args = {
        "urls": url,
        "load_more": 1
    }
    
    process.crawl('poulpeo_avis', **spider_args)
    
    print("🚀 Démarrage du scraping Poulpeo...")
    process.start()
    print("✅ Exécution terminée !")

if __name__ == "__main__":
    run_spider()