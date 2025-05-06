import os
import time
import argparse
import logging
import requests
import json
import random
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import urllib.parse

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download_docs_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Fonti di documentazione per ogni linguaggio
DOC_SOURCES = {
    "python": [
        "https://docs.python.org/3/",
        "https://numpy.org/doc/stable/",
        "https://pandas.pydata.org/docs/",
        "https://scikit-learn.org/stable/documentation.html",
        "https://www.tensorflow.org/api_docs",
        "https://pytorch.org/docs/stable/index.html",
        "https://flask.palletsprojects.com/en/2.0.x/",
        "https://docs.djangoproject.com/en/3.2/"
    ],
    "javascript": [
        "https://developer.mozilla.org/en-US/docs/Web/JavaScript",
        "https://reactjs.org/docs/getting-started.html",
        "https://vuejs.org/v2/guide/",
        "https://angular.io/docs",
        "https://nodejs.org/en/docs/",
        "https://expressjs.com/en/guide/routing.html",
        "https://www.typescriptlang.org/docs/"
    ],
    "java": [
        "https://docs.oracle.com/en/java/javase/16/docs/api/index.html",
        "https://docs.spring.io/spring-framework/docs/current/reference/html/",
        "https://docs.spring.io/spring-boot/docs/current/reference/html/",
        "https://docs.oracle.com/javaee/7/tutorial/",
        "https://hibernate.org/orm/documentation/5.5/"
    ],
    "c": [
        "https://devdocs.io/c/",
        "https://en.cppreference.com/w/c",
        "https://www.gnu.org/software/libc/manual/",
        "https://www.kernel.org/doc/html/latest/"
    ],
    "cpp": [
        "https://en.cppreference.com/w/cpp",
        "https://www.cplusplus.com/reference/",
        "https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines",
        "https://www.boost.org/doc/libs/"
    ],
    "sql": [
        "https://www.postgresql.org/docs/current/",
        "https://dev.mysql.com/doc/",
        "https://www.sqlite.org/docs.html",
        "https://docs.microsoft.com/en-us/sql/sql-server/"
    ],
    "csharp": [
        "https://docs.microsoft.com/en-us/dotnet/csharp/",
        "https://docs.microsoft.com/en-us/aspnet/core/?view=aspnetcore-5.0",
        "https://docs.microsoft.com/en-us/ef/core/"
    ],
    "go": [
        "https://golang.org/doc/",
        "https://pkg.go.dev/std",
        "https://gobyexample.com/"
    ],
    "rust": [
        "https://doc.rust-lang.org/book/",
        "https://doc.rust-lang.org/std/",
        "https://rustwasm.github.io/docs/book/"
    ]
}

# Fonti di tutorial per ogni linguaggio
TUTORIAL_SOURCES = {
    "python": [
        "https://realpython.com/",
        "https://www.learnpython.org/",
        "https://www.w3schools.com/python/",
        "https://www.tutorialspoint.com/python/index.htm",
        "https://www.kaggle.com/learn/python"
    ],
    "javascript": [
        "https://javascript.info/",
        "https://www.w3schools.com/js/",
        "https://developer.mozilla.org/en-US/docs/Learn/JavaScript",
        "https://www.codecademy.com/learn/introduction-to-javascript"
    ],
    "java": [
        "https://www.baeldung.com/",
        "https://www.tutorialspoint.com/java/index.htm",
        "https://www.w3schools.com/java/",
        "https://www.javatpoint.com/java-tutorial"
    ],
    "c": [
        "https://www.learn-c.org/",
        "https://www.tutorialspoint.com/cprogramming/index.htm",
        "https://www.w3schools.in/c-tutorial/"
    ],
    "cpp": [
        "https://www.learncpp.com/",
        "https://www.tutorialspoint.com/cplusplus/index.htm",
        "https://www.w3schools.com/cpp/"
    ],
    "sql": [
        "https://www.w3schools.com/sql/",
        "https://www.tutorialspoint.com/sql/index.htm",
        "https://sqlzoo.net/",
        "https://mode.com/sql-tutorial/"
    ]
}

# Fonti di Stack Overflow per ogni linguaggio
STACKOVERFLOW_URLS = {
    "python": "https://api.stackexchange.com/2.3/questions?pagesize=100&order=desc&sort=votes&tagged=python&site=stackoverflow&filter=withbody",
    "javascript": "https://api.stackexchange.com/2.3/questions?pagesize=100&order=desc&sort=votes&tagged=javascript&site=stackoverflow&filter=withbody",
    "java": "https://api.stackexchange.com/2.3/questions?pagesize=100&order=desc&sort=votes&tagged=java&site=stackoverflow&filter=withbody",
    "c": "https://api.stackexchange.com/2.3/questions?pagesize=100&order=desc&sort=votes&tagged=c&site=stackoverflow&filter=withbody",
    "cpp": "https://api.stackexchange.com/2.3/questions?pagesize=100&order=desc&sort=votes&tagged=c%2B%2B&site=stackoverflow&filter=withbody",
    "sql": "https://api.stackexchange.com/2.3/questions?pagesize=100&order=desc&sort=votes&tagged=sql&site=stackoverflow&filter=withbody"
}

def parse_arguments():
    parser = argparse.ArgumentParser(description='Scarica documentazione, tutorial e Q&A per addestrare modelli AI')
    parser.add_argument('--save_dir', type=str, default="Z:/the_stack_data",
                        help='Directory di base dove salvare i dati (default: Z:/the_stack_data)')
    parser.add_argument('--languages', type=str, nargs='+', default=["python", "javascript", "java", "c", "cpp", "sql"],
                        help='Linguaggi da scaricare (default: Tier 1)')
    parser.add_argument('--docs', action='store_true', help='Scarica documentazione')
    parser.add_argument('--tutorials', action='store_true', help='Scarica tutorial')
    parser.add_argument('--stackoverflow', action='store_true', help='Scarica Q&A da Stack Overflow')
    parser.add_argument('--all', action='store_true', help='Scarica tutto (docs, tutorials, stackoverflow)')
    parser.add_argument('--workers', type=int, default=5,
                        help='Numero di worker per il download parallelo (default: 5)')
    parser.add_argument('--max_pages', type=int, default=50,
                        help='Numero massimo di pagine da scaricare per fonte (default: 50)')
    return parser.parse_args()

def download_page(url, max_retries=3):
    """Scarica una pagina web con tentativi multipli"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                return response.text
            elif response.status_code == 429:  # Too Many Requests
                wait_time = (2 ** attempt) * 5  # Backoff esponenziale
                logger.warning(f"Rate limit raggiunto (429). Attesa di {wait_time} secondi...")
                time.sleep(wait_time)
            else:
                logger.error(f"Errore nel download di {url}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Errore durante il download di {url}: {e}")
            time.sleep(5)
    
    return None

def extract_links(html, base_url):
    """Estrae link da una pagina HTML"""
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    
    for a in soup.find_all('a', href=True):
        href = a['href']
        
        # Converti URL relativi in assoluti
        if not href.startswith('http'):
            href = urllib.parse.urljoin(base_url, href)
        
        # Filtra link non pertinenti
        if (href.startswith(base_url) and 
            not href.endswith(('.png', '.jpg', '.jpeg', '.gif', '.pdf', '.zip', '.tar.gz')) and
            '#' not in href):
            links.append(href)
    
    return links

def download_documentation(language, base_url, save_dir, max_pages=50):
    """Scarica documentazione da un URL base"""
    doc_dir = os.path.join(save_dir, language, "documentation")
    os.makedirs(doc_dir, exist_ok=True)
    
    logger.info(f"Download documentazione per {language} da {base_url}")
    
    # Scarica la pagina principale
    html = download_page(base_url)
    if not html:
        logger.error(f"Impossibile scaricare la pagina principale: {base_url}")
        return False
    
    # Salva la pagina principale
    domain = urllib.parse.urlparse(base_url).netloc
    main_file = os.path.join(doc_dir, f"{domain}_index.html")
    with open(main_file, 'w', encoding='utf-8') as f:
        f.write(html)
    
    # Estrai e scarica link
    visited = {base_url}
    to_visit = extract_links(html, base_url)
    pages_downloaded = 1
    
    while to_visit and pages_downloaded < max_pages:
        url = to_visit.pop(0)
        
        if url in visited:
            continue
        
        visited.add(url)
        
        # Scarica la pagina
        html = download_page(url)
        if not html:
            continue
        
        # Crea un nome file basato sull'URL
        path = urllib.parse.urlparse(url).path
        filename = re.sub(r'[^\w\-_\.]', '_', path)
        if not filename or filename == '/':
            filename = f"page_{pages_downloaded}"
        if not filename.endswith('.html'):
            filename += '.html'
        
        # Salva la pagina
        file_path = os.path.join(doc_dir, f"{domain}{filename}")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        # Aggiorna contatore
        pages_downloaded += 1
        if pages_downloaded % 10 == 0:
            logger.info(f"Scaricate {pages_downloaded} pagine per {language} da {domain}")
        
        # Aggiungi nuovi link
        new_links = extract_links(html, base_url)
        for link in new_links:
            if link not in visited and link not in to_visit:
                to_visit.append(link)
        
        # Pausa per evitare di sovraccaricare il server
        time.sleep(random.uniform(1, 3))
    
    logger.info(f"✓ {language}: Scaricate {pages_downloaded} pagine di documentazione da {domain}")
    return True

def download_stackoverflow_qa(language, save_dir, max_pages=10):
    """Scarica domande e risposte da Stack Overflow"""
    qa_dir = os.path.join(save_dir, language, "stackoverflow")
    os.makedirs(qa_dir, exist_ok=True)
    
    if language not in STACKOVERFLOW_URLS:
        logger.warning(f"Nessun URL Stack Overflow definito per {language}")
        return False
    
    base_url = STACKOVERFLOW_URLS[language]
    logger.info(f"Download Q&A Stack Overflow per {language}")
    
    questions_downloaded = 0
    page = 1
    
    while page <= max_pages:
        url = f"{base_url}&page={page}"
        
        try:
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"Errore nel download di Stack Overflow per {language}: {response.status_code}")
                break
            
            data = response.json()
            
            if not data.get('items'):
                logger.info(f"Nessun'altra domanda disponibile per {language}")
                break
            
            # Salva le domande
            for item in data['items']:
                question_id = item['question_id']
                question_file = os.path.join(qa_dir, f"question_{question_id}.json")
                
                with open(question_file, 'w', encoding='utf-8') as f:
                    json.dump(item, f, indent=2)
                
                questions_downloaded += 1
            
            logger.info(f"Scaricate {questions_downloaded} domande per {language} (pagina {page})")
            
            # Controlla se ci sono altre pagine
            if not data.get('has_more', False):
                break
            
            page += 1
            time.sleep(2)  # Pausa per rispettare i limiti di rate
            
        except Exception as e:
            logger.error(f"Errore durante il download di Stack Overflow per {language}: {e}")
            break
    
    logger.info(f"✓ {language}: Scaricate {questions_downloaded} domande da Stack Overflow")
    return questions_downloaded > 0

def process_language(language, save_dir, download_docs=False, download_tutorials=False, download_stackoverflow=False, max_pages=50, workers=5):
    """Processa un linguaggio scaricando documentazione, tutorial e Q&A"""
    logger.info(f"Elaborazione di {language}...")
    success = True
    
    # Crea directory per il linguaggio
    lang_dir = os.path.join(save_dir, language)
    os.makedirs(lang_dir, exist_ok=True)
    
    # Scarica documentazione
    if download_docs and language in DOC_SOURCES:
        doc_sources = DOC_SOURCES[language]
        logger.info(f"Download di {len(doc_sources)} fonti di documentazione per {language}")
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for url in doc_sources:
                futures.append(executor.submit(download_documentation, language, url, save_dir, max_pages))
            
            for future in as_completed(futures):
                try:
                    if not future.result():
                        success = False
                except Exception as e:
                    logger.error(f"Errore durante il download della documentazione per {language}: {e}")
                    success = False
    
    # Scarica tutorial
    if download_tutorials and language in TUTORIAL_SOURCES:
        tutorial_sources = TUTORIAL_SOURCES[language]
        logger.info(f"Download di {len(tutorial_sources)} fonti di tutorial per {language}")
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for url in tutorial_sources:
                futures.append(executor.submit(download_documentation, language, url, save_dir, max_pages))
            
            for future in as_completed(futures):
                try:
                    if not future.result():
                        success = False
                except Exception as e:
                    logger.error(f"Errore durante il download dei tutorial per {language}: {e}")
                    success = False
    
    # Scarica Q&A da Stack Overflow
    if download_stackoverflow:
        if not download_stackoverflow_qa(language, save_dir, max_pages):
            success = False
    
    return success

def main():
    args = parse_arguments()
    
    # Imposta flag in base all'opzione --all
    if args.all:
        args.docs = True
        args.tutorials = True
        args.stackoverflow = True
    
    # Crea la directory di base
    os.makedirs(args.save_dir, exist_ok=True)
    logger.info(f"Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    
    # Mostra riepilogo
    logger.info(f"Verranno elaborati {len(args.languages)} linguaggi: {args.languages}")
    logger.info(f"Download documentazione: {args.docs}")
    logger.info(f"Download tutorial: {args.tutorials}")
    logger.info(f"Download Stack Overflow: {args.stackoverflow}")
    logger.info(f"Numero massimo di pagine per fonte: {args.max_pages}")
    
    # Inizia il download
    successful = 0
    failed = 0
    
    for idx, lang in enumerate(args.languages):
        logger.info(f"Progresso: {idx+1}/{len(args.languages)} - Elaborazione di {lang}")
        if process_language(
            lang, 
            args.save_dir, 
            args.docs, 
            args.tutorials, 
            args.stackoverflow, 
            args.max_pages, 
            args.workers
        ):
            successful += 1
        else:
            failed += 1
    
    # Riepilogo finale
    logger.info("=" * 50)
    logger.info(f"Elaborazione completata. Risultati:")
    logger.info(f"- Linguaggi elaborati con successo: {successful}")
    logger.info(f"- Linguaggi con errori: {failed}")
    logger.info(f"- Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
