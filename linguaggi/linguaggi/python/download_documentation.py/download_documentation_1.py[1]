import os
import time
import argparse
import logging
import requests
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import re

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("documentation_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Linguaggi supportati e URL della documentazione ufficiale
DOCUMENTATION_SOURCES = {
    "python": [
        {"name": "Python Official Docs", "url": "https://docs.python.org/3/", "type": "sphinx"},
        {"name": "NumPy", "url": "https://numpy.org/doc/stable/", "type": "sphinx"},
        {"name": "Pandas", "url": "https://pandas.pydata.org/docs/", "type": "sphinx"},
        {"name": "TensorFlow", "url": "https://www.tensorflow.org/api_docs/python", "type": "devsite"},
        {"name": "PyTorch", "url": "https://pytorch.org/docs/stable/", "type": "sphinx"}
    ],
    "javascript": [
        {"name": "MDN Web Docs", "url": "https://developer.mozilla.org/en-US/docs/Web/JavaScript", "type": "mdn"},
        {"name": "Node.js", "url": "https://nodejs.org/en/docs/", "type": "nodejs"},
        {"name": "React", "url": "https://reactjs.org/docs/getting-started.html", "type": "react"},
        {"name": "Vue.js", "url": "https://vuejs.org/guide/introduction.html", "type": "vue"}
    ],
    "java": [
        {"name": "Java SE", "url": "https://docs.oracle.com/en/java/javase/17/docs/api/", "type": "javadoc"},
        {"name": "Spring", "url": "https://docs.spring.io/spring-framework/docs/current/reference/html/", "type": "spring"}
    ],
    "c": [
        {"name": "C Reference", "url": "https://en.cppreference.com/w/c", "type": "cppreference"}
    ],
    "cpp": [
        {"name": "C++ Reference", "url": "https://en.cppreference.com/w/cpp", "type": "cppreference"},
        {"name": "Boost", "url": "https://www.boost.org/doc/libs/", "type": "boost"}
    ],
    "sql": [
        {"name": "PostgreSQL", "url": "https://www.postgresql.org/docs/current/", "type": "postgres"},
        {"name": "MySQL", "url": "https://dev.mysql.com/doc/refman/8.0/en/", "type": "mysql"}
    ],
    "csharp": [
        {"name": "C# Documentation", "url": "https://docs.microsoft.com/en-us/dotnet/csharp/", "type": "microsoft"},
        {"name": ".NET API", "url": "https://docs.microsoft.com/en-us/dotnet/api/", "type": "microsoft"}
    ],
    "go": [
        {"name": "Go Documentation", "url": "https://golang.org/doc/", "type": "golang"},
        {"name": "Go Package Docs", "url": "https://pkg.go.dev/std", "type": "golang-pkg"}
    ],
    "rust": [
        {"name": "Rust Documentation", "url": "https://doc.rust-lang.org/book/", "type": "mdbook"},
        {"name": "Rust API", "url": "https://doc.rust-lang.org/std/", "type": "rustdoc"}
    ],
    "php": [
        {"name": "PHP Manual", "url": "https://www.php.net/manual/en/", "type": "php"},
        {"name": "Laravel", "url": "https://laravel.com/docs", "type": "laravel"}
    ],
    "ruby": [
        {"name": "Ruby Documentation", "url": "https://ruby-doc.org/core", "type": "ruby"},
        {"name": "Ruby on Rails", "url": "https://guides.rubyonrails.org/", "type": "rails"}
    ],
    "swift": [
        {"name": "Swift Documentation", "url": "https://swift.org/documentation/", "type": "swift"},
        {"name": "Swift API", "url": "https://developer.apple.com/documentation/swift", "type": "apple"}
    ],
    "kotlin": [
        {"name": "Kotlin Documentation", "url": "https://kotlinlang.org/docs/home.html", "type": "kotlin"}
    ],
    "scala": [
        {"name": "Scala Documentation", "url": "https://docs.scala-lang.org/", "type": "scala"}
    ],
    "shell": [
        {"name": "Bash Reference", "url": "https://www.gnu.org/software/bash/manual/bash.html", "type": "gnu"}
    ],
    "r": [
        {"name": "R Documentation", "url": "https://cran.r-project.org/manuals.html", "type": "r"}
    ],
    "haskell": [
        {"name": "Haskell Documentation", "url": "https://www.haskell.org/documentation/", "type": "haskell"}
    ],
    "julia": [
        {"name": "Julia Documentation", "url": "https://docs.julialang.org/en/v1/", "type": "julia"}
    ],
    "lua": [
        {"name": "Lua Documentation", "url": "https://www.lua.org/docs.html", "type": "lua"}
    ],
    "elixir": [
        {"name": "Elixir Documentation", "url": "https://elixir-lang.org/docs.html", "type": "elixir"}
    ],
    "dart": [
        {"name": "Dart Documentation", "url": "https://dart.dev/guides", "type": "dart"},
        {"name": "Flutter", "url": "https://flutter.dev/docs", "type": "flutter"}
    ]
}

def parse_arguments():
    parser = argparse.ArgumentParser(description='Scarica documentazione ufficiale per vari linguaggi')
    parser.add_argument('--save_dir', type=str, default="Z:/the_stack_data/complementary/documentation",
                        help='Directory dove salvare i dati (default: Z:/the_stack_data/complementary/documentation)')
    parser.add_argument('--languages', type=str, nargs='+', default=list(DOCUMENTATION_SOURCES.keys()),
                        help=f'Linguaggi da scaricare (default: tutti)')
    parser.add_argument('--max_pages', type=int, default=500,
                        help='Numero massimo di pagine da scaricare per fonte (default: 500)')
    parser.add_argument('--max_workers', type=int, default=5,
                        help='Numero massimo di worker per il download parallelo (default: 5)')
    parser.add_argument('--wait_time', type=int, default=1,
                        help='Tempo di attesa tra le richieste in secondi (default: 1)')
    return parser.parse_args()

def get_page_links(url, doc_type):
    """Ottiene i link alle pagine dalla documentazione"""
    try:
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Errore nel recupero dei link da {url}: {response.status_code}")
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        
        if doc_type == "sphinx":
            # Per documentazione Sphinx (Python, NumPy, ecc.)
            for a in soup.select('a.reference'):
                href = a.get('href')
                if href and not href.startswith(('http://', 'https://', '#', 'mailto:')):
                    if href.endswith('.html'):
                        full_url = url + href if not url.endswith('/') else url + '/' + href
                        links.append(full_url)
        
        elif doc_type == "mdn":
            # Per MDN Web Docs (JavaScript)
            for a in soup.select('a[href^="/"]'):
                href = a.get('href')
                if href and '/docs/' in href and not href.endswith(('#', '/')):
                    full_url = "https://developer.mozilla.org" + href
                    links.append(full_url)
        
        elif doc_type == "cppreference":
            # Per C/C++ Reference
            for a in soup.select('a[href]'):
                href = a.get('href')
                if href and not href.startswith(('http://', 'https://', '#', 'mailto:')):
                    if '/' in href:
                        base_url = '/'.join(url.split('/')[:-1])
                        full_url = base_url + '/' + href
                        links.append(full_url)
        
        # Aggiungi altri tipi di documentazione se necessario
        
        else:
            # Approccio generico
            for a in soup.select('a[href]'):
                href = a.get('href')
                if href and not href.startswith(('http://', 'https://', '#', 'mailto:')):
                    if '/' in href:
                        base_url = '/'.join(url.split('/')[:-1])
                        full_url = base_url + '/' + href
                        links.append(full_url)
        
        # Rimuovi duplicati
        links = list(set(links))
        
        return links
    
    except Exception as e:
        logger.error(f"Errore durante il recupero dei link da {url}: {e}")
        return []

def download_page(url, save_path, wait_time):
    """Scarica una singola pagina di documentazione"""
    try:
        time.sleep(wait_time)  # Pausa per evitare di sovraccaricare il server
        
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Errore nel download della pagina {url}: {response.status_code}")
            return False
        
        # Estrai il contenuto HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Rimuovi script e stili
        for script in soup(["script", "style"]):
            script.extract()
        
        # Estrai il contenuto principale
        main_content = None
        
        # Prova a trovare il contenuto principale in base alla struttura del sito
        for selector in [
            'main', 'article', '.content', '.document', '#content', '#main',
            '.main-content', '.documentation', '.doc-content'
        ]:
            content = soup.select_one(selector)
            if content:
                main_content = content
                break
        
        # Se non è stato trovato un contenitore specifico, usa il body
        if not main_content:
            main_content = soup.body
        
        # Crea un dizionario con i dati della pagina
        page_data = {
            "url": url,
            "title": soup.title.string if soup.title else "Untitled",
            "content_html": str(main_content) if main_content else "",
            "content_text": main_content.get_text(separator="\n", strip=True) if main_content else "",
            "timestamp": time.time()
        }
        
        # Salva la pagina
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(page_data, f, ensure_ascii=False, indent=2)
        
        return True
    
    except Exception as e:
        logger.error(f"Errore durante il download della pagina {url}: {e}")
        return False

def download_documentation_source(source, lang_dir, max_pages, wait_time):
    """Scarica la documentazione da una fonte specifica"""
    source_name = source["name"]
    source_url = source["url"]
    source_type = source["type"]
    
    logger.info(f"Download della documentazione da {source_name} ({source_url})...")
    
    # Crea directory per la fonte
    source_dir = os.path.join(lang_dir, re.sub(r'[^\w\-\.]', '_', source_name))
    os.makedirs(source_dir, exist_ok=True)
    
    # Ottieni i link alle pagine
    links = get_page_links(source_url, source_type)
    
    if not links:
        logger.warning(f"Nessun link trovato per {source_name} ({source_url})")
        return 0
    
    # Limita il numero di pagine
    if len(links) > max_pages:
        logger.info(f"Limitando a {max_pages} pagine per {source_name} (trovate {len(links)})")
        links = links[:max_pages]
    
    logger.info(f"Trovate {len(links)} pagine da scaricare per {source_name}")
    
    # Scarica le pagine
    pages_downloaded = 0
    
    for i, link in enumerate(links):
        # Crea un nome file basato sull'URL
        file_name = f"page_{i+1}.json"
        save_path = os.path.join(source_dir, file_name)
        
        # Scarica la pagina
        if download_page(link, save_path, wait_time):
            pages_downloaded += 1
            
            if pages_downloaded % 10 == 0:
                logger.info(f"Scaricate {pages_downloaded}/{len(links)} pagine da {source_name}")
    
    logger.info(f"✓ Scaricate {pages_downloaded} pagine da {source_name}")
    return pages_downloaded

def download_documentation_for_language(language, save_dir, max_pages, wait_time):
    """Scarica la documentazione per un linguaggio specifico"""
    if language not in DOCUMENTATION_SOURCES:
        logger.error(f"Nessuna fonte di documentazione definita per {language}")
        return 0
    
    lang_dir = os.path.join(save_dir, language)
    os.makedirs(lang_dir, exist_ok=True)
    
    logger.info(f"Download della documentazione per {language}...")
    
    sources = DOCUMENTATION_SOURCES[language]
    total_pages = 0
    
    for source in sources:
        pages = download_documentation_source(source, lang_dir, max_pages, wait_time)
        total_pages += pages
    
    logger.info(f"✓ Scaricate in totale {total_pages} pagine di documentazione per {language}")
    return total_pages

def main():
    args = parse_arguments()
    
    # Crea la directory di base
    os.makedirs(args.save_dir, exist_ok=True)
    logger.info(f"Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    
    # Filtra i linguaggi validi
    languages = [lang for lang in args.languages if lang in DOCUMENTATION_SOURCES]
    
    if not languages:
        logger.error(f"Nessun linguaggio valido specificato. Linguaggi supportati: {', '.join(DOCUMENTATION_SOURCES.keys())}")
        return
    
    logger.info(f"Scaricamento documentazione per {len(languages)} linguaggi: {languages}")
    logger.info(f"Massimo {args.max_pages} pagine per fonte")
    
    # Download parallelo per linguaggi
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                download_documentation_for_language, 
                language, 
                args.save_dir, 
                args.max_pages, 
                args.wait_time
            ): language for language in languages
        }
        
        total_pages = 0
        
        for future in as_completed(futures):
            language = futures[future]
            try:
                pages_count = future.result()
                total_pages += pages_count
            except Exception as e:
                logger.error(f"Errore durante il download per {language}: {e}")
    
    logger.info("=" * 50)
    logger.info(f"Download completato. Totale pagine scaricate: {total_pages}")
    logger.info(f"Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
