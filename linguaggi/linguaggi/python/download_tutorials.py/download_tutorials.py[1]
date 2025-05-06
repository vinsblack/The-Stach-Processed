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
        logging.FileHandler("tutorials_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Fonti di tutorial per ogni linguaggio
TUTORIAL_SOURCES = {
    "python": [
        {"name": "Real Python", "url": "https://realpython.com/tutorials/all/", "type": "realpython"},
        {"name": "Python Tutorial", "url": "https://www.pythontutorial.net/", "type": "pythontutorial"},
        {"name": "Programiz", "url": "https://www.programiz.com/python-programming", "type": "programiz"}
    ],
    "javascript": [
        {"name": "JavaScript.info", "url": "https://javascript.info/", "type": "jsinfo"},
        {"name": "W3Schools JS", "url": "https://www.w3schools.com/js/", "type": "w3schools"},
        {"name": "MDN Tutorials", "url": "https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide", "type": "mdn"}
    ],
    "java": [
        {"name": "Baeldung", "url": "https://www.baeldung.com/java-tutorials", "type": "baeldung"},
        {"name": "Tutorialspoint", "url": "https://www.tutorialspoint.com/java/", "type": "tutorialspoint"}
    ],
    "c": [
        {"name": "Learn-C", "url": "https://www.learn-c.org/", "type": "learnc"},
        {"name": "Tutorialspoint C", "url": "https://www.tutorialspoint.com/cprogramming/", "type": "tutorialspoint"}
    ],
    "cpp": [
        {"name": "LearnCpp", "url": "https://www.learncpp.com/", "type": "learncpp"},
        {"name": "Tutorialspoint C++", "url": "https://www.tutorialspoint.com/cplusplus/", "type": "tutorialspoint"}
    ],
    "sql": [
        {"name": "SQLZoo", "url": "https://sqlzoo.net/wiki/SQL_Tutorial", "type": "sqlzoo"},
        {"name": "W3Schools SQL", "url": "https://www.w3schools.com/sql/", "type": "w3schools"}
    ],
    "csharp": [
        {"name": "C# Station", "url": "https://csharp-station.com/Tutorial/CSharp/", "type": "csharpstation"},
        {"name": "Tutorialspoint C#", "url": "https://www.tutorialspoint.com/csharp/", "type": "tutorialspoint"}
    ],
    "go": [
        {"name": "Go by Example", "url": "https://gobyexample.com/", "type": "gobyexample"},
        {"name": "Tutorialspoint Go", "url": "https://www.tutorialspoint.com/go/", "type": "tutorialspoint"}
    ],
    "rust": [
        {"name": "Rust by Example", "url": "https://doc.rust-lang.org/rust-by-example/", "type": "rustbyexample"},
        {"name": "Tutorialspoint Rust", "url": "https://www.tutorialspoint.com/rust/", "type": "tutorialspoint"}
    ],
    "php": [
        {"name": "PHP.net Tutorials", "url": "https://www.php.net/manual/en/tutorial.php", "type": "phpnet"},
        {"name": "W3Schools PHP", "url": "https://www.w3schools.com/php/", "type": "w3schools"}
    ],
    "ruby": [
        {"name": "Ruby Monk", "url": "https://rubymonk.com/", "type": "rubymonk"},
        {"name": "Tutorialspoint Ruby", "url": "https://www.tutorialspoint.com/ruby/", "type": "tutorialspoint"}
    ],
    "swift": [
        {"name": "Swift Tutorials", "url": "https://www.tutorialspoint.com/swift/", "type": "tutorialspoint"},
        {"name": "Hackingwithswift", "url": "https://www.hackingwithswift.com/", "type": "hackingwithswift"}
    ],
    "kotlin": [
        {"name": "Kotlin Tutorials", "url": "https://kotlinlang.org/docs/tutorials/", "type": "kotlinlang"},
        {"name": "Tutorialspoint Kotlin", "url": "https://www.tutorialspoint.com/kotlin/", "type": "tutorialspoint"}
    ],
    "scala": [
        {"name": "Scala Tutorials", "url": "https://docs.scala-lang.org/tutorials/", "type": "scalalang"},
        {"name": "Tutorialspoint Scala", "url": "https://www.tutorialspoint.com/scala/", "type": "tutorialspoint"}
    ],
    "shell": [
        {"name": "Shell Scripting", "url": "https://www.shellscript.sh/", "type": "shellscript"},
        {"name": "Tutorialspoint Shell", "url": "https://www.tutorialspoint.com/unix/shell_scripting.htm", "type": "tutorialspoint"}
    ],
    "r": [
        {"name": "R Tutorials", "url": "https://www.tutorialspoint.com/r/", "type": "tutorialspoint"},
        {"name": "R-bloggers", "url": "https://www.r-bloggers.com/how-to-learn-r-2/", "type": "rbloggers"}
    ],
    "haskell": [
        {"name": "Learn You a Haskell", "url": "http://learnyouahaskell.com/chapters", "type": "learnyouahaskell"},
        {"name": "Tutorialspoint Haskell", "url": "https://www.tutorialspoint.com/haskell/", "type": "tutorialspoint"}
    ],
    "julia": [
        {"name": "Julia Tutorials", "url": "https://julialang.org/learning/", "type": "julialang"},
        {"name": "Tutorialspoint Julia", "url": "https://www.tutorialspoint.com/julia/", "type": "tutorialspoint"}
    ],
    "lua": [
        {"name": "Lua Tutorial", "url": "http://lua-users.org/wiki/TutorialDirectory", "type": "luausers"},
        {"name": "Tutorialspoint Lua", "url": "https://www.tutorialspoint.com/lua/", "type": "tutorialspoint"}
    ],
    "elixir": [
        {"name": "Elixir School", "url": "https://elixirschool.com/en/", "type": "elixirschool"},
        {"name": "Tutorialspoint Elixir", "url": "https://www.tutorialspoint.com/elixir/", "type": "tutorialspoint"}
    ],
    "dart": [
        {"name": "Dart Tutorials", "url": "https://dart.dev/tutorials", "type": "dartdev"},
        {"name": "Tutorialspoint Dart", "url": "https://www.tutorialspoint.com/dart_programming/", "type": "tutorialspoint"}
    ]
}

def parse_arguments():
    parser = argparse.ArgumentParser(description='Scarica tutorial per vari linguaggi di programmazione')
    parser.add_argument('--save_dir', type=str, default="Z:/the_stack_data/complementary/tutorials",
                        help='Directory dove salvare i dati (default: Z:/the_stack_data/complementary/tutorials)')
    parser.add_argument('--languages', type=str, nargs='+', default=list(TUTORIAL_SOURCES.keys()),
                        help=f'Linguaggi da scaricare (default: tutti)')
    parser.add_argument('--max_tutorials', type=int, default=100,
                        help='Numero massimo di tutorial da scaricare per fonte (default: 100)')
    parser.add_argument('--max_workers', type=int, default=5,
                        help='Numero massimo di worker per il download parallelo (default: 5)')
    parser.add_argument('--wait_time', type=int, default=1,
                        help='Tempo di attesa tra le richieste in secondi (default: 1)')
    return parser.parse_args()

def get_tutorial_links(source):
    """Ottiene i link ai tutorial da una fonte specifica"""
    url = source["url"]
    source_type = source["type"]
    
    try:
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Errore nel recupero dei link da {url}: {response.status_code}")
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        
        if source_type == "realpython":
            # Per Real Python
            for article in soup.select('article.card'):
                a = article.select_one('a.card-link')
                if a and a.get('href'):
                    href = a.get('href')
                    if not href.startswith(('http://', 'https://')):
                        href = "https://realpython.com" + href
                    links.append(href)
        
        elif source_type == "w3schools":
            # Per W3Schools
            for a in soup.select('div.w3-bar-item a'):
                href = a.get('href')
                if href and not href.startswith(('#', 'javascript:')):
                    if not href.startswith(('http://', 'https://')):
                        base_url = '/'.join(url.split('/')[:-1])
                        href = base_url + '/' + href
                    links.append(href)
        
        elif source_type == "tutorialspoint":
            # Per Tutorialspoint
            for a in soup.select('ul.toc a'):
                href = a.get('href')
                if href and not href.startswith(('#', 'javascript:')):
                    if not href.startswith(('http://', 'https://')):
                        base_url = '/'.join(url.split('/')[:-1])
                        href = base_url + '/' + href
                    links.append(href)
        
        elif source_type == "mdn":
            # Per MDN
            for a in soup.select('ol.multi-column a'):
                href = a.get('href')
                if href and not href.startswith(('#', 'javascript:')):
                    if not href.startswith(('http://', 'https://')):
                        href = "https://developer.mozilla.org" + href
                    links.append(href)
        
        else:
            # Approccio generico
            for a in soup.select('a[href]'):
                href = a.get('href')
                if href and not href.startswith(('#', 'javascript:')):
                    if not href.startswith(('http://', 'https://')):
                        base_url = '/'.join(url.split('/')[:-1])
                        href = base_url + '/' + href
                    links.append(href)
        
        # Rimuovi duplicati
        links = list(set(links))
        
        return links
    
    except Exception as e:
        logger.error(f"Errore durante il recupero dei link da {url}: {e}")
        return []

def download_tutorial(url, save_path, wait_time):
    """Scarica un singolo tutorial"""
    try:
        time.sleep(wait_time)  # Pausa per evitare di sovraccaricare il server
        
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Errore nel download del tutorial {url}: {response.status_code}")
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
            'main', 'article', '.content', '.tutorial-content', '#content', '#main',
            '.main-content', '.tutorial', '.post-content', '.entry-content'
        ]:
            content = soup.select_one(selector)
            if content:
                main_content = content
                break
        
        # Se non è stato trovato un contenitore specifico, usa il body
        if not main_content:
            main_content = soup.body
        
        # Estrai i blocchi di codice
        code_blocks = []
        for code in main_content.select('pre, code'):
            code_text = code.get_text(strip=True)
            if code_text:
                code_blocks.append(code_text)
        
        # Crea un dizionario con i dati del tutorial
        tutorial_data = {
            "url": url,
            "title": soup.title.string if soup.title else "Untitled",
            "content_html": str(main_content) if main_content else "",
            "content_text": main_content.get_text(separator="\n", strip=True) if main_content else "",
            "code_blocks": code_blocks,
            "timestamp": time.time()
        }
        
        # Salva il tutorial
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(tutorial_data, f, ensure_ascii=False, indent=2)
        
        return True
    
    except Exception as e:
        logger.error(f"Errore durante il download del tutorial {url}: {e}")
        return False

def download_tutorials_from_source(source, lang_dir, max_tutorials, wait_time):
    """Scarica tutorial da una fonte specifica"""
    source_name = source["name"]
    
    logger.info(f"Download di tutorial da {source_name} ({source['url']})...")
    
    # Crea directory per la fonte
    source_dir = os.path.join(lang_dir, re.sub(r'[^\w\-\.]', '_', source_name))
    os.makedirs(source_dir, exist_ok=True)
    
    # Ottieni i link ai tutorial
    links = get_tutorial_links(source)
    
    if not links:
        logger.warning(f"Nessun link trovato per {source_name} ({source['url']})")
        return 0
    
    # Limita il numero di tutorial
    if len(links) > max_tutorials:
        logger.info(f"Limitando a {max_tutorials} tutorial per {source_name} (trovati {len(links)})")
        links = links[:max_tutorials]
    
    logger.info(f"Trovati {len(links)} tutorial da scaricare per {source_name}")
    
    # Scarica i tutorial
    tutorials_downloaded = 0
    
    for i, link in enumerate(links):
        # Crea un nome file basato sull'URL
        file_name = f"tutorial_{i+1}.json"
        save_path = os.path.join(source_dir, file_name)
        
        # Scarica il tutorial
        if download_tutorial(link, save_path, wait_time):
            tutorials_downloaded += 1
            
            if tutorials_downloaded % 10 == 0:
                logger.info(f"Scaricati {tutorials_downloaded}/{len(links)} tutorial da {source_name}")
    
    logger.info(f"✓ Scaricati {tutorials_downloaded} tutorial da {source_name}")
    return tutorials_downloaded

def download_tutorials_for_language(language, save_dir, max_tutorials, wait_time):
    """Scarica tutorial per un linguaggio specifico"""
    if language not in TUTORIAL_SOURCES:
        logger.error(f"Nessuna fonte di tutorial definita per {language}")
        return 0
    
    lang_dir = os.path.join(save_dir, language)
    os.makedirs(lang_dir, exist_ok=True)
    
    logger.info(f"Download di tutorial per {language}...")
    
    sources = TUTORIAL_SOURCES[language]
    total_tutorials = 0
    
    for source in sources:
        tutorials = download_tutorials_from_source(source, lang_dir, max_tutorials, wait_time)
        total_tutorials += tutorials
    
    logger.info(f"✓ Scaricati in totale {total_tutorials} tutorial per {language}")
    return total_tutorials

def main():
    args = parse_arguments()
    
    # Crea la directory di base
    os.makedirs(args.save_dir, exist_ok=True)
    logger.info(f"Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    
    # Filtra i linguaggi validi
    languages = [lang for lang in args.languages if lang in TUTORIAL_SOURCES]
    
    if not languages:
        logger.error(f"Nessun linguaggio valido specificato. Linguaggi supportati: {', '.join(TUTORIAL_SOURCES.keys())}")
        return
    
    logger.info(f"Scaricamento tutorial per {len(languages)} linguaggi: {languages}")
    logger.info(f"Massimo {args.max_tutorials} tutorial per fonte")
    
    # Download parallelo per linguaggi
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                download_tutorials_for_language, 
                language, 
                args.save_dir, 
                args.max_tutorials, 
                args.wait_time
            ): language for language in languages
        }
        
        total_tutorials = 0
        
        for future in as_completed(futures):
            language = futures[future]
            try:
                tutorials_count = future.result()
                total_tutorials += tutorials_count
            except Exception as e:
                logger.error(f"Errore durante il download per {language}: {e}")
    
    logger.info("=" * 50)
    logger.info(f"Download completato. Totale tutorial scaricati: {total_tutorials}")
    logger.info(f"Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
