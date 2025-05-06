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
        logging.FileHandler("stackoverflow_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Linguaggi supportati
LANGUAGES = [
    "python", "javascript", "java", "c", "cpp", "sql", "csharp", "go", 
    "rust", "php", "ruby", "swift", "kotlin", "scala", "shell", "r", 
    "haskell", "julia", "lua", "elixir", "dart"
]

# Tag popolari per ogni linguaggio
LANGUAGE_TAGS = {
    "python": ["python", "pandas", "numpy", "django", "flask", "tensorflow", "pytorch"],
    "javascript": ["javascript", "node.js", "react", "vue.js", "angular", "express"],
    "java": ["java", "spring", "hibernate", "android", "maven", "gradle"],
    "c": ["c", "c-programming", "pointers", "embedded"],
    "cpp": ["c++", "c++11", "c++14", "c++17", "c++20"],
    "sql": ["sql", "mysql", "postgresql", "sql-server", "oracle"],
    "csharp": ["c#", ".net", "asp.net", "entity-framework", "xamarin"],
    "go": ["go", "golang", "goroutine", "gorm"],
    "rust": ["rust", "cargo", "rustc"],
    "php": ["php", "laravel", "symfony", "wordpress"],
    "ruby": ["ruby", "ruby-on-rails", "rspec"],
    "swift": ["swift", "ios", "swiftui", "cocoa-touch"],
    "kotlin": ["kotlin", "android-kotlin", "kotlin-coroutines"],
    "scala": ["scala", "akka", "spark", "play-framework"],
    "shell": ["bash", "shell", "shell-script", "linux", "unix"],
    "r": ["r", "ggplot2", "data.table", "dplyr", "tidyr"],
    "haskell": ["haskell", "ghc", "cabal", "stack"],
    "julia": ["julia", "julia-lang"],
    "lua": ["lua", "luajit", "love2d"],
    "elixir": ["elixir", "phoenix-framework", "ecto"],
    "dart": ["dart", "flutter", "dart-web"]
}

def parse_arguments():
    parser = argparse.ArgumentParser(description='Scarica domande e risposte da Stack Overflow')
    parser.add_argument('--save_dir', type=str, default="Z:/the_stack_data/complementary/stackoverflow",
                        help='Directory dove salvare i dati (default: Z:/the_stack_data/complementary/stackoverflow)')
    parser.add_argument('--languages', type=str, nargs='+', default=LANGUAGES,
                        help=f'Linguaggi da scaricare (default: tutti)')
    parser.add_argument('--questions_per_language', type=int, default=1000,
                        help='Numero di domande da scaricare per linguaggio (default: 1000)')
    parser.add_argument('--max_workers', type=int, default=5,
                        help='Numero massimo di worker per il download parallelo (default: 5)')
    parser.add_argument('--wait_time', type=int, default=2,
                        help='Tempo di attesa tra le richieste API in secondi (default: 2)')
    return parser.parse_args()

def get_stackoverflow_questions(language, tag, page=1, pagesize=100):
    """Ottiene domande da Stack Overflow API per un tag specifico"""
    url = "https://api.stackexchange.com/2.3/questions"
    
    params = {
        "page": page,
        "pagesize": pagesize,
        "order": "desc",
        "sort": "votes",
        "tagged": tag,
        "site": "stackoverflow",
        "filter": "withbody"  # Include il corpo della domanda
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("items", [])
        else:
            logger.error(f"Errore nell'API di Stack Overflow: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Errore durante la richiesta a Stack Overflow: {e}")
        return []

def get_stackoverflow_answers(question_id):
    """Ottiene le risposte a una domanda specifica"""
    url = f"https://api.stackexchange.com/2.3/questions/{question_id}/answers"
    
    params = {
        "order": "desc",
        "sort": "votes",
        "site": "stackoverflow",
        "filter": "withbody"  # Include il corpo della risposta
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("items", [])
        else:
            logger.error(f"Errore nell'API di Stack Overflow per le risposte: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Errore durante la richiesta delle risposte: {e}")
        return []

def extract_code_blocks(html_content):
    """Estrae blocchi di codice da contenuto HTML"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Trova tutti i blocchi di codice
    code_blocks = []
    
    # Cerca tag <code> e <pre>
    for code in soup.find_all(['code', 'pre']):
        code_text = code.get_text()
        if code_text.strip():
            code_blocks.append(code_text)
    
    return code_blocks

def clean_html(html_content):
    """Rimuove i tag HTML mantenendo il testo"""
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup.get_text()

def download_questions_for_language(language, save_dir, questions_per_language, wait_time):
    """Scarica domande e risposte per un linguaggio specifico"""
    lang_dir = os.path.join(save_dir, language)
    os.makedirs(lang_dir, exist_ok=True)
    
    logger.info(f"Download di domande per {language}...")
    
    # Ottieni tag per il linguaggio
    tags = LANGUAGE_TAGS.get(language, [language])
    
    questions_downloaded = 0
    page = 1
    
    while questions_downloaded < questions_per_language:
        # Seleziona un tag casuale tra quelli disponibili per il linguaggio
        tag = random.choice(tags)
        
        logger.info(f"Scaricamento domande con tag '{tag}' per {language} (pagina {page})...")
        
        questions = get_stackoverflow_questions(language, tag, page=page, pagesize=100)
        
        if not questions:
            logger.warning(f"Nessuna domanda trovata per {language} con tag {tag} alla pagina {page}")
            page += 1
            if page > 10:  # Limita a 10 pagine per tag
                break
            time.sleep(wait_time)
            continue
        
        for question in questions:
            question_id = question.get("question_id")
            
            if not question_id:
                continue
            
            # Crea file per la domanda
            question_file = os.path.join(lang_dir, f"question_{question_id}.json")
            
            # Se il file esiste già, salta
            if os.path.exists(question_file):
                continue
            
            # Ottieni le risposte
            answers = get_stackoverflow_answers(question_id)
            time.sleep(wait_time)  # Pausa tra le richieste
            
            # Estrai blocchi di codice
            question_code_blocks = []
            if "body" in question:
                question_code_blocks = extract_code_blocks(question["body"])
            
            answer_data = []
            for answer in answers:
                answer_code_blocks = []
                if "body" in answer:
                    answer_code_blocks = extract_code_blocks(answer["body"])
                
                answer_data.append({
                    "answer_id": answer.get("answer_id"),
                    "score": answer.get("score", 0),
                    "is_accepted": answer.get("is_accepted", False),
                    "text": clean_html(answer.get("body", "")),
                    "code_blocks": answer_code_blocks
                })
            
            # Crea oggetto domanda
            question_data = {
                "question_id": question_id,
                "title": question.get("title", ""),
                "tags": question.get("tags", []),
                "score": question.get("score", 0),
                "view_count": question.get("view_count", 0),
                "text": clean_html(question.get("body", "")),
                "code_blocks": question_code_blocks,
                "answers": answer_data,
                "link": question.get("link", "")
            }
            
            # Salva la domanda
            with open(question_file, 'w', encoding='utf-8') as f:
                json.dump(question_data, f, ensure_ascii=False, indent=2)
            
            questions_downloaded += 1
            
            if questions_downloaded % 10 == 0:
                logger.info(f"Scaricate {questions_downloaded}/{questions_per_language} domande per {language}")
            
            if questions_downloaded >= questions_per_language:
                break
        
        page += 1
        time.sleep(wait_time)
    
    logger.info(f"✓ Scaricate {questions_downloaded} domande per {language}")
    return questions_downloaded

def main():
    args = parse_arguments()
    
    # Crea la directory di base
    os.makedirs(args.save_dir, exist_ok=True)
    logger.info(f"Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    
    # Filtra i linguaggi validi
    languages = [lang for lang in args.languages if lang in LANGUAGES]
    
    if not languages:
        logger.error(f"Nessun linguaggio valido specificato. Linguaggi supportati: {', '.join(LANGUAGES)}")
        return
    
    logger.info(f"Scaricamento domande per {len(languages)} linguaggi: {languages}")
    logger.info(f"Domande per linguaggio: {args.questions_per_language}")
    
    # Download parallelo per linguaggi
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                download_questions_for_language, 
                language, 
                args.save_dir, 
                args.questions_per_language, 
                args.wait_time
            ): language for language in languages
        }
        
        total_questions = 0
        
        for future in as_completed(futures):
            language = futures[future]
            try:
                questions_count = future.result()
                total_questions += questions_count
            except Exception as e:
                logger.error(f"Errore durante il download per {language}: {e}")
    
    logger.info("=" * 50)
    logger.info(f"Download completato. Totale domande scaricate: {total_questions}")
    logger.info(f"Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
