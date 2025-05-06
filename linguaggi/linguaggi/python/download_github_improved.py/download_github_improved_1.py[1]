import os
import time
import argparse
import logging
import requests
import json
import random
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download_improved_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Definisci i tier di linguaggi da scaricare
TIERS = {
    1: ["python", "javascript", "java", "c", "cpp", "sql"],  # Fondamentali
    2: ["csharp", "go", "rust", "php", "ruby"],              # Alta priorità
    3: ["swift", "kotlin", "scala", "shell", "r"],           # Selettivi
    4: ["haskell", "julia", "lua", "elixir", "dart"]         # Nicchia
}

# Mappatura tra nomi di linguaggi e estensioni di file
LANGUAGE_EXTENSIONS = {
    "python": [".py"],
    "javascript": [".js", ".jsx", ".ts", ".tsx"],
    "java": [".java"],
    "c": [".c", ".h"],
    "cpp": [".cpp", ".hpp", ".cc", ".hh", ".cxx", ".hxx"],
    "sql": [".sql"],
    "csharp": [".cs"],
    "go": [".go"],
    "rust": [".rs"],
    "php": [".php"],
    "ruby": [".rb"],
    "swift": [".swift"],
    "kotlin": [".kt", ".kts"],
    "scala": [".scala"],
    "shell": [".sh", ".bash"],
    "r": [".r", ".R"],
    "haskell": [".hs", ".lhs"],
    "julia": [".jl"],
    "lua": [".lua"],
    "elixir": [".ex", ".exs"],
    "dart": [".dart"]
}

# Query di ricerca per ogni linguaggio (versione migliorata)
LANGUAGE_QUERIES = {
    "python": [
        "language:python stars:>100 size:<10000",
        "language:python topic:machine-learning",
        "language:python topic:data-science",
        "language:python topic:web-framework",
        "language:python topic:utility"
    ],
    "javascript": [
        "language:javascript stars:>100 size:<10000",
        "language:javascript topic:frontend",
        "language:javascript topic:react",
        "language:javascript topic:vue",
        "language:javascript topic:node"
    ],
    "java": [
        "language:java stars:>100 size:<10000",
        "language:java topic:android",
        "language:java topic:spring",
        "language:java topic:library",
        "language:java topic:api"
    ],
    "c": [
        "language:c stars:>100 size:<10000",
        "language:c topic:embedded",
        "language:c topic:system",
        "language:c topic:library",
        "language:c topic:algorithm"
    ],
    "cpp": [
        "language:cpp stars:>100 size:<10000",
        "language:cpp topic:game-engine",
        "language:cpp topic:library",
        "language:cpp topic:algorithm",
        "language:cpp topic:system"
    ],
    "sql": [
        "language:sql stars:>50 size:<10000",
        "language:plsql stars:>10",
        "language:tsql stars:>10",
        "topic:sql stars:>50",
        "topic:database language:sql"
    ],
    "csharp": [
        "language:csharp stars:>100 size:<10000",
        "language:csharp topic:dotnet",
        "language:csharp topic:unity",
        "language:csharp topic:xamarin",
        "language:csharp topic:library"
    ],
    "go": [
        "language:go stars:>100 size:<10000",
        "language:go topic:microservice",
        "language:go topic:api",
        "language:go topic:cli",
        "language:go topic:library"
    ],
    "rust": [
        "language:rust stars:>100 size:<10000",
        "language:rust topic:system",
        "language:rust topic:wasm",
        "language:rust topic:cli",
        "language:rust topic:library"
    ],
    "php": [
        "language:php stars:>100 size:<10000",
        "language:php topic:laravel",
        "language:php topic:wordpress",
        "language:php topic:symfony",
        "language:php topic:cms"
    ],
    "ruby": [
        "language:ruby stars:>100 size:<10000",
        "language:ruby topic:rails",
        "language:ruby topic:gem",
        "language:ruby topic:api",
        "language:ruby topic:library"
    ]
}

# Fonti alternative per SQL (poiché GitHub ha meno repository SQL)
SQL_ALTERNATIVE_SOURCES = [
    {"name": "SQLite", "url": "https://github.com/sqlite/sqlite"},
    {"name": "PostgreSQL", "url": "https://github.com/postgres/postgres"},
    {"name": "MySQL", "url": "https://github.com/mysql/mysql-server"},
    {"name": "SQL Server", "url": "https://github.com/microsoft/sql-server-samples"},
    {"name": "Oracle PL/SQL", "url": "https://github.com/oracle/plsql-utils"},
    {"name": "SQLAlchemy", "url": "https://github.com/sqlalchemy/sqlalchemy"}
]

def parse_arguments():
    parser = argparse.ArgumentParser(description='Scarica codice sorgente da GitHub per linguaggi selezionati (versione migliorata)')
    parser.add_argument('--save_dir', type=str, default="Z:/the_stack_data",
                        help='Directory di base dove salvare i dati (default: Z:/the_stack_data)')
    parser.add_argument('--tiers', type=int, nargs='+', default=[1, 2],
                        help='Tier di linguaggi da scaricare (1-4)')
    parser.add_argument('--languages', type=str, nargs='+', default=[],
                        help='Linguaggi specifici da scaricare (sovrascrive tiers)')
    parser.add_argument('--skip_languages', type=str, nargs='+', default=[],
                        help='Linguaggi da saltare')
    parser.add_argument('--wait_time', type=int, default=2,
                        help='Tempo di attesa tra le richieste API in secondi (default: 2)')
    parser.add_argument('--max_repos', type=int, default=100,
                        help='Numero massimo di repository da scaricare per linguaggio (default: 100)')
    parser.add_argument('--max_files', type=int, default=2000,
                        help='Numero massimo di file da scaricare per linguaggio (default: 2000)')
    parser.add_argument('--github_token', type=str, default=None,
                        help='Token di autenticazione GitHub (opzionale ma consigliato)')
    parser.add_argument('--workers', type=int, default=5,
                        help='Numero di worker per il download parallelo (default: 5)')
    parser.add_argument('--retry_count', type=int, default=3,
                        help='Numero di tentativi per le richieste fallite (default: 3)')
    parser.add_argument('--use_alternatives', action='store_true',
                        help='Usa fonti alternative per linguaggi problematici come SQL')
    return parser.parse_args()

def search_github_repos(language, token=None, max_repos=100, retry_count=3):
    """Cerca repository su GitHub per un linguaggio specifico con più query e retry"""
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"
    
    queries = LANGUAGE_QUERIES.get(language, [f"language:{language} stars:>10"])
    
    repos = []
    
    for query in queries:
        if len(repos) >= max_repos:
            break
            
        url = f"https://api.github.com/search/repositories?q={query}&sort=stars&order=desc&per_page=100"
        
        for attempt in range(retry_count):
            try:
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    items = data.get("items", [])
                    
                    if not items:
                        break
                    
                    # Filtra repository già aggiunti
                    new_repos = [repo["full_name"] for repo in items if repo["full_name"] not in repos]
                    repos.extend(new_repos)
                    
                    if len(repos) >= max_repos:
                        repos = repos[:max_repos]
                        break
                    
                    # Rispetta i limiti di rate dell'API GitHub
                    if "X-RateLimit-Remaining" in response.headers and int(response.headers["X-RateLimit-Remaining"]) < 10:
                        logger.warning("Rate limit GitHub quasi raggiunto. Attesa di 60 secondi...")
                        time.sleep(60)
                    
                    break  # Richiesta riuscita, esci dal ciclo di retry
                elif response.status_code == 403:
                    logger.warning(f"Rate limit raggiunto (403). Attesa di 60 secondi... (tentativo {attempt+1}/{retry_count})")
                    time.sleep(60)
                else:
                    logger.error(f"Errore nella ricerca dei repository: {response.status_code}")
                    logger.error(response.text)
                    break
            except Exception as e:
                logger.error(f"Errore durante la ricerca dei repository: {e}")
                if attempt < retry_count - 1:
                    logger.info(f"Nuovo tentativo in 5 secondi... (tentativo {attempt+1}/{retry_count})")
                    time.sleep(5)
                else:
                    break
            
            time.sleep(2)  # Pausa tra i tentativi
    
    return repos

def get_repo_files(repo, language, token=None, max_files=1000, retry_count=3):
    """Ottiene i file di un repository specifico per un linguaggio con retry"""
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"
    
    extensions = LANGUAGE_EXTENSIONS.get(language, [f".{language}"])
    
    for attempt in range(retry_count):
        try:
            # Ottieni la struttura del repository
            url = f"https://api.github.com/repos/{repo}/git/trees/master?recursive=1"
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                
                # Filtra i file per estensione
                files = []
                for item in data.get("tree", []):
                    if item["type"] == "blob" and any(item["path"].endswith(ext) for ext in extensions):
                        files.append({"path": item["path"], "url": item["url"]})
                        
                        if len(files) >= max_files:
                            break
                
                # Seleziona casualmente se ci sono troppi file
                if len(files) > max_files:
                    files = random.sample(files, max_files)
                
                return files
            elif response.status_code == 404:
                # Prova con "main" se "master" non funziona
                url = f"https://api.github.com/repos/{repo}/git/trees/main?recursive=1"
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Filtra i file per estensione
                    files = []
                    for item in data.get("tree", []):
                        if item["type"] == "blob" and any(item["path"].endswith(ext) for ext in extensions):
                            files.append({"path": item["path"], "url": item["url"]})
                            
                            if len(files) >= max_files:
                                break
                    
                    # Seleziona casualmente se ci sono troppi file
                    if len(files) > max_files:
                        files = random.sample(files, max_files)
                    
                    return files
                else:
                    # Prova con "development" se "main" non funziona
                    url = f"https://api.github.com/repos/{repo}/git/trees/development?recursive=1"
                    response = requests.get(url, headers=headers)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Filtra i file per estensione
                        files = []
                        for item in data.get("tree", []):
                            if item["type"] == "blob" and any(item["path"].endswith(ext) for ext in extensions):
                                files.append({"path": item["path"], "url": item["url"]})
                                
                                if len(files) >= max_files:
                                    break
                        
                        # Seleziona casualmente se ci sono troppi file
                        if len(files) > max_files:
                            files = random.sample(files, max_files)
                        
                        return files
            
            elif response.status_code == 403:
                logger.warning(f"Rate limit raggiunto (403). Attesa di 60 secondi... (tentativo {attempt+1}/{retry_count})")
                time.sleep(60)
            else:
                logger.warning(f"Non è possibile ottenere la struttura del repository {repo}: {response.status_code}")
                if attempt < retry_count - 1:
                    logger.info(f"Nuovo tentativo in 5 secondi... (tentativo {attempt+1}/{retry_count})")
                    time.sleep(5)
                else:
                    return []
        
        except Exception as e:
            logger.error(f"Errore durante l'ottenimento dei file dal repository {repo}: {e}")
            if attempt < retry_count - 1:
                logger.info(f"Nuovo tentativo in 5 secondi... (tentativo {attempt+1}/{retry_count})")
                time.sleep(5)
            else:
                return []
    
    return []

def download_file_content(repo, file_info, token=None, retry_count=3):
    """Scarica il contenuto di un file da GitHub con retry"""
    headers = {"Accept": "application/vnd.github.v3.raw"}
    if token:
        headers["Authorization"] = f"token {token}"
    
    for attempt in range(retry_count):
        try:
            response = requests.get(file_info["url"], headers=headers)
            
            if response.status_code == 200:
                try:
                    # Prova a decodificare il contenuto base64 se presente
                    content_data = response.json()
                    if "content" in content_data and content_data.get("encoding") == "base64":
                        content = base64.b64decode(content_data["content"]).decode('utf-8', errors='replace')
                    else:
                        content = response.text
                except:
                    # Se non è JSON, usa direttamente il testo
                    content = response.text
                    
                return {
                    "repo": repo,
                    "path": file_info["path"],
                    "content": content
                }
            elif response.status_code == 403:
                logger.warning(f"Rate limit raggiunto (403). Attesa di 60 secondi... (tentativo {attempt+1}/{retry_count})")
                time.sleep(60)
            else:
                logger.warning(f"Non è possibile scaricare il file {file_info['path']} da {repo}: {response.status_code}")
                if attempt < retry_count - 1:
                    logger.info(f"Nuovo tentativo in 5 secondi... (tentativo {attempt+1}/{retry_count})")
                    time.sleep(5)
                else:
                    return None
        
        except Exception as e:
            logger.error(f"Errore durante il download del file {file_info['path']} da {repo}: {e}")
            if attempt < retry_count - 1:
                logger.info(f"Nuovo tentativo in 5 secondi... (tentativo {attempt+1}/{retry_count})")
                time.sleep(5)
            else:
                return None
    
    return None

def download_alternative_sql_sources(save_dir, token=None, wait_time=2, workers=5, retry_count=3):
    """Scarica file SQL da fonti alternative"""
    sql_dir = os.path.join(save_dir, "sql")
    os.makedirs(sql_dir, exist_ok=True)
    
    total_files = 0
    
    for source in SQL_ALTERNATIVE_SOURCES:
        repo_name = source["url"].split("/")[-2] + "/" + source["url"].split("/")[-1]
        repo_dir = os.path.join(sql_dir, repo_name.replace("/", "_"))
        os.makedirs(repo_dir, exist_ok=True)
        
        logger.info(f"Download di file SQL da {source['name']} ({source['url']})...")
        
        # Ottieni i file del repository
        files = get_repo_files(repo_name, "sql", token, max_files=500, retry_count=retry_count)
        
        if not files:
            logger.warning(f"Nessun file trovato in {source['name']} ({source['url']})")
            continue
        
        logger.info(f"Trovati {len(files)} file in {source['name']}")
        
        # Download parallelo dei file
        downloaded = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_file = {executor.submit(download_file_content, repo_name, file, token, retry_count): file for file in files}
            
            for future in as_completed(future_to_file):
                file_info = future_to_file[future]
                try:
                    result = future.result()
                    if result:
                        # Salva il file
                        file_path = os.path.join(repo_dir, os.path.basename(file_info["path"]))
                        with open(file_path, 'w', encoding='utf-8', errors='replace') as f:
                            f.write(result["content"])
                        
                        downloaded += 1
                        total_files += 1
                        
                        # Log ogni 10 file
                        if downloaded % 10 == 0:
                            logger.info(f"Scaricati {downloaded}/{len(files)} file da {source['name']}")
                except Exception as e:
                    logger.error(f"Errore durante il download di {file_info['path']}: {e}")
        
        logger.info(f"Scaricati {downloaded} file da {source['name']}")
        time.sleep(wait_time)
    
    logger.info(f"Totale file SQL scaricati da fonti alternative: {total_files}")
    return total_files

def download_language_code(language, save_dir, token=None, max_repos=100, max_files=1000, wait_time=2, workers=5, retry_count=3, use_alternatives=False):
    """Scarica codice per un linguaggio specifico"""
    lang_dir = os.path.join(save_dir, language)
    os.makedirs(lang_dir, exist_ok=True)
    
    # Per SQL, usa fonti alternative se richiesto
    if language == "sql" and use_alternatives:
        logger.info("Utilizzo fonti alternative per SQL...")
        return download_alternative_sql_sources(save_dir, token, wait_time, workers, retry_count)
    
    try:
        logger.info(f"Ricerca repository per {language}...")
        repos = search_github_repos(language, token, max_repos, retry_count)
        
        if not repos:
            logger.error(f"Nessun repository trovato per {language}")
            return False
        
        logger.info(f"Trovati {len(repos)} repository per {language}")
        
        total_files = 0
        total_repos_with_files = 0
        
        for repo_idx, repo in enumerate(repos):
            logger.info(f"Elaborazione repository {repo_idx+1}/{len(repos)}: {repo}")
            
            # Crea directory per il repository
            repo_dir = os.path.join(lang_dir, repo.replace("/", "_"))
            os.makedirs(repo_dir, exist_ok=True)
            
            # Ottieni i file del repository
            files = get_repo_files(repo, language, token, max_files // max_repos, retry_count)
            
            if not files:
                logger.warning(f"Nessun file trovato nel repository {repo}")
                continue
            
            logger.info(f"Trovati {len(files)} file in {repo}")
            
            # Download parallelo dei file
            downloaded = 0
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_file = {executor.submit(download_file_content, repo, file, token, retry_count): file for file in files}
                
                for future in as_completed(future_to_file):
                    file_info = future_to_file[future]
                    try:
                        result = future.result()
                        if result:
                            # Salva il file
                            file_path = os.path.join(repo_dir, os.path.basename(file_info["path"]))
                            with open(file_path, 'w', encoding='utf-8', errors='replace') as f:
                                f.write(result["content"])
                            
                            downloaded += 1
                            total_files += 1
                            
                            # Log ogni 10 file
                            if downloaded % 10 == 0:
                                logger.info(f"Scaricati {downloaded}/{len(files)} file da {repo}")
                    except Exception as e:
                        logger.error(f"Errore durante il download di {file_info['path']}: {e}")
            
            if downloaded > 0:
                total_repos_with_files += 1
                logger.info(f"Scaricati {downloaded} file da {repo}")
            
            # Pausa tra i repository
            if repo_idx < len(repos) - 1:
                logger.info(f"Attesa di {wait_time} secondi prima del prossimo repository...")
                time.sleep(wait_time)
            
            # Se abbiamo raggiunto il limite di file, interrompi
            if total_files >= max_files:
                logger.info(f"Raggiunto il limite di {max_files} file per {language}")
                break
        
        logger.info(f"✓ {language} scaricato con successo: {total_files} file da {total_repos_with_files} repository")
        return total_files > 0
    
    except Exception as e:
        logger.error(f"× Errore nel download di {language}: {e}")
        return False

def main():
    args = parse_arguments()
    
    # Crea la directory di base
    os.makedirs(args.save_dir, exist_ok=True)
    logger.info(f"Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    
    # Determina quali linguaggi scaricare
    languages_to_download = []
    
    if args.languages:
        # Se sono specificati linguaggi specifici, usa quelli
        languages_to_download = args.languages
        logger.info(f"Linguaggi specifici selezionati: {languages_to_download}")
    else:
        # Altrimenti usa i tier specificati
        for tier in args.tiers:
            if tier in TIERS:
                languages_to_download.extend(TIERS[tier])
                logger.info(f"Aggiunto Tier {tier}: {TIERS[tier]}")
            else:
                logger.warning(f"Tier {tier} non valido. I tier validi sono 1-4.")
    
    # Rimuovi i linguaggi da saltare
    if args.skip_languages:
        languages_to_download = [lang for lang in languages_to_download if lang not in args.skip_languages]
        logger.info(f"Linguaggi saltati: {args.skip_languages}")
    
    # Rimuovi duplicati
    languages_to_download = list(dict.fromkeys(languages_to_download))
    
    # Mostra riepilogo
    logger.info(f"Verranno scaricati {len(languages_to_download)} linguaggi: {languages_to_download}")
    logger.info(f"Max repository per linguaggio: {args.max_repos}")
    logger.info(f"Max file per linguaggio: {args.max_files}")
    
    # Inizia il download
    successful = 0
    failed = 0
    
    for idx, lang in enumerate(languages_to_download):
        logger.info(f"Progresso: {idx+1}/{len(languages_to_download)} - Scaricamento di {lang}")
        if download_language_code(lang, args.save_dir, args.github_token, args.max_repos, args.max_files, args.wait_time, args.workers, args.retry_count, args.use_alternatives):
            successful += 1
        else:
            failed += 1
    
    # Riepilogo finale
    logger.info("=" * 50)
    logger.info(f"Download completato. Risultati:")
    logger.info(f"- Linguaggi scaricati con successo: {successful}")
    logger.info(f"- Linguaggi falliti: {failed}")
    logger.info(f"- Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
