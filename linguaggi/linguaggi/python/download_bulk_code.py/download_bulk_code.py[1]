import os
import time
import argparse
import logging
import requests
import json
import random
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import shutil

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download_bulk_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Definisci i tier di linguaggi
TIERS = {
    1: ["python", "javascript", "java", "c", "cpp", "sql"],  # Fondamentali
    2: ["csharp", "go", "rust", "php", "ruby"],              # Alta priorità
    3: ["swift", "kotlin", "scala", "shell", "r"],           # Selettivi
    4: ["haskell", "julia", "lua", "elixir", "dart"]         # Nicchia
}

# Dimensioni target per linguaggio (in GB)
TARGET_SIZES = {
    "python": 300, "javascript": 250, "java": 180, "c": 150, "cpp": 180, "sql": 70,
    "csharp": 120, "go": 100, "rust": 90, "php": 90, "ruby": 70,
    "swift": 60, "kotlin": 50, "scala": 40, "shell": 40, "r": 30,
    "haskell": 20, "julia": 15, "lua": 15, "elixir": 15, "dart": 15
}

# Repository popolari e grandi per ogni linguaggio
POPULAR_REPOS = {
    "python": [
        "tensorflow/tensorflow", "pytorch/pytorch", "django/django", "pallets/flask",
        "scikit-learn/scikit-learn", "pandas-dev/pandas", "numpy/numpy", "keras-team/keras",
        "ansible/ansible", "saltstack/salt", "odoo/odoo", "scrapy/scrapy",
        "psf/requests", "python/cpython", "openai/openai-python", "huggingface/transformers"
    ],
    "javascript": [
        "facebook/react", "vuejs/vue", "angular/angular", "vercel/next.js",
        "facebook/react-native", "nodejs/node", "mui-org/material-ui", "d3/d3",
        "axios/axios", "expressjs/express", "webpack/webpack", "jquery/jquery",
        "lodash/lodash", "moment/moment", "chartjs/Chart.js", "mrdoob/three.js"
    ],
    "java": [
        "elastic/elasticsearch", "spring-projects/spring-boot", "google/guava",
        "apache/hadoop", "apache/kafka", "square/retrofit", "square/okhttp",
        "spring-projects/spring-framework", "apache/dubbo", "netty/netty",
        "iluwatar/java-design-patterns", "ReactiveX/RxJava", "apache/flink",
        "google/gson", "apache/cassandra", "apache/spark"
    ],
    "c": [
        "torvalds/linux", "git/git", "FFmpeg/FFmpeg", "php/php-src",
        "nginx/nginx", "redis/redis", "postgres/postgres", "antirez/redis",
        "ggreer/the_silver_searcher", "curl/curl", "libuv/libuv",
        "tmux/tmux", "vim/vim", "jgm/cmark", "openssl/openssl"
    ],
    "cpp": [
        "electron/electron", "microsoft/terminal", "opencv/opencv",
        "apple/swift", "google/protobuf", "tensorflow/tensorflow",
        "facebook/rocksdb", "google/leveldb", "bitcoin/bitcoin",
        "microsoft/calculator", "BVLC/caffe", "tesseract-ocr/tesseract",
        "godotengine/godot", "nlohmann/json", "grpc/grpc", "microsoft/CNTK"
    ],
    "sql": [
        "postgres/postgres", "mysql/mysql-server", "cockroachdb/cockroach",
        "MariaDB/server", "h2database/h2database", "sqlitebrowser/sqlitebrowser",
        "dbeaver/dbeaver", "liquibase/liquibase", "flyway/flyway",
        "sqlmapproject/sqlmap", "prestodb/presto", "apache/drill",
        "timescale/timescaledb", "citusdata/citus", "apache/hive"
    ]
}

def parse_arguments():
    parser = argparse.ArgumentParser(description='Scarica grandi quantità di codice per addestrare modelli AI')
    parser.add_argument('--save_dir', type=str, default="Z:/the_stack_data",
                        help='Directory di base dove salvare i dati (default: Z:/the_stack_data)')
    parser.add_argument('--languages', type=str, nargs='+', default=TIERS[1],
                        help=f'Linguaggi da scaricare (default: Tier 1)')
    parser.add_argument('--github_token', type=str, default=None,
                        help='Token di autenticazione GitHub (opzionale ma consigliato)')
    parser.add_argument('--clone_depth', type=int, default=1,
                        help='Profondità di clonazione dei repository (default: 1)')
    parser.add_argument('--max_repos_per_language', type=int, default=50,
                        help='Numero massimo di repository da scaricare per linguaggio (default: 50)')
    parser.add_argument('--use_popular_repos', action='store_true',
                        help='Usa repository popolari predefiniti')
    parser.add_argument('--workers', type=int, default=5,
                        help='Numero di worker per il download parallelo (default: 5)')
    parser.add_argument('--clean_after_clone', action='store_true',
                        help='Rimuovi directory .git dopo la clonazione per risparmiare spazio')
    return parser.parse_args()

def search_large_repos(language, token=None, max_repos=50):
    """Cerca repository grandi su GitHub per un linguaggio specifico"""
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"
    
    # Query per trovare repository grandi
    queries = [
        f"language:{language} stars:>1000 size:>50000",
        f"language:{language} stars:>500 size:>100000",
        f"language:{language} stars:>100 size:>500000",
        f"language:{language} stars:>50 size:>1000000"
    ]
    
    repos = []
    
    for query in queries:
        if len(repos) >= max_repos:
            break
            
        url = f"https://api.github.com/search/repositories?q={query}&sort=stars&order=desc&per_page=100"
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                
                for repo in items:
                    if repo["full_name"] not in repos:
                        repos.append(repo["full_name"])
                        
                        if len(repos) >= max_repos:
                            break
            elif response.status_code == 403:
                logger.warning(f"Rate limit raggiunto (403). Attesa di 60 secondi...")
                time.sleep(60)
            else:
                logger.error(f"Errore nella ricerca dei repository: {response.status_code}")
                logger.error(response.text)
        except Exception as e:
            logger.error(f"Errore durante la ricerca dei repository: {e}")
        
        time.sleep(2)  # Pausa tra le query
    
    return repos

def clone_repository(repo, save_dir, depth=1, clean_after_clone=False):
    """Clona un repository GitHub"""
    repo_dir = os.path.join(save_dir, repo.replace("/", "_"))
    
    if os.path.exists(repo_dir):
        logger.info(f"Repository {repo} già esistente in {repo_dir}")
        return True
    
    try:
        logger.info(f"Clonazione di {repo} in {repo_dir}...")
        
        # Clona il repository con la profondità specificata
        cmd = ["git", "clone", "--depth", str(depth), f"https://github.com/{repo}.git", repo_dir]
        
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            logger.error(f"Errore durante la clonazione di {repo}: {stderr.decode('utf-8', errors='replace')}")
            return False
        
        # Rimuovi la directory .git se richiesto
        if clean_after_clone:
            git_dir = os.path.join(repo_dir, ".git")
            if os.path.exists(git_dir):
                logger.info(f"Rimozione della directory .git da {repo_dir}...")
                shutil.rmtree(git_dir)
        
        logger.info(f"Repository {repo} clonato con successo")
        return True
    
    except Exception as e:
        logger.error(f"Errore durante la clonazione di {repo}: {e}")
        return False

def download_language_code(language, save_dir, token=None, max_repos=50, depth=1, use_popular_repos=False, clean_after_clone=False, workers=5):
    """Scarica codice per un linguaggio specifico"""
    lang_dir = os.path.join(save_dir, language)
    os.makedirs(lang_dir, exist_ok=True)
    
    try:
        # Determina i repository da scaricare
        if use_popular_repos and language in POPULAR_REPOS:
            repos = POPULAR_REPOS[language][:max_repos]
            logger.info(f"Utilizzo di {len(repos)} repository popolari predefiniti per {language}")
        else:
            logger.info(f"Ricerca repository grandi per {language}...")
            repos = search_large_repos(language, token, max_repos)
        
        if not repos:
            logger.error(f"Nessun repository trovato per {language}")
            return False
        
        logger.info(f"Trovati {len(repos)} repository per {language}")
        
        # Clona i repository in parallelo
        successful = 0
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_repo = {executor.submit(clone_repository, repo, lang_dir, depth, clean_after_clone): repo for repo in repos}
            
            for future in as_completed(future_to_repo):
                repo = future_to_repo[future]
                try:
                    if future.result():
                        successful += 1
                except Exception as e:
                    logger.error(f"Errore durante la clonazione di {repo}: {e}")
        
        logger.info(f"✓ {language}: {successful}/{len(repos)} repository clonati con successo")
        return successful > 0
    
    except Exception as e:
        logger.error(f"× Errore nel download di {language}: {e}")
        return False

def main():
    args = parse_arguments()
    
    # Crea la directory di base
    os.makedirs(args.save_dir, exist_ok=True)
    logger.info(f"Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    
    # Mostra riepilogo
    logger.info(f"Verranno scaricati {len(args.languages)} linguaggi: {args.languages}")
    logger.info(f"Max repository per linguaggio: {args.max_repos_per_language}")
    logger.info(f"Profondità di clonazione: {args.clone_depth}")
    logger.info(f"Usa repository popolari: {args.use_popular_repos}")
    logger.info(f"Rimuovi .git dopo clonazione: {args.clean_after_clone}")
    
    # Inizia il download
    successful = 0
    failed = 0
    
    for idx, lang in enumerate(args.languages):
        logger.info(f"Progresso: {idx+1}/{len(args.languages)} - Scaricamento di {lang}")
        if download_language_code(
            lang, 
            args.save_dir, 
            args.github_token, 
            args.max_repos_per_language, 
            args.clone_depth, 
            args.use_popular_repos, 
            args.clean_after_clone, 
            args.workers
        ):
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
