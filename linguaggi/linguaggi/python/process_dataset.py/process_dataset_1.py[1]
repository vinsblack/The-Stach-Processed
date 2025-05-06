#!/usr/bin/env python3
"""
Dataset Processing Script
Questo script analizza e prepara il dataset scaricato per l'addestramento di modelli AI.
Esegue pulizia dei dati, deduplicazione e genera statistiche sul dataset.
"""

import os
import sys
import json
import argparse
import logging
import shutil
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from datetime import datetime
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dataset_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Estensioni dei file per linguaggio
FILE_EXTENSIONS = {
    "python": [".py", ".pyx", ".pyw", ".ipynb"],
    "javascript": [".js", ".jsx", ".ts", ".tsx"],
    "java": [".java"],
    "c": [".c", ".h"],
    "cpp": [".cpp", ".cc", ".cxx", ".hpp", ".hxx", ".h"],
    "sql": [".sql"],
    "csharp": [".cs"],
    "go": [".go"],
    "rust": [".rs"],
    "php": [".php"],
    "ruby": [".rb"],
    "swift": [".swift"],
    "kotlin": [".kt", ".kts"],
    "scala": [".scala"],
    "haskell": [".hs", ".lhs"],
    "julia": [".jl"],
    "shell": [".sh", ".bash"],
    "r": [".r", ".R"],
    "lua": [".lua"],
    "elixir": [".ex", ".exs"],
    "dart": [".dart"]
}

# File binari o da escludere
EXCLUDE_PATTERNS = [
    # Binari
    r"\.(exe|bin|obj|dll|so|a|o|class|jar|war|ear|zip|tar|gz|rar|7z)$",
    # Cache e oggetti compilati
    r"\.(pyc|pyo|cache|min\.js)$",
    # File da ignorare nei repository
    r"(\.git|\.svn|node_modules|venv|__pycache__|\.gradle|\.idea|\.vscode)",
    # File di lock o dipendenze
    r"(package-lock\.json|yarn\.lock|Gemfile\.lock|poetry\.lock)",
    # File di risorse
    r"\.(jpg|jpeg|png|gif|bmp|svg|ico|mp3|mp4|avi|mov|pdf|doc|docx|xls|xlsx|ppt|pptx)$",
]

def parse_arguments():
    parser = argparse.ArgumentParser(description='Processa e prepara il dataset scaricato')
    parser.add_argument('--data_dir', type=str, default="Z:/the_stack_data",
                        help='Directory contenente i dati scaricati')
    parser.add_argument('--output_dir', type=str, default="Z:/the_stack_processed",
                        help='Directory di output per i dati processati')
    parser.add_argument('--workers', type=int, default=4,
                        help='Numero di processi per l\'elaborazione parallela')
    parser.add_argument('--max_file_size', type=int, default=1024*1024,
                        help='Dimensione massima dei file in bytes (default: 1MB)')
    parser.add_argument('--only_analyze', action='store_true',
                        help='Esegui solo l\'analisi senza copiare i file')
    parser.add_argument('--languages', type=str, nargs='+',
                        help='Lingue specifiche da processare (opzionale)')
    parser.add_argument('--deduplicate', action='store_true',
                        help='Esegui deduplicazione dei file identici')
    return parser.parse_args()

def should_exclude(file_path):
    """Verifica se il file deve essere escluso basandosi sul percorso"""
    for pattern in EXCLUDE_PATTERNS:
        if re.search(pattern, file_path):
            return True
    return False

def get_language_by_extension(file_path):
    """Identifica il linguaggio basandosi sull'estensione del file"""
    ext = os.path.splitext(file_path)[1].lower()
    for lang, extensions in FILE_EXTENSIONS.items():
        if ext in extensions:
            return lang
    return None

def is_valid_file(file_path, max_size):
    """Verifica se il file è valido per l'inclusione nel dataset"""
    try:
        # Verifica se il file deve essere escluso
        if should_exclude(file_path):
            return False
        
        # Verifica la dimensione del file
        file_size = os.path.getsize(file_path)
        if file_size > max_size:
            return False
        
        # Verifica se il file è vuoto
        if file_size == 0:
            return False
        
        # Controlla se il file è binario
        with open(file_path, 'rb') as f:
            try:
                content = f.read(1024)  # Leggi i primi 1024 bytes
                return b'\0' not in content  # File non binario se non contiene byte null
            except (UnicodeDecodeError, IOError):
                return False
                
    except Exception as e:
        logger.error(f"Errore nell'analisi del file {file_path}: {e}")
        return False
    
    return True

def process_directory(dir_path, output_dir, max_file_size, only_analyze=False, deduplicate=False):
    """Processa tutti i file in una directory"""
    stats = {
        "total_files": 0,
        "processed_files": 0,
        "skipped_files": 0,
        "by_language": defaultdict(int),
        "by_reason": defaultdict(int)
    }
    
    # Set per deduplicazione
    seen_hashes = set() if deduplicate else None
    
    for root, _, files in os.walk(dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            stats["total_files"] += 1
            
            try:
                # Ottieni il linguaggio
                language = get_language_by_extension(file_path)
                
                # Salta se non è un file di codice
                if not language:
                    stats["skipped_files"] += 1
                    stats["by_reason"]["no_language"] += 1
                    continue
                
                # Verifica validità del file
                if not is_valid_file(file_path, max_file_size):
                    stats["skipped_files"] += 1
                    stats["by_reason"]["invalid_file"] += 1
                    continue
                
                # Se facciamo solo analisi, incrementa counter e salta
                if only_analyze:
                    stats["processed_files"] += 1
                    stats["by_language"][language] += 1
                    continue
                
                # Se deduplicazione è attiva, calcola hash del file
                if deduplicate:
                    with open(file_path, 'rb') as f:
                        file_hash = hash(f.read())
                    
                    if file_hash in seen_hashes:
                        stats["skipped_files"] += 1
                        stats["by_reason"]["duplicate"] += 1
                        continue
                    
                    seen_hashes.add(file_hash)
                
                # Copia il file nella directory di output
                language_dir = os.path.join(output_dir, language)
                os.makedirs(language_dir, exist_ok=True)
                
                # Genera un nuovo nome file per evitare conflitti
                repo_name = os.path.basename(dir_path)
                rel_path = os.path.relpath(file_path, dir_path)
                safe_path = rel_path.replace('/', '_').replace('\\', '_')
                new_file_path = os.path.join(language_dir, f"{repo_name}_{safe_path}")
                
                # Copia il file
                shutil.copy2(file_path, new_file_path)
                
                stats["processed_files"] += 1
                stats["by_language"][language] += 1
            
            except Exception as e:
                logger.error(f"Errore nel processare {file_path}: {e}")
                stats["skipped_files"] += 1
                stats["by_reason"]["error"] += 1
    
    return stats

def main():
    args = parse_arguments()
    
    start_time = datetime.now()
    logger.info(f"Inizio elaborazione dataset alle {start_time}")
    
    # Crea directory di output se non in modalità solo analisi
    if not args.only_analyze:
        os.makedirs(args.output_dir, exist_ok=True)
        logger.info(f"Directory di output: {os.path.abspath(args.output_dir)}")
    
    # Trova tutte le directory dei linguaggi o delle repository
    if args.languages:
        language_dirs = [os.path.join(args.data_dir, lang) for lang in args.languages if os.path.exists(os.path.join(args.data_dir, lang))]
    else:
        language_dirs = [os.path.join(args.data_dir, d) for d in os.listdir(args.data_dir) if os.path.isdir(os.path.join(args.data_dir, d))]
    
    logger.info(f"Trovate {len(language_dirs)} directory da processare")
    
    # Statistiche totali
    total_stats = {
        "total_files": 0,
        "processed_files": 0,
        "skipped_files": 0,
        "by_language": defaultdict(int),
        "by_reason": defaultdict(int)
    }
    
    # Processa le directory in parallelo
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        
        for lang_dir in language_dirs:
            logger.info(f"Avvio processamento di {lang_dir}")
            future = executor.submit(
                process_directory, 
                lang_dir, 
                args.output_dir, 
                args.max_file_size,
                args.only_analyze,
                args.deduplicate
            )
            futures.append(future)
        
        # Raccogli i risultati
        for future in futures:
            try:
                stats = future.result()
                
                # Aggiorna le statistiche totali
                total_stats["total_files"] += stats["total_files"]
                total_stats["processed_files"] += stats["processed_files"]
                total_stats["skipped_files"] += stats["skipped_files"]
                
                for lang, count in stats["by_language"].items():
                    total_stats["by_language"][lang] += count
                
                for reason, count in stats["by_reason"].items():
                    total_stats["by_reason"][reason] += count
                
            except Exception as e:
                logger.error(f"Errore nell'elaborazione di una directory: {e}")
    
    # Stampa le statistiche finali
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("=" * 50)
    logger.info(f"Elaborazione completata in {duration}")
    logger.info(f"File totali analizzati: {total_stats['total_files']}")
    logger.info(f"File processati: {total_stats['processed_files']}")
    logger.info(f"File saltati: {total_stats['skipped_files']}")
    
    # Statistiche per linguaggio
    logger.info("\nDistribuzione per linguaggio:")
    for lang, count in sorted(total_stats["by_language"].items(), key=lambda x: x[1], reverse=True):
        logger.info(f"- {lang}: {count} file")
    
    # Statistiche per motivo di esclusione
    logger.info("\nMotivi di esclusione:")
    for reason, count in sorted(total_stats["by_reason"].items(), key=lambda x: x[1], reverse=True):
        logger.info(f"- {reason}: {count} file")
    
    # Salva le statistiche in JSON
    stats_file = os.path.join(args.output_dir if not args.only_analyze else args.data_dir, "dataset_stats.json")
    with open(stats_file, 'w') as f:
        json.dump({
            "processing_date": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": duration.total_seconds(),
            "stats": total_stats
        }, f, indent=2)
    
    logger.info(f"\nStatistiche salvate in {stats_file}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
