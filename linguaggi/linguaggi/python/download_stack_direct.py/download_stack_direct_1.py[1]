import os
import time
import argparse
import logging
from datasets import load_dataset
from huggingface_hub import login
import json

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download_log.txt"),
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

# Dimensioni stimate per linguaggio (in GB)
ESTIMATED_SIZES = {
    "python": 300, "javascript": 250, "java": 180, "c": 150, "cpp": 180, "sql": 70,
    "csharp": 120, "go": 100, "rust": 90, "php": 90, "ruby": 70,
    "swift": 60, "kotlin": 50, "scala": 40, "shell": 40, "r": 30,
    "haskell": 20, "julia": 15, "lua": 15, "elixir": 15, "dart": 15
}

def parse_arguments():
    parser = argparse.ArgumentParser(description='Scarica linguaggi selezionati da The Stack dataset')
    parser.add_argument('--save_dir', type=str, default="Z:/the_stack_data",
                        help='Directory di base dove salvare i dati (default: Z:/the_stack_data)')
    parser.add_argument('--tiers', type=int, nargs='+', default=[1, 2],
                        help='Tier di linguaggi da scaricare (1-4)')
    parser.add_argument('--languages', type=str, nargs='+', default=[],
                        help='Linguaggi specifici da scaricare (sovrascrive tiers)')
    parser.add_argument('--skip_languages', type=str, nargs='+', default=[],
                        help='Linguaggi da saltare')
    parser.add_argument('--wait_time', type=int, default=30,
                        help='Tempo di attesa tra i download (secondi)')
    parser.add_argument('--max_size', type=int, default=None,
                        help='Dimensione massima totale da scaricare (GB)')
    parser.add_argument('--token', type=str, default=None,
                        help='Token di autenticazione Hugging Face')
    parser.add_argument('--sample_size', type=int, default=None,
                        help='Numero di esempi da scaricare per linguaggio (None = tutti)')
    return parser.parse_args()

def estimate_download_size(languages_to_download):
    """Stima la dimensione totale del download in GB"""
    total_size = sum(ESTIMATED_SIZES.get(lang, 50) for lang in languages_to_download)
    return total_size

def download_language(lang, save_dir, wait_time=30, sample_size=None):
    """Scarica un singolo linguaggio da The Stack"""
    lang_dir = os.path.join(save_dir, lang)
    os.makedirs(lang_dir, exist_ok=True)
    
    try:
        logger.info(f"Inizio download di {lang} (dimensione stimata: ~{ESTIMATED_SIZES.get(lang, 'N/A')}GB)")
        
        # Approccio diretto: carica il dataset completo e filtra per linguaggio
        logger.info(f"Caricamento del dataset The Stack v1.2 e filtraggio per {lang}...")
        
        # Utilizziamo la versione 1.2 del dataset che è più accessibile
        dataset = load_dataset("bigcode/the-stack-v1.2", split="train", streaming=True)
        
        # Filtra per linguaggio
        filtered_dataset = dataset.filter(lambda example: example.get("lang") == lang)
        
        # Limita il numero di esempi se specificato
        if sample_size:
            logger.info(f"Limitazione a {sample_size} esempi per {lang}")
            filtered_dataset = filtered_dataset.take(sample_size)
        
        # Salva il dataset su disco
        logger.info(f"Salvataggio di {lang} su disco...")
        
        # Crea un file JSON per ogni esempio
        count = 0
        for i, example in enumerate(filtered_dataset):
            file_path = os.path.join(lang_dir, f"example_{i}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(example, f, ensure_ascii=False, indent=2)
            count += 1
            
            # Log ogni 1000 esempi
            if count % 1000 == 0:
                logger.info(f"Salvati {count} esempi di {lang}...")
        
        logger.info(f"✓ {lang} scaricato con successo: {count} esempi salvati in {lang_dir}")
        return True
    except Exception as e:
        logger.error(f"× Errore nel download di {lang}: {e}")
        return False
    finally:
        if wait_time > 0:
            logger.info(f"Attesa di {wait_time} secondi prima del prossimo download...")
            time.sleep(wait_time)

def main():
    args = parse_arguments()
    
    # Imposta il token di autenticazione Hugging Face
    if args.token:
        logger.info("Impostazione del token Hugging Face...")
        os.environ["HF_TOKEN"] = args.token
        os.environ["HUGGING_FACE_HUB_TOKEN"] = args.token
        login(token=args.token)
    else:
        logger.warning("Nessun token di autenticazione fornito. Potrebbero verificarsi errori di autorizzazione.")
        logger.info("Per ottenere un token, visita: https://huggingface.co/settings/tokens")
        logger.info("Poi esegui lo script con: --token YOUR_TOKEN_HERE")
        
        proceed = input("Vuoi procedere senza token? (s/n): ").lower() == 's'
        if not proceed:
            logger.info("Download annullato dall'utente")
            return
    
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
    
    # Verifica la dimensione stimata
    estimated_size = estimate_download_size(languages_to_download)
    logger.info(f"Dimensione totale stimata: ~{estimated_size}GB")
    
    if args.max_size and estimated_size > args.max_size:
        logger.warning(f"La dimensione stimata ({estimated_size}GB) supera il limite specificato ({args.max_size}GB)")
        proceed = input("Vuoi procedere comunque? (s/n): ").lower() == 's'
        if not proceed:
            logger.info("Download annullato dall'utente")
            return
    
    # Mostra riepilogo
    logger.info(f"Verranno scaricati {len(languages_to_download)} linguaggi: {languages_to_download}")
    if args.sample_size:
        logger.info(f"Limitazione a {args.sample_size} esempi per linguaggio")
    
    # Inizia il download
    successful = 0
    failed = 0
    
    for lang in languages_to_download:
        logger.info(f"Progresso: {successful + failed}/{len(languages_to_download)} completati")
        if download_language(lang, args.save_dir, args.wait_time, args.sample_size):
            successful += 1
        else:
            failed += 1
    
    # Riepilogo finale
    logger.info("=" * 50)
    logger.info(f"Download completato. Risultati:")
    logger.info(f"- Linguaggi scaricati con successo: {successful}")
    logger.info(f"- Linguaggi falliti: {failed}")
    logger.info(f"- Dimensione totale stimata: ~{estimated_size}GB")
    logger.info(f"- Directory di salvataggio: {os.path.abspath(args.save_dir)}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
