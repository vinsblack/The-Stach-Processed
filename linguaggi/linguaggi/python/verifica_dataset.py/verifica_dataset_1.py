import os
import argparse
import logging
import json
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("verifica_dataset_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Definisci i tier di linguaggi con dimensioni target
TIERS = {
    1: {  # Fondamentali
        "python": 300,       # GB
        "javascript": 250,   # GB
        "java": 180,         # GB
        "c": 150,            # GB
        "cpp": 180,          # GB
        "sql": 70            # GB
    },
    2: {  # Alta priorità
        "csharp": 120,       # GB
        "go": 100,           # GB
        "rust": 90,          # GB
        "php": 90,           # GB
        "ruby": 70           # GB
    },
    3: {  # Selettivi
        "swift": 60,         # GB
        "kotlin": 50,        # GB
        "scala": 40,         # GB
        "shell": 40,         # GB
        "r": 30              # GB
    },
    4: {  # Nicchia
        "haskell": 20,       # GB
        "julia": 15,         # GB
        "lua": 15,           # GB
        "elixir": 15,        # GB
        "dart": 15           # GB
    }
}

def parse_arguments():
    parser = argparse.ArgumentParser(description='Verifica lo stato del dataset')
    parser.add_argument('--data_dir', type=str, default="Z:/the_stack_data",
                        help='Directory del dataset (default: Z:/the_stack_data)')
    parser.add_argument('--output', type=str, default="stato_dataset.json",
                        help='File di output per il report (default: stato_dataset.json)')
    parser.add_argument('--generate_plots', action='store_true',
                        help='Genera grafici per visualizzare lo stato del dataset')
    return parser.parse_args()

def get_directory_size(path):
    """Calcola la dimensione di una directory in bytes"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp):
                total_size += os.path.getsize(fp)
    return total_size

def count_files(path):
    """Conta il numero di file in una directory"""
    file_count = 0
    for dirpath, dirnames, filenames in os.walk(path):
        file_count += len(filenames)
    return file_count

def count_repositories(path):
    """Conta il numero di repository (directory di primo livello) in una directory"""
    if not os.path.exists(path):
        return 0
    return len([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])

def analyze_dataset(data_dir):
    """Analizza lo stato del dataset"""
    report = {
        "languages": {},
        "complementary": {},
        "total_size_bytes": 0,
        "total_size_gb": 0,
        "total_files": 0,
        "total_repositories": 0
    }
    
    # Analizza i linguaggi
    all_languages = {}
    for tier in TIERS.values():
        all_languages.update(tier)
    
    for lang, target_size in all_languages.items():
        lang_dir = os.path.join(data_dir, lang)
        
        if os.path.exists(lang_dir):
            size_bytes = get_directory_size(lang_dir)
            size_gb = size_bytes / (1024**3)
            files = count_files(lang_dir)
            repos = count_repositories(lang_dir)
            
            # Trova il tier del linguaggio
            lang_tier = None
            for tier, langs in TIERS.items():
                if lang in langs:
                    lang_tier = tier
                    break
            
            report["languages"][lang] = {
                "tier": lang_tier,
                "target_size_gb": target_size,
                "current_size_bytes": size_bytes,
                "current_size_gb": size_gb,
                "progress_percentage": (size_gb / target_size) * 100 if target_size > 0 else 0,
                "files": files,
                "repositories": repos
            }
            
            report["total_size_bytes"] += size_bytes
            report["total_files"] += files
            report["total_repositories"] += repos
    
    # Analizza i dataset complementari
    complementary_dir = os.path.join(data_dir, "complementary")
    if os.path.exists(complementary_dir):
        for comp_type in ["stackoverflow", "documentation", "tutorials"]:
            comp_dir = os.path.join(complementary_dir, comp_type)
            
            if os.path.exists(comp_dir):
                size_bytes = get_directory_size(comp_dir)
                size_gb = size_bytes / (1024**3)
                files = count_files(comp_dir)
                
                report["complementary"][comp_type] = {
                    "size_bytes": size_bytes,
                    "size_gb": size_gb,
                    "files": files
                }
                
                report["total_size_bytes"] += size_bytes
                report["total_files"] += files
    
    # Calcola la dimensione totale in GB
    report["total_size_gb"] = report["total_size_bytes"] / (1024**3)
    
    return report

def generate_tier_summary(report):
    """Genera un riepilogo per tier"""
    tier_summary = {
        1: {"languages": [], "target_size_gb": 0, "current_size_gb": 0, "progress_percentage": 0, "files": 0, "repositories": 0},
        2: {"languages": [], "target_size_gb": 0, "current_size_gb": 0, "progress_percentage": 0, "files": 0, "repositories": 0},
        3: {"languages": [], "target_size_gb": 0, "current_size_gb": 0, "progress_percentage": 0, "files": 0, "repositories": 0},
        4: {"languages": [], "target_size_gb": 0, "current_size_gb": 0, "progress_percentage": 0, "files": 0, "repositories": 0}
    }
    
    for lang, data in report["languages"].items():
        tier = data["tier"]
        if tier is not None:
            tier_summary[tier]["languages"].append(lang)
            tier_summary[tier]["target_size_gb"] += data["target_size_gb"]
            tier_summary[tier]["current_size_gb"] += data["current_size_gb"]
            tier_summary[tier]["files"] += data["files"]
            tier_summary[tier]["repositories"] += data["repositories"]
    
    # Calcola le percentuali di progresso per tier
    for tier, data in tier_summary.items():
        if data["target_size_gb"] > 0:
            data["progress_percentage"] = (data["current_size_gb"] / data["target_size_gb"]) * 100
    
    return tier_summary

def generate_plots(report, output_prefix):
    """Genera grafici per visualizzare lo stato del dataset"""
    # Prepara i dati per i grafici
    langs = []
    current_sizes = []
    target_sizes = []
    progress = []
    
    for lang, data in sorted(report["languages"].items(), key=lambda x: x[1]["tier"]):
        langs.append(lang)
        current_sizes.append(data["current_size_gb"])
        target_sizes.append(data["target_size_gb"])
        progress.append(data["progress_percentage"])
    
    # Crea un DataFrame per facilitare la creazione dei grafici
    df = pd.DataFrame({
        "Linguaggio": langs,
        "Dimensione Attuale (GB)": current_sizes,
        "Dimensione Target (GB)": target_sizes,
        "Progresso (%)": progress
    })
    
    # Grafico 1: Dimensione attuale vs target
    plt.figure(figsize=(12, 8))
    bar_width = 0.35
    index = range(len(langs))
    
    plt.bar(index, df["Dimensione Target (GB)"], bar_width, label="Target", color="lightblue")
    plt.bar([i + bar_width for i in index], df["Dimensione Attuale (GB)"], bar_width, label="Attuale", color="darkblue")
    
    plt.xlabel("Linguaggio")
    plt.ylabel("Dimensione (GB)")
    plt.title("Dimensione Attuale vs Target per Linguaggio")
    plt.xticks([i + bar_width/2 for i in index], langs, rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_dimensioni.png")
    
    # Grafico 2: Progresso percentuale
    plt.figure(figsize=(12, 8))
    plt.bar(langs, progress, color="green")
    plt.axhline(y=100, color="red", linestyle="--")
    
    plt.xlabel("Linguaggio")
    plt.ylabel("Progresso (%)")
    plt.title("Percentuale di Completamento per Linguaggio")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_progresso.png")
    
    # Grafico 3: Riepilogo per tier
    tier_summary = generate_tier_summary(report)
    
    tiers = []
    tier_current = []
    tier_target = []
    tier_progress = []
    
    for tier, data in tier_summary.items():
        tiers.append(f"Tier {tier}")
        tier_current.append(data["current_size_gb"])
        tier_target.append(data["target_size_gb"])
        tier_progress.append(data["progress_percentage"])
    
    plt.figure(figsize=(10, 6))
    bar_width = 0.35
    index = range(len(tiers))
    
    plt.bar(index, tier_target, bar_width, label="Target", color="lightblue")
    plt.bar([i + bar_width for i in index], tier_current, bar_width, label="Attuale", color="darkblue")
    
    plt.xlabel("Tier")
    plt.ylabel("Dimensione (GB)")
    plt.title("Dimensione Attuale vs Target per Tier")
    plt.xticks([i + bar_width/2 for i in index], tiers)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_tier_dimensioni.png")
    
    # Grafico 4: Progresso percentuale per tier
    plt.figure(figsize=(10, 6))
    plt.bar(tiers, tier_progress, color="green")
    plt.axhline(y=100, color="red", linestyle="--")
    
    plt.xlabel("Tier")
    plt.ylabel("Progresso (%)")
    plt.title("Percentuale di Completamento per Tier")
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_tier_progresso.png")
    
    logger.info(f"Grafici salvati con prefisso: {output_prefix}")

def print_report(report):
    """Stampa un report leggibile sullo stato del dataset"""
    logger.info("=" * 80)
    logger.info("STATO DEL DATASET")
    logger.info("=" * 80)
    
    logger.info(f"Dimensione totale: {report['total_size_gb']:.2f} GB")
    logger.info(f"Totale file: {report['total_files']}")
    logger.info(f"Totale repository: {report['total_repositories']}")
    
    # Riepilogo per tier
    tier_summary = generate_tier_summary(report)
    
    for tier in sorted(tier_summary.keys()):
        data = tier_summary[tier]
        logger.info("\n" + "=" * 40)
        logger.info(f"TIER {tier}")
        logger.info("=" * 40)
        logger.info(f"Linguaggi: {', '.join(data['languages'])}")
        logger.info(f"Dimensione target: {data['target_size_gb']:.2f} GB")
        logger.info(f"Dimensione attuale: {data['current_size_gb']:.2f} GB")
        logger.info(f"Progresso: {data['progress_percentage']:.2f}%")
        logger.info(f"File: {data['files']}")
        logger.info(f"Repository: {data['repositories']}")
    
    # Dettagli per linguaggio
    logger.info("\n" + "=" * 40)
    logger.info("DETTAGLI PER LINGUAGGIO")
    logger.info("=" * 40)
    
    # Ordina i linguaggi per tier e poi per nome
    sorted_languages = sorted(
        report["languages"].items(),
        key=lambda x: (x[1]["tier"] if x[1]["tier"] is not None else 999, x[0])
    )
    
    for lang, data in sorted_languages:
        logger.info(f"\nLinguaggio: {lang} (Tier {data['tier']})")
        logger.info(f"  Dimensione target: {data['target_size_gb']:.2f} GB")
        logger.info(f"  Dimensione attuale: {data['current_size_gb']:.2f} GB")
        logger.info(f"  Progresso: {data['progress_percentage']:.2f}%")
        logger.info(f"  File: {data['files']}")
        logger.info(f"  Repository: {data['repositories']}")
    
    # Dettagli per dataset complementari
    if report["complementary"]:
        logger.info("\n" + "=" * 40)
        logger.info("DATASET COMPLEMENTARI")
        logger.info("=" * 40)
        
        for comp_type, data in report["complementary"].items():
            logger.info(f"\nTipo: {comp_type}")
            logger.info(f"  Dimensione: {data['size_gb']:.2f} GB")
            logger.info(f"  File: {data['files']}")

def main():
    args = parse_arguments()
    
    logger.info(f"Analisi del dataset in {args.data_dir}...")
    
    # Analizza il dataset
    report = analyze_dataset(args.data_dir)
    
    # Salva il report in formato JSON
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Report salvato in {args.output}")
    
    # Stampa il report
    print_report(report)
    
    # Genera grafici se richiesto
    if args.generate_plots:
        output_prefix = os.path.splitext(args.output)[0]
        generate_plots(report, output_prefix)

if __name__ == "__main__":
    main()
