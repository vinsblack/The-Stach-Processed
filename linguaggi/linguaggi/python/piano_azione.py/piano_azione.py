import os
import argparse
import logging
import json
from datetime import datetime

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("piano_azione_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Definizione dei tier di linguaggi con dimensioni stimate
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

# Dataset complementari
COMPLEMENTARY_DATASETS = {
    "stackoverflow": 250,    # GB
    "documentation": 125,    # GB
    "tutorials": 75          # GB
}

def parse_arguments():
    parser = argparse.ArgumentParser(description='Piano di azione per il download del dataset')
    parser.add_argument('--base_dir', type=str, default="Z:/the_stack_data",
                        help='Directory di base dove salvare i dati (default: Z:/the_stack_data)')
    parser.add_argument('--max_size', type=int, default=3500,
                        help='Dimensione massima totale in GB (default: 3500)')
    parser.add_argument('--tiers', type=int, nargs='+', default=[1, 2],
                        help='Tier di linguaggi da includere nel piano (default: 1 2)')
    parser.add_argument('--languages', type=str, nargs='+', default=[],
                        help='Linguaggi specifici da includere (sovrascrive tiers)')
    parser.add_argument('--skip_languages', type=str, nargs='+', default=[],
                        help='Linguaggi da escludere dal piano')
    parser.add_argument('--include_complementary', action='store_true',
                        help='Includi dataset complementari nel piano')
    parser.add_argument('--tier3_count', type=int, default=3,
                        help='Numero di linguaggi da selezionare dal Tier 3 (default: 3)')
    parser.add_argument('--tier4_count', type=int, default=2,
                        help='Numero di linguaggi da selezionare dal Tier 4 (default: 2)')
    parser.add_argument('--output', type=str, default="piano_azione.json",
                        help='File di output per il piano (default: piano_azione.json)')
    return parser.parse_args()

def generate_plan(args):
    """Genera un piano di azione per il download del dataset"""
    plan = {
        "created_at": datetime.now().isoformat(),
        "base_directory": args.base_dir,
        "max_size_gb": args.max_size,
        "languages": {},
        "complementary_datasets": {},
        "total_size_gb": 0,
        "download_commands": [],
        "phases": []
    }
    
    # Determina quali linguaggi includere
    languages_to_include = {}
    
    if args.languages:
        # Se sono specificati linguaggi specifici, usa quelli
        for lang in args.languages:
            for tier, langs in TIERS.items():
                if lang in langs and lang not in args.skip_languages:
                    languages_to_include[lang] = {
                        "tier": tier,
                        "size_gb": langs[lang]
                    }
    else:
        # Altrimenti usa i tier specificati
        for tier in args.tiers:
            if tier in TIERS:
                for lang, size in TIERS[tier].items():
                    if lang not in args.skip_languages:
                        languages_to_include[lang] = {
                            "tier": tier,
                            "size_gb": size
                        }
        
        # Aggiungi linguaggi selezionati dal Tier 3 se non è già incluso completamente
        if 3 not in args.tiers and args.tier3_count > 0:
            tier3_langs = list(TIERS[3].keys())
            tier3_langs = [lang for lang in tier3_langs if lang not in args.skip_languages]
            tier3_langs = sorted(tier3_langs, key=lambda x: TIERS[3][x], reverse=True)
            
            for lang in tier3_langs[:args.tier3_count]:
                languages_to_include[lang] = {
                    "tier": 3,
                    "size_gb": TIERS[3][lang]
                }
        
        # Aggiungi linguaggi selezionati dal Tier 4 se non è già incluso completamente
        if 4 not in args.tiers and args.tier4_count > 0:
            tier4_langs = list(TIERS[4].keys())
            tier4_langs = [lang for lang in tier4_langs if lang not in args.skip_languages]
            tier4_langs = sorted(tier4_langs, key=lambda x: TIERS[4][x], reverse=True)
            
            for lang in tier4_langs[:args.tier4_count]:
                languages_to_include[lang] = {
                    "tier": 4,
                    "size_gb": TIERS[4][lang]
                }
    
    # Calcola la dimensione totale dei linguaggi
    languages_size = sum(info["size_gb"] for info in languages_to_include.values())
    
    # Aggiungi dataset complementari se richiesto e c'è spazio
    complementary_size = 0
    if args.include_complementary:
        remaining_space = args.max_size - languages_size
        
        for dataset, size in COMPLEMENTARY_DATASETS.items():
            if remaining_space >= size:
                plan["complementary_datasets"][dataset] = {
                    "size_gb": size
                }
                complementary_size += size
                remaining_space -= size
    
    # Aggiorna il piano con i linguaggi selezionati
    plan["languages"] = languages_to_include
    plan["total_size_gb"] = languages_size + complementary_size
    
    # Genera comandi di download per ogni linguaggio
    for lang, info in languages_to_include.items():
        command = f"python download_github_code.py --languages {lang} --max_repos 100 --max_files 5000 --github_token YOUR_GITHUB_TOKEN"
        plan["download_commands"].append({
            "language": lang,
            "tier": info["tier"],
            "command": command
        })
    
    # Genera comandi per dataset complementari
    for dataset in plan["complementary_datasets"]:
        if dataset == "stackoverflow":
            command = f"python download_stackoverflow.py --save_dir {os.path.join(args.base_dir, 'complementary', 'stackoverflow')}"
        elif dataset == "documentation":
            command = f"python download_documentation.py --save_dir {os.path.join(args.base_dir, 'complementary', 'documentation')}"
        elif dataset == "tutorials":
            command = f"python download_tutorials.py --save_dir {os.path.join(args.base_dir, 'complementary', 'tutorials')}"
        
        plan["download_commands"].append({
            "dataset": dataset,
            "command": command
        })
    
    # Organizza il download in fasi
    phases = []
    
    # Fase 1: Tier 1
    phase1_langs = [lang for lang, info in languages_to_include.items() if info["tier"] == 1]
    if phase1_langs:
        phases.append({
            "name": "Fase 1: Linguaggi Fondamentali (Tier 1)",
            "languages": phase1_langs,
            "estimated_size_gb": sum(languages_to_include[lang]["size_gb"] for lang in phase1_langs)
        })
    
    # Fase 2: Tier 2
    phase2_langs = [lang for lang, info in languages_to_include.items() if info["tier"] == 2]
    if phase2_langs:
        phases.append({
            "name": "Fase 2: Linguaggi ad Alta Priorità (Tier 2)",
            "languages": phase2_langs,
            "estimated_size_gb": sum(languages_to_include[lang]["size_gb"] for lang in phase2_langs)
        })
    
    # Fase 3: Tier 3 selezionati
    phase3_langs = [lang for lang, info in languages_to_include.items() if info["tier"] == 3]
    if phase3_langs:
        phases.append({
            "name": "Fase 3: Linguaggi Selettivi (Tier 3)",
            "languages": phase3_langs,
            "estimated_size_gb": sum(languages_to_include[lang]["size_gb"] for lang in phase3_langs)
        })
    
    # Fase 4: Tier 4 selezionati
    phase4_langs = [lang for lang, info in languages_to_include.items() if info["tier"] == 4]
    if phase4_langs:
        phases.append({
            "name": "Fase 4: Linguaggi di Nicchia (Tier 4)",
            "languages": phase4_langs,
            "estimated_size_gb": sum(languages_to_include[lang]["size_gb"] for lang in phase4_langs)
        })
    
    # Fase 5: Dataset complementari
    if plan["complementary_datasets"]:
        phases.append({
            "name": "Fase 5: Dataset Complementari",
            "datasets": list(plan["complementary_datasets"].keys()),
            "estimated_size_gb": complementary_size
        })
    
    plan["phases"] = phases
    
    return plan

def save_plan(plan, output_file):
    """Salva il piano in un file JSON"""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(plan, f, ensure_ascii=False, indent=2)
    logger.info(f"Piano salvato in {output_file}")

def print_plan_summary(plan):
    """Stampa un riepilogo del piano"""
    logger.info("=" * 80)
    logger.info(f"PIANO DI AZIONE PER IL DATASET")
    logger.info("=" * 80)
    
    logger.info(f"Directory di base: {plan['base_directory']}")
    logger.info(f"Dimensione massima: {plan['max_size_gb']} GB")
    logger.info(f"Dimensione totale stimata: {plan['total_size_gb']} GB")
    logger.info(f"Spazio rimanente: {plan['max_size_gb'] - plan['total_size_gb']} GB")
    
    logger.info("\nLinguaggi inclusi:")
    for tier in range(1, 5):
        tier_langs = [(lang, info) for lang, info in plan["languages"].items() if info["tier"] == tier]
        if tier_langs:
            tier_size = sum(info["size_gb"] for _, info in tier_langs)
            logger.info(f"\nTier {tier}:")
            for lang, info in tier_langs:
                logger.info(f"  - {lang}: {info['size_gb']} GB")
            logger.info(f"  Totale Tier {tier}: {tier_size} GB")
    
    if plan["complementary_datasets"]:
        logger.info("\nDataset complementari:")
        for dataset, info in plan["complementary_datasets"].items():
            logger.info(f"  - {dataset}: {info['size_gb']} GB")
    
    logger.info("\nFasi di download:")
    for i, phase in enumerate(plan["phases"], 1):
        logger.info(f"\n{phase['name']}")
        logger.info(f"  Dimensione stimata: {phase['estimated_size_gb']} GB")
        if "languages" in phase:
            for lang in phase["languages"]:
                logger.info(f"  - {lang}: {plan['languages'][lang]['size_gb']} GB")
        if "datasets" in phase:
            for dataset in phase["datasets"]:
                logger.info(f"  - {dataset}: {plan['complementary_datasets'][dataset]['size_gb']} GB")
    
    logger.info("\nComandi di download:")
    for cmd in plan["download_commands"]:
        if "language" in cmd:
            logger.info(f"\n# Download di {cmd['language']} (Tier {cmd['tier']}):")
        else:
            logger.info(f"\n# Download del dataset {cmd['dataset']}:")
        logger.info(f"{cmd['command']}")
    
    logger.info("=" * 80)

def main():
    args = parse_arguments()
    
    # Genera il piano
    plan = generate_plan(args)
    
    # Salva il piano
    save_plan(plan, args.output)
    
    # Stampa il riepilogo
    print_plan_summary(plan)

if __name__ == "__main__":
    main()
