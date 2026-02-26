#!/usr/bin/env python3
"""
E5 - ETL : Pipeline Principal
Projet Data Engineering - Region Hauts-de-France

Ce script orchestre l'ensemble du processus ETL :
1. Chargement des donnees sources vers staging
2. Alimentation des dimensions
3. Alimentation des tables de faits
4. Actualisation des statistiques

Usage:
    python run_etl.py --full           # Pipeline complet
    python run_etl.py --dimensions     # Dimensions uniquement
    python run_etl.py --facts          # Faits uniquement
    python run_etl.py --refresh        # Rafraichir les vues
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

# Configuration par defaut
DEFAULT_CONFIG = {
    'server': os.getenv('AZURE_SQL_SERVER', 'sqlelbrek-prod.database.windows.net'),
    'database': os.getenv('AZURE_SQL_DATABASE', 'projet_data_eng'),
    'user': os.getenv('AZURE_SQL_USER', 'sqladmin'),
    'password': os.getenv('AZURE_SQL_PASSWORD', ''),
}


def run_script(script_name: str, args: list = None) -> bool:
    """Execute un script Python."""
    script_path = Path(__file__).parent / script_name
    cmd = [sys.executable, str(script_path)] + (args or [])

    print(f"\n{'='*60}")
    print(f"[RUN] {script_name}")
    print('='*60)

    result = subprocess.run(cmd, cwd=Path(__file__).parent)
    return result.returncode == 0


def run_sql_script(script_path: str, config: dict) -> bool:
    """Execute un script SQL via pyodbc."""
    try:
        import pyodbc

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={config['server']},1433;"
            f"DATABASE={config['database']};"
            f"UID={config['user']};"
            f"PWD={config['password']};"
            f"Encrypt=yes;TrustServerCertificate=yes;"
        )

        with open(script_path, 'r', encoding='utf-8-sig') as f:
            sql_content = f.read()

        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()

        # Separer par GO
        import re
        blocks = re.split(r'\bGO\b', sql_content, flags=re.IGNORECASE)

        for block in blocks:
            block = block.strip()
            if block and not block.startswith('--'):
                try:
                    cursor.execute(block)
                except pyodbc.Error as e:
                    print(f"  [WARN] {str(e)[:80]}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"[ERROR] {e}")
        return False


def step_load_staging(config: dict) -> bool:
    """Etape 1: Charger les donnees sources vers staging."""
    print("\n" + "="*60)
    print("ETAPE 1: CHARGEMENT STAGING")
    print("="*60)

    # Utiliser le script export_to_sql existant
    export_script = Path(__file__).parent.parent / 'export_to_sql.py'
    if export_script.exists():
        cmd = [sys.executable, str(export_script)]
        result = subprocess.run(cmd, cwd=export_script.parent)
        return result.returncode == 0
    else:
        print("[WARN] Script export_to_sql.py non trouve")
        print("[INFO] Les donnees de staging doivent etre chargees manuellement")
        return True


def step_load_dimensions(config: dict, communes_path: str = None) -> bool:
    """Etape 2: Alimenter les dimensions."""
    print("\n" + "="*60)
    print("ETAPE 2: CHARGEMENT DIMENSIONS")
    print("="*60)

    args = [
        '--server', config['server'],
        '--database', config['database'],
        '--user', config['user'],
        '--password', config['password'],
    ]

    if communes_path:
        args.extend(['--communes', communes_path])

    return run_script('load_dimensions.py', args)


def step_load_facts(config: dict) -> bool:
    """Etape 3: Alimenter les tables de faits."""
    print("\n" + "="*60)
    print("ETAPE 3: CHARGEMENT TABLES DE FAITS")
    print("="*60)

    args = [
        '--server', config['server'],
        '--database', config['database'],
        '--user', config['user'],
        '--password', config['password'],
    ]

    return run_script('load_facts.py', args)


def step_refresh_views(config: dict) -> bool:
    """Etape 4: Actualiser les statistiques et vues."""
    print("\n" + "="*60)
    print("ETAPE 4: ACTUALISATION STATISTIQUES")
    print("="*60)

    sql = """
    -- Mise a jour des statistiques
    EXEC sp_updatestats;

    -- Verification des datamarts
    SELECT 'dm.vm_demographie_departement' AS vue, COUNT(*) AS lignes
    FROM dm.vm_demographie_departement
    UNION ALL
    SELECT 'dm.vm_entreprises_departement', COUNT(*)
    FROM dm.vm_entreprises_departement
    UNION ALL
    SELECT 'dm.vm_revenus_departement', COUNT(*)
    FROM dm.vm_revenus_departement;
    """

    try:
        import pyodbc

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={config['server']},1433;"
            f"DATABASE={config['database']};"
            f"UID={config['user']};"
            f"PWD={config['password']};"
            f"Encrypt=yes;TrustServerCertificate=yes;"
        )

        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()

        cursor.execute("EXEC sp_updatestats")
        print("  [OK] Statistiques mises a jour")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"  [WARN] {e}")
        return True


def main():
    parser = argparse.ArgumentParser(description='E5 - Pipeline ETL Principal')
    parser.add_argument('--full', action='store_true', help='Pipeline complet')
    parser.add_argument('--staging', action='store_true', help='Staging uniquement')
    parser.add_argument('--dimensions', action='store_true', help='Dimensions uniquement')
    parser.add_argument('--facts', action='store_true', help='Faits uniquement')
    parser.add_argument('--refresh', action='store_true', help='Rafraichir stats/vues')
    parser.add_argument('--communes', help='Chemin vers communes.json')
    parser.add_argument('--server', help='Serveur SQL')
    parser.add_argument('--database', help='Base de donnees')
    parser.add_argument('--user', help='Utilisateur SQL')
    parser.add_argument('--password', help='Mot de passe SQL')
    args = parser.parse_args()

    # Si aucune option, executer le pipeline complet
    if not any([args.full, args.staging, args.dimensions, args.facts, args.refresh]):
        args.full = True

    print("=" * 60)
    print("E5 - PIPELINE ETL DATA WAREHOUSE")
    print("Projet Data Engineering - Hauts-de-France")
    print(f"Date: {datetime.now().isoformat()}")
    print("=" * 60)

    # Configuration
    config = {
        'server': args.server or DEFAULT_CONFIG['server'],
        'database': args.database or DEFAULT_CONFIG['database'],
        'user': args.user or DEFAULT_CONFIG['user'],
        'password': args.password or DEFAULT_CONFIG['password'],
    }

    print(f"\n[CONFIG] Serveur: {config['server']}")
    print(f"[CONFIG] Database: {config['database']}")

    success = True
    steps_run = 0
    steps_ok = 0

    # Executer les etapes demandees
    if args.full or args.staging:
        steps_run += 1
        if step_load_staging(config):
            steps_ok += 1
        else:
            success = False

    if args.full or args.dimensions:
        steps_run += 1
        if step_load_dimensions(config, args.communes):
            steps_ok += 1
        else:
            success = False

    if args.full or args.facts:
        steps_run += 1
        if step_load_facts(config):
            steps_ok += 1
        else:
            success = False

    if args.full or args.refresh:
        steps_run += 1
        if step_refresh_views(config):
            steps_ok += 1
        else:
            success = False

    # Resume
    print("\n" + "=" * 60)
    print("RESUME DU PIPELINE ETL")
    print("=" * 60)
    print(f"Etapes executees: {steps_run}")
    print(f"Etapes reussies:  {steps_ok}")
    print(f"Statut:           {'SUCCES' if success else 'ECHEC'}")
    print("=" * 60)

    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
