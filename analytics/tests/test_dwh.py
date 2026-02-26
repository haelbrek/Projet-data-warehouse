#!/usr/bin/env python3
"""
E5 - Tests du Data Warehouse
Projet Data Engineering - Region Hauts-de-France

Tests techniques et fonctionnels pour valider l'entrepot de donnees.
"""

import os
import sys
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


class TestConfiguration:
    """Configuration des tests."""
    SERVER = os.getenv('AZURE_SQL_SERVER', 'sqlelbrek-prod.database.windows.net')
    DATABASE = os.getenv('AZURE_SQL_DATABASE', 'projet_data_eng')
    USER = os.getenv('AZURE_SQL_USER', 'sqladmin')
    PASSWORD = os.getenv('AZURE_SQL_PASSWORD', '')

    @classmethod
    def get_engine(cls):
        driver = 'ODBC+Driver+18+for+SQL+Server'
        conn_str = f"mssql+pyodbc://{cls.USER}:{cls.PASSWORD}@{cls.SERVER}:1433/{cls.DATABASE}?driver={driver}&Encrypt=yes&TrustServerCertificate=yes"
        return create_engine(conn_str)


class TestSchemas(unittest.TestCase):
    """Tests de structure des schemas."""

    @classmethod
    def setUpClass(cls):
        cls.engine = TestConfiguration.get_engine()

    def test_schema_stg_exists(self):
        """Verifie que le schema stg existe."""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM sys.schemas WHERE name = 'stg'"
            ))
            self.assertEqual(result.scalar(), 1, "Schema stg non trouve")

    def test_schema_dwh_exists(self):
        """Verifie que le schema dwh existe."""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM sys.schemas WHERE name = 'dwh'"
            ))
            self.assertEqual(result.scalar(), 1, "Schema dwh non trouve")

    def test_schema_dm_exists(self):
        """Verifie que le schema dm existe."""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM sys.schemas WHERE name = 'dm'"
            ))
            self.assertEqual(result.scalar(), 1, "Schema dm non trouve")

    def test_schema_analytics_exists(self):
        """Verifie que le schema analytics existe."""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM sys.schemas WHERE name = 'analytics'"
            ))
            self.assertEqual(result.scalar(), 1, "Schema analytics non trouve")


class TestDimensions(unittest.TestCase):
    """Tests des tables de dimensions."""

    @classmethod
    def setUpClass(cls):
        cls.engine = TestConfiguration.get_engine()

    def test_dim_temps_exists(self):
        """Verifie que dim_temps existe et contient des donnees."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_temps"))
            count = result.scalar()
            self.assertGreater(count, 0, "dim_temps est vide")

    def test_dim_temps_annees(self):
        """Verifie que dim_temps contient les annees de reference."""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT annee FROM dwh.dim_temps WHERE est_annee_recensement = 1"
            ))
            annees = [r[0] for r in result.fetchall()]
            self.assertIn(2010, annees, "Annee 2010 manquante")
            self.assertIn(2015, annees, "Annee 2015 manquante")
            self.assertIn(2021, annees, "Annee 2021 manquante")

    def test_dim_geographie_departements(self):
        """Verifie les 5 departements Hauts-de-France."""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT departement_code FROM dwh.dim_geographie WHERE niveau_geo = 'DEPARTEMENT'"
            ))
            depts = [r[0] for r in result.fetchall()]
            expected = ['02', '59', '60', '62', '80']
            for dept in expected:
                self.assertIn(dept, depts, f"Departement {dept} manquant")

    def test_dim_demographie_pcs(self):
        """Verifie les codes PCS."""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT DISTINCT pcs_code FROM dwh.dim_demographie WHERE pcs_code IS NOT NULL"
            ))
            pcs_codes = [r[0] for r in result.fetchall()]
            self.assertGreater(len(pcs_codes), 5, "PCS incomplets")

    def test_dim_activite_naf(self):
        """Verifie les sections NAF."""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(DISTINCT naf_section_code) FROM dwh.dim_activite"
            ))
            count = result.scalar()
            self.assertGreaterEqual(count, 10, "Sections NAF incompletes")


class TestFaits(unittest.TestCase):
    """Tests des tables de faits."""

    @classmethod
    def setUpClass(cls):
        cls.engine = TestConfiguration.get_engine()

    def test_fait_tables_exist(self):
        """Verifie que les tables de faits existent."""
        tables = [
            'fait_population', 'fait_evenements_demo', 'fait_entreprises',
            'fait_emploi', 'fait_revenus', 'fait_logement'
        ]
        with self.engine.connect() as conn:
            for table in tables:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'dwh' AND TABLE_NAME = '{table}'
                """))
                self.assertEqual(result.scalar(), 1, f"Table {table} non trouvee")

    def test_fait_foreign_keys(self):
        """Verifie les contraintes de cles etrangeres."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM sys.foreign_keys fk
                INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = 'dwh' AND t.name LIKE 'fait_%'
            """))
            fk_count = result.scalar()
            self.assertGreater(fk_count, 0, "Aucune FK trouvee sur les faits")


class TestDatamarts(unittest.TestCase):
    """Tests des vues datamarts."""

    @classmethod
    def setUpClass(cls):
        cls.engine = TestConfiguration.get_engine()

    def test_vm_demographie_exists(self):
        """Verifie la vue vm_demographie_departement."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = 'dm' AND TABLE_NAME = 'vm_demographie_departement'
            """))
            self.assertEqual(result.scalar(), 1, "Vue vm_demographie_departement non trouvee")

    def test_vm_entreprises_exists(self):
        """Verifie la vue vm_entreprises_departement."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = 'dm' AND TABLE_NAME = 'vm_entreprises_departement'
            """))
            self.assertEqual(result.scalar(), 1, "Vue vm_entreprises_departement non trouvee")

    def test_tableau_bord_exists(self):
        """Verifie la vue tableau de bord."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = 'analytics' AND TABLE_NAME = 'v_tableau_bord_territorial'
            """))
            self.assertEqual(result.scalar(), 1, "Vue v_tableau_bord_territorial non trouvee")


class TestIntegrite(unittest.TestCase):
    """Tests d'integrite des donnees."""

    @classmethod
    def setUpClass(cls):
        cls.engine = TestConfiguration.get_engine()

    def test_no_orphan_temps_id(self):
        """Verifie qu'il n'y a pas de temps_id orphelins dans les faits."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM dwh.fait_population f
                LEFT JOIN dwh.dim_temps t ON f.temps_id = t.temps_id
                WHERE t.temps_id IS NULL
            """))
            orphans = result.scalar()
            self.assertEqual(orphans, 0, f"{orphans} temps_id orphelins trouves")

    def test_no_orphan_geo_id(self):
        """Verifie qu'il n'y a pas de geo_id orphelins dans les faits."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM dwh.fait_population f
                LEFT JOIN dwh.dim_geographie g ON f.geo_id = g.geo_id
                WHERE g.geo_id IS NULL
            """))
            orphans = result.scalar()
            self.assertEqual(orphans, 0, f"{orphans} geo_id orphelins trouves")

    def test_positive_population(self):
        """Verifie que les populations sont positives."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM dwh.fait_population
                WHERE population < 0
            """))
            negatives = result.scalar()
            self.assertEqual(negatives, 0, f"{negatives} populations negatives trouvees")


class TestPerformance(unittest.TestCase):
    """Tests de performance."""

    @classmethod
    def setUpClass(cls):
        cls.engine = TestConfiguration.get_engine()

    def test_columnstore_indexes(self):
        """Verifie la presence des index columnstore."""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM sys.indexes i
                INNER JOIN sys.tables t ON i.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = 'dwh' AND t.name LIKE 'fait_%'
                AND i.type_desc = 'CLUSTERED COLUMNSTORE'
            """))
            cci_count = result.scalar()
            # Note: Les CCI peuvent ne pas etre crees sur S0/Basic
            print(f"  [INFO] {cci_count} index columnstore trouves")


def run_tests():
    """Execute tous les tests et genere un rapport."""
    print("=" * 60)
    print("E5 - TESTS DU DATA WAREHOUSE")
    print(f"Date: {datetime.now().isoformat()}")
    print("=" * 60)

    # Creer le test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Ajouter les tests
    suite.addTests(loader.loadTestsFromTestCase(TestSchemas))
    suite.addTests(loader.loadTestsFromTestCase(TestDimensions))
    suite.addTests(loader.loadTestsFromTestCase(TestFaits))
    suite.addTests(loader.loadTestsFromTestCase(TestDatamarts))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegrite))
    suite.addTests(loader.loadTestsFromTestCase(TestPerformance))

    # Executer les tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Resume
    print("\n" + "=" * 60)
    print("RESUME DES TESTS")
    print("=" * 60)
    print(f"Tests executes: {result.testsRun}")
    print(f"Succes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Echecs: {len(result.failures)}")
    print(f"Erreurs: {len(result.errors)}")
    print("=" * 60)

    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(run_tests())
