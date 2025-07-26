#!/usr/bin/env python3
"""
Medical Dictionary Database Migration Script
Migrates extracted medical terms from JSON to PostgreSQL database
Works with existing subjects and specialities tables
"""

import os
import json
import psycopg2
import psycopg2.extras
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
import uuid
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('medical_migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection"""
    db_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'brightly'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        logger.info(f"✅ Connected to database: {db_params['database']}@{db_params['host']}")
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

class MedicalDictionaryMigrator:
    def __init__(self, json_file_path: str):
        self.json_file_path = Path(json_file_path)
        self.data = None
        self.conn = None
        self.cursor = None
        
        # Migration statistics
        self.stats = {
            'total_terms': 0,
            'inserted_terms': 0,
            'updated_terms': 0,
            'skipped_terms': 0,
            'errors': 0,
            'categories_created': 0,
            'existing_subjects_found': 0,
            'missing_subjects': 0
        }
    
    def load_json_data(self):
        """Load medical dictionary data from JSON file"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            
            logger.info(f"📄 Loaded JSON data from: {self.json_file_path}")
            logger.info(f"📊 Total subjects: {self.data['metadata']['total_subjects']}")
            logger.info(f"📊 Total terms: {self.data['metadata']['total_terms']}")
            logger.info(f"📊 Categories: {len(self.data['metadata']['categories'])}")
            
            self.stats['total_terms'] = self.data['metadata']['total_terms']
            
        except Exception as e:
            logger.error(f"❌ Failed to load JSON data: {e}")
            raise
    
    def validate_existing_tables(self):
        """Validate that existing subjects and specialities tables exist"""
        try:
            logger.info("🔍 Validating existing database structure...")
            
            # Check if subjects table exists
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'subjects'
                );
            """)
            subjects_exists = self.cursor.fetchone()[0]
            
            # Check if specialities table exists
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'specialities'
                );
            """)
            specialities_exists = self.cursor.fetchone()[0]
            
            if not subjects_exists:
                raise Exception("❌ 'subjects' table not found. Please ensure it exists before migration.")
            
            if not specialities_exists:
                raise Exception("❌ 'specialities' table not found. Please ensure it exists before migration.")
            
            # Get subject count and sample
            self.cursor.execute("SELECT COUNT(*) FROM subjects")
            subject_count = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM specialities")
            speciality_count = self.cursor.fetchone()[0]
            
            logger.info(f"✅ Found existing subjects table with {subject_count} subjects")
            logger.info(f"✅ Found existing specialities table with {speciality_count} specialities")
            
            # Check if our subject IDs exist
            subject_ids = list(self.data['subjects'].keys())
            format_strings = ','.join(['%s'] * len(subject_ids))
            
            self.cursor.execute(f"""
                SELECT id, name FROM subjects 
                WHERE id::text IN ({format_strings})
            """, subject_ids)
            
            existing_subjects = self.cursor.fetchall()
            self.stats['existing_subjects_found'] = len(existing_subjects)
            self.stats['missing_subjects'] = len(subject_ids) - len(existing_subjects)
            
            logger.info(f"✅ Found {len(existing_subjects)} matching subjects in database")
            if self.stats['missing_subjects'] > 0:
                logger.warning(f"⚠️  {self.stats['missing_subjects']} subject IDs from JSON not found in database")
            
        except Exception as e:
            logger.error(f"❌ Database validation failed: {e}")
            raise
    
    def create_database_schema(self):
        """Create only the new medical dictionary schema (not subjects/specialities)"""
        try:
            # Enable UUID extension
            self.cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
            
            # Create categories table
            create_categories_table = """
            CREATE TABLE IF NOT EXISTS medical_categories (
                category_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name VARCHAR(100) UNIQUE NOT NULL,
                description TEXT,
                color_code VARCHAR(7), -- For UI theming
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Create main medical terms table (references existing subjects table)
            create_terms_table = """
            CREATE TABLE IF NOT EXISTS medical_terms (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                term VARCHAR(255) NOT NULL,
                definition TEXT NOT NULL,
                category_id UUID REFERENCES medical_categories(category_id),
                subject_id UUID REFERENCES subjects(id), -- References existing subjects table
                synonyms TEXT[], -- Array of synonyms
                related_terms TEXT[], -- Array of related terms
                description TEXT,
                expansion TEXT, -- For acronyms
                clinical_significance TEXT,
                normal_values TEXT,
                image_search_terms TEXT[], -- Array of image search terms
                confidence_score DECIMAL(3,2),
                source_file TEXT,
                extracted_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(term, category_id) -- Prevent duplicates per category
            );
            """
            
            # Create indexes for better performance
            create_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_term ON medical_terms USING gin(to_tsvector('french', term));",
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_definition ON medical_terms USING gin(to_tsvector('french', definition));",
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_category ON medical_terms(category_id);",
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_subject ON medical_terms(subject_id);",
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_confidence ON medical_terms(confidence_score);",
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_term_lower ON medical_terms(LOWER(term));",
                "CREATE INDEX IF NOT EXISTS idx_medical_categories_name ON medical_categories(LOWER(name));"
            ]
            
            # Create search function for terms
            create_search_function = """
            CREATE OR REPLACE FUNCTION search_medical_terms(search_query TEXT)
            RETURNS TABLE(
                term_id UUID,
                term VARCHAR,
                definition TEXT,
                category VARCHAR,
                confidence_score DECIMAL,
                rank REAL
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT 
                    mt.id,
                    mt.term,
                    mt.definition,
                    mc.name,
                    mt.confidence_score,
                    ts_rank(
                        to_tsvector('french', mt.term || ' ' || mt.definition), 
                        plainto_tsquery('french', search_query)
                    ) as rank
                FROM medical_terms mt
                JOIN medical_categories mc ON mt.category_id = mc.category_id
                WHERE to_tsvector('french', mt.term || ' ' || mt.definition) @@ plainto_tsquery('french', search_query)
                ORDER BY rank DESC, mt.confidence_score DESC;
            END;
            $$ LANGUAGE plpgsql;
            """
            
            # Execute schema creation
            logger.info("🏗️  Creating medical dictionary schema...")
            
            self.cursor.execute(create_categories_table)
            logger.info("✅ Created medical_categories table")
            
            self.cursor.execute(create_terms_table)
            logger.info("✅ Created medical_terms table (referencing existing subjects)")
            
            for index_sql in create_indexes:
                self.cursor.execute(index_sql)
            logger.info("✅ Created indexes for performance")
            
            self.cursor.execute(create_search_function)
            logger.info("✅ Created search function")
            
            self.conn.commit()
            logger.info("🎉 Medical dictionary schema created successfully!")
            
        except Exception as e:
            logger.error(f"❌ Failed to create schema: {e}")
            self.conn.rollback()
            raise
    
    def migrate_categories(self):
        """Migrate categories to database"""
        try:
            logger.info("📂 Migrating categories...")
            
            # Category colors for UI
            category_colors = {
                'anatomie': '#3B82F6',      # Blue
                'pathologie': '#EF4444',    # Red
                'pharmacologie': '#10B981', # Green
                'physiologie': '#8B5CF6',   # Purple
                'imagerie': '#F59E0B',      # Amber
                'classification': '#6B7280', # Gray
                'microbiologie': '#EC4899', # Pink
                'thérapeutique': '#14B8A6', # Teal
                'acronyme': '#F97316',      # Orange
                'immunologie': '#84CC16',   # Lime
                'vaccination': '#06B6D4',   # Cyan
                'toxicologie': '#DC2626',   # Red-600
                'procédure médicale': '#7C3AED', # Violet
                'chirurgie': '#059669',     # Emerald
                'obstétrique': '#D946EF',   # Fuchsia
                'biologie': '#65A30D',      # Green-600
                'génétique': '#2563EB',     # Blue-600
                'oncologie': '#9333EA',     # Purple-600
                'psychologie': '#0891B2',   # Sky-600
                'psychothérapie': '#0D9488', # Teal-600
                'traitement': '#CA8A04',    # Yellow-600
                'épidémiologie': '#7C2D12', # Orange-800
                'enzymologie': '#166534',   # Green-800
                'radiothérapie': '#7E22CE', # Purple-700
                'symptôme': '#BE185D',      # Pink-700
                'biochimie': '#1E40AF'      # Blue-700
            }
            
            categories = self.data['metadata']['categories']
            
            for category in categories:
                try:
                    # Insert category if not exists
                    insert_category = """
                    INSERT INTO medical_categories (name, description, color_code)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING category_id;
                    """
                    
                    description = f"Termes médicaux relatifs à {category}"
                    color = category_colors.get(category, '#6B7280')
                    
                    self.cursor.execute(insert_category, (category, description, color))
                    result = self.cursor.fetchone()
                    
                    if result:
                        self.stats['categories_created'] += 1
                        logger.debug(f"✅ Created category: {category}")
                
                except Exception as e:
                    logger.error(f"❌ Failed to insert category {category}: {e}")
                    self.stats['errors'] += 1
            
            self.conn.commit()
            logger.info(f"📂 Categories migration completed: {self.stats['categories_created']} created")
            
        except Exception as e:
            logger.error(f"❌ Categories migration failed: {e}")
            self.conn.rollback()
            raise
    
    def migrate_terms(self):
        """Migrate medical terms to database"""
        try:
            logger.info("📚 Migrating medical terms...")
            
            # Get category ID mapping
            self.cursor.execute("SELECT name, category_id FROM medical_categories")
            category_mapping = dict(self.cursor.fetchall())
            
            # Get existing subject IDs from database
            self.cursor.execute("SELECT id FROM subjects")
            existing_subject_ids = {str(row[0]) for row in self.cursor.fetchall()}
            
            processed_count = 0
            
            for subject_id, subject_data in self.data['subjects'].items():
                # Skip if subject doesn't exist in database
                if subject_id not in existing_subject_ids:
                    logger.warning(f"⚠️  Subject {subject_id[:8]} not found in database, skipping its terms")
                    continue
                
                terms = subject_data.get('terms', [])
                logger.info(f"Processing {len(terms)} terms from subject {subject_id[:8]}...")
                
                for term_data in terms:
                    try:
                        processed_count += 1
                        
                        # Get category ID
                        category_name = term_data.get('category')
                        category_id = category_mapping.get(category_name)
                        
                        if not category_id:
                            logger.warning(f"⚠️  Category not found: {category_name}")
                            self.stats['skipped_terms'] += 1
                            continue
                        
                        # Prepare arrays (handle None values)
                        synonyms = term_data.get('synonyms') or []
                        related_terms = term_data.get('related_terms') or []
                        image_search_terms = term_data.get('image_search_terms') or []
                        
                        # Parse extracted_at timestamp
                        extracted_at = None
                        if term_data.get('extracted_at'):
                            try:
                                extracted_at = datetime.fromisoformat(term_data['extracted_at'].replace('Z', '+00:00'))
                            except:
                                extracted_at = None
                        
                        insert_term = """
                        INSERT INTO medical_terms (
                            term, definition, category_id, subject_id,
                            synonyms, related_terms, description, expansion,
                            clinical_significance, normal_values, image_search_terms,
                            confidence_score, source_file, extracted_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (term, category_id) DO UPDATE SET
                            definition = EXCLUDED.definition,
                            synonyms = EXCLUDED.synonyms,
                            related_terms = EXCLUDED.related_terms,
                            description = EXCLUDED.description,
                            clinical_significance = EXCLUDED.clinical_significance,
                            confidence_score = EXCLUDED.confidence_score,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING id;
                        """
                        
                        self.cursor.execute(insert_term, (
                            term_data['term'],
                            term_data['definition'],
                            category_id,
                            subject_id,
                            synonyms,
                            related_terms,
                            term_data.get('description'),
                            term_data.get('expansion'),
                            term_data.get('clinical_significance'),
                            term_data.get('normal_values'),
                            image_search_terms,
                            term_data.get('confidence_score'),
                            term_data.get('source_file'),
                            extracted_at
                        ))
                        
                        result = self.cursor.fetchone()
                        if result:
                            self.stats['inserted_terms'] += 1
                        
                        # Commit every 100 terms for progress
                        if processed_count % 100 == 0:
                            self.conn.commit()
                            logger.info(f"📊 Progress: {processed_count} terms processed, {self.stats['inserted_terms']} inserted")
                    
                    except Exception as e:
                        logger.error(f"❌ Failed to insert term '{term_data.get('term', 'unknown')}': {e}")
                        self.stats['errors'] += 1
                        continue
            
            # Final commit
            self.conn.commit()
            logger.info(f"📚 Terms migration completed!")
            logger.info(f"✅ Inserted: {self.stats['inserted_terms']} terms")
            logger.info(f"⚠️  Skipped: {self.stats['skipped_terms']} terms") 
            logger.info(f"❌ Errors: {self.stats['errors']} terms")
            
        except Exception as e:
            logger.error(f"❌ Terms migration failed: {e}")
            self.conn.rollback()
            raise
    
    def create_api_views(self):
        """Create useful views for API queries using existing subjects and specialities tables"""
        try:
            logger.info("🔍 Creating API views...")
            
            # View for term details with category, subject, and specialty info
            create_term_details_view = """
            CREATE OR REPLACE VIEW v_medical_term_details AS
            SELECT 
                mt.id,
                mt.term,
                mt.definition,
                mc.name as category,
                mc.color_code as category_color,
                s.name as subject_name,
                sp.name as specialty_name,
                mt.synonyms,
                mt.related_terms,
                mt.description,
                mt.clinical_significance,
                mt.image_search_terms,
                mt.confidence_score,
                mt.created_at,
                mt.updated_at
            FROM medical_terms mt
            JOIN medical_categories mc ON mt.category_id = mc.id
            LEFT JOIN subjects s ON mt.subject_id = s.id
            LEFT JOIN specialities sp ON s.speciality_id = sp.id
            ORDER BY mt.confidence_score DESC, mt.term;
            """
            
            # View for category statistics
            create_category_stats_view = """
            CREATE OR REPLACE VIEW v_category_statistics AS
            SELECT 
                mc.category_id,
                mc.name as category,
                mc.color_code,
                COUNT(mt.id) as term_count,
                AVG(mt.confidence_score) as avg_confidence,
                COUNT(CASE WHEN mt.clinical_significance IS NOT NULL THEN 1 END) as terms_with_clinical_notes,
                COUNT(CASE WHEN array_length(mt.synonyms, 1) > 0 THEN 1 END) as terms_with_synonyms
            FROM medical_categories mc
            LEFT JOIN medical_terms mt ON mc.category_id = mt.category_id
            GROUP BY mc.category_id, mc.name, mc.color_code
            ORDER BY term_count DESC;
            """
            
            # View for subject statistics using existing subjects table
            create_subject_stats_view = """
            CREATE OR REPLACE VIEW v_subject_statistics AS
            SELECT 
                s.id,
                s.name as subject_name,
                sp.name as specialty_name,
                COUNT(mt.id) as term_count,
                AVG(mt.confidence_score) as avg_confidence,
                COUNT(DISTINCT mt.category_id) as categories_covered
            FROM subjects s
            LEFT JOIN specialities sp ON s.speciality_id = sp.id
            LEFT JOIN medical_terms mt ON s.id = mt.subject_id
            GROUP BY s.id, s.name, sp.name
            ORDER BY term_count DESC;
            """
            
            # View for specialty statistics
            create_specialty_stats_view = """
            CREATE OR REPLACE VIEW v_specialty_statistics AS
            SELECT 
                sp.id,
                sp.name as specialty_name,
                COUNT(DISTINCT s.id) as subjects_count,
                COUNT(mt.id) as total_terms,
                AVG(mt.confidence_score) as avg_confidence,
                COUNT(DISTINCT mt.category_id) as categories_covered
            FROM specialities sp
            LEFT JOIN subjects s ON sp.id = s.speciality_id
            LEFT JOIN medical_terms mt ON s.id = mt.subject_id
            GROUP BY sp.id, sp.name
            ORDER BY total_terms DESC;
            """
            
            self.cursor.execute(create_term_details_view)
            self.cursor.execute(create_category_stats_view)
            self.cursor.execute(create_subject_stats_view)
            self.cursor.execute(create_specialty_stats_view)
            
            self.conn.commit()
            logger.info("✅ API views created successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to create views: {e}")
            self.conn.rollback()
            raise
    
    def validate_migration(self):
        """Validate the migration results"""
        try:
            logger.info("🔍 Validating migration...")
            
            # Count terms by category
            self.cursor.execute("""
                SELECT mc.name, COUNT(mt.id) as count
                FROM medical_categories mc
                LEFT JOIN medical_terms mt ON mc.id = mt.category_id
                GROUP BY mc.name
                ORDER BY count DESC
            """)
            
            category_counts = self.cursor.fetchall()
            logger.info("📊 Terms per category:")
            for category, count in category_counts[:10]:  # Top 10
                logger.info(f"   📌 {category}: {count} terms")
            
            # Count terms by specialty
            self.cursor.execute("""
                SELECT sp.name, COUNT(mt.id) as count
                FROM specialities sp
                LEFT JOIN subjects s ON sp.id = s.speciality_id
                LEFT JOIN medical_terms mt ON s.id = mt.subject_id
                GROUP BY sp.name
                ORDER BY count DESC
            """)
            
            specialty_counts = self.cursor.fetchall()
            logger.info("🏥 Terms per specialty:")
            for specialty, count in specialty_counts:
                logger.info(f"   🔬 {specialty}: {count} terms")
            
            # Overall statistics
            self.cursor.execute("SELECT COUNT(*) FROM medical_terms")
            total_terms = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM medical_categories")
            total_categories = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM subjects WHERE id::text IN (SELECT DISTINCT subject_id::text FROM medical_terms)")
            subjects_with_terms = self.cursor.fetchone()[0]
            
            logger.info(f"✅ Migration validation:")
            logger.info(f"   📚 Total terms: {total_terms}")
            logger.info(f"   📂 Total categories: {total_categories}")
            logger.info(f"   🏥 Subjects with terms: {subjects_with_terms}")
            
            # Test search function
            self.cursor.execute("SELECT * FROM search_medical_terms('choc') LIMIT 3")
            search_results = self.cursor.fetchall()
            logger.info(f"🔍 Search test for 'choc': {len(search_results)} results")
            
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
    
    def run_migration(self):
        """Run the complete migration process"""
        try:
            logger.info("🚀 Starting medical dictionary migration...")
            
            # Load data
            self.load_json_data()
            
            # Connect to database
            self.conn = get_db_connection()
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Run migration steps
            self.create_database_schema()
            self.migrate_categories()
            self.migrate_terms()
            self.create_api_views()
            self.validate_migration()
            
            logger.info("🎉 Migration completed successfully!")
            
        except Exception as e:
            logger.error(f"💥 Migration failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
    
    def print_summary(self):
        """Print migration summary"""
        logger.info("=" * 60)
        logger.info("📊 MEDICAL DICTIONARY MIGRATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"📄 Source file: {self.json_file_path}")
        logger.info(f"📚 Total terms processed: {self.stats['total_terms']}")
        logger.info(f"✅ Successfully inserted: {self.stats['inserted_terms']}")
        logger.info(f"⚠️  Skipped: {self.stats['skipped_terms']}")
        logger.info(f"❌ Errors: {self.stats['errors']}")
        logger.info(f"📂 Categories created: {self.stats['categories_created']}")
        logger.info(f"🏥 Existing subjects found: {self.stats['existing_subjects_found']}")
        logger.info(f"❓ Missing subjects: {self.stats['missing_subjects']}")
        
        if self.stats['inserted_terms'] > 0:
            success_rate = (self.stats['inserted_terms'] / self.stats['total_terms']) * 100
            logger.info(f"📈 Success rate: {success_rate:.1f}%")
            logger.info("🎯 Database ready for medical education platform!")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate medical dictionary to PostgreSQL')
    parser.add_argument('json_file', help='Path to complete_medical_db_*.json file')
    parser.add_argument('--drop-tables', action='store_true', help='Drop existing medical dictionary tables before migration')
    parser.add_argument('--validate-only', action='store_true', help='Only validate existing data')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.json_file):
        logger.error(f"❌ JSON file not found: {args.json_file}")
        return 1
    
    try:
        migrator = MedicalDictionaryMigrator(args.json_file)
        
        if args.drop_tables:
            logger.warning("🗑️  Dropping existing medical dictionary tables...")
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Only drop medical dictionary tables, not subjects/specialities
            drop_commands = [
                "DROP VIEW IF EXISTS v_specialty_statistics CASCADE;",
                "DROP VIEW IF EXISTS v_subject_statistics CASCADE;",
                "DROP VIEW IF EXISTS v_category_statistics CASCADE;",
                "DROP VIEW IF EXISTS v_medical_term_details CASCADE;",
                "DROP FUNCTION IF EXISTS search_medical_terms(TEXT) CASCADE;",
                "DROP TABLE IF EXISTS medical_terms CASCADE;",
                "DROP TABLE IF EXISTS medical_categories CASCADE;"
            ]
            
            for cmd in drop_commands:
                cursor.execute(cmd)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("✅ Medical dictionary tables dropped successfully")
        
        if args.validate_only:
            conn = get_db_connection()
            cursor = conn.cursor()
            migrator.conn = conn
            migrator.cursor = cursor
            migrator.validate_migration()
            cursor.close()
            conn.close()
        else:
            migrator.run_migration()
            migrator.print_summary()
        
        return 0
        
    except Exception as e:
        logger.error(f"💥 Migration script failed: {e}")
        return 1

def main():
        """Create the database schema for medical dictionary"""
        try:
            # Enable UUID extension
            self.cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
            
            # Create categories table
            create_categories_table = """
            CREATE TABLE IF NOT EXISTS medical_categories (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name VARCHAR(100) UNIQUE NOT NULL,
                description TEXT,
                color_code VARCHAR(7), -- For UI theming
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Create subjects table  
            create_subjects_table = """
            CREATE TABLE IF NOT EXISTS medical_subjects (
                id UUID PRIMARY KEY,
                name VARCHAR(255),
                specialty VARCHAR(100),
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Create main medical terms table
            create_terms_table = """
            CREATE TABLE IF NOT EXISTS medical_terms (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                term VARCHAR(255) NOT NULL,
                definition TEXT NOT NULL,
                category_id UUID REFERENCES medical_categories(id),
                subject_id UUID REFERENCES medical_subjects(id),
                synonyms TEXT[], -- Array of synonyms
                related_terms TEXT[], -- Array of related terms
                description TEXT,
                expansion TEXT, -- For acronyms
                clinical_significance TEXT,
                normal_values TEXT,
                image_search_terms TEXT[], -- Array of image search terms
                confidence_score DECIMAL(3,2),
                source_file TEXT,
                extracted_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(term, category_id) -- Prevent duplicates per category
            );
            """
            
            # Create indexes for better performance
            create_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_term ON medical_terms USING gin(to_tsvector('french', term));",
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_definition ON medical_terms USING gin(to_tsvector('french', definition));",
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_category ON medical_terms(category_id);",
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_subject ON medical_terms(subject_id);",
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_confidence ON medical_terms(confidence_score);",
                "CREATE INDEX IF NOT EXISTS idx_medical_terms_term_lower ON medical_terms(LOWER(term));",
                "CREATE INDEX IF NOT EXISTS idx_medical_categories_name ON medical_categories(LOWER(name));"
            ]
            
            # Create search function for terms
            create_search_function = """
            CREATE OR REPLACE FUNCTION search_medical_terms(search_query TEXT)
            RETURNS TABLE(
                term_id UUID,
                term VARCHAR,
                definition TEXT,
                category VARCHAR,
                confidence_score DECIMAL,
                rank REAL
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT 
                    mt.id,
                    mt.term,
                    mt.definition,
                    mc.name,
                    mt.confidence_score,
                    ts_rank(
                        to_tsvector('french', mt.term || ' ' || mt.definition), 
                        plainto_tsquery('french', search_query)
                    ) as rank
                FROM medical_terms mt
                JOIN medical_categories mc ON mt.category_id = mc.id
                WHERE to_tsvector('french', mt.term || ' ' || mt.definition) @@ plainto_tsquery('french', search_query)
                ORDER BY rank DESC, mt.confidence_score DESC;
            END;
            $$ LANGUAGE plpgsql;
            """
            
            # Execute schema creation
            logger.info("🏗️  Creating database schema...")
            
            self.cursor.execute(create_categories_table)
            logger.info("✅ Created medical_categories table")
            
            self.cursor.execute(create_subjects_table)
            logger.info("✅ Created medical_subjects table")
            
            self.cursor.execute(create_terms_table)
            logger.info("✅ Created medical_terms table")
            
            for index_sql in create_indexes:
                self.cursor.execute(index_sql)
            logger.info("✅ Created indexes for performance")
            
            self.cursor.execute(create_search_function)
            logger.info("✅ Created search function")
            
            self.conn.commit()
            logger.info("🎉 Database schema created successfully!")
            
        except Exception as e:
            logger.error(f"❌ Failed to create schema: {e}")
            self.conn.rollback()
            raise
    
    def migrate_categories(self):
        """Migrate categories to database"""
        try:
            logger.info("📂 Migrating categories...")
            
            # Category colors for UI
            category_colors = {
                'anatomie': '#3B82F6',      # Blue
                'pathologie': '#EF4444',    # Red
                'pharmacologie': '#10B981', # Green
                'physiologie': '#8B5CF6',   # Purple
                'imagerie': '#F59E0B',      # Amber
                'classification': '#6B7280', # Gray
                'microbiologie': '#EC4899', # Pink
                'thérapeutique': '#14B8A6', # Teal
                'acronyme': '#F97316',      # Orange
                'immunologie': '#84CC16',   # Lime
                'vaccination': '#06B6D4',   # Cyan
                'toxicologie': '#DC2626',   # Red-600
                'procédure médicale': '#7C3AED', # Violet
                'chirurgie': '#059669',     # Emerald
                'obstétrique': '#D946EF',   # Fuchsia
                'biologie': '#65A30D',      # Green-600
                'génétique': '#2563EB',     # Blue-600
                'oncologie': '#9333EA',     # Purple-600
                'psychologie': '#0891B2',   # Sky-600
                'psychothérapie': '#0D9488', # Teal-600
                'traitement': '#CA8A04',    # Yellow-600
                'épidémiologie': '#7C2D12', # Orange-800
                'enzymologie': '#166534',   # Green-800
                'radiothérapie': '#7E22CE', # Purple-700
                'symptôme': '#BE185D',      # Pink-700
                'biochimie': '#1E40AF'      # Blue-700
            }
            
            categories = self.data['metadata']['categories']
            
            for category in categories:
                try:
                    # Insert category if not exists
                    insert_category = """
                    INSERT INTO medical_categories (name, description, color_code)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING id;
                    """
                    
                    description = f"Termes médicaux relatifs à {category}"
                    color = category_colors.get(category, '#6B7280')
                    
                    self.cursor.execute(insert_category, (category, description, color))
                    result = self.cursor.fetchone()
                    
                    if result:
                        self.stats['categories_created'] += 1
                        logger.debug(f"✅ Created category: {category}")
                
                except Exception as e:
                    logger.error(f"❌ Failed to insert category {category}: {e}")
                    self.stats['errors'] += 1
            
            self.conn.commit()
            logger.info(f"📂 Categories migration completed: {self.stats['categories_created']} created")
            
        except Exception as e:
            logger.error(f"❌ Categories migration failed: {e}")
            self.conn.rollback()
            raise
    
    def migrate_subjects(self):
        """Migrate subjects to database"""
        try:
            logger.info("🏥 Migrating subjects...")
            
            # Subject mapping from your original data
            subject_names = {
                "d843d886-89ef-4860-9e06-466c8d4e31b8": "État de Choc Hémorragique",
                "1c9ec5d3-5063-4969-b17a-e65b0c141f28": "Douleurs Thoraciques Aiguës",
                "532af22c-1dd9-4770-9cd1-105d802814e0": "Endocardites Infectieuses",
                "3236ac16-2076-4f06-b710-f330699d1af7": "Hypertension Artérielle",
                "cb851055-1b65-4a0d-8cb2-1271eb2074e7": "Ischémie Aiguë des Membres",
                "3243d452-f9e5-4ed9-b0f1-8235f4da9e43": "Maladies Veineuses Thrombo-Emboliques",
                "23c142db-8071-434d-8bd2-877ef709cf58": "Syndromes Coronariens Aigus",
                "b6c0543d-7c40-415c-bb18-783e0c572a83": "Cancer du Cavum",
                # Add more as needed
            }
            
            # Determine specialty from subject name
            def get_specialty(subject_name):
                if any(word in subject_name.lower() for word in ['cardiaque', 'coronarien', 'thoracique', 'cardiogénique']):
                    return 'Cardiologie'
                elif any(word in subject_name.lower() for word in ['cavum', 'cancer', 'tumeur', 'oncologie']):
                    return 'Oncologie'
                elif any(word in subject_name.lower() for word in ['choc', 'hémorragique', 'réanimation']):
                    return 'Réanimation'
                elif any(word in subject_name.lower() for word in ['infectieuse', 'infection']):
                    return 'Infectiologie'
                else:
                    return 'Médecine Générale'
            
            for subject_id, subject_data in self.data['subjects'].items():
                try:
                    subject_name = subject_names.get(subject_id, f"Subject {subject_id[:8]}")
                    specialty = get_specialty(subject_name)
                    
                    insert_subject = """
                    INSERT INTO medical_subjects (id, name, specialty, description)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        specialty = EXCLUDED.specialty,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id;
                    """
                    
                    description = f"Termes médicaux extraits du cours: {subject_name}"
                    
                    self.cursor.execute(insert_subject, (
                        subject_id,
                        subject_name,
                        specialty,
                        description
                    ))
                    
                    result = self.cursor.fetchone()
                    if result:
                        self.stats['subjects_created'] += 1
                        logger.debug(f"✅ Created/updated subject: {subject_name}")
                
                except Exception as e:
                    logger.error(f"❌ Failed to insert subject {subject_id}: {e}")
                    self.stats['errors'] += 1
            
            self.conn.commit()
            logger.info(f"🏥 Subjects migration completed: {self.stats['subjects_created']} processed")
            
        except Exception as e:
            logger.error(f"❌ Subjects migration failed: {e}")
            self.conn.rollback()
            raise
    
    def migrate_terms(self):
        """Migrate medical terms to database"""
        try:
            logger.info("📚 Migrating medical terms...")
            
            # Get category ID mapping
            self.cursor.execute("SELECT name, id FROM medical_categories")
            category_mapping = dict(self.cursor.fetchall())
            
            processed_count = 0
            
            for subject_id, subject_data in self.data['subjects'].items():
                terms = subject_data.get('terms', [])
                
                logger.info(f"Processing {len(terms)} terms from subject {subject_id[:8]}...")
                
                for term_data in terms:
                    try:
                        processed_count += 1
                        
                        # Get category ID
                        category_name = term_data.get('category')
                        category_id = category_mapping.get(category_name)
                        
                        if not category_id:
                            logger.warning(f"⚠️  Category not found: {category_name}")
                            self.stats['skipped_terms'] += 1
                            continue
                        
                        # Prepare arrays (handle None values)
                        synonyms = term_data.get('synonyms') or []
                        related_terms = term_data.get('related_terms') or []
                        image_search_terms = term_data.get('image_search_terms') or []
                        
                        # Parse extracted_at timestamp
                        extracted_at = None
                        if term_data.get('extracted_at'):
                            try:
                                extracted_at = datetime.fromisoformat(term_data['extracted_at'].replace('Z', '+00:00'))
                            except:
                                extracted_at = None
                        
                        insert_term = """
                        INSERT INTO medical_terms (
                            term, definition, category_id, subject_id,
                            synonyms, related_terms, description, expansion,
                            clinical_significance, normal_values, image_search_terms,
                            confidence_score, source_file, extracted_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (term, category_id) DO UPDATE SET
                            definition = EXCLUDED.definition,
                            synonyms = EXCLUDED.synonyms,
                            related_terms = EXCLUDED.related_terms,
                            description = EXCLUDED.description,
                            clinical_significance = EXCLUDED.clinical_significance,
                            confidence_score = EXCLUDED.confidence_score,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING id;
                        """
                        
                        self.cursor.execute(insert_term, (
                            term_data['term'],
                            term_data['definition'],
                            category_id,
                            subject_id,
                            synonyms,
                            related_terms,
                            term_data.get('description'),
                            term_data.get('expansion'),
                            term_data.get('clinical_significance'),
                            term_data.get('normal_values'),
                            image_search_terms,
                            term_data.get('confidence_score'),
                            term_data.get('source_file'),
                            extracted_at
                        ))
                        
                        result = self.cursor.fetchone()
                        if result:
                            self.stats['inserted_terms'] += 1
                        
                        # Commit every 100 terms for progress
                        if processed_count % 100 == 0:
                            self.conn.commit()
                            logger.info(f"📊 Progress: {processed_count}/{self.stats['total_terms']} terms processed")
                    
                    except Exception as e:
                        logger.error(f"❌ Failed to insert term '{term_data.get('term', 'unknown')}': {e}")
                        self.stats['errors'] += 1
                        continue
            
            # Final commit
            self.conn.commit()
            logger.info(f"📚 Terms migration completed!")
            logger.info(f"✅ Inserted: {self.stats['inserted_terms']} terms")
            logger.info(f"⚠️  Skipped: {self.stats['skipped_terms']} terms") 
            logger.info(f"❌ Errors: {self.stats['errors']} terms")
            
        except Exception as e:
            logger.error(f"❌ Terms migration failed: {e}")
            self.conn.rollback()
            raise
    
    def create_api_views(self):
        """Create useful views for API queries"""
        try:
            logger.info("🔍 Creating API views...")
            
            # View for term details with category and subject info
            create_term_details_view = """
            CREATE OR REPLACE VIEW v_medical_term_details AS
            SELECT 
                mt.id,
                mt.term,
                mt.definition,
                mc.name as category,
                mc.color_code as category_color,
                ms.name as subject_name,
                ms.specialty,
                mt.synonyms,
                mt.related_terms,
                mt.description,
                mt.clinical_significance,
                mt.image_search_terms,
                mt.confidence_score,
                mt.created_at,
                mt.updated_at
            FROM medical_terms mt
            JOIN medical_categories mc ON mt.category_id = mc.id
            LEFT JOIN medical_subjects ms ON mt.subject_id = ms.id
            ORDER BY mt.confidence_score DESC, mt.term;
            """
            
            # View for category statistics
            create_category_stats_view = """
            CREATE OR REPLACE VIEW v_category_statistics AS
            SELECT 
                mc.id,
                mc.name as category,
                mc.color_code,
                COUNT(mt.id) as term_count,
                AVG(mt.confidence_score) as avg_confidence,
                COUNT(CASE WHEN mt.clinical_significance IS NOT NULL THEN 1 END) as terms_with_clinical_notes,
                COUNT(CASE WHEN array_length(mt.synonyms, 1) > 0 THEN 1 END) as terms_with_synonyms
            FROM medical_categories mc
            LEFT JOIN medical_terms mt ON mc.id = mt.category_id
            GROUP BY mc.id, mc.name, mc.color_code
            ORDER BY term_count DESC;
            """
            
            # View for subject statistics
            create_subject_stats_view = """
            CREATE OR REPLACE VIEW v_subject_statistics AS
            SELECT 
                ms.id,
                ms.name as subject_name,
                ms.specialty,
                COUNT(mt.id) as term_count,
                AVG(mt.confidence_score) as avg_confidence,
                COUNT(DISTINCT mt.category_id) as categories_covered
            FROM medical_subjects ms
            LEFT JOIN medical_terms mt ON ms.id = mt.subject_id
            GROUP BY ms.id, ms.name, ms.specialty
            ORDER BY term_count DESC;
            """
            
            self.cursor.execute(create_term_details_view)
            self.cursor.execute(create_category_stats_view)
            self.cursor.execute(create_subject_stats_view)
            
            self.conn.commit()
            logger.info("✅ API views created successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to create views: {e}")
            self.conn.rollback()
            raise
    
    def validate_migration(self):
        """Validate the migration results"""
        try:
            logger.info("🔍 Validating migration...")
            
            # Count terms by category
            self.cursor.execute("""
                SELECT mc.name, COUNT(mt.id) as count
                FROM medical_categories mc
                LEFT JOIN medical_terms mt ON mc.id = mt.category_id
                GROUP BY mc.name
                ORDER BY count DESC
            """)
            
            category_counts = self.cursor.fetchall()
            logger.info("📊 Terms per category:")
            for category, count in category_counts:
                logger.info(f"   📌 {category}: {count} terms")
            
            # Overall statistics
            self.cursor.execute("SELECT COUNT(*) FROM medical_terms")
            total_terms = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM medical_categories")
            total_categories = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(*) FROM medical_subjects")
            total_subjects = self.cursor.fetchone()[0]
            
            logger.info(f"✅ Migration validation:")
            logger.info(f"   📚 Total terms: {total_terms}")
            logger.info(f"   📂 Total categories: {total_categories}")
            logger.info(f"   🏥 Total subjects: {total_subjects}")
            
            # Test search function
            self.cursor.execute("SELECT * FROM search_medical_terms('choc') LIMIT 3")
            search_results = self.cursor.fetchall()
            logger.info(f"🔍 Search test for 'choc': {len(search_results)} results")
            
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
    
    def run_migration(self):
        """Run the complete migration process"""
        try:
            logger.info("🚀 Starting medical dictionary migration...")
            
            # Load data
            self.load_json_data()
            
            # Connect to database
            self.conn = get_db_connection()
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Run migration steps
            self.create_database_schema()
            self.migrate_categories()
            self.migrate_subjects()
            self.migrate_terms()
            self.create_api_views()
            self.validate_migration()
            
            logger.info("🎉 Migration completed successfully!")
            
        except Exception as e:
            logger.error(f"💥 Migration failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
    
    def print_summary(self):
        """Print migration summary"""
        logger.info("=" * 60)
        logger.info("📊 MEDICAL DICTIONARY MIGRATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"📄 Source file: {self.json_file_path}")
        logger.info(f"📚 Total terms processed: {self.stats['total_terms']}")
        logger.info(f"✅ Successfully inserted: {self.stats['inserted_terms']}")
        logger.info(f"🔄 Updated: {self.stats['updated_terms']}")
        logger.info(f"⚠️  Skipped: {self.stats['skipped_terms']}")
        logger.info(f"❌ Errors: {self.stats['errors']}")
        logger.info(f"📂 Categories created: {self.stats['categories_created']}")
        logger.info(f"🏥 Subjects processed: {self.stats['subjects_created']}")
        
        if self.stats['inserted_terms'] > 0:
            success_rate = (self.stats['inserted_terms'] / self.stats['total_terms']) * 100
            logger.info(f"📈 Success rate: {success_rate:.1f}%")
            logger.info("🎯 Database ready for medical education platform!")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate medical dictionary to PostgreSQL')
    parser.add_argument('json_file', help='Path to complete_medical_db_*.json file')
    parser.add_argument('--drop-tables', action='store_true', help='Drop existing tables before migration')
    parser.add_argument('--validate-only', action='store_true', help='Only validate existing data')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.json_file):
        logger.error(f"❌ JSON file not found: {args.json_file}")
        return 1
    
    try:
        migrator = MedicalDictionaryMigrator(args.json_file)
        
        if args.drop_tables:
            logger.warning("🗑️  Dropping existing tables...")
            conn = get_db_connection()
            cursor = conn.cursor()
            
            drop_commands = [
                "DROP VIEW IF EXISTS v_subject_statistics CASCADE;",
                "DROP VIEW IF EXISTS v_category_statistics CASCADE;",
                "DROP VIEW IF EXISTS v_medical_term_details CASCADE;",
                "DROP FUNCTION IF EXISTS search_medical_terms(TEXT) CASCADE;",
                "DROP TABLE IF EXISTS medical_terms CASCADE;",
                "DROP TABLE IF EXISTS medical_subjects CASCADE;",
                "DROP TABLE IF EXISTS medical_categories CASCADE;"
            ]
            
            for cmd in drop_commands:
                cursor.execute(cmd)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("✅ Tables dropped successfully")
        
        if args.validate_only:
            conn = get_db_connection()
            cursor = conn.cursor()
            migrator.conn = conn
            migrator.cursor = cursor
            migrator.validate_migration()
            cursor.close()
            conn.close()
        else:
            migrator.run_migration()
            migrator.print_summary()
        
        return 0
        
    except Exception as e:
        logger.error(f"💥 Migration script failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
