#!/usr/bin/env python3
"""
Simple Medical Terms Migration Script
Migrates only the medical terms to existing PostgreSQL tables
"""

import os
import json
import psycopg2
import psycopg2.extras
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('simple_migration.log'),
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

def migrate_terms(json_file_path):
    """Migrate medical terms to database"""
    try:
        # Load JSON data
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"📄 Loaded JSON data from: {json_file_path}")
        logger.info(f"📊 Total subjects: {data['metadata']['total_subjects']}")
        logger.info(f"📊 Total terms: {data['metadata']['total_terms']}")
        
        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get category ID mapping
        cursor.execute("SELECT name, category_id FROM medical_categories")
        rows = cursor.fetchall()
        logger.info(f"📂 Raw query returned {len(rows)} rows")
        category_mapping = {row['name']: row['category_id'] for row in rows}
        logger.info(f"📂 Found {len(category_mapping)} categories in database")
        if len(category_mapping) > 0:
            logger.info(f"Sample categories: {list(category_mapping.keys())[:5]}")
        else:
            logger.error("No categories found in mapping!")
        
        # Get existing subject IDs from database
        cursor.execute("SELECT subject_id FROM subjects")
        existing_subject_ids = {str(row['subject_id']) for row in cursor.fetchall()}
        logger.info(f"🏥 Found {len(existing_subject_ids)} subjects in database")
        
        # Check how many JSON subjects match
        json_subject_ids = set(data['subjects'].keys())
        matching_subjects = existing_subject_ids.intersection(json_subject_ids)
        logger.info(f"🎯 {len(matching_subjects)} subjects from JSON match database subjects")
        
        # Statistics
        stats = {
            'total_terms': data['metadata']['total_terms'],
            'inserted_terms': 0,
            'skipped_terms': 0,
            'errors': 0
        }
        
        processed_count = 0
        
        logger.info("📚 Starting terms migration...")
        
        for subject_id, subject_data in data['subjects'].items():
            # Skip if subject doesn't exist in database
            if subject_id not in existing_subject_ids:
                logger.warning(f"⚠️  Subject {subject_id[:8]} not found in database, skipping its terms")
                continue
            
            terms = subject_data.get('terms', [])
            if terms:
                logger.info(f"Processing {len(terms)} terms from subject {subject_id[:8]}...")
            
            for term_data in terms:
                try:
                    processed_count += 1
                    
                    # Get category ID
                    category_name = term_data.get('category')
                    category_id = category_mapping.get(category_name)
                    
                    if not category_id:
                        logger.warning(f"⚠️  Category not found: {category_name}")
                        stats['skipped_terms'] += 1
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
                    RETURNING term_id;
                    """
                    
                    cursor.execute(insert_term, (
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
                    
                    result = cursor.fetchone()
                    if result:
                        stats['inserted_terms'] += 1
                    
                    # Commit every 100 terms for progress
                    if processed_count % 100 == 0:
                        conn.commit()
                        logger.info(f"📊 Progress: {processed_count} terms processed, {stats['inserted_terms']} inserted")
                
                except Exception as e:
                    logger.error(f"❌ Failed to insert term '{term_data.get('term', 'unknown')}': {e}")
                    logger.debug(f"Term data: {term_data}")
                    stats['errors'] += 1
                    # Rollback this transaction and continue
                    conn.rollback()
                    continue
        
        # Final commit
        conn.commit()
        logger.info("🎉 Terms migration completed!")
        logger.info(f"✅ Inserted: {stats['inserted_terms']} terms")
        logger.info(f"⚠️  Skipped: {stats['skipped_terms']} terms") 
        logger.info(f"❌ Errors: {stats['errors']} terms")
        
        if stats['inserted_terms'] > 0:
            success_rate = (stats['inserted_terms'] / stats['total_terms']) * 100
            logger.info(f"📈 Success rate: {success_rate:.1f}%")
        
        cursor.close()
        conn.close()
        
        return stats
        
    except Exception as e:
        import traceback
        logger.error(f"❌ Migration failed: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python simple_terms_migration.py <json_file>")
        sys.exit(1)
    
    json_file = sys.argv[1]
    
    if not os.path.exists(json_file):
        logger.error(f"❌ JSON file not found: {json_file}")
        sys.exit(1)
    
    try:
        migrate_terms(json_file)
        logger.info("🎯 Migration completed successfully!")
    except Exception as e:
        logger.error(f"💥 Migration failed: {e}")
        sys.exit(1)