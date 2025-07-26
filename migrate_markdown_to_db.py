#!/usr/bin/env python3
"""
Medical Studies Content Migration Script
Migrates courses and resumes from folder structure to PostgreSQL database
"""

# Test what would be added (dry run)
# python migrate_markdown_to_db.py "/Users/mac/Documents/GitHub/Medical Studies Content" --dry-run

# Actually add new files
# python migrate_markdown_to_db.py "/Users/mac/Documents/GitHub/Medical Studies Content"

import os
import psycopg2
import psycopg2.extras
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import uuid
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
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
    return psycopg2.connect(**db_params)

# Subject ID mapping from previous analysis
SUBJECT_MAPPING = {
    "Douleurs Thoraciques Aiguës": "1c9ec5d3-5063-4969-b17a-e65b0c141f28",
    "Endocardites Infectieuses": "532af22c-1dd9-4770-9cd1-105d802814e0",
    "Hypertension Artérielle": "3236ac16-2076-4f06-b710-f330699d1af7",
    "Ischémie Aiguë des Membres": "cb851055-1b65-4a0d-8cb2-1271eb2074e7",
    "Maladies Veineuses Thrombo-Emboliques": "3243d452-f9e5-4ed9-b0f1-8235f4da9e43",
    "Syndromes Coronariens Aigus": "23c142db-8071-434d-8bd2-877ef709cf58",
    "Appendicite Aiguë": "77f84fc1-abaf-4a92-ba2e-52ee1bc14531",
    "Cancers Colorectaux": "99718615-fb84-4204-94fb-d25d1b33cf8a",
    "Hydatidoses Hépatiques & Pulmonaires": "b8a1dc5f-6226-4b79-864a-7ce86f45f7ad",
    "Hémorragies Digestives": "fc7f2549-640b-4e71-9d3a-36ec003f1ad6",
    "Occlusions Intestinales Aiguës": "62944f9f-c2eb-46fb-b776-a87f48a7c4f2",
    "Péritonites Aiguës": "2c23a9e2-1caa-408a-9bee-0fe4797f3ad8",
    "Diabète Sucré": "016e98b0-b750-4193-92c9-f9852c530377",
    "Dyslipidémies": "30c83622-a484-4f09-891d-43956057bee3",
    "Hypercalcémies": "d1726ddd-6711-42ca-bd2c-896c293e88a1",
    "Hyperthyroïdies": "f155b530-0f9c-4659-a8b9-7a447bc524de",
    "Hypothyroïdies de l'Enfant et de l'Adulte": "10fa1db1-be23-4053-b2e5-f2eec96e5c4b",
    "Insuffisance Surrénalienne Aiguë": "caa3d614-70c1-454c-8cd6-d001bfcb32c9",
    "Diarrhées Chroniques": "10d61997-5e47-4ae3-89ff-b29ec17aca67",
    "Hépatites Virales": "035f8491-94a4-4042-ac4c-64206bcfdf82",
    "Ictères": "aceefc9b-08aa-4943-9af4-7965eabab428",
    "Ulcère Gastrique & Duodénal": "e3a1f2f8-bd23-4833-b1b5-96833fc01a2e",
    "Cancer du Col de l'Utérus": "235d050b-5735-4531-8292-3bac338df812",
    "Cancer du Sein": "4375d34c-623e-42fa-bc64-cb978cd240db",
    "Contraception": "2ac116d9-ff8a-443e-8fce-8b930d99330b",
    "Grossesse Extra-Utérine": "8f39203d-b603-4e92-a5ab-0e1927b41fa8",
    "Métrorragies": "7541b256-da30-4867-8a07-0662c6abbc94",
    "Prééclampsies & Éclampsies": "4846353b-dac3-4c89-9f60-dcd37a278a89",
    "Adénopathies Superficielles": "0be29f02-209a-4f3a-8c4c-cb2fae09ba7d",
    "Anémies": "3d24722f-41d2-4115-9beb-154ad206301a",
    "Purpuras": "46d39f90-48d8-46d1-8735-03c9e2ba008d",
    "Splénomégalies": "272fe529-b3a4-4d17-9c43-c44bcaf8fbc2",
    "Transfusion Sanguine": "6c7cbb98-a28b-40d8-a5f9-7f6fde381c01",
    "Infections Sexuellement Transmissibles": "94af644e-95d2-4fb0-806f-c318eadd5341",
    "Méningites Bactériennes & Virales": "5331152a-4408-42a0-a3c0-5d44b45e2866",
    "Accident Vasculaire Cérébral": "27ce0bc6-451a-4e28-8b31-413552f18d6e",
    "Céphalées": "61c8f675-1fa5-40c4-b6f2-eae8abbc869f",
    "Épilepsies": "486c2f04-d7e0-4ba2-b8b7-663634f57a8c",
    "Hématuries": "f6ae7379-0639-4d5a-952f-ff07c2b44db6",
    "Insuffisance Rénale Aiguë": "57ce5540-be23-4900-b8c5-ee4b167690fd",
    "Troubles Acido-Basiques": "d2ed4115-fd25-487b-9ebf-b4021f2da7d0",
    "Troubles de l'Hydratation, Dyskaliémies": "34309d3a-e668-47a9-911e-10d63d122a21",
    "Œdèmes": "40f1d16d-b91b-47b4-8b6e-bbbafed109cd",
    "Cancer du Cavum": "b6c0543d-7c40-415c-bb18-783e0c572a83",
    "Dysphagies": "c9e734bf-4d1a-46f9-b2f0-12e1817d8c46",
    "Infections des Voies Aériennes Supérieures": "c661f5a9-e023-4429-87a1-e67ed531b41a",
    "Œil Rouge": "7f9c2932-4002-40f7-ac92-26bfa2a943af",
    "Arthrite Septique": "762e1d7b-3473-4fcc-aa24-9d14d081f1a3",
    "Fractures Ouvertes de la Jambe": "a1133d5b-7b93-47fa-acbc-53b306c0eb65",
    "Polyarthrite Rhumatoïde": "4803800d-6462-4f34-bad6-1f1a3a138495",
    "Bronchiolite": "d0cd0cd5-3bba-4427-a0b3-e258aaddf40b",
    "Déshydratations Aiguës de l'Enfant": "31f02921-34a4-4f5f-8227-773720c00ca2",
    "Vaccinations": "02afb7ad-cdd7-43d4-a6f0-66dc3061dabc",
    "Asthme": "ac2eddde-3d6b-465a-8c0d-a2ba958eeaa3",
    "Broncho-Pneumopathie Chronique Obstructive": "0c2cf38b-af7d-4b5c-84c9-f01bb5914ee6",
    "Cancers Broncho-Pulmonaires": "e697f089-b302-4d85-87d3-5e1011e23375",
    "Infections Respiratoires Basses": "293d6305-c692-4ee9-9bff-674a4c9512a9",
    "Tuberculose Pulmonaire Commune": "ed64f20c-9b40-4226-b9a3-6e04b687debf",
    "Schizophrénie": "88e0b809-f08a-40ec-8739-91242ff57ca7",
    "Troubles Anxieux": "d42ee3e4-deed-453c-b2c4-863d1bee23ec",
    "Troubles de l'Humeur": "263837d1-c7c8-4fb3-afac-5f4c99f61310",
    "États Confusionnels": "f0a1e16c-7721-499c-ba3a-805d653a9738",
    "Arrêt Cardiorespiratoire": "bdc975a4-e451-49d1-b280-2cc12aa7c846",
    "Brûlures Cutanées": "5bc42931-819f-4e94-baae-0beee11324e3",
    "Coma": "d751796b-356e-48d4-b9b3-b66bcac611cb",
    "Intoxications Aiguës": "700125e5-4ed8-4a91-aa34-3e06e8967a22",
    "P.E.C Douleur Aiguë": "39f823a0-6f44-4701-9fe3-8e59c330d418",
    "Polytraumatismes": "8193f532-fffc-4b78-a471-afa156a7c9ef",
    "Traumatisme Crânien": "0fb8c7cb-8d39-442c-8872-2e32891d470d",
    "État de Choc Cardiogénique": "f15e9140-64ec-4642-8ba4-26d8b10ab7f6",
    "État de Choc Hémorragique": "d843d886-89ef-4860-9e06-466c8d4e31b8",
    "États Septiques Graves": "b37c6746-00ea-4eb0-86b5-8fd2f1127fec",
    "Infections Urinaires": "22a6187b-77a4-42f6-83df-a36dd53dd672",
    "Lithiase Urinaire": "939d794c-98f5-4335-97a3-18d6396684eb",
    "Tumeur de la Prostate": "b2f94455-04c8-41e3-a58f-2eef293f85ad"
}

# Specialty mapping (you'll need to provide the actual specialty IDs)
SPECIALTY_MAPPING = {
    "Cardiologie CCV": "4cd82790-5f44-4c76-88b6-e6f21ecaa299",
    "Chirurgie Générale": "specialty-id-chirurgie",  # Replace with actual ID
    "Endocrinologie": "specialty-id-endocrinologie",  # Replace with actual ID
    "Gastrologie-Entérologie": "specialty-id-gastro",  # Replace with actual ID
    "Gynécologie-Obstétrique": "specialty-id-gyno",  # Replace with actual ID
    "Hématologie": "specialty-id-hematologie",  # Replace with actual ID
    "Infectieux": "specialty-id-infectieux",  # Replace with actual ID
    "Neurologie": "specialty-id-neurologie",  # Replace with actual ID
    "Néphrologie": "specialty-id-nephrologie",  # Replace with actual ID
    "ORL, Ophtalmologie": "specialty-id-orl",  # Replace with actual ID
    "Ortho, Rhumato": "specialty-id-ortho",  # Replace with actual ID
    "Pédiatrie": "specialty-id-pediatrie",  # Replace with actual ID
    "Pneumologie": "specialty-id-pneumologie",  # Replace with actual ID
    "Psychiatrie": "specialty-id-psychiatrie",  # Replace with actual ID
    "Réanimation": "specialty-id-reanimation",  # Replace with actual ID
    "Urologie": "specialty-id-urologie",  # Replace with actual ID
}

class MedicalContentMigrator:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.processed_folders = {}
        self.processed_documents = 0
        self.errors = []
        
    def read_file_content(self, file_path: Path) -> Optional[str]:
        """Read file content with error handling"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except UnicodeDecodeError:
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    return f.read()
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                return None
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return None
    
    def get_file_size_info(self, file_path: Path) -> str:
        """Get file size information"""
        try:
            size_bytes = file_path.stat().st_size
            if size_bytes < 1024:
                return f"{size_bytes} bytes"
            elif size_bytes < 1024 * 1024:
                return f"{size_bytes / 1024:.1f} KB"
            else:
                return f"{size_bytes / (1024 * 1024):.1f} MB"
        except Exception:
            return "Unknown size"
    
    def create_or_get_folder(self, cursor, subject_name: str) -> Optional[str]:
        """Create folder for subject or get existing one"""
        if subject_name in self.processed_folders:
            return self.processed_folders[subject_name]
        
        subject_id = SUBJECT_MAPPING.get(subject_name)
        if not subject_id:
            logger.error(f"No subject ID found for: {subject_name}")
            return None
        
        # Check if folder already exists
        cursor.execute(
            "SELECT folder_id FROM folders WHERE subject_id = %s",
            (subject_id,)
        )
        existing_folder = cursor.fetchone()
        
        if existing_folder:
            folder_id = existing_folder[0]
            logger.info(f"Using existing folder for {subject_name}: {folder_id}")
        else:
            # Create new folder
            folder_id = str(uuid.uuid4())
            cursor.execute(
                """
                INSERT INTO folders (folder_id, subject_id, name, documents_count)
                VALUES (%s, %s, %s, 0)
                """,
                (folder_id, subject_id, subject_name)
            )
            logger.info(f"Created new folder for {subject_name}: {folder_id}")
        
        self.processed_folders[subject_name] = folder_id
        return folder_id
    
    def get_next_document_order(self, cursor, folder_id: str) -> int:
        """Get the next order number for a document in a folder"""
        cursor.execute(
            'SELECT COALESCE(MAX("order"), 0) + 1 FROM documents WHERE folder_id = %s',
            (folder_id,)
        )
        return cursor.fetchone()[0]
    
    def document_exists(self, cursor, folder_id: str, filename: str) -> bool:
        """Check if a document with the same filename already exists in the folder"""
        # Extract the base filename without the full path for comparison
        base_filename = Path(filename).name
        
        cursor.execute(
            """
            SELECT COUNT(*) FROM documents 
            WHERE folder_id = %s AND (
                content LIKE %s OR 
                content LIKE %s
            )
            """,
            (folder_id, f"%{base_filename}%", f"%{base_filename.replace('.md', '')}%")
        )
        
        count = cursor.fetchone()[0]
        return count > 0
    
    def insert_document(self, cursor, folder_id: str, doc_type: str, 
                       content: str, file_path: Path) -> bool:
        """Insert a document into the database only if it doesn't already exist"""
        try:
            # Check if document already exists
            if self.document_exists(cursor, folder_id, file_path.name):
                logger.info(f"Document already exists, skipping: {file_path.name}")
                return False
            
            document_id = str(uuid.uuid4())
            order = self.get_next_document_order(cursor, folder_id)
            size = self.get_file_size_info(file_path)
            
            cursor.execute(
                """
                INSERT INTO documents 
                (document_id, folder_id, type, content, size, "order", extension)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (document_id, folder_id, doc_type, content, size, order, 'markdown')
            )
            
            logger.info(f"Inserted NEW {doc_type} document: {file_path.name}")
            self.processed_documents += 1
            return True
            
        except Exception as e:
            error_msg = f"Failed to insert document {file_path}: {e}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return False
    
    def process_subject_folder(self, cursor, subject_path: Path, subject_name: str):
        """Process a single subject folder and add new documents only"""
        logger.info(f"Processing subject: {subject_name}")
        
        # Create folder for this subject
        folder_id = self.create_or_get_folder(cursor, subject_name)
        if not folder_id:
            return
        
        new_documents_count = 0
        
        # Process all possible folder types dynamically
        for subfolder in subject_path.iterdir():
            if not subfolder.is_dir():
                continue
                
            folder_type = subfolder.name.lower()
            
            # Map folder names to document types
            type_mapping = {
                'courses': 'course',
                'resumes': 'resume', 
                'flashcards': 'flashcard',
                'quizzes': 'quiz',
                'tests': 'quiz',
                'mindmaps': 'mindcard',
                'summaries': 'summary',
                'notes': 'note',
                'recordings': 'recording',
                'audio': 'recording'
            }
            
            # Get the document type based on folder name
            doc_type = type_mapping.get(folder_type, folder_type)  # Use folder name as type if not in mapping
            
            logger.info(f"  Processing {folder_type} folder -> document type: {doc_type}")
            
            # Process all markdown files in this subfolder
            md_files = list(subfolder.glob("*.md"))
            if md_files:
                logger.info(f"    Found {len(md_files)} markdown files")
                
                for file_path in md_files:
                    content = self.read_file_content(file_path)
                    if content:
                        if self.insert_document(cursor, folder_id, doc_type, content, file_path):
                            new_documents_count += 1
            else:
                logger.info(f"    No markdown files found in {folder_type}")
        
        if new_documents_count > 0:
            logger.info(f"Added {new_documents_count} new documents to {subject_name}")
        else:
            logger.info(f"No new documents to add for {subject_name}")
    
    def scan_files_dry_run(self):
        """Scan files without database operations for dry run"""
        logger.info("Scanning files for dry run...")
        
        found_files = []
        
        # Iterate through specialty folders
        for specialty_path in self.base_path.iterdir():
            if not specialty_path.is_dir():
                continue
            
            specialty_name = specialty_path.name
            if specialty_name in ["tree.md", "__pycache__", "tree_generator.py", "venv", ".git"]:  # Skip non-medical folders
                continue
            
            logger.info(f"Scanning specialty: {specialty_name}")
            
            # Iterate through subject folders within each specialty
            for subject_path in specialty_path.iterdir():
                if not subject_path.is_dir():
                    continue
                
                subject_name = subject_path.name
                if subject_name in SUBJECT_MAPPING:
                    logger.info(f"  Found subject: {subject_name}")
                    
                    # Scan all possible subfolders dynamically
                    for subfolder in subject_path.iterdir():
                        if not subfolder.is_dir():
                            continue
                            
                        folder_type = subfolder.name.lower()
                        
                        # Map folder names to document types
                        type_mapping = {
                            'courses': 'course',
                            'resumes': 'resume', 
                            'flashcards': 'flashcard',
                            'quizzes': 'quiz',
                            'tests': 'quiz',
                            'mindmaps': 'mindcard',
                            'summaries': 'summary',
                            'notes': 'note',
                            'recordings': 'recording',
                            'audio': 'recording'
                        }
                        
                        doc_type = type_mapping.get(folder_type, folder_type)
                        
                        md_files = list(subfolder.glob("*.md"))
                        for file_path in md_files:
                            found_files.append({
                                'specialty': specialty_name,
                                'subject': subject_name,
                                'type': doc_type,
                                'folder': folder_type,
                                'file': file_path,
                                'size': self.get_file_size_info(file_path)
                            })
                            logger.info(f"    {doc_type.title()}: {file_path.name} ({self.get_file_size_info(file_path)})")
                
                else:
                    logger.warning(f"  Subject not in mapping: {subject_name}")
        
        return found_files
    
    def scan_and_migrate(self, dry_run=False):
        """Main migration process"""
        if dry_run:
            found_files = self.scan_files_dry_run()
            self.processed_documents = len(found_files)
            unique_subjects = set(f['subject'] for f in found_files)
            for subject in unique_subjects:
                self.processed_folders[subject] = f"dry-run-{subject}"
            return
        
        logger.info("Starting medical content migration...")
        
        try:
            with get_db_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    
                    # Iterate through specialty folders
                    for specialty_path in self.base_path.iterdir():
                        if not specialty_path.is_dir():
                            continue
                        
                        specialty_name = specialty_path.name
                        if specialty_name in ["tree.md", "__pycache__", "tree_generator.py", "venv", ".git"]:  # Skip non-medical folders
                            continue
                        
                        logger.info(f"Processing specialty: {specialty_name}")
                        
                        # Iterate through subject folders within each specialty
                        for subject_path in specialty_path.iterdir():
                            if not subject_path.is_dir():
                                continue
                            
                            subject_name = subject_path.name
                            if subject_name in SUBJECT_MAPPING:
                                self.process_subject_folder(cursor, subject_path, subject_name)
                    
                    # Commit all changes
                    conn.commit()
                    logger.info("Migration completed successfully!")
                    
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
    
    def print_summary(self):
        """Print migration summary"""
        logger.info("=== MIGRATION SUMMARY ===")
        logger.info(f"Folders created/processed: {len(self.processed_folders)}")
        logger.info(f"NEW documents migrated: {self.processed_documents}")
        logger.info(f"Errors encountered: {len(self.errors)}")
        
        if self.processed_documents > 0:
            logger.info(f"✅ Successfully added {self.processed_documents} new documents to the database")
        else:
            logger.info("ℹ️  No new documents were found to add")
        
        if self.errors:
            logger.info("❌ Errors:")
            for error in self.errors:
                logger.error(f"  - {error}")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate medical content to database')
    parser.add_argument('base_path', help='Base path to Medical Studies Content folder')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without database changes')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.base_path):
        logger.error(f"Base path does not exist: {args.base_path}")
        return 1
    
    try:
        migrator = MedicalContentMigrator(args.base_path)
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No database changes will be made")
            migrator.scan_and_migrate(dry_run=True)
        else:
            migrator.scan_and_migrate(dry_run=False)
        
        migrator.print_summary()
        return 0
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
