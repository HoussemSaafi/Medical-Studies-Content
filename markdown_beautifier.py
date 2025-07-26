#!/usr/bin/env python3
"""
Medical Markdown Beautifier Script
Enhances markdown formatting while preserving all content word-for-word
Uses Mistral AI API with multiple workers for concurrent processing
"""

import os
import re
import json
import time
import logging
import requests
import threading
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue as ThreadQueue
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Worker-%(thread)d] %(message)s',
    handlers=[
        logging.FileHandler('markdown_beautifier.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Mistral API configuration
MISTRAL_API_KEYS = [
    "R5LVIIcfyTw42PUN8XW8Bf4TtOd8bG85",
    "pFL2BXaV6Q1Je1Nkg5oAFsY0uCDS2h5v",
    "4P1XX0iaDIHCc4KbjWpeC5IRtrCkUj0s",
    "hPZyLQqACzOic8U1jep6uOMJnintFFxc",
    "16RtgR4NenVkjLfGFeaxVjl5oLLgSf14"
]

MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"
MAX_CHUNK_SIZE = 4000  # Maximum characters per API request
RATE_LIMIT_DELAY = 1  # Seconds between API calls per worker
MAX_RETRIES = 3

# Subject ID mapping (from your original script)
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

class MarkdownBeautifier:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.api_keys = MISTRAL_API_KEYS.copy()
        self.processed_files = 0
        self.total_files = 0
        self.errors = []
        self.success_count = 0
        self.max_chunk_size = MAX_CHUNK_SIZE  # Instance variable
        
        # Thread-safe counters
        self.lock = threading.Lock()
        
        # Track processed files to avoid duplicates
        self.processed_file_hashes = set()
    
    def get_content_hash(self, content: str) -> str:
        """Generate a hash of the content for duplicate detection"""
        return hashlib.md5(content.encode('utf-8')).hexdigest()[:8]
    
    def split_markdown_into_chunks(self, content: str) -> List[str]:
        """Split markdown content into manageable chunks for API processing"""
        if len(content) <= self.max_chunk_size:
            return [content]
        
        chunks = []
        lines = content.split('\n')
        current_chunk = ""
        
        for line in lines:
            # If adding this line would exceed the limit, start a new chunk
            if len(current_chunk) + len(line) + 1 > self.max_chunk_size and current_chunk:
                chunks.append(current_chunk.strip())
                current_chunk = line
            else:
                if current_chunk:
                    current_chunk += '\n' + line
                else:
                    current_chunk = line
        
        # Add the last chunk
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def call_mistral_api(self, content: str, api_key: str, worker_id: int) -> Optional[str]:
        """Call Mistral API to beautify markdown content"""
        prompt = f"""You are a markdown formatting expert. Your task is to enhance the visual presentation of the following medical markdown content while STRICTLY preserving every single word, number, medical term, and piece of information.

CRITICAL REQUIREMENTS:
1. DO NOT change, remove, add, or modify ANY word or content
2. DO NOT translate or alter medical terms
3. DO NOT change numbers, dosages, or medical values
4. ONLY improve markdown formatting for better visual presentation

What you CAN do:
- Add proper heading hierarchies (# ## ### #### #####)
- Use **bold** for important medical terms and key concepts
- Use *italics* for emphasis where appropriate
- Create proper bullet points and numbered lists
- Add horizontal rules (---) for section breaks
- Use `code blocks` for dosages, measurements, or technical terms
- Create proper tables if data is presented in tabular format
- Add line breaks for better readability
- Use > blockquotes for important notes or warnings

Here is the markdown content to beautify:

{content}

Return ONLY the beautified markdown with improved formatting. Preserve every word exactly as written."""

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "mistral-small-latest",
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,  # Low temperature for consistent formatting
            "max_tokens": 8000
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Worker {worker_id}: Making API call (attempt {attempt + 1})")
                response = requests.post(MISTRAL_API_URL, headers=headers, json=data, timeout=60)
                
                if response.status_code == 200:
                    result = response.json()
                    if 'choices' in result and len(result['choices']) > 0:
                        beautified_content = result['choices'][0]['message']['content'].strip()
                        logger.info(f"Worker {worker_id}: Successfully beautified chunk")
                        return beautified_content
                    else:
                        logger.error(f"Worker {worker_id}: No choices in API response")
                        return None
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"Worker {worker_id}: Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Worker {worker_id}: API error {response.status_code}: {response.text}")
                    if attempt == MAX_RETRIES - 1:
                        return None
                    time.sleep(2)
                
            except requests.exceptions.Timeout:
                logger.error(f"Worker {worker_id}: API timeout (attempt {attempt + 1})")
                if attempt == MAX_RETRIES - 1:
                    return None
                time.sleep(5)
            except Exception as e:
                logger.error(f"Worker {worker_id}: API call failed: {e}")
                if attempt == MAX_RETRIES - 1:
                    return None
                time.sleep(2)
        
        return None
    
    def beautify_markdown_file(self, file_path: Path, api_key: str, worker_id: int) -> bool:
        """Beautify a single markdown file"""
        try:
            # Read original content
            with open(file_path, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            # Check if already processed (avoid duplicates)
            content_hash = self.get_content_hash(original_content)
            if content_hash in self.processed_file_hashes:
                logger.info(f"Worker {worker_id}: Skipping duplicate content in {file_path.name}")
                return True
            
            self.processed_file_hashes.add(content_hash)
            
            # Skip if file is very small or empty
            if len(original_content.strip()) < 50:
                logger.info(f"Worker {worker_id}: Skipping small file {file_path.name}")
                return True
            
            logger.info(f"Worker {worker_id}: Processing {file_path.name} ({len(original_content)} chars)")
            
            # Split content into chunks
            chunks = self.split_markdown_into_chunks(original_content)
            logger.info(f"Worker {worker_id}: Split into {len(chunks)} chunks")
            
            # Process each chunk
            beautified_chunks = []
            for i, chunk in enumerate(chunks):
                logger.info(f"Worker {worker_id}: Processing chunk {i+1}/{len(chunks)}")
                
                beautified_chunk = self.call_mistral_api(chunk, api_key, worker_id)
                if beautified_chunk is None:
                    logger.error(f"Worker {worker_id}: Failed to beautify chunk {i+1}")
                    return False
                
                beautified_chunks.append(beautified_chunk)
                
                # Rate limiting between chunks
                time.sleep(RATE_LIMIT_DELAY)
            
            # Combine all beautified chunks
            final_content = '\n\n---\n\n'.join(beautified_chunks)
            
            # Create backup of original file
            backup_path = file_path.with_suffix('.md.backup')
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(original_content)
            
            # Write beautified content
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(final_content)
            
            logger.info(f"Worker {worker_id}: Successfully beautified {file_path.name}")
            
            with self.lock:
                self.success_count += 1
            
            return True
            
        except UnicodeDecodeError:
            try:
                # Try with latin-1 encoding
                with open(file_path, 'r', encoding='latin-1') as f:
                    original_content = f.read()
                
                # Process with utf-8 output
                chunks = self.split_markdown_into_chunks(original_content)
                beautified_chunks = []
                
                for i, chunk in enumerate(chunks):
                    beautified_chunk = self.call_mistral_api(chunk, api_key, worker_id)
                    if beautified_chunk is None:
                        return False
                    beautified_chunks.append(beautified_chunk)
                    time.sleep(RATE_LIMIT_DELAY)
                
                final_content = '\n\n---\n\n'.join(beautified_chunks)
                
                # Create backup and write new content
                backup_path = file_path.with_suffix('.md.backup')
                with open(backup_path, 'w', encoding='latin-1') as f:
                    f.write(original_content)
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(final_content)
                
                logger.info(f"Worker {worker_id}: Successfully beautified {file_path.name} (latin-1)")
                
                with self.lock:
                    self.success_count += 1
                
                return True
                
            except Exception as e:
                error_msg = f"Worker {worker_id}: Failed to read {file_path}: {e}"
                logger.error(error_msg)
                with self.lock:
                    self.errors.append(error_msg)
                return False
        except Exception as e:
            error_msg = f"Worker {worker_id}: Failed to process {file_path}: {e}"
            logger.error(error_msg)
            with self.lock:
                self.errors.append(error_msg)
            return False
    
    def collect_all_markdown_files(self) -> List[Path]:
        """Collect all markdown files from the directory structure"""
        md_files = []
        
        # Iterate through specialty folders
        for specialty_path in self.base_path.iterdir():
            if not specialty_path.is_dir():
                continue
            
            specialty_name = specialty_path.name
            if specialty_name in ["tree.md", "__pycache__", "tree_generator.py", "venv", ".git", "images"]:
                continue
            
            logger.info(f"Scanning specialty: {specialty_name}")
            
            # Iterate through subject folders
            for subject_path in specialty_path.iterdir():
                if not subject_path.is_dir():
                    continue
                
                subject_name = subject_path.name
                if subject_name not in SUBJECT_MAPPING:
                    logger.warning(f"Subject not in mapping: {subject_name}")
                    continue
                
                # Scan all subfolders for markdown files
                for subfolder in subject_path.iterdir():
                    if not subfolder.is_dir():
                        continue
                    
                    # Find all markdown files
                    for md_file in subfolder.glob("*.md"):
                        md_files.append(md_file)
        
        self.total_files = len(md_files)
        logger.info(f"Found {self.total_files} markdown files to process")
        return md_files
    
    def worker_function(self, file_queue: ThreadQueue, api_key: str, worker_id: int):
        """Worker function to process files from the queue"""
        logger.info(f"Worker {worker_id} started with API key: {api_key[:10]}...")
        
        while True:
            try:
                file_path = file_queue.get(timeout=1)
                if file_path is None:  # Sentinel value to stop worker
                    break
                
                success = self.beautify_markdown_file(file_path, api_key, worker_id)
                
                with self.lock:
                    self.processed_files += 1
                    progress = (self.processed_files / self.total_files) * 100
                    logger.info(f"Progress: {self.processed_files}/{self.total_files} ({progress:.1f}%)")
                
                file_queue.task_done()
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Unexpected error: {e}")
                break
        
        logger.info(f"Worker {worker_id} finished")
    
    def beautify_all_files(self, dry_run: bool = False):
        """Main function to beautify all markdown files using multiple workers"""
        if dry_run:
            logger.info("DRY RUN MODE - No files will be modified")
            md_files = self.collect_all_markdown_files()
            logger.info(f"Would process {len(md_files)} markdown files")
            return
        
        logger.info("Starting markdown beautification process...")
        logger.info(f"Using {len(self.api_keys)} API keys with {len(self.api_keys)} workers")
        
        # Collect all markdown files
        md_files = self.collect_all_markdown_files()
        if not md_files:
            logger.warning("No markdown files found to process")
            return
        
        # Create file queue
        file_queue = ThreadQueue()
        
        # Add all files to queue
        for file_path in md_files:
            file_queue.put(file_path)
        
        # Add sentinel values to stop workers
        for _ in range(len(self.api_keys)):
            file_queue.put(None)
        
        # Start workers
        workers = []
        for i, api_key in enumerate(self.api_keys):
            worker = threading.Thread(
                target=self.worker_function,
                args=(file_queue, api_key, i + 1)
            )
            worker.start()
            workers.append(worker)
        
        # Wait for all workers to complete
        for worker in workers:
            worker.join()
        
        logger.info("All workers completed")
    
    def print_summary(self):
        """Print beautification summary"""
        logger.info("=== MARKDOWN BEAUTIFICATION SUMMARY ===")
        logger.info(f"Total files found: {self.total_files}")
        logger.info(f"Files processed: {self.processed_files}")
        logger.info(f"Successfully beautified: {self.success_count}")
        logger.info(f"Errors encountered: {len(self.errors)}")
        
        if self.success_count > 0:
            logger.info(f"✅ Successfully beautified {self.success_count} markdown files")
            logger.info("📁 Original files backed up with .backup extension")
        
        if self.errors:
            logger.info("❌ Errors:")
            for error in self.errors[:10]:  # Show first 10 errors
                logger.error(f"  - {error}")
            if len(self.errors) > 10:
                logger.error(f"  ... and {len(self.errors) - 10} more errors")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Beautify markdown files while preserving all content')
    parser.add_argument('base_path', help='Base path to Medical Studies Content folder')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without modifying files')
    parser.add_argument('--max-chunk-size', type=int, default=4000, 
                       help=f'Maximum characters per API request (default: 4000)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.base_path):
        logger.error(f"Base path does not exist: {args.base_path}")
        return 1
    
    try:
        # Create beautifier with custom chunk size if specified
        beautifier = MarkdownBeautifier(args.base_path)
        if args.max_chunk_size != 4000:
            beautifier.max_chunk_size = args.max_chunk_size
            logger.info(f"Using custom chunk size: {args.max_chunk_size}")
        
        if args.dry_run:
            beautifier.beautify_all_files(dry_run=True)
        else:
            logger.warning("This will modify your markdown files. Original files will be backed up with .backup extension.")
            confirm = input("Continue? (y/N): ")
            if confirm.lower() != 'y':
                logger.info("Operation cancelled")
                return 0
            
            beautifier.beautify_all_files(dry_run=False)
        
        beautifier.print_summary()
        return 0
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Markdown beautification failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
