#!/usr/bin/env python3
"""
Medical Knowledge Extractor & Augmentation Script
Uses Mistral Large to extract and enhance medical terminology from markdown content
Generates a comprehensive medical knowledge database with definitions, imagery, and context
"""

import os
import re
import json
import time
import logging
import requests
import threading
import hashlib
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue as ThreadQueue
from datetime import datetime
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [Worker-%(thread)d] %(message)s',
    handlers=[
        logging.FileHandler('medical_knowledge_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Mistral API configuration - using Large model for enhanced reasoning
MISTRAL_API_KEYS = [
    "R5LVIIcfyTw42PUN8XW8Bf4TtOd8bG85",
    "pFL2BXaV6Q1Je1Nkg5oAFsY0uCDS2h5v",
    "4P1XX0iaDIHCc4KbjWpeC5IRtrCkUj0s",
    "hPZyLQqACzOic8U1jep6uOMJnintFFxc",
    "16RtgR4NenVkjLfGFeaxVjl5oLLgSf14"
]

MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"
MAX_CHUNK_SIZE = 3500  # Smaller chunks for better term extraction
RATE_LIMIT_DELAY = 2   # Longer delay for Large model
MAX_RETRIES = 3

# Subject ID mapping for context tracking
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

class MedicalKnowledgeExtractor:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.api_keys = MISTRAL_API_KEYS.copy()
        self.processed_files = 0
        self.total_files = 0
        self.total_terms_extracted = 0
        self.errors = []
        self.success_count = 0
        self.max_chunk_size = MAX_CHUNK_SIZE
        
        # Thread-safe counters and storage
        self.lock = threading.Lock()
        self.extracted_terms = {}  # Store all extracted terms with deduplication
        self.subject_term_mapping = {}  # Track which subject each term belongs to
        
        # Output file
        self.output_file = f"medical_knowledge_database_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    def get_medical_extraction_prompt(self) -> str:
        """Generate sophisticated prompt for medical term extraction"""
        return """Vous êtes un expert médical français spécialisé dans l'extraction et la définition de terminologie médicale. Votre tâche est d'identifier et de définir avec précision tous les termes médicaux significatifs dans le texte fourni.

OBJECTIF PRINCIPAL:
Extraire UNIQUEMENT les termes médicaux qui nécessitent une définition, une explication ou une illustration pour améliorer la compréhension du contenu médical.

CRITÈRES D'EXTRACTION:
1. **Anatomie**: Structures corporelles, organes, systèmes (ex: nasopharynx, myocarde, cortex cérébral)
2. **Pathologie**: Maladies, syndromes, troubles (ex: embolie pulmonaire, syndrome coronarien aigu)
3. **Pharmacologie**: Médicaments, classes thérapeutiques, molécules (ex: bêta-bloquants, morphine)
4. **Physiologie**: Processus biologiques, mécanismes (ex: hémostase, glycolyse)
5. **Imagerie médicale**: Techniques, signes radiologiques (ex: IRM, aspect en "verre dépoli")
6. **Classifications**: Scores, stades, classifications (ex: TNM, Glasgow, NYHA)
7. **Acronymes médicaux**: Sigles utilisés en médecine (ex: SCA, AVC, BPCO)
8. **Techniques médicales**: Procédures, examens, interventions

INSTRUCTIONS STRICTES:
- Ne PAS extraire les mots communs, adjectifs simples, ou termes non-médicaux
- Ne PAS inventer ou halluciner d'informations
- Fournir UNIQUEMENT des définitions factuelles et vérifiées
- Utiliser la terminologie médicale française standard
- Si un terme nécessite une image pour être mieux compris, suggérer une description d'image appropriée

FORMAT DE RÉPONSE OBLIGATOIRE:
Retournez UNIQUEMENT un JSON valid contenant un array d'objets. Chaque terme doit respecter cette structure exacte:

```json
[
  {
    "term": "terme médical exact tel qu'il apparaît dans le texte",
    "definition": "définition médicale précise et concise en français",
    "category": "anatomy|pathology|pharmacology|physiology|imaging|classification|procedure|acronym",
    "synonyms": ["synonyme1", "synonyme2"],
    "relatedTerms": ["terme_connexe1", "terme_connexe2"],
    "imageDescription": "Description précise d'une image médicale qui aiderait à visualiser ce terme (optionnel)",
    "clinicalRelevance": "Pourquoi ce terme est important dans le contexte médical"
  }
]
```

EXEMPLES DE QUALITÉ ATTENDUE:
- ✅ "nasopharynx" → Terme anatomique nécessitant définition et localisation
- ✅ "carcinome épidermoïde" → Terme pathologique complexe
- ✅ "IRM" → Acronyme médical important
- ❌ "patient" → Mot commun, non spécialisé
- ❌ "important" → Adjectif général
- ❌ "traitement" → Terme trop générique

ATTENTION: 
- Retournez UNIQUEMENT le JSON, aucun autre texte
- Assurez-vous que le JSON est parfaitement formaté et valide
- Si aucun terme médical significatif n'est trouvé, retournez: []
- Maximum 15 termes par chunk pour maintenir la qualité

TEXTE À ANALYSER:
"""

    def split_content_into_chunks(self, content: str) -> List[str]:
        """Split content into semantic chunks for better term extraction"""
        if len(content) <= self.max_chunk_size:
            return [content]
        
        chunks = []
        
        # First, try to split by major sections (headers)
        sections = re.split(r'\n(?=#{1,3}\s)', content)
        
        current_chunk = ""
        for section in sections:
            # If adding this section would exceed the limit, finalize current chunk
            if len(current_chunk) + len(section) > self.max_chunk_size and current_chunk:
                chunks.append(current_chunk.strip())
                current_chunk = section
            else:
                if current_chunk:
                    current_chunk += '\n' + section
                else:
                    current_chunk = section
        
        # Handle remaining content
        if current_chunk.strip():
            # If the last chunk is still too big, split by paragraphs
            if len(current_chunk) > self.max_chunk_size:
                paragraphs = current_chunk.split('\n\n')
                temp_chunk = ""
                
                for paragraph in paragraphs:
                    if len(temp_chunk) + len(paragraph) > self.max_chunk_size and temp_chunk:
                        chunks.append(temp_chunk.strip())
                        temp_chunk = paragraph
                    else:
                        if temp_chunk:
                            temp_chunk += '\n\n' + paragraph
                        else:
                            temp_chunk = paragraph
                
                if temp_chunk.strip():
                    chunks.append(temp_chunk.strip())
            else:
                chunks.append(current_chunk.strip())
        
        return chunks if chunks else [content]

    def call_mistral_api(self, content: str, api_key: str, worker_id: int, subject_name: str) -> Optional[List[Dict]]:
        """Call Mistral Large API to extract medical terms"""
        prompt = self.get_medical_extraction_prompt() + content

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "mistral-large-latest",
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.2,  # Low temperature for factual extraction
            "max_tokens": 4000   # Enough for detailed terms
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Worker {worker_id}: Making API call for {subject_name} (attempt {attempt + 1})")
                response = requests.post(MISTRAL_API_URL, headers=headers, json=data, timeout=90)
                
                if response.status_code == 200:
                    result = response.json()
                    if 'choices' in result and len(result['choices']) > 0:
                        content_text = result['choices'][0]['message']['content'].strip()
                        
                        # Clean up the response to extract JSON
                        content_text = content_text.replace('```json', '').replace('```', '').strip()
                        
                        try:
                            extracted_terms = json.loads(content_text)
                            if isinstance(extracted_terms, list):
                                logger.info(f"Worker {worker_id}: Extracted {len(extracted_terms)} terms")
                                return extracted_terms
                            else:
                                logger.warning(f"Worker {worker_id}: Invalid response format")
                                return []
                        except json.JSONDecodeError as e:
                            logger.error(f"Worker {worker_id}: JSON parsing error: {e}")
                            logger.error(f"Raw response: {content_text[:500]}...")
                            return []
                    else:
                        logger.error(f"Worker {worker_id}: No choices in API response")
                        return []
                        
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 10  # Longer wait for Large model
                    logger.warning(f"Worker {worker_id}: Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Worker {worker_id}: API error {response.status_code}: {response.text}")
                    if attempt == MAX_RETRIES - 1:
                        return []
                    time.sleep(5)
                
            except requests.exceptions.Timeout:
                logger.error(f"Worker {worker_id}: API timeout (attempt {attempt + 1})")
                if attempt == MAX_RETRIES - 1:
                    return []
                time.sleep(10)
            except Exception as e:
                logger.error(f"Worker {worker_id}: API call failed: {e}")
                if attempt == MAX_RETRIES - 1:
                    return []
                time.sleep(5)
        
        return []

    def validate_and_enhance_term(self, term_data: Dict, subject_id: str) -> Optional[Dict]:
        """Validate and enhance extracted term data"""
        required_fields = ['term', 'definition', 'category']
        
        # Check required fields
        for field in required_fields:
            if field not in term_data or not term_data[field]:
                return None
        
        # Validate category
        valid_categories = ['anatomy', 'pathology', 'pharmacology', 'physiology', 'imaging', 'classification', 'procedure', 'acronym']
        if term_data['category'] not in valid_categories:
            return None
        
        # Clean and validate term
        term = term_data['term'].strip()
        if len(term) < 2 or len(term) > 100:  # Reasonable term length
            return None
        
        # Enhanced term object
        enhanced_term = {
            'id': str(uuid.uuid4()),
            'term': term,
            'definition': term_data['definition'].strip(),
            'category': term_data['category'],
            'subjectId': subject_id,
            'synonyms': term_data.get('synonyms', []),
            'relatedTerms': term_data.get('relatedTerms', []),
            'imageDescription': term_data.get('imageDescription', '').strip(),
            'clinicalRelevance': term_data.get('clinicalRelevance', '').strip(),
            'extractedAt': datetime.now().isoformat(),
            'confidence': 'high'  # Mistral Large provides high-quality extractions
        }
        
        return enhanced_term

    def process_markdown_file(self, file_path: Path, subject_name: str, subject_id: str, api_key: str, worker_id: int) -> int:
        """Process a single markdown file and extract medical terms"""
        try:
            # Read file content
            with open(file_path, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            # Skip very small files
            if len(original_content.strip()) < 100:
                logger.info(f"Worker {worker_id}: Skipping small file {file_path.name}")
                return 0
            
            logger.info(f"Worker {worker_id}: Processing {file_path.name} ({len(original_content)} chars)")
            
            # Split content into chunks
            chunks = self.split_content_into_chunks(original_content)
            logger.info(f"Worker {worker_id}: Split into {len(chunks)} chunks")
            
            # Process each chunk
            total_terms_extracted = 0
            
            for i, chunk in enumerate(chunks):
                logger.info(f"Worker {worker_id}: Processing chunk {i+1}/{len(chunks)}")
                
                extracted_terms = self.call_mistral_api(chunk, api_key, worker_id, subject_name)
                if extracted_terms is None:
                    logger.error(f"Worker {worker_id}: Failed to extract terms from chunk {i+1}")
                    continue
                
                # Validate and store terms
                for term_data in extracted_terms:
                    enhanced_term = self.validate_and_enhance_term(term_data, subject_id)
                    if enhanced_term:
                        term_key = enhanced_term['term'].lower()
                        
                        with self.lock:
                            # Store with deduplication
                            if term_key not in self.extracted_terms:
                                self.extracted_terms[term_key] = enhanced_term
                                self.total_terms_extracted += 1
                                
                                # Track subject association
                                if subject_id not in self.subject_term_mapping:
                                    self.subject_term_mapping[subject_id] = []
                                self.subject_term_mapping[subject_id].append(term_key)
                            else:
                                # Update existing term with additional context if needed
                                existing_term = self.extracted_terms[term_key]
                                if subject_id not in existing_term.get('sources', []):
                                    if 'sources' not in existing_term:
                                        existing_term['sources'] = []
                                    existing_term['sources'].append(subject_id)
                
                total_terms_extracted += len(extracted_terms) if extracted_terms else 0
                
                # Rate limiting between chunks
                time.sleep(RATE_LIMIT_DELAY)
            
            logger.info(f"Worker {worker_id}: Extracted {total_terms_extracted} terms from {file_path.name}")
            
            with self.lock:
                self.success_count += 1
            
            return total_terms_extracted
            
        except UnicodeDecodeError:
            try:
                # Try with latin-1 encoding
                with open(file_path, 'r', encoding='latin-1') as f:
                    original_content = f.read()
                # Recursive call with decoded content
                return self.process_markdown_file(file_path, subject_name, subject_id, api_key, worker_id)
            except Exception as e:
                error_msg = f"Worker {worker_id}: Failed to read {file_path}: {e}"
                logger.error(error_msg)
                with self.lock:
                    self.errors.append(error_msg)
                return 0
        except Exception as e:
            error_msg = f"Worker {worker_id}: Failed to process {file_path}: {e}"
            logger.error(error_msg)
            with self.lock:
                self.errors.append(error_msg)
            return 0

    def collect_all_markdown_files(self) -> List[Tuple[Path, str, str]]:
        """Collect all markdown files with their subject information"""
        file_info = []
        
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
                
                subject_id = SUBJECT_MAPPING[subject_name]
                
                # Scan all subfolders for markdown files
                for subfolder in subject_path.iterdir():
                    if not subfolder.is_dir():
                        continue
                    
                    # Find all markdown files
                    for md_file in subfolder.glob("*.md"):
                        file_info.append((md_file, subject_name, subject_id))
        
        self.total_files = len(file_info)
        logger.info(f"Found {self.total_files} markdown files to process")
        return file_info

    def worker_function(self, file_queue: ThreadQueue, api_key: str, worker_id: int):
        """Worker function to process files from the queue"""
        logger.info(f"Worker {worker_id} started with API key: {api_key[:10]}...")
        
        while True:
            try:
                file_info = file_queue.get(timeout=1)
                if file_info is None:  # Sentinel value to stop worker
                    break
                
                file_path, subject_name, subject_id = file_info
                terms_extracted = self.process_markdown_file(file_path, subject_name, subject_id, api_key, worker_id)
                
                with self.lock:
                    self.processed_files += 1
                    progress = (self.processed_files / self.total_files) * 100
                    logger.info(f"Progress: {self.processed_files}/{self.total_files} ({progress:.1f}%) - Total terms: {self.total_terms_extracted}")
                
                file_queue.task_done()
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Unexpected error: {e}")
                break
        
        logger.info(f"Worker {worker_id} finished")

    def extract_all_medical_knowledge(self, dry_run: bool = False):
        """Main function to extract medical knowledge using multiple workers"""
        if dry_run:
            logger.info("DRY RUN MODE - No API calls will be made")
            file_info = self.collect_all_markdown_files()
            logger.info(f"Would process {len(file_info)} markdown files")
            return
        
        logger.info("Starting medical knowledge extraction process...")
        logger.info(f"Using {len(self.api_keys)} API keys with {len(self.api_keys)} workers")
        
        # Collect all markdown files
        file_info = self.collect_all_markdown_files()
        if not file_info:
            logger.warning("No markdown files found to process")
            return
        
        # Create file queue
        file_queue = ThreadQueue()
        
        # Add all files to queue
        for info in file_info:
            file_queue.put(info)
        
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
        
        # Save extracted knowledge to file
        self.save_knowledge_database()

    def save_knowledge_database(self):
        """Save the extracted medical knowledge database"""
        try:
            # Prepare final database structure
            database = {
                'metadata': {
                    'generatedAt': datetime.now().isoformat(),
                    'totalTerms': len(self.extracted_terms),
                    'totalSubjects': len(self.subject_term_mapping),
                    'extractionStats': {
                        'filesProcessed': self.processed_files,
                        'successfulExtractions': self.success_count,
                        'errors': len(self.errors)
                    }
                },
                'terms': list(self.extracted_terms.values()),
                'subjectMapping': self.subject_term_mapping,
                'categories': self._generate_category_stats()
            }
            
            # Save to JSON file
            output_path = self.base_path / self.output_file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(database, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ Medical knowledge database saved to: {output_path}")
            logger.info(f"📊 Database contains {len(self.extracted_terms)} unique medical terms")
            
        except Exception as e:
            logger.error(f"Failed to save knowledge database: {e}")

    def _generate_category_stats(self) -> Dict[str, int]:
        """Generate statistics by category"""
        category_stats = {}
        for term_data in self.extracted_terms.values():
            category = term_data.get('category', 'unknown')
            category_stats[category] = category_stats.get(category, 0) + 1
        return category_stats

    def print_summary(self):
        """Print extraction summary"""
        logger.info("=== MEDICAL KNOWLEDGE EXTRACTION SUMMARY ===")
        logger.info(f"Total files found: {self.total_files}")
        logger.info(f"Files processed: {self.processed_files}")
        logger.info(f"Successful extractions: {self.success_count}")
        logger.info(f"Total unique terms extracted: {len(self.extracted_terms)}")
        logger.info(f"Errors encountered: {len(self.errors)}")
        
        # Category breakdown
        category_stats = self._generate_category_stats()
        logger.info(f"Terms by category:")
        for category, count in sorted(category_stats.items()):
            logger.info(f"  📋 {category}: {count} terms")
        
        if self.total_terms_extracted > 0:
            logger.info(f"✅ Successfully extracted medical knowledge database")
            logger.info(f"💾 Saved to: {self.output_file}")
        
        if self.errors:
            logger.info("❌ Errors:")
            for error in self.errors[:5]:  # Show first 5 errors
                logger.error(f"  - {error}")
            if len(self.errors) > 5:
                logger.error(f"  ... and {len(self.errors) - 5} more errors")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract medical knowledge and terminology from markdown files')
    parser.add_argument('base_path', help='Base path to Medical Studies Content folder')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without making API calls')
    parser.add_argument('--max-chunk-size', type=int, default=3500, 
                       help=f'Maximum characters per API request (default: 3500)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.base_path):
        logger.error(f"Base path does not exist: {args.base_path}")
        return 1
    
    try:
        # Create extractor with custom chunk size if specified
        extractor = MedicalKnowledgeExtractor(args.base_path)
        if args.max_chunk_size != 3500:
            extractor.max_chunk_size = args.max_chunk_size
            logger.info(f"Using custom chunk size: {args.max_chunk_size}")
        
        if args.dry_run:
            extractor.extract_all_medical_knowledge(dry_run=True)
        else:
            logger.warning("This will make extensive API calls to Mistral Large. Costs may be significant.")
            confirm = input("Continue? (y/N): ")
            if confirm.lower() != 'y':
                logger.info("Operation cancelled")
                return 0
            
            extractor.extract_all_medical_knowledge(dry_run=False)
        
        extractor.print_summary()
        return 0
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Medical knowledge extraction failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())