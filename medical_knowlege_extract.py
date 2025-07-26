#!/usr/bin/env python3
"""
Medical Knowledge Extractor & Augmenter
Uses Mistral Large to extract medical terms and build comprehensive knowledge database
"""

import os
import re
import json
import time
import logging
import requests
import threading
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
import hashlib
from datetime import datetime

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

# Mistral API configuration
MISTRAL_API_KEYS = [
    "R5LVIIcfyTw42PUN8XW8Bf4TtOd8bG85",
    "pFL2BXaV6Q1Je1Nkg5oAFsY0uCDS2h5v",
    "4P1XX0iaDIHCc4KbjWpeC5IRtrCkUj0s",
    "hPZyLQqACzOic8U1jep6uOMJnintFFxc",
    "16RtgR4NenVkjLfGFeaxVjl5oLLgSf14"
]

MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"
LARGE_CHUNK_SIZE = 8000  # Larger chunks for better context
RATE_LIMIT_DELAY = 2  # Seconds between API calls per worker
MAX_RETRIES = 3

# Subject ID mapping (extended from your original script)
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

@dataclass
class MedicalTerm:
    term: str
    definition: str
    category: str  # 'anatomie', 'pathologie', 'pharmacologie', 'physiologie', 'imagerie', 'classification', 'microbiologie', 'acronyme'
    synonyms: Optional[List[str]] = None
    related_terms: Optional[List[str]] = None
    description: Optional[str] = None
    expansion: Optional[str] = None  # For acronyms
    clinical_significance: Optional[str] = None
    normal_values: Optional[str] = None  # For lab values, measurements
    image_search_terms: Optional[List[str]] = None
    subject_id: Optional[str] = None
    source_file: Optional[str] = None
    confidence_score: Optional[float] = None

@dataclass
class ExtractionResult:
    chunk_id: str
    subject_id: str
    source_file: str
    terms: List[MedicalTerm]
    processing_time: float
    chunk_text_preview: str

class MedicalKnowledgeExtractor:
    def __init__(self, base_path: str, output_dir: str = "medical_knowledge_db"):
        self.base_path = Path(base_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.api_keys = MISTRAL_API_KEYS.copy()
        self.processed_chunks = 0
        self.total_chunks = 0
        self.total_terms_extracted = 0
        self.errors = []
        
        # Thread-safe counters
        self.lock = threading.Lock()
        
        # Track processed content to avoid duplicates
        self.processed_content_hashes = set()
        
        # Store all extracted terms
        self.all_extracted_terms: List[MedicalTerm] = []
        
        # Load existing terms to avoid duplicates
        self.existing_terms = self.load_existing_terms()
    
    def load_existing_terms(self) -> Set[str]:
        """Load existing terms from previous extractions"""
        existing_file = self.output_dir / "existing_terms.json"
        if existing_file.exists():
            try:
                with open(existing_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('terms', []))
            except Exception as e:
                logger.warning(f"Could not load existing terms: {e}")
        return set()
    
    def save_existing_terms(self):
        """Save list of existing terms"""
        existing_file = self.output_dir / "existing_terms.json"
        all_terms = set(term.term.lower() for term in self.all_extracted_terms)
        all_terms.update(self.existing_terms)
        
        with open(existing_file, 'w', encoding='utf-8') as f:
            json.dump({
                'terms': list(all_terms),
                'last_updated': datetime.now().isoformat(),
                'total_count': len(all_terms)
            }, f, ensure_ascii=False, indent=2)
    
    def create_medical_extraction_prompt(self, content: str, subject_context: str) -> str:
        """Create a comprehensive prompt for medical term extraction"""
        return f"""Tu es un expert médical français spécialisé dans l'extraction et la définition de terminologie médicale. Ton rôle est d'analyser le contenu médical fourni et d'extraire UNIQUEMENT les termes médicaux spécialisés qui nécessitent une définition ou une explication approfondie.

CONTEXTE MÉDICAL: {subject_context}

INSTRUCTIONS CRITIQUES:
1. Extrais SEULEMENT les termes médicaux spécialisés, techniques ou scientifiques
2. Ignore les mots courants, les connecteurs, et les termes non-médicaux
3. Concentre-toi sur les termes qui bénéficieraient d'une définition pour un étudiant en médecine
4. Fournis des définitions précises, factuelles et cliniquement pertinentes
5. INTERDICTION ABSOLUE d'inventer ou d'halluciner des informations
6. Si tu n'es pas certain d'une définition, n'inclus pas le terme

EXEMPLES DE TERMES À EXTRAIRE:
- Anatomie: "nasopharynx", "apophyse basilaire", "sinus caverneux"
- Pathologie: "carcinome épidermoïde", "adénocarcinome", "métastase"
- Pharmacologie: "cisplatine", "5-fluorouracile", "bêta-bloquants"
- Physiologie: "hémodynamique", "clearance rénale", "compliance"
- Imagerie: "IRM", "tomodensitométrie", "scintigraphie"
- Classifications: "TNM", "NYHA", "Glasgow"
- Microbiologie: "EBV", "Helicobacter pylori", "streptocoque"
- Acronymes médicaux: "VADS", "UCNT", "AVC"

EXEMPLES DE TERMES À IGNORER:
- Mots courants: "patient", "traitement", "diagnostic", "médecin"
- Connecteurs: "cependant", "néanmoins", "par conséquent"
- Termes généraux: "maladie", "symptôme", "examen"

FORMAT DE RÉPONSE REQUIS (JSON STRICT):
```json
{{
  "extracted_terms": [
    {{
      "term": "terme exact du texte",
      "definition": "définition précise et factuelle",
      "category": "anatomie|pathologie|pharmacologie|physiologie|imagerie|classification|microbiologie|acronyme",
      "synonyms": ["synonyme1", "synonyme2"],
      "related_terms": ["terme_relié1", "terme_relié2"],
      "description": "explication clinique approfondie si pertinente",
      "expansion": "forme développée pour les acronymes",
      "clinical_significance": "importance clinique si significative",
      "normal_values": "valeurs normales si applicable",
      "image_search_terms": ["terme_recherche_image1", "terme_recherche_image2"],
      "confidence_score": 0.95
    }}
  ],
  "extraction_metadata": {{
    "total_terms_found": 5,
    "high_confidence_terms": 4,
    "subject_context": "{subject_context}"
  }}
}}
```

CRITÈRES DE QUALITÉ:
- Confidence score ≥ 0.8 pour tous les termes
- Définitions de 10-100 mots, précises et cliniques
- Catégorisation correcte selon les domaines médicaux
- Termes de recherche d'images pertinents pour visualisation
- Significance clinique seulement si importante

CONTENU À ANALYSER:
{content}

RÉPONDS UNIQUEMENT EN JSON VALIDE. Aucun texte avant ou après le JSON."""

    def extract_terms_from_chunk(self, content: str, subject_name: str, api_key: str, worker_id: int) -> Optional[List[MedicalTerm]]:
        """Extract medical terms from a content chunk using Mistral Large"""
        
        prompt = self.create_medical_extraction_prompt(content, subject_name)
        
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
            "temperature": 0.1,  # Low temperature for factual extraction
            "max_tokens": 12000,
            "response_format": {"type": "json_object"}  # Ensure JSON response
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Worker {worker_id}: Extracting terms (attempt {attempt + 1})")
                response = requests.post(MISTRAL_API_URL, headers=headers, json=data, timeout=120)
                
                if response.status_code == 200:
                    result = response.json()
                    if 'choices' in result and len(result['choices']) > 0:
                        json_content = result['choices'][0]['message']['content'].strip()
                        
                        try:
                            # Parse the JSON response
                            extracted_data = json.loads(json_content)
                            terms_data = extracted_data.get('extracted_terms', [])
                            
                            # Convert to MedicalTerm objects
                            medical_terms = []
                            subject_id = SUBJECT_MAPPING.get(subject_name)
                            
                            for term_data in terms_data:
                                # Validate required fields
                                if not all(k in term_data for k in ['term', 'definition', 'category']):
                                    logger.warning(f"Worker {worker_id}: Skipping incomplete term: {term_data}")
                                    continue
                                
                                # Check if term already exists
                                if term_data['term'].lower() in self.existing_terms:
                                    logger.debug(f"Worker {worker_id}: Skipping existing term: {term_data['term']}")
                                    continue
                                
                                medical_term = MedicalTerm(
                                    term=term_data['term'],
                                    definition=term_data['definition'],
                                    category=term_data['category'],
                                    synonyms=term_data.get('synonyms'),
                                    related_terms=term_data.get('related_terms'),
                                    description=term_data.get('description'),
                                    expansion=term_data.get('expansion'),
                                    clinical_significance=term_data.get('clinical_significance'),
                                    normal_values=term_data.get('normal_values'),
                                    image_search_terms=term_data.get('image_search_terms'),
                                    subject_id=subject_id,
                                    confidence_score=term_data.get('confidence_score', 0.8)
                                )
                                
                                # Only include high-confidence terms
                                if medical_term.confidence_score >= 0.8:
                                    medical_terms.append(medical_term)
                                    self.existing_terms.add(medical_term.term.lower())
                            
                            logger.info(f"Worker {worker_id}: Extracted {len(medical_terms)} high-quality terms")
                            return medical_terms
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Worker {worker_id}: JSON parsing error: {e}")
                            logger.error(f"Worker {worker_id}: Raw response: {json_content[:500]}...")
                            return None
                    else:
                        logger.error(f"Worker {worker_id}: No choices in API response")
                        return None
                        
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 10
                    logger.warning(f"Worker {worker_id}: Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Worker {worker_id}: API error {response.status_code}: {response.text}")
                    if attempt == MAX_RETRIES - 1:
                        return None
                    time.sleep(5)
                
            except requests.exceptions.Timeout:
                logger.error(f"Worker {worker_id}: API timeout (attempt {attempt + 1})")
                if attempt == MAX_RETRIES - 1:
                    return None
                time.sleep(10)
            except Exception as e:
                logger.error(f"Worker {worker_id}: API call failed: {e}")
                if attempt == MAX_RETRIES - 1:
                    return None
                time.sleep(5)
        
        return None
    
    def split_content_into_chunks(self, content: str) -> List[str]:
        """Split content into larger, context-aware chunks"""
        if len(content) <= LARGE_CHUNK_SIZE:
            return [content]
        
        chunks = []
        
        # First, try to split by sections (## or ###)
        sections = re.split(r'\n(?=#{2,3}\s)', content)
        
        current_chunk = ""
        for section in sections:
            if len(current_chunk) + len(section) + 2 <= LARGE_CHUNK_SIZE:
                if current_chunk:
                    current_chunk += "\n\n" + section
                else:
                    current_chunk = section
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                
                # If section is still too large, split by paragraphs
                if len(section) > LARGE_CHUNK_SIZE:
                    paragraphs = section.split('\n\n')
                    current_chunk = ""
                    
                    for para in paragraphs:
                        if len(current_chunk) + len(para) + 2 <= LARGE_CHUNK_SIZE:
                            if current_chunk:
                                current_chunk += "\n\n" + para
                            else:
                                current_chunk = para
                        else:
                            if current_chunk:
                                chunks.append(current_chunk.strip())
                            current_chunk = para
                else:
                    current_chunk = section
        
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def process_markdown_file(self, file_path: Path, subject_name: str, subject_id: str, api_key: str, worker_id: int) -> List[ExtractionResult]:
        """Process a markdown file and extract medical terms"""
        try:
            # Read file content
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Skip if content is too small or mostly non-medical
            if len(content.strip()) < 200:
                logger.info(f"Worker {worker_id}: Skipping small file {file_path.name}")
                return []
            
            # Check for duplicate content
            content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()[:16]
            with self.lock:
                if content_hash in self.processed_content_hashes:
                    logger.info(f"Worker {worker_id}: Skipping duplicate content in {file_path.name}")
                    return []
                self.processed_content_hashes.add(content_hash)
            
            logger.info(f"Worker {worker_id}: Processing {file_path.name} ({len(content)} chars)")
            
            # Split content into chunks
            chunks = self.split_content_into_chunks(content)
            logger.info(f"Worker {worker_id}: Split into {len(chunks)} chunks")
            
            extraction_results = []
            
            # Process each chunk
            for i, chunk in enumerate(chunks):
                chunk_id = f"{subject_id}_{file_path.stem}_{i+1}"
                start_time = time.time()
                
                logger.info(f"Worker {worker_id}: Processing chunk {i+1}/{len(chunks)}")
                
                extracted_terms = self.extract_terms_from_chunk(chunk, subject_name, api_key, worker_id)
                processing_time = time.time() - start_time
                
                if extracted_terms:
                    # Add source file information
                    for term in extracted_terms:
                        term.source_file = str(file_path.relative_to(self.base_path))
                    
                    result = ExtractionResult(
                        chunk_id=chunk_id,
                        subject_id=subject_id,
                        source_file=str(file_path.relative_to(self.base_path)),
                        terms=extracted_terms,
                        processing_time=processing_time,
                        chunk_text_preview=chunk[:200] + "..." if len(chunk) > 200 else chunk
                    )
                    
                    extraction_results.append(result)
                    
                    with self.lock:
                        self.total_terms_extracted += len(extracted_terms)
                        self.all_extracted_terms.extend(extracted_terms)
                    
                    logger.info(f"Worker {worker_id}: Extracted {len(extracted_terms)} terms from chunk {i+1}")
                else:
                    logger.warning(f"Worker {worker_id}: No terms extracted from chunk {i+1}")
                
                # Rate limiting between chunks
                time.sleep(RATE_LIMIT_DELAY)
            
            return extraction_results
            
        except Exception as e:
            error_msg = f"Worker {worker_id}: Failed to process {file_path}: {e}"
            logger.error(error_msg)
            with self.lock:
                self.errors.append(error_msg)
            return []
    
    def save_extraction_results(self, results: List[ExtractionResult]):
        """Save extraction results to organized JSON files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save by subject
        subjects_data = {}
        all_terms_by_category = {}
        
        for result in results:
            subject_id = result.subject_id
            if subject_id not in subjects_data:
                subjects_data[subject_id] = {
                    'subject_id': subject_id,
                    'terms': [],
                    'source_files': set(),
                    'total_terms': 0
                }
            
            for term in result.terms:
                term_dict = asdict(term)
                subjects_data[subject_id]['terms'].append(term_dict)
                subjects_data[subject_id]['source_files'].add(result.source_file)
                
                # Categorize terms
                category = term.category
                if category not in all_terms_by_category:
                    all_terms_by_category[category] = []
                all_terms_by_category[category].append(term_dict)
        
        # Convert sets to lists for JSON serialization
        for subject_data in subjects_data.values():
            subject_data['source_files'] = list(subject_data['source_files'])
            subject_data['total_terms'] = len(subject_data['terms'])
        
        # Save subject-specific files
        subjects_dir = self.output_dir / "by_subject"
        subjects_dir.mkdir(exist_ok=True)
        
        for subject_id, data in subjects_data.items():
            subject_file = subjects_dir / f"{subject_id}_{timestamp}.json"
            with open(subject_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {data['total_terms']} terms for subject {subject_id}")
        
        # Save category-specific files
        categories_dir = self.output_dir / "by_category"
        categories_dir.mkdir(exist_ok=True)
        
        for category, terms in all_terms_by_category.items():
            category_file = categories_dir / f"{category}_{timestamp}.json"
            with open(category_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'category': category,
                    'terms': terms,
                    'total_count': len(terms),
                    'extraction_date': timestamp
                }, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(terms)} terms for category {category}")
        
        # Save complete database
        complete_db = {
            'metadata': {
                'extraction_date': timestamp,
                'total_subjects': len(subjects_data),
                'total_terms': sum(len(data['terms']) for data in subjects_data.values()),
                'categories': list(all_terms_by_category.keys()),
                'extraction_stats': {
                    'processed_chunks': self.processed_chunks,
                    'total_files': len(set(r.source_file for r in results)),
                    'errors_count': len(self.errors)
                }
            },
            'subjects': subjects_data,
            'by_category': all_terms_by_category
        }
        
        complete_file = self.output_dir / f"complete_medical_db_{timestamp}.json"
        with open(complete_file, 'w', encoding='utf-8') as f:
            json.dump(complete_db, f, ensure_ascii=False, indent=2)
        
        # Save summary statistics
        self.save_extraction_summary(complete_db, timestamp)
        
        logger.info(f"Complete database saved to {complete_file}")
        return complete_file
    
    def save_extraction_summary(self, complete_db: dict, timestamp: str):
        """Save extraction summary and statistics"""
        metadata = complete_db['metadata']
        
        # Calculate detailed statistics
        category_stats = {}
        for category, terms in complete_db['by_category'].items():
            category_stats[category] = {
                'count': len(terms),
                'avg_confidence': sum(t.get('confidence_score', 0.8) for t in terms) / len(terms),
                'with_synonyms': sum(1 for t in terms if t.get('synonyms')),
                'with_related_terms': sum(1 for t in terms if t.get('related_terms')),
                'with_clinical_significance': sum(1 for t in terms if t.get('clinical_significance'))
            }
        
        subject_stats = {}
        for subject_id, data in complete_db['subjects'].items():
            subject_name = next((name for name, id in SUBJECT_MAPPING.items() if id == subject_id), subject_id)
            subject_stats[subject_name] = {
                'subject_id': subject_id,
                'term_count': data['total_terms'],
                'source_files_count': len(data['source_files']),
                'categories_covered': list(set(t['category'] for t in data['terms']))
            }
        
        summary = {
            'extraction_summary': {
                'timestamp': timestamp,
                'total_terms_extracted': metadata['total_terms'],
                'unique_categories': len(metadata['categories']),
                'subjects_processed': metadata['total_subjects'],
                'processing_errors': metadata['extraction_stats']['errors_count']
            },
            'category_breakdown': category_stats,
            'subject_breakdown': subject_stats,
            'quality_metrics': {
                'high_confidence_terms': sum(1 for subject_data in complete_db['subjects'].values() 
                                           for term in subject_data['terms'] 
                                           if term.get('confidence_score', 0.8) >= 0.9),
                'terms_with_images': sum(1 for subject_data in complete_db['subjects'].values() 
                                       for term in subject_data['terms'] 
                                       if term.get('image_search_terms')),
                'terms_with_clinical_notes': sum(1 for subject_data in complete_db['subjects'].values() 
                                               for term in subject_data['terms'] 
                                               if term.get('clinical_significance'))
            },
            'top_categories': sorted(category_stats.items(), key=lambda x: x[1]['count'], reverse=True)[:10],
            'most_productive_subjects': sorted(subject_stats.items(), key=lambda x: x[1]['term_count'], reverse=True)[:10]
        }
        
        summary_file = self.output_dir / f"extraction_summary_{timestamp}.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        # Also save as readable markdown report
        self.create_markdown_report(summary, timestamp)
    
    def create_markdown_report(self, summary: dict, timestamp: str):
        """Create a human-readable markdown report"""
        report_content = f"""# Medical Knowledge Extraction Report
        
## Extraction Summary
- **Date**: {timestamp}
- **Total Terms Extracted**: {summary['extraction_summary']['total_terms_extracted']}
- **Categories Covered**: {summary['extraction_summary']['unique_categories']}
- **Subjects Processed**: {summary['extraction_summary']['subjects_processed']}
- **Processing Errors**: {summary['extraction_summary']['processing_errors']}

## Quality Metrics
- **High Confidence Terms (≥0.9)**: {summary['quality_metrics']['high_confidence_terms']}
- **Terms with Image References**: {summary['quality_metrics']['terms_with_images']}
- **Terms with Clinical Notes**: {summary['quality_metrics']['terms_with_clinical_notes']}

## Top Categories by Term Count
"""
        for category, stats in summary['top_categories'][:10]:
            report_content += f"- **{category}**: {stats['count']} terms (avg confidence: {stats['avg_confidence']:.2f})\n"
        
        report_content += "\n## Most Productive Subjects\n"
        for subject_name, stats in summary['most_productive_subjects'][:10]:
            report_content += f"- **{subject_name}**: {stats['term_count']} terms from {stats['source_files_count']} files\n"
        
        report_content += "\n## Category Breakdown\n"
        for category, stats in summary['category_breakdown'].items():
            report_content += f"""
### {category.title()}
- **Total Terms**: {stats['count']}
- **Average Confidence**: {stats['avg_confidence']:.2f}
- **With Synonyms**: {stats['with_synonyms']}
- **With Related Terms**: {stats['with_related_terms']}
- **With Clinical Significance**: {stats['with_clinical_significance']}
"""
        
        report_file = self.output_dir / f"extraction_report_{timestamp}.md"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Markdown report saved to {report_file}")
    
    def collect_all_markdown_files(self) -> List[Tuple[Path, str, str]]:
        """Collect all markdown files with subject information"""
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
                
                subject_id = SUBJECT_MAPPING[subject_name]
                
                # Scan all subfolders for markdown files
                for subfolder in subject_path.iterdir():
                    if not subfolder.is_dir():
                        continue
                    
                    # Find all markdown files
                    for md_file in subfolder.glob("*.md"):
                        md_files.append((md_file, subject_name, subject_id))
        
        self.total_chunks = len(md_files)  # Rough estimate
        logger.info(f"Found {len(md_files)} markdown files to process")
        return md_files
    
    def worker_function(self, file_queue: list, api_key: str, worker_id: int) -> List[ExtractionResult]:
        """Worker function to process files from the queue"""
        logger.info(f"Worker {worker_id} started with API key: {api_key[:10]}...")
        
        all_results = []
        
        for file_path, subject_name, subject_id in file_queue:
            try:
                results = self.process_markdown_file(file_path, subject_name, subject_id, api_key, worker_id)
                all_results.extend(results)
                
                with self.lock:
                    self.processed_chunks += len(results)
                    progress = (self.processed_chunks / max(self.total_chunks, 1)) * 100
                    logger.info(f"Progress: {self.processed_chunks} chunks processed ({progress:.1f}%)")
                
            except Exception as e:
                error_msg = f"Worker {worker_id}: Unexpected error processing {file_path}: {e}"
                logger.error(error_msg)
                with self.lock:
                    self.errors.append(error_msg)
        
        logger.info(f"Worker {worker_id} finished with {len(all_results)} extraction results")
        return all_results
    
    def extract_all_knowledge(self, dry_run: bool = False, max_files: Optional[int] = None):
        """Main function to extract medical knowledge from all files"""
        if dry_run:
            logger.info("DRY RUN MODE - No actual extraction will be performed")
            md_files = self.collect_all_markdown_files()
            if max_files:
                md_files = md_files[:max_files]
            logger.info(f"Would process {len(md_files)} markdown files")
            return
        
        logger.info("Starting medical knowledge extraction process...")
        logger.info(f"Using {len(self.api_keys)} API keys with {len(self.api_keys)} workers")
        
        # Collect all markdown files
        md_files = self.collect_all_markdown_files()
        if not md_files:
            logger.warning("No markdown files found to process")
            return
        
        if max_files:
            md_files = md_files[:max_files]
            logger.info(f"Limited to {max_files} files for testing")
        
        # Distribute files among workers
        files_per_worker = len(md_files) // len(self.api_keys)
        remainder = len(md_files) % len(self.api_keys)
        
        file_queues = []
        start_idx = 0
        
        for i in range(len(self.api_keys)):
            end_idx = start_idx + files_per_worker + (1 if i < remainder else 0)
            file_queues.append(md_files[start_idx:end_idx])
            start_idx = end_idx
        
        # Start workers
        all_results = []
        with ThreadPoolExecutor(max_workers=len(self.api_keys)) as executor:
            future_to_worker = {
                executor.submit(self.worker_function, queue, self.api_keys[i], i + 1): i + 1
                for i, queue in enumerate(file_queues)
            }
            
            for future in as_completed(future_to_worker):
                worker_id = future_to_worker[future]
                try:
                    worker_results = future.result()
                    all_results.extend(worker_results)
                    logger.info(f"Worker {worker_id} completed successfully")
                except Exception as e:
                    logger.error(f"Worker {worker_id} failed: {e}")
        
        logger.info("All workers completed")
        
        # Save results
        if all_results:
            output_file = self.save_extraction_results(all_results)
            self.save_existing_terms()
            logger.info(f"Extraction complete! Results saved to {output_file}")
        else:
            logger.warning("No results to save")
    
    def print_summary(self):
        """Print extraction summary"""
        logger.info("=== MEDICAL KNOWLEDGE EXTRACTION SUMMARY ===")
        logger.info(f"Files processed: {self.processed_chunks}")
        logger.info(f"Total terms extracted: {self.total_terms_extracted}")
        logger.info(f"Unique terms in database: {len(self.existing_terms)}")
        logger.info(f"Errors encountered: {len(self.errors)}")
        
        if self.total_terms_extracted > 0:
            logger.info(f"✅ Successfully extracted {self.total_terms_extracted} medical terms")
            logger.info(f"📊 Database now contains {len(self.existing_terms)} unique terms")
        
        if self.errors:
            logger.info("❌ Errors:")
            for error in self.errors[:5]:  # Show first 5 errors
                logger.error(f"  - {error}")
            if len(self.errors) > 5:
                logger.error(f"  ... and {len(self.errors) - 5} more errors")

def main():
    """Main function"""
    import argparse
    global LARGE_CHUNK_SIZE
    
    parser = argparse.ArgumentParser(description='Extract medical knowledge and build terminology database')
    parser.add_argument('base_path', help='Base path to Medical Studies Content folder')
    parser.add_argument('--output-dir', default='medical_knowledge_db', 
                       help='Output directory for extracted knowledge')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without API calls')
    parser.add_argument('--max-files', type=int, help='Limit number of files to process (for testing)')
    parser.add_argument('--chunk-size', type=int, default=LARGE_CHUNK_SIZE, 
                       help=f'Maximum characters per chunk (default: {LARGE_CHUNK_SIZE})')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.base_path):
        logger.error(f"Base path does not exist: {args.base_path}")
        return 1
    
    # Update global chunk size if specified
    LARGE_CHUNK_SIZE = args.chunk_size
    
    try:
        extractor = MedicalKnowledgeExtractor(args.base_path, args.output_dir)
        
        if args.dry_run:
            extractor.extract_all_knowledge(dry_run=True, max_files=args.max_files)
        else:
            logger.warning("This will make extensive API calls to extract medical knowledge.")
            logger.warning("Estimated cost: $5-20 depending on content volume.")
            confirm = input("Continue? (y/N): ")
            if confirm.lower() != 'y':
                logger.info("Operation cancelled")
                return 0
            
            extractor.extract_all_knowledge(dry_run=False, max_files=args.max_files)
        
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
