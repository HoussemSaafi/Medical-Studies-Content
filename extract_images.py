#!/usr/bin/env python3
"""
Medical Images URL Replacement Script
Replaces base64 images in markdown files with GitHub URLs
"""

# python extract_images.py "/Users/mac/Documents/GitHub/Medical Studies Content"

import os
import re
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('image_url_replacement.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Subject ID mapping from previous script
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

class MedicalImageURLReplacer:
    def __init__(self, base_path: str, github_base_url: str = "https://raw.githubusercontent.com/brightly-dev/images/refs/heads/main"):
        self.base_path = Path(base_path)
        self.github_base_url = github_base_url
        self.processed_files = 0
        self.total_replacements = 0
        self.errors = []
    
    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe filesystem usage"""
        # Remove or replace problematic characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'\s+', '_', filename)  # Replace spaces with underscores
        filename = re.sub(r'[àáâãäå]', 'a', filename)  # Replace accented characters
        filename = re.sub(r'[èéêë]', 'e', filename)
        filename = re.sub(r'[ìíîï]', 'i', filename)
        filename = re.sub(r'[òóôõö]', 'o', filename)
        filename = re.sub(r'[ùúûü]', 'u', filename)
        filename = re.sub(r'[ç]', 'c', filename)
        filename = re.sub(r'[ñ]', 'n', filename)
        filename = re.sub(r'[œ]', 'oe', filename)
        filename = re.sub(r'[æ]', 'ae', filename)
        filename = filename.replace('&', 'and')
        filename = re.sub(r'[^\w\-_.]', '_', filename)  # Replace any remaining special chars
        # Remove multiple consecutive underscores
        filename = re.sub(r'_+', '_', filename)
        # Remove leading/trailing underscores
        filename = filename.strip('_')
        return filename
    
    def extract_base64_images_info(self, content: str) -> List[Tuple[str, str]]:
        """
        Extract base64 images info from markdown content
        Returns list of tuples: (alt_text, full_match)
        """
        images_info = []
        
        # Pattern to match markdown images with base64 data
        # ![alt](data:image/format;base64,data)
        pattern = r'!\[([^\]]*)\]\(data:image/[^;]+;base64,[^)]+\)'
        
        matches = re.finditer(pattern, content)
        
        for match in matches:
            alt_text = match.group(1).strip()
            
            # Clean up alt text if empty
            if not alt_text:
                alt_text = "medical_image"
            
            images_info.append((alt_text, match.group(0)))
            
        return images_info
    
    def replace_base64_with_github_urls(self, content: str, subject_name: str, subject_id: str) -> Tuple[str, int]:
        """
        Replace base64 images with GitHub URLs
        Returns tuple: (updated_content, number_of_replacements)
        """
        # Create folder name (same logic as image extraction script)
        safe_subject_name = self.sanitize_filename(subject_name)
        folder_name = f"{safe_subject_name}_{subject_id[:8]}"
        
        replacements_made = 0
        
        # Pattern to match markdown images with base64 data
        pattern = r'!\[([^\]]*)\]\(data:image/[^;]+;base64,[^)]+\)'
        
        def replace_image(match):
            nonlocal replacements_made
            alt_text = match.group(1).strip()
            
            # Use original alt text or fallback to medical_image
            if not alt_text:
                alt_text = "medical_image"
            
            # Sanitize alt text for filename (same as extraction script)
            safe_alt = self.sanitize_filename(alt_text)
            
            # Add .jpeg extension if not present
            if not safe_alt.lower().endswith('.jpg') and not safe_alt.lower().endswith('.jpeg'):
                filename = f"{safe_alt}.jpeg"
            else:
                # Ensure .jpeg extension (GitHub images are .jpeg)
                if safe_alt.lower().endswith('.jpg'):
                    filename = safe_alt.replace('.jpg', '.jpeg')
                else:
                    filename = safe_alt
            
            # Create GitHub URL
            github_url = f"{self.github_base_url}/{folder_name}/{filename}"
            
            replacements_made += 1
            
            # Return new markdown image syntax
            return f"![{alt_text}]({github_url})"
        
        # Replace all base64 images with GitHub URLs
        updated_content = re.sub(pattern, replace_image, content)
        
        return updated_content, replacements_made
    
    def process_markdown_file(self, file_path: Path, subject_name: str, subject_id: str, dry_run: bool = False) -> int:
        """Process a single markdown file and replace base64 images with GitHub URLs"""
        try:
            # Read file content
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check if file has base64 images
            images_info = self.extract_base64_images_info(content)
            
            if not images_info:
                return 0
            
            if dry_run:
                logger.info(f"Would replace {len(images_info)} images in {file_path}")
                return len(images_info)
            
            # Replace base64 images with GitHub URLs
            updated_content, replacements_made = self.replace_base64_with_github_urls(content, subject_name, subject_id)
            
            if replacements_made > 0:
                # Write updated content back to file
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(updated_content)
                
                logger.info(f"Replaced {replacements_made} images in {file_path.name}")
                self.total_replacements += replacements_made
            
            return replacements_made
            
        except UnicodeDecodeError:
            try:
                # Try with latin-1 encoding
                with open(file_path, 'r', encoding='latin-1') as f:
                    content = f.read()
                
                # Process with latin-1 content
                images_info = self.extract_base64_images_info(content)
                if not images_info:
                    return 0
                
                if dry_run:
                    logger.info(f"Would replace {len(images_info)} images in {file_path}")
                    return len(images_info)
                
                updated_content, replacements_made = self.replace_base64_with_github_urls(content, subject_name, subject_id)
                
                if replacements_made > 0:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(updated_content)
                    
                    logger.info(f"Replaced {replacements_made} images in {file_path.name}")
                    self.total_replacements += replacements_made
                
                return replacements_made
                
            except Exception as e:
                error_msg = f"Failed to read {file_path}: {e}"
                logger.error(error_msg)
                self.errors.append(error_msg)
                return 0
        except Exception as e:
            error_msg = f"Failed to process {file_path}: {e}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return 0
    
    def process_subject_folder(self, subject_path: Path, subject_name: str, dry_run: bool = False):
        """Process all markdown files in a subject folder"""
        subject_id = SUBJECT_MAPPING.get(subject_name)
        if not subject_id:
            logger.warning(f"No subject ID found for: {subject_name}")
            return
        
        logger.info(f"Processing subject: {subject_name}")
        
        # Process all markdown files in all subfolders
        total_replacements = 0
        
        for subfolder in subject_path.iterdir():
            if not subfolder.is_dir():
                continue
            
            folder_type = subfolder.name
            logger.info(f"  Processing {folder_type} folder")
            
            # Find all markdown files
            md_files = list(subfolder.glob("*.md"))
            
            for md_file in md_files:
                self.processed_files += 1
                replacements = self.process_markdown_file(md_file, subject_name, subject_id, dry_run)
                total_replacements += replacements
        
        if total_replacements > 0:
            action = "Would replace" if dry_run else "Replaced"
            logger.info(f"{action} {total_replacements} images in {subject_name}")
        else:
            logger.info(f"No images found in {subject_name}")
    
    def replace_all_images(self, dry_run=False):
        """Main replacement process"""
        action = "Scanning for" if dry_run else "Starting"
        logger.info(f"{action} image URL replacement process...")
        
        if dry_run:
            logger.info("DRY RUN MODE - No files will be modified")
        
        # Iterate through specialty folders
        for specialty_path in self.base_path.iterdir():
            if not specialty_path.is_dir():
                continue
            
            specialty_name = specialty_path.name
            if specialty_name in ["tree.md", "__pycache__", "tree_generator.py", "venv", ".git", "images"]:
                continue
            
            logger.info(f"Processing specialty: {specialty_name}")
            
            # Iterate through subject folders
            for subject_path in specialty_path.iterdir():
                if not subject_path.is_dir():
                    continue
                
                subject_name = subject_path.name
                if subject_name in SUBJECT_MAPPING:
                    self.process_subject_folder(subject_path, subject_name, dry_run)
                else:
                    logger.warning(f"Subject not in mapping: {subject_name}")
        
        action = "scan" if dry_run else "replacement"
        logger.info(f"Image URL {action} completed!")
    
    def print_summary(self):
        """Print replacement summary"""
        logger.info("=== IMAGE URL REPLACEMENT SUMMARY ===")
        logger.info(f"Files processed: {self.processed_files}")
        logger.info(f"Total image replacements: {self.total_replacements}")
        logger.info(f"GitHub base URL: {self.github_base_url}")
        logger.info(f"Errors encountered: {len(self.errors)}")
        
        if self.total_replacements > 0:
            logger.info(f"✅ Successfully replaced {self.total_replacements} base64 images with GitHub URLs")
        else:
            logger.info("ℹ️  No base64 images were found to replace")
        
        if self.errors:
            logger.info("❌ Errors:")
            for error in self.errors:
                logger.error(f"  - {error}")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Replace base64 images in markdown files with GitHub URLs')
    parser.add_argument('base_path', help='Base path to Medical Studies Content folder')
    parser.add_argument('--github-url', default='https://raw.githubusercontent.com/brightly-dev/images/refs/heads/main', 
                       help='GitHub base URL for images')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without modifying files')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.base_path):
        logger.error(f"Base path does not exist: {args.base_path}")
        return 1
    
    try:
        replacer = MedicalImageURLReplacer(args.base_path, args.github_url)
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No files will be modified")
            replacer.replace_all_images(dry_run=True)
        else:
            logger.warning("This will modify your markdown files. Make sure you have backups!")
            confirm = input("Continue? (y/N): ")
            if confirm.lower() != 'y':
                logger.info("Operation cancelled")
                return 0
            
            replacer.replace_all_images(dry_run=False)
        
        replacer.print_summary()
        return 0
        
    except Exception as e:
        logger.error(f"Image URL replacement failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
