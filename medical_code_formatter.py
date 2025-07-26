#!/usr/bin/env python3
"""
Simple Medical Code Tag Fixer
Specifically targets medical measurements, comparisons, and ranges
"""

import os
import re
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_medical_code_tags(content: str) -> tuple[str, int]:
    """Fix medical code tags in content"""
    
    replacements_made = 0
    original_content = content
    
    # Define patterns for medical content that should NOT be in code tags
    patterns = [
        # Medical measurements with comparison operators (≥ 0,1 mV, < 40 ans, etc.)
        (r'`([≥≤<>]\s*\d+(?:[,\.]\d+)?\s*(?:mV|mg|g|ml|l|mcg|μg|kg|cm|mm|Gy|ans?|années?))`', r'**\1**'),
        
        # Medical ranges like V2-V3
        (r'`([V]\d+-[V]\d+)`', r'**\1**'),
        
        # Age comparisons (< 40, ≥ 40, etc.)
        (r'`([<>≤≥]\s*\d+)`(?=\s*ans)', r'**\1**'),
        
        # Simple dosages and measurements (0,1 mV, 40 ans, etc.)
        (r'`(\d+(?:[,\.]\d+)?\s*(?:mV|mg|g|ml|l|mcg|μg|kg|cm|mm|Gy|ans?|années?))`', r'**\1**'),
        
        # Percentages
        (r'`(\d+(?:[,\.]\d+)?%)`', r'**\1**'),
        
        # Medical classifications (T1, N0, etc.)
        (r'`([TN]\d+)`', r'**\1**'),
        
        # Roman numerals
        (r'`([IVX]+)`', r'**\1**'),
        
        # Medical abbreviations (2-5 capital letters)
        (r'`([A-Z]{2,5})`(?=\s*(?:\)|,|\.|\s|$))', r'**\1**'),
        
        # Fractions
        (r'`(\d+/\d+)`', r'**\1**'),
        
        # Simple numbers followed by medical units
        (r'`(\d+)`(\s*(?:cas|patients?|ans?|mois|jours?|heures?|minutes?))', r'**\1**\2'),
    ]
    
    # Apply each pattern
    for pattern, replacement in patterns:
        before_count = len(re.findall(pattern, content))
        content = re.sub(pattern, replacement, content)
        after_count = len(re.findall(pattern, content))
        replacements_made += before_count - after_count
    
    return content, replacements_made

def process_file(file_path: Path, dry_run: bool = False) -> dict:
    """Process a single markdown file"""
    try:
        # Read file
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        # Fix code tags
        updated_content, replacements_made = fix_medical_code_tags(original_content)
        
        if dry_run:
            return {
                'file': file_path.name,
                'replacements': replacements_made,
                'preview': updated_content != original_content
            }
        
        if replacements_made > 0:
            # Create backup
            backup_path = file_path.with_suffix('.md.backup')
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(original_content)
            
            # Write updated content
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(updated_content)
            
            logger.info(f"Fixed {replacements_made} code tags in {file_path.name}")
        
        return {
            'file': file_path.name,
            'replacements': replacements_made,
            'success': True
        }
        
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return {
            'file': file_path.name,
            'error': str(e),
            'success': False
        }

def find_markdown_files(base_path: Path) -> list[Path]:
    """Find all markdown files in the directory structure"""
    md_files = []
    
    for specialty_path in base_path.iterdir():
        if not specialty_path.is_dir():
            continue
        
        specialty_name = specialty_path.name
        if specialty_name in ["tree.md", "__pycache__", "tree_generator.py", "venv", ".git", "images"]:
            continue
        
        # Iterate through subject folders
        for subject_path in specialty_path.iterdir():
            if not subject_path.is_dir():
                continue
            
            # Scan all subfolders for markdown files
            for subfolder in subject_path.iterdir():
                if not subfolder.is_dir():
                    continue
                
                # Find all markdown files
                for md_file in subfolder.glob("*.md"):
                    md_files.append(md_file)
    
    return md_files

def test_on_sample():
    """Test the function on your sample text"""
    sample = '''Syndrome coronarien aigu

Il peut objectiver :

- Un **sus-décalage du segment ST** évoque un **SCA ST (+)** s'il est convexe en haut, localisé dans un **territoire systématisé**, avec **image en «miroir»** (Tableau, figure 1). Un **bloc de branche gauche** récent ou présumé récent peut également être révélateur d'une **ischémie myocardique** ;
- Un **sous décalage du segment ST** ou des **changements de l'onde T** évoquent un **SCA sans sus décalage persistant de ST**.

> **Note:** Les seuils pour le sus-décalage du segment ST sont les suivants: `≥ 0,1 mV` dans toutes les dérivations sauf `V2-V3` où les seuils seront : `≥ 0,2 mV` chez les hommes `≥ 40` ans; `≥ 0,25 mV` chez les hommes `< 40` ans ou `≥ 0,15 mV` chez les femmes.'''
    
    fixed_content, replacements = fix_medical_code_tags(sample)
    
    print("ORIGINAL:")
    print(sample)
    print("\n" + "="*50 + "\n")
    print("FIXED:")
    print(fixed_content)
    print(f"\nReplacements made: {replacements}")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fix medical code tags in markdown files')
    parser.add_argument('base_path', nargs='?', help='Base path to Medical Studies Content folder')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    parser.add_argument('--test', action='store_true', help='Test on sample text')
    
    args = parser.parse_args()
    
    if args.test:
        test_on_sample()
        return 0
    
    if not args.base_path:
        logger.error("Please provide base path or use --test")
        return 1
    
    base_path = Path(args.base_path)
    if not base_path.exists():
        logger.error(f"Base path does not exist: {base_path}")
        return 1
    
    # Find all markdown files
    md_files = find_markdown_files(base_path)
    logger.info(f"Found {len(md_files)} markdown files")
    
    if not md_files:
        logger.warning("No markdown files found")
        return 0
    
    # Process files
    total_replacements = 0
    successful_files = 0
    
    for md_file in md_files:
        result = process_file(md_file, dry_run=args.dry_run)
        
        if result.get('success', False):
            successful_files += 1
            total_replacements += result.get('replacements', 0)
        elif args.dry_run and result.get('preview', False):
            logger.info(f"Would fix {result.get('replacements', 0)} code tags in {result['file']}")
    
    # Summary
    action = "Would fix" if args.dry_run else "Fixed"
    logger.info(f"\n=== SUMMARY ===")
    logger.info(f"Files processed: {len(md_files)}")
    logger.info(f"Successful: {successful_files}")
    logger.info(f"{action} {total_replacements} code tags total")
    
    if not args.dry_run and total_replacements > 0:
        logger.info("✅ Original files backed up with .backup extension")

if __name__ == "__main__":
    exit(main())
