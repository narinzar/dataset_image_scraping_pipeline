#!/usr/bin/env python3
"""
Water Damage Image Pipeline
- Scrapes water damage images based on search terms in a JSON file
- Detects and removes duplicates 
- Uploads the consolidated dataset to Hugging Face
"""

import os
import argparse
import logging
import json
from datetime import datetime

from scraper import RobustImageScraper
from deduplicate import organize_duplicates
from hf_uploader import upload_to_huggingface

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'water_damage_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the complete pipeline"""
    parser = argparse.ArgumentParser(description='Water Damage Image Pipeline')
    parser.add_argument('--search-terms-file', default='search_terms.json',
                        help='JSON file containing search terms (default: search_terms.json)')
    parser.add_argument('--output-dir', default='water_damage_images',
                        help='Directory to store scraped images (default: water_damage_images)')
    parser.add_argument('--audit-dir', default='water_damage_images_audit',
                        help='Directory to store duplicate audit (default: water_damage_images_audit)')
    parser.add_argument('--target-count', type=int, default=10000,
                        help='Target number of images to collect (default: 10000)')
    parser.add_argument('--batch-size', type=int, default=500,
                        help='Batch size for HF uploads (default: 500)')
    parser.add_argument('--hf-dataset-name', default='water_damage_dataset',
                        help='Name for the HuggingFace dataset (default: water_damage_dataset)')
    parser.add_argument('--skip-scraping', action='store_true',
                        help='Skip scraping and only run deduplication and upload')
    parser.add_argument('--skip-deduplication', action='store_true',
                        help='Skip deduplication and only run scraping and upload')
    parser.add_argument('--skip-upload', action='store_true',
                        help='Skip HuggingFace upload and only run scraping and deduplication')
    parser.add_argument('--max-per-term', type=int, default=100,
                        help='Maximum images to collect per search term (default: 100)')
    parser.add_argument('--session-duration', type=int, default=7200,
                        help='Maximum session duration in seconds (default: 7200 - 2 hours)')
    
    args = parser.parse_args()
    
    # Load search terms
    try:
        with open(args.search_terms_file, 'r', encoding='utf-8') as f:
            search_terms_data = json.load(f)
            search_terms = search_terms_data.get('search_terms', [])
        
        if not search_terms:
            logger.error(f"No search terms found in {args.search_terms_file}")
            return 1
            
        logger.info(f"Loaded {len(search_terms)} search terms from {args.search_terms_file}")
    except Exception as e:
        logger.error(f"Failed to load search terms from {args.search_terms_file}: {str(e)}")
        return 1
    
    # Create output directories
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.audit_dir, exist_ok=True)
    
    # Step 1: Scrape images
    if not args.skip_scraping:
        logger.info("Starting image scraping phase")
        scraper = RobustImageScraper(
            output_dir=args.output_dir,
            target_count=args.target_count,
            max_per_term=args.max_per_term,
            session_duration=args.session_duration
        )
        scraper.crawl_images(search_terms)
        logger.info(f"Scraping completed. {scraper.success_count} images downloaded to {args.output_dir}")
    else:
        logger.info("Skipping image scraping phase")
    
    # Step 2: Detect and organize duplicates
    consolidated_dir = os.path.join(args.audit_dir, 'consolidated_files')
    if not args.skip_deduplication:
        logger.info("Starting image deduplication phase")
        organize_duplicates(args.output_dir, args.audit_dir)
        logger.info(f"Deduplication completed. Consolidated images available in {consolidated_dir}")
    else:
        logger.info("Skipping image deduplication phase")
        # Ensure the consolidated directory exists even if we skip deduplication
        consolidated_dir = args.output_dir
    
    # Step 3: Upload to Hugging Face
    if not args.skip_upload:
        logger.info("Starting Hugging Face upload phase")
        upload_to_huggingface(
            consolidated_dir, 
            args.hf_dataset_name,
            batch_size=args.batch_size
        )
        logger.info(f"Upload completed. Dataset available at {args.hf_dataset_name}")
    else:
        logger.info("Skipping Hugging Face upload phase")
    
    logger.info("Pipeline completed successfully!")
    return 0

if __name__ == "__main__":
    exit(main())