#!/usr/bin/env python3
"""
HuggingFace dataset uploader module.
Uploads images to HuggingFace datasets in batches.
"""

import os
import logging
import json
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from huggingface_hub import HfApi, CommitOperationAdd, create_repo

# Load environment variables from .env file
load_dotenv()

# Initialize logger
logger = logging.getLogger(__name__)

def upload_to_huggingface(source_dir, dataset_name, batch_size=500, version_name=None):
    """
    Upload images to HuggingFace datasets in batches.
    
    Args:
        source_dir (str): Directory containing images to upload
        dataset_name (str): Name of the HuggingFace dataset
        batch_size (int, optional): Number of images per batch. Defaults to 500.
        version_name (str, optional): Version name for the commit. Defaults to timestamp.
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Check for HF token in environment
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        logger.error("HF_TOKEN environment variable not found. Please set it in .env file.")
        return False
    
    # Initialize HuggingFace API
    api = HfApi(token=hf_token)
    
    # Create dataset repo if it doesn't exist
    # Try to determine if the repo exists
    try:
        api.repo_info(repo_id=dataset_name, repo_type="dataset")
        logger.info(f"Dataset {dataset_name} already exists.")
    except Exception as e:
        # Check if the error indicates the repo doesn't exist OR if there's a conflict
        if "Repository Not Found" in str(e) and "409" not in str(e):
            logger.info(f"Creating new dataset: {dataset_name}")
            try:
                create_repo(dataset_name, repo_type="dataset", token=hf_token)
            except Exception as create_err:
                if "You already created this dataset repo" in str(create_err):
                    logger.info(f"Dataset {dataset_name} already exists (verified by error message).")
                else:
                    logger.error(f"Error creating dataset: {str(create_err)}")
                    return False
        else:
            # If it's a different error, log it and continue
            logger.warning(f"Error checking dataset existence: {str(e)}")
            logger.info("Attempting to proceed with upload anyway...")
    
    # Get list of image files
    image_files = []
    for root, _, files in os.walk(source_dir):
        for file in files:
            # Check if file is an image
            ext = os.path.splitext(file)[1].lower()
            if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.tiff']:
                image_files.append(os.path.join(root, file))
    
    if not image_files:
        logger.error(f"No image files found in {source_dir}")
        return False
    
    logger.info(f"Found {len(image_files)} image files to upload")
    
    # Create a dataset card if it doesn't exist
    dataset_card = f"""---
license: cc-by-4.0
tags:
- water-damage
- building-damage
- property-damage
- dataset
---

# Water Damage Images Dataset

This dataset contains images of water damage in residential buildings. The images show various types of water damage including:

- Ceiling water damage
- Wall water damage
- Floor water damage
- Mold caused by water damage
- Bathroom/kitchen water damage
- Structural water damage

## Dataset Details

- Total images: {len(image_files)}
- Last updated: {datetime.now().strftime('%Y-%m-%d')}
- Source: Various web sources
- Resolution: Mixed
- Format: Primarily JPG, with some PNG

## Intended Use

This dataset is intended for training machine learning models to detect and classify water damage in residential properties.

## Citation

If you use this dataset, please cite:

```
@dataset{{water_damage,
  author       = {{Water Damage Dataset Contributors}},
  title        = {{Water Damage Image Dataset}},
  year         = {datetime.now().year},
}}
```
"""

    # Add dataset card to operations
    operations = [
        CommitOperationAdd(
            path_in_repo="README.md",
            path_or_fileobj=dataset_card.encode(),
        )
    ]
    
    # Create metadata.json with basic information
    metadata = {
        "description": "Water damage images dataset for residential properties",
        "license": "cc-by-4.0",
        "total_images": len(image_files),
        "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "image_types": ["ceiling", "wall", "floor", "mold", "bathroom", "kitchen", "structural"],
    }
    
    operations.append(
        CommitOperationAdd(
            path_in_repo="metadata.json",
            path_or_fileobj=json.dumps(metadata, indent=2).encode(),
        )
    )
    
    # Process in batches
    total_batches = (len(image_files) + batch_size - 1) // batch_size
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(image_files))
        batch_files = image_files[start_idx:end_idx]
        
        batch_operations = operations.copy() if batch_idx == 0 else []
        
        for img_path in batch_files:
            # Get relative path for storing in HF
            filename = os.path.basename(img_path)
            dest_path = f"images/{filename}"
            
            # Add file to operations
            with open(img_path, "rb") as f:
                batch_operations.append(
                    CommitOperationAdd(
                        path_in_repo=dest_path,
                        path_or_fileobj=f.read(),
                    )
                )
        
        # Generate commit message
        if version_name is None:
            version_name = datetime.now().strftime("%Y%m%d_%H%M%S")
            
        commit_message = f"Upload batch {batch_idx+1}/{total_batches} - {version_name}"
        
        # Upload batch
        logger.info(f"Uploading batch {batch_idx+1}/{total_batches} ({len(batch_files)} files)")
        try:
            api.create_commit(
                repo_id=dataset_name,
                repo_type="dataset",
                operations=batch_operations,
                commit_message=commit_message,
            )
            logger.info(f"Successfully uploaded batch {batch_idx+1}")
        except Exception as e:
            logger.error(f"Error uploading batch {batch_idx+1}: {str(e)}")
            return False
        
        # Short delay between batches
        if batch_idx < total_batches - 1:
            time.sleep(5)
    
    logger.info(f"Upload completed. Dataset available at: https://huggingface.co/datasets/{dataset_name}")
    return True

if __name__ == "__main__":
    import argparse
    
    # Configure logging for standalone usage
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('hf_uploader.log'),
            logging.StreamHandler()
        ]
    )
    
    parser = argparse.ArgumentParser(description='Upload images to HuggingFace')
    parser.add_argument('source_dir', help='Directory containing images to upload')
    parser.add_argument('--dataset-name', required=True, help='Name of the HuggingFace dataset')
    parser.add_argument('--batch-size', type=int, default=500, help='Number of images per batch (default: 500)')
    parser.add_argument('--version', help='Version name for the commit (default: timestamp)')
    
    args = parser.parse_args()
    
    if not os.path.isdir(args.source_dir):
        logger.error(f"Error: {args.source_dir} is not a valid directory")
        exit(1)
        
    success = upload_to_huggingface(args.source_dir, args.dataset_name, args.batch_size, args.version)
    
    if success:
        logger.info("Upload completed successfully")
        exit(0)
    else:
        logger.error("Upload failed")
        exit(1)
