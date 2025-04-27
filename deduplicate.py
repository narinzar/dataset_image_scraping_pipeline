#!/usr/bin/env python3
"""
Image duplication detection and organization module.
Finds exact duplicates using MD5 hashing and similar images using perceptual hashing.
"""

import os
import hashlib
import shutil
import logging
from PIL import Image
import imagehash

logger = logging.getLogger(__name__)

def organize_duplicates(source_directory, output_base_dir='./duplicates_audit'):
    """
    Find and organize duplicate and similar images.
    
    Args:
        source_directory (str): Directory to scan for duplicates
        output_base_dir (str): Base directory for output
        
    Returns:
        int: Number of unique files found
    """
    # Create output directories
    exact_dup_dir = os.path.join(output_base_dir, 'exact_duplicates')
    similar_files_dir = os.path.join(output_base_dir, 'similar_files')
    consolidated_dir = os.path.join(output_base_dir, 'consolidated_files')
    
    os.makedirs(exact_dup_dir, exist_ok=True)
    os.makedirs(similar_files_dir, exist_ok=True)
    os.makedirs(consolidated_dir, exist_ok=True)
    
    # First pass: find exact duplicates using MD5 hash
    logger.info("Finding exact duplicates...")
    hashes = {}
    file_count = 0
    
    for root, _, files in os.walk(source_directory):
        for file in files:
            file_count += 1
            if file_count % 100 == 0:
                logger.info(f"Processed {file_count} files...")
                
            filepath = os.path.join(root, file)
            try:
                with open(filepath, 'rb') as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()
                if file_hash not in hashes:
                    hashes[file_hash] = []
                hashes[file_hash].append(filepath)
            except Exception as e:
                logger.warning(f"Skipping {filepath}: {str(e)}")
    
    # Process exact duplicates
    exact_dup_groups = [paths for paths in hashes.values() if len(paths) > 1]
    logger.info(f"Found {len(exact_dup_groups)} groups of exact duplicates")
    
    # Track all processed files for consolidated directory
    processed_hashes = set()
    file_counter = 1  # Counter for sequential file naming
    
    # Mapping dictionary to keep track of original file to new name
    file_mapping = {}
    
    # Process exact duplicate groups
    for i, group in enumerate(exact_dup_groups):
        group_dir = os.path.join(exact_dup_dir, f"group_{i+1}")
        os.makedirs(group_dir, exist_ok=True)
        
        # Get hash for this group (all files in group have same hash)
        with open(group[0], 'rb') as f:
            group_hash = hashlib.md5(f.read()).hexdigest()
        
        processed_hashes.add(group_hash)
        
        # Copy first file to consolidated directory with sequential name
        first_file = group[0]
        original_ext = os.path.splitext(first_file)[1]
        new_filename = f"{file_counter:05d}{original_ext}"
        file_counter += 1
        
        consolidated_dest = os.path.join(consolidated_dir, new_filename)
        shutil.copy2(first_file, consolidated_dest)
        
        # Store mapping info
        file_mapping[consolidated_dest] = [path for path in group]
        
        # Create record of duplicates in consolidated directory
        with open(os.path.join(consolidated_dir, "duplicates_index.txt"), "a") as f:
            f.write(f"File: {new_filename}\n")
            f.write(f"Hash: {group_hash}\n")
            f.write("Duplicates:\n")
            for path in group:
                f.write(f"  - {path}\n")
            f.write("\n")
        
        # Copy all files to the exact duplicates directory with original names
        for j, filepath in enumerate(group):
            filename = os.path.basename(filepath)
            dest_path = os.path.join(group_dir, filename)
            
            # Handle filename conflicts
            if os.path.exists(dest_path):
                name, ext = os.path.splitext(filename)
                dest_path = os.path.join(group_dir, f"{name}_dup{j}{ext}")
                
            shutil.copy2(filepath, dest_path)
            
            # Create a text file with original paths
            with open(os.path.join(group_dir, "original_paths.txt"), "a") as f:
                f.write(f"{os.path.basename(dest_path)} => {filepath}\n")
    
    # Copy unique files (not part of any duplicate group) to consolidated directory
    logger.info("Copying unique files to consolidated directory...")
    
    for file_hash, paths in hashes.items():
        if len(paths) == 1 and file_hash not in processed_hashes:
            filepath = paths[0]
            original_ext = os.path.splitext(filepath)[1]
            new_filename = f"{file_counter:05d}{original_ext}"
            file_counter += 1
            
            dest_path = os.path.join(consolidated_dir, new_filename)
            shutil.copy2(filepath, dest_path)
            
            # Store mapping info
            file_mapping[dest_path] = [filepath]
            
            # Add to processed hashes
            processed_hashes.add(file_hash)
    
    # Create a master index file with all mappings
    with open(os.path.join(consolidated_dir, "master_file_index.txt"), "w") as f:
        f.write("# Master File Index\n")
        f.write("# New Filename => Original File(s)\n\n")
        
        for new_file, original_files in file_mapping.items():
            f.write(f"{os.path.basename(new_file)}:\n")
            for orig in original_files:
                f.write(f"  - {orig}\n")
            f.write("\n")
    
    # Second pass: Find perceptually similar images
    if has_image_files(source_directory):
        logger.info("Finding similar images...")
        
        # Filter out files already in exact duplicates
        exact_dup_files = set()
        for group in exact_dup_groups:
            for filepath in group:
                exact_dup_files.add(filepath)
        
        # Collect image hashes
        image_hashes = []
        file_count = 0
        
        for root, _, files in os.walk(source_directory):
            for file in files:
                filepath = os.path.join(root, file)
                
                # Skip if already in exact duplicates
                if filepath in exact_dup_files:
                    continue
                    
                try:
                    with Image.open(filepath) as img:
                        file_count += 1
                        if file_count % 50 == 0:
                            logger.info(f"Processed {file_count} images...")
                        h = imagehash.phash(img)
                        image_hashes.append((h, filepath))
                except Exception as e:
                    # Not an image or can't be processed
                    logger.warning(f"Error processing {filepath}: {str(e)}")
                    pass
        
        # Compare hashes and group similar images
        logger.info("Comparing image hashes...")
        similar_groups = []
        checked = set()
        threshold = 5  # Adjust this value to control sensitivity
        
        for i, (h1, path1) in enumerate(image_hashes):
            if path1 in checked:
                continue
                
            current_group = [path1]
            
            for h2, path2 in image_hashes[i+1:]:
                if path2 in checked:
                    continue
                    
                if h1 - h2 <= threshold:  # Hamming distance <= threshold
                    current_group.append(path2)
                    checked.add(path2)
                    
            if len(current_group) > 1:
                similar_groups.append(current_group)
                checked.add(path1)
        
        # Process similar files
        logger.info(f"Found {len(similar_groups)} groups of similar images")
        
        for i, group in enumerate(similar_groups):
            group_dir = os.path.join(similar_files_dir, f"similar_group_{i+1}")
            os.makedirs(group_dir, exist_ok=True)
            
            for j, filepath in enumerate(group):
                filename = os.path.basename(filepath)
                dest_path = os.path.join(group_dir, filename)
                
                # Handle filename conflicts
                if os.path.exists(dest_path):
                    name, ext = os.path.splitext(filename)
                    dest_path = os.path.join(group_dir, f"{name}_similar{j}{ext}")
                    
                shutil.copy2(filepath, dest_path)
                
                # Create a text file with original paths and similarity scores
                with open(os.path.join(group_dir, "original_paths.txt"), "a") as f:
                    f.write(f"{os.path.basename(dest_path)} => {filepath}\n")
    
    logger.info("Audit complete!")
    logger.info(f"Exact duplicates saved to: {exact_dup_dir}")
    logger.info(f"Similar files saved to: {similar_files_dir}")
    logger.info(f"Consolidated files (with duplicates removed) saved to: {consolidated_dir}")
    logger.info(f"Total unique files: {file_counter - 1}")
    
    return file_counter - 1

def has_image_files(directory):
    """
    Check if directory has any files that might be images
    
    Args:
        directory (str): Directory to check
        
    Returns:
        bool: True if directory contains images, False otherwise
    """
    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'}
    
    for root, _, files in os.walk(directory):
        for file in files:
            ext = os.path.splitext(file)[1].lower()
            if ext in image_extensions:
                return True
    return False

if __name__ == "__main__":
    import argparse
    import logging
    
    # Configure logging for standalone usage
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('deduplicate.log'),
            logging.StreamHandler()
        ]
    )
    
    parser = argparse.ArgumentParser(description='Find and organize duplicate and similar files')
    parser.add_argument('source_dir', help='Directory to scan for duplicates')
    parser.add_argument('--output', default='./duplicates_audit', 
                        help='Base directory for output (default: ./duplicates_audit)')
    
    args = parser.parse_args()
    
    if not os.path.isdir(args.source_dir):
        logger.error(f"Error: {args.source_dir} is not a valid directory")
        exit(1)
        
    organize_duplicates(args.source_dir, args.output)