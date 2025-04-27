# Water Damage Image Pipeline

A comprehensive pipeline for:
1. Scraping water damage images from the web
2. Detecting and removing duplicates
3. Uploading the consolidated dataset to Hugging Face

## Features

- **Robust Image Scraping**: Uses multiple search engines and techniques to collect images efficiently
- **Comprehensive Search Terms**: Over 100 carefully crafted search terms for different types of water damage
- **Duplicate Detection**: Finds both exact duplicates (using MD5 hashing) and visually similar images (using perceptual hashing)
- **Organized Results**: Creates a structured dataset with duplicates properly documented
- **Hugging Face Integration**: Automatically uploads the dataset to Hugging Face in configurable batches

## Installation

### Prerequisites

- Python 3.7+
- Chrome browser (for Selenium-based scraping)

### Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/water-damage-pipeline.git
   cd water-damage-pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create your `.env` file with your Hugging Face token:
   ```bash
   cp .env.sample .env
   # Edit .env with your preferred text editor and add your HF_TOKEN
   ```

## Usage

### Quick Start

Run the entire pipeline with default settings:

```bash
python main.py
```

This will:
- Scrape images based on search terms in `search_terms.json`
- Detect and remove duplicates
- Upload the consolidated dataset to Hugging Face

### Advanced Usage

You can customize each step of the pipeline with command-line arguments:

```bash
python main.py \
  --search-terms-file custom_terms.json \
  --output-dir my_water_damage_images \
  --audit-dir my_deduplicated_images \
  --target-count 5000 \
  --batch-size 300 \
  --hf-dataset-name yourusername/custom-dataset-name
```

### Running Specific Phases

You can skip specific phases of the pipeline:

```bash
# Skip scraping (use existing images)
python main.py --skip-scraping

# Skip deduplication
python main.py --skip-deduplication

# Skip Hugging Face upload
python main.py --skip-upload

# Only run deduplication and upload (skip scraping)
python main.py --skip-scraping
```

### Running Individual Components

Each component can also be run independently:

#### Image Scraper

```bash
python scraper.py
```

#### Duplicate Detector

```bash
python deduplicate.py source_dir --output output_dir
```

#### Hugging Face Uploader

```bash
python hf_uploader.py source_dir --dataset-name username/dataset-name --batch-size 500
```

## Project Structure

```
water-damage-pipeline/
├── main.py              # Main pipeline script
├── scraper.py           # Image scraping module
├── deduplicate.py       # Duplicate detection module
├── hf_uploader.py       # Hugging Face upload module
├── search_terms.json    # Search terms for image scraping
├── .env.sample          # Sample environment variables
├── .env                 # Your environment variables (create this)
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

## Search Terms

The `search_terms.json` file contains over 100 search terms categorized by:

- General water damage terms
- Location-specific damage
- Cause-specific damage
- Material-specific damage
- Effect-specific damage
- Time-specific damage
- Severity variations
- Perspective variations
- And more...

You can customize this file to focus on specific types of water damage or add additional terms.

## Output Structure

After running the pipeline, you'll have:

```
water_damage_images/
└── [Original scraped images]

water_damage_images_audit/
├── exact_duplicates/
│   └── [Groups of exact duplicate images]
├── similar_files/
│   └── [Groups of visually similar images]
└── consolidated_files/
    ├── [Deduplicated image collection]
    ├── duplicates_index.txt
    └── master_file_index.txt
```

## Hugging Face Dataset

The uploaded dataset will include:

- All deduplicated images
- A README.md file with dataset description
- A metadata.json file with dataset information

You can find your dataset at: `https://huggingface.co/datasets/[your-username]/[dataset-name]`

## Requirements

See `requirements.txt` for the full list of dependencies. Key packages include:

- icrawler
- selenium
- webdriver-manager
- fake-useragent
- Pillow
- imagehash
- huggingface_hub
- python-dotenv

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.