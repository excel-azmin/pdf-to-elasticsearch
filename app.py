import fitz  # PyMuPDF
from elasticsearch import Elasticsearch, helpers
import os

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file."""
    document = fitz.open(pdf_path)
    text = ""
    for page_num in range(len(document)):
        page = document.load_page(page_num)
        text += page.get_text()
    return text

def clean_and_structure_text(text):
    """Clean and structure text data. Customize this function based on your PDF structure."""
    # Example: Split text into paragraphs
    paragraphs = text.split('\n\n')
    structured_data = [{"paragraph": para} for para in paragraphs if para.strip()]
    return structured_data

def load_data_to_elasticsearch(data, index_name, es_host="localhost", es_port=9200):
    """Load data into Elasticsearch."""
    es = Elasticsearch([{'host': es_host, 'port': es_port}])

    actions = [
        {
            "_index": index_name,
            "_source": item
        }
        for item in data
    ]

    helpers.bulk(es, actions)
    print(f"Data successfully inserted into Elasticsearch index '{index_name}'.")

def process_pdf_files_in_directory(directory, index_name, es_host="localhost", es_port=9200):
    for filename in os.listdir(directory):
        if filename.endswith(".pdf"):
            pdf_path = os.path.join(directory, filename)
            print(f"Processing file: {pdf_path}")
            
            # Step 1: Extract text from PDF
            extracted_text = extract_text_from_pdf(pdf_path)
            
            # Step 2: Clean and structure the extracted text
            structured_data = clean_and_structure_text(extracted_text)
            
            # Step 3: Load structured data into Elasticsearch
            load_data_to_elasticsearch(structured_data, index_name, es_host, es_port)

if __name__ == "__main__":
    directory = "/data"  # Directory containing PDF files
    index_name = "your_index_name"
    es_host = "your_elasticsearch_host"  # Default is 'localhost'
    es_port = 9200  # Default is 9200
    process_pdf_files_in_directory(directory, index_name, es_host, es_port)
