import pymupdf 
import io

def extract_txt_from_pdf_path(pdf_path):
    doc = pymupdf .open(pdf_path)
    text = ''
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        text += page.get_text()
    return text

def extract_txt_from_pdf_binary(pdf_data):
    pdf_stream = io.BytesIO(pdf_data)
    doc = pymupdf.open(stream=pdf_stream, filetype="pdf")
    text = ''
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        text += page.get_text()
    return text