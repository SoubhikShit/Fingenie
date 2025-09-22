# import boto3
# import json
# import csv
# import io
# import zipfile
# import os
# import urllib.request
# import urllib.parse
# import email
# import base64
# import hashlibgit 
# import re
# from datetime import datetime
# from PyPDF2 import PdfReader
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText
# from email.mime.base import MIMEBase
# from email import encoders
# # Removed PIL import - not needed for OpenAI Vision API

# Initialize AWS clients
s3 = boto3.client('s3')
ses = boto3.client('ses')

def call_openai_api(prompt, api_key, function_definition, max_retries=3):
    """
    Direct API call to OpenAI using urllib (built-in Python library)
    Enhanced with retry logic for rate limits
    """
    url = 'https://api.openai.com/v1/chat/completions'
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": "You are an invoice analysis assistant."},
            {"role": "user", "content": prompt}
        ],
        "tools": [{"type": "function", "function": function_definition}],
        "tool_choice": {"type": "function", "function": {"name": function_definition["name"]}},
        "max_tokens": 4096,
        "temperature": 0
    }
    
    for attempt in range(max_retries):
        try:
            # Prepare the request
            data = json.dumps(payload).encode('utf-8')
            
            req = urllib.request.Request(url, data=data)
            req.add_header('Authorization', f'Bearer {api_key}')
            req.add_header('Content-Type', 'application/json')
            
            # Make the request
            with urllib.request.urlopen(req, timeout=60) as response:
                result = json.loads(response.read().decode('utf-8'))
                
                if result.get('choices') and result['choices'][0]['message'].get('tool_calls'):
                    function_args = json.loads(result['choices'][0]['message']['tool_calls'][0]['function']['arguments'])
                    return function_args
                else:
                    print("No tool calls in response")
                    return {}
                    
        except urllib.error.HTTPError as e:
            error_message = e.read().decode()
            print(f"OpenAI API HTTP error: {e.code} - {error_message}")
            
            # Check if it's a rate limit error
            if e.code == 429 and attempt < max_retries - 1:
                import time
                import random
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Rate limited, waiting {wait_time:.2f} seconds before retry {attempt + 1}")
                time.sleep(wait_time)
                continue
            
            return {}
        except Exception as e:
            print(f"Error calling OpenAI API (attempt {attempt + 1}): {str(e)}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)  # Brief pause before retry
                continue
            return {}
    
    return {}

def call_openai_vision_api(prompt, image_base64, api_key, max_retries=3):
    """
    Call OpenAI Vision API for image processing
    """
    url = 'https://api.openai.com/v1/chat/completions'
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{image_base64}"
                        }
                    }
                ]
            }
        ],
        "max_tokens": 4096,
        "temperature": 0
    }
    
    for attempt in range(max_retries):
        try:
            data = json.dumps(payload).encode('utf-8')
            
            req = urllib.request.Request(url, data=data)
            req.add_header('Authorization', f'Bearer {api_key}')
            req.add_header('Content-Type', 'application/json')
            
            with urllib.request.urlopen(req, timeout=60) as response:
                result = json.loads(response.read().decode('utf-8'))
                
                if result.get('choices') and result['choices'][0]['message'].get('content'):
                    return result['choices'][0]['message']['content']
                else:
                    print("No content in vision response")
                    return ""
                    
        except urllib.error.HTTPError as e:
            error_message = e.read().decode()
            print(f"OpenAI Vision API HTTP error: {e.code} - {error_message}")
            
            if e.code == 429 and attempt < max_retries - 1:
                import time
                import random
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Rate limited, waiting {wait_time:.2f} seconds before retry {attempt + 1}")
                time.sleep(wait_time)
                continue
            
            return ""
        except Exception as e:
            print(f"Error calling OpenAI Vision API (attempt {attempt + 1}): {str(e)}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
                continue
            return ""
    
    return ""

def clean_amount(amount_str):
    """
    Clean up amount strings to contain only numbers and decimal point
    """
    if not amount_str or amount_str in ['API_ERROR', 'ERROR', 'NO_TEXT', 'NOT_INVOICE', 'ZIP_ERROR']:
        return amount_str
    
    # Remove all non-digit and non-decimal characters except for common currency symbols at the start
    # Keep negative signs and commas for thousands
    cleaned = re.sub(r'[^\d.,\-]', '', str(amount_str))
    
    # Handle comma as thousands separator (e.g., 1,234.56)
    if ',' in cleaned and '.' in cleaned:
        # If both comma and dot, treat comma as thousands separator
        cleaned = cleaned.replace(',', '')
    elif ',' in cleaned and cleaned.count(',') == 1 and len(cleaned.split(',')[1]) <= 2:
        # If comma might be decimal separator (European format)
        cleaned = cleaned.replace(',', '.')
    elif ',' in cleaned:
        # Remove commas (thousands separators)
        cleaned = cleaned.replace(',', '')
    
    return cleaned

def extract_currency(amount_str):
    """
    Extract currency from amount string
    """
    if not amount_str or amount_str in ['API_ERROR', 'ERROR', 'NO_TEXT', 'NOT_INVOICE', 'ZIP_ERROR']:
        return 'UNKNOWN'
    
    # Common currency symbols and codes
    currency_patterns = {
        r'\$': 'USD',
        r'USD': 'USD',
        r'₹': 'INR',
        r'INR': 'INR',
        r'€': 'EUR',
        r'EUR': 'EUR',
        r'£': 'GBP',
        r'GBP': 'GBP',
        r'¥': 'JPY',
        r'JPY': 'JPY',
        r'₽': 'RUB',
        r'RUB': 'RUB'
    }
    
    amount_upper = str(amount_str).upper()
    
    for pattern, currency in currency_patterns.items():
        if re.search(pattern, amount_upper):
            return currency
    
    return 'UNKNOWN'

def get_processed_emails(bucket):
    """
    Get processed emails using Message-ID based tracking
    """
    processed_records = {}
    tracking_key = "processed_emails_tracking.json"
    
    try:
        response = s3.get_object(Bucket=bucket, Key=tracking_key)
        tracking_data = json.loads(response['Body'].read().decode('utf-8'))
        processed_records = tracking_data.get('processed_records', {})
        print(f"Found {len(processed_records)} previously processed email records")
        
    except s3.exceptions.NoSuchKey:
        print("No tracking file found - this is the first run")
    except Exception as e:
        print(f"Error reading tracking file: {str(e)}")
    
    return processed_records

def get_sent_emails(bucket):
    """
    Get emails that have already had their results sent back to users
    """
    sent_records = {}
    tracking_key = "sent_emails_tracking.json"
    
    try:
        response = s3.get_object(Bucket=bucket, Key=tracking_key)
        tracking_data = json.loads(response['Body'].read().decode('utf-8'))
        sent_records = tracking_data.get('sent_records', {})
        print(f"Found {len(sent_records)} emails that already had results sent")
        
    except s3.exceptions.NoSuchKey:
        print("No sent emails tracking file found - this is the first run")
    except Exception as e:
        print(f"Error reading sent emails tracking file: {str(e)}")
    
    return sent_records

def create_email_signature(email_result):
    """
    Create a unique signature for each email based on Message-ID and sender
    This ensures each unique email is processed exactly once
    """
    sender = email_result.get('sender_email', 'unknown')
    message_id = email_result.get('message_id', '')
    subject = email_result.get('subject', 'no_subject')
    date = email_result.get('date', '')
    
    # Use Message-ID as primary identifier (globally unique)
    if message_id and message_id.strip():
        signature = f"{sender}|{message_id}"
    else:
        # Fallback: create hash from sender + subject + date
        content_to_hash = f"{sender}|{subject}|{date}"
        content_hash = hashlib.md5(content_to_hash.encode()).hexdigest()[:10]
        signature = f"{sender}|HASH_{content_hash}"
    
    return signature

def mark_email_as_processed(bucket, email_key, email_result, processing_results):
    """
    Mark email as processed with detailed tracking
    """
    try:
        tracking_key = "processed_emails_tracking.json"
        
        # Get existing records
        processed_records = get_processed_emails(bucket)
        
        # Create unique signature for this email
        email_signature = create_email_signature(email_result)
        
        # Store detailed processing info
        processed_records[email_signature] = {
            "email_key": email_key,
            "sender_email": email_result.get('sender_email'),
            "subject": email_result.get('subject'),
            "message_id": email_result.get('message_id'),
            "processed_date": datetime.utcnow().isoformat(),
            "attachment_count": len(email_result.get('attachments', [])),
            "processing_results_count": len(processing_results),
            "status": "processed"
        }
        
        # Update tracking file
        tracking_data = {
            "processed_records": processed_records,
            "last_updated": datetime.utcnow().isoformat(),
            "total_processed": len(processed_records)
        }
        
        s3.put_object(
            Bucket=bucket,
            Key=tracking_key,
            Body=json.dumps(tracking_data, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"Marked email as processed: {email_signature}")
        return email_signature
        
    except Exception as e:
        print(f"Error marking email as processed {email_key}: {str(e)}")
        return None

def mark_email_as_sent(bucket, email_signature, sender_email, subject, results_count):
    """
    Mark email as having had its results sent back to the user
    """
    try:
        tracking_key = "sent_emails_tracking.json"
        
        # Get existing records
        sent_records = get_sent_emails(bucket)
        
        # Store detailed sending info
        sent_records[email_signature] = {
            "sender_email": sender_email,
            "subject": subject,
            "sent_date": datetime.utcnow().isoformat(),
            "results_count": results_count,
            "status": "sent"
        }
        
        # Update tracking file
        tracking_data = {
            "sent_records": sent_records,
            "last_updated": datetime.utcnow().isoformat(),
            "total_sent": len(sent_records)
        }
        
        s3.put_object(
            Bucket=bucket,
            Key=tracking_key,
            Body=json.dumps(tracking_data, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"Marked email results as sent: {email_signature}")
        return True
        
    except Exception as e:
        print(f"Error marking email as sent {email_signature}: {str(e)}")
        return False

def is_email_already_processed(email_result, processed_records):
    """
    Check if this specific email was already processed based on Message-ID
    """
    email_signature = create_email_signature(email_result)
    
    if email_signature in processed_records:
        record = processed_records[email_signature]
        print(f"Email already processed on {record.get('processed_date')}")
        print(f"Original sender: {record.get('sender_email')}")
        print(f"Original subject: {record.get('subject')}")
        return True
    
    return False

def is_email_already_sent(email_result, sent_records):
    """
    Check if this email's results have already been sent back to the user
    """
    email_signature = create_email_signature(email_result)
    
    if email_signature in sent_records:
        record = sent_records[email_signature]
        print(f"Email results already sent on {record.get('sent_date')}")
        print(f"Sender: {record.get('sender_email')}")
        print(f"Results count: {record.get('results_count')}")
        return True
    
    return False

def extract_attachments_from_email(bucket, email_key):
    """
    Extract attachments from a raw email message stored by SES in S3
    Now supports images in addition to PDFs and ZIPs
    """
    try:
        # Download raw email from S3
        response = s3.get_object(Bucket=bucket, Key=email_key)
        raw_email_content = response['Body'].read()
        
        # Parse the raw email message
        msg = email.message_from_bytes(raw_email_content)
        
        # Extract sender information - handle different email formats
        sender_email = msg.get('From', '')
        # Clean up sender email - extract just the email address if in format "Name <email@domain.com>"
        if '<' in sender_email and '>' in sender_email:
            sender_email = sender_email.split('<')[1].split('>')[0].strip()
        
        subject = msg.get('Subject', '')
        message_id = msg.get('Message-ID', '')
        date = msg.get('Date', '')
        
        print(f"Processing raw email from: {sender_email}")
        print(f"Subject: {subject}")
        print(f"Date: {date}")
        print(f"Message-ID: {message_id}")
        
        attachments = []
        
        # Walk through all parts of the multipart email
        for part in msg.walk():
            # Get content disposition
            content_disposition = str(part.get("Content-Disposition", ""))
            content_type = part.get_content_type()
            
            print(f"Processing part: Content-Type: {content_type}, Content-Disposition: {content_disposition}")
            
            # Check if this part is an attachment
            if "attachment" in content_disposition.lower():
                filename = part.get_filename()
                if filename:
                    # Decode the attachment content
                    content = part.get_payload(decode=True)
                    
                    if content:
                        # Process PDF, ZIP, and image files
                        if filename.lower().endswith(('.pdf', '.zip', '.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.webp')):
                            attachments.append((content, filename, content_type))
                            print(f"Found attachment: {filename} ({len(content)} bytes, type: {content_type})")
                        else:
                            print(f"Skipping attachment: {filename} (unsupported file type)")
                    else:
                        print(f"Warning: Could not decode attachment {filename}")
                else:
                    print(f"Warning: Attachment found but no filename available")
            
            # Also check for inline files that might be attachments without explicit Content-Disposition
            elif content_type in ['application/pdf', 'application/zip'] or content_type.startswith('image/'):
                filename = part.get_filename()
                if not filename:
                    # Try to get filename from Content-Type parameters
                    if hasattr(part, 'get_param'):
                        filename = part.get_param('name')
                    
                    # Generate filename if still not found
                    if not filename:
                        if content_type == 'application/pdf':
                            extension = '.pdf'
                        elif content_type == 'application/zip':
                            extension = '.zip'
                        elif content_type.startswith('image/'):
                            extension = '.jpg'  # Default image extension
                        else:
                            extension = '.unknown'
                        filename = f"attachment_{len(attachments)+1}{extension}"
                
                content = part.get_payload(decode=True)
                if content and filename:
                    attachments.append((content, filename, content_type))
                    print(f"Found inline attachment: {filename} ({len(content)} bytes, type: {content_type})")
        
        if not attachments:
            print("No PDF, ZIP, or image attachments found in email")
            # Debug: Print all parts found
            print("All email parts found:")
            for i, part in enumerate(msg.walk()):
                print(f"  Part {i}: {part.get_content_type()}, disposition: {part.get('Content-Disposition', 'None')}")
        
        return {
            "success": True,
            "sender_email": sender_email,
            "subject": subject,
            "message_id": message_id,
            "date": date,
            "attachments": attachments
        }
        
    except Exception as e:
        print(f"Error extracting attachments from {email_key}: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {
            "success": False,
            "error": str(e),
            "sender_email": "",
            "subject": "",
            "message_id": "",
            "date": "",
            "attachments": []
        }

def extract_text_from_pdf(pdf_content):
    """Extract raw text from PDF content"""
    try:
        reader = PdfReader(io.BytesIO(pdf_content))
        full_text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
        
        return {
            "success": True,
            "text": full_text,
            "page_count": len(reader.pages)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "text": "",
            "page_count": 0
        }

def process_image_with_vision(image_content, filename, api_key):
    """
    Process image using OpenAI Vision API to extract text
    """
    try:
        # Convert image to base64
        image_base64 = base64.b64encode(image_content).decode('utf-8')
        
        # Vision prompt for invoice/receipt analysis
        prompt = """
        Analyze this image and extract any text content, especially if it appears to be an invoice, receipt, or bill.
        
        Please extract all readable text from the image, maintaining the structure and layout as much as possible.
        Include:
        - All text content (handwritten or printed)
        - Numbers, amounts, dates
        - Company names, addresses
        - Item descriptions
        - Any other readable information
        
        If this appears to be a financial document (invoice, receipt, bill), note that specifically.
        
        Return the extracted text content:
        """
        
        extracted_text = call_openai_vision_api(prompt, image_base64, api_key)
        
        if extracted_text:
            return {
                "success": True,
                "text": extracted_text,
                "source": "vision_api"
            }
        else:
            return {
                "success": False,
                "error": "No text extracted from image",
                "text": "",
                "source": "vision_api"
            }
            
    except Exception as e:
        print(f"Error processing image {filename} with Vision API: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "text": "",
            "source": "vision_api"
        }

def check_document_type(text, api_key):
    """
    Check if the document is a bill/invoice or something else.
    More lenient classification to catch all types of invoices.
    """
    prompt = f"""
    You are a document classifier. Analyze the following document text and determine if it is:
    1. A BILL/INVOICE - ANY document that shows amounts to be paid, charges, fees, costs, or financial obligations
    2. OTHER - clearly non-financial documents like contracts, reports, manuals, etc.
    
    BILL/INVOICE includes (be very inclusive):
    - Traditional invoices with invoice numbers and line items
    - Bills (utility, phone, internet, etc.)
    - Statements with charges or amounts due (even if called "statement")
    - AWS bills, cloud service bills, subscription bills
    - Tax bills, government fees, penalty notices
    - Service charges, professional service bills
    - Receipts with amounts paid or due
    - ANY financial document showing money owed or charges
    - Handwritten receipts or bills from local markets/vendors
    
    IMPORTANT CLASSIFICATION RULES:
    - If you see ANY dollar amounts, fees, charges, or costs → BILL_INVOICE
    - If you see terms like "amount due", "total", "charges", "bill", "invoice" → BILL_INVOICE
    - If you see vendor/company names with amounts → BILL_INVOICE
    - If you see dates with financial amounts → BILL_INVOICE
    - Only classify as OTHER if it's clearly non-financial (contracts, manuals, reports with no charges)
    - When in doubt, classify as BILL_INVOICE (better to process than skip)
    
    Return ONLY a JSON object with these keys:
    - document_type: "BILL_INVOICE" or "OTHER" 
    - confidence: "HIGH", "MEDIUM", or "LOW"
    - reason: Brief explanation for the classification
    
    Document text (first 2000 characters):
    {text[:2000]}
    """
    
    function_definition = {
        "name": "ClassifyDocument",
        "description": "Classify document type",
        "parameters": {
            "type": "object",
            "properties": {
                "document_type": {
                    "type": "string",
                    "enum": ["BILL_INVOICE", "OTHER"],
                    "description": "Type of document"
                },
                "confidence": {
                    "type": "string",
                    "enum": ["HIGH", "MEDIUM", "LOW"],
                    "description": "Confidence level"
                },
                "reason": {
                    "type": "string",
                    "description": "Brief explanation"
                }
            },
            "required": ["document_type", "confidence", "reason"]
        }
    }
    
    result = call_openai_api(prompt, api_key, function_definition)
    
    if not result:
        # Default to BILL_INVOICE if API fails - better to process than skip
        return {"document_type": "BILL_INVOICE", "confidence": "LOW", "reason": "API call failed - defaulting to process"}
    
    return result

def extract_billing_info_with_gpt(text, api_key):
    """
    Extracts billing information from PDF text using GPT.
    Updated with more flexible PO number detection, better field names, and items list.
    """
    prompt = f"""
    You are a professional invoice analyzer. Extract the following information from this invoice text:
    1. PO number - Look for ANY number that appears after "PO", "P.O.", "Purchase Order", "PO#", "PO:", "PO-", etc. Can be any length (3-10 digits). If no PO number is found, return "NOT_FOUND"
    2. Bill To (company/person the invoice is billed to)
    3. Bill From (company/vendor issuing the invoice)
    4. Total Amount (final total amount on the invoice - return ONLY the numeric value without currency symbols)
    5. Amount Due (amount that needs to be paid - return ONLY the numeric value without currency symbols)
    6. Currency (currency code like USD, INR, EUR, etc.)
    7. Bill ID/Invoice number
    8. Bill Date (invoice date in format YYYY-MM-DD)
    9. Items/Services (list of products, services, or items purchased - comma separated)
    
    FLEXIBLE PO NUMBER RULES:
    - Look for patterns like: "PO: 124555", "P.O. 124555", "Purchase Order 124555", "PO-124555", "PO# 124555"
    - Can be 3-10 digits long
    - Can start with any digit (not just 2 or 3)
    - Examples of VALID patterns: "PO-124555", "PO: 20001", "Purchase Order 987654", "P.O.# 12345"
    - If multiple PO numbers found, pick the most prominent/first one
    - If no PO reference found at all, set to "NOT_FOUND"
    
    AMOUNT RULES:
    - Total Amount: Look for "Total", "Grand Total", "Amount", "Invoice Total" - return only numbers (e.g., "1234.56")
    - Amount Due: Look for "Amount Due", "Balance Due", "Due", "Pay This Amount" - return only numbers (e.g., "1234.56")
    - Remove all currency symbols, commas, and special characters from amounts
    
    CURRENCY RULES:
    - Extract currency separately (USD, INR, EUR, GBP, etc.)
    - Look for currency symbols ($, ₹, €, £) or currency codes
    
    ITEMS/SERVICES RULES:
    - List all products, services, subscriptions, or items mentioned in the invoice
    - Include descriptions, product names, service types
    - Separate multiple items with commas
    - Examples: "Web Hosting Service, Domain Registration" or "Electricity Bill, Service Charges"
    
    Return ONLY a JSON object with these keys: po_number, bill_to, bill_from, total_amount, amount_due, currency, bill_id, bill_date, items_services
    
    Here's the invoice text:
    {text}
    """
    
    function_definition = {
        "name": "ExtractInvoiceData",
        "description": "Extract structured data from invoice text",
        "parameters": {
            "type": "object",
            "properties": {
                "po_number": {
                    "type": "string",
                    "description": "Purchase Order number - any number found after PO/Purchase Order references"
                },
                "bill_to": {
                    "type": "string",
                    "description": "Company or person the invoice is billed to"
                },
                "bill_from": {
                    "type": "string",
                    "description": "Company or vendor issuing the invoice"
                },
                "total_amount": {
                    "type": "string",
                    "description": "Total amount on the invoice - numbers only, no currency symbols"
                },
                "amount_due": {
                    "type": "string",
                    "description": "Amount due to be paid - numbers only, no currency symbols"
                },
                "currency": {
                    "type": "string",
                    "description": "Currency code (USD, INR, EUR, etc.)"
                },
                "bill_id": {
                    "type": "string",
                    "description": "Invoice number or ID"
                },
                "bill_date": {
                    "type": "string",
                    "description": "Date of the invoice in format YYYY-MM-DD"
                },
                "items_services": {
                    "type": "string",
                    "description": "Comma-separated list of items, products, or services purchased"
                }
            },
            "required": ["po_number", "bill_to", "bill_from", "total_amount", "amount_due", "currency", "bill_id", "bill_date", "items_services"]
        }
    }
    
    result = call_openai_api(prompt, api_key, function_definition)
    
    if not result:
        return {
            "po_number": "API_ERROR",
            "bill_to": "API_ERROR",
            "bill_from": "API_ERROR",
            "total_amount": "API_ERROR",
            "amount_due": "API_ERROR",
            "currency": "API_ERROR",
            "bill_id": "API_ERROR",
            "bill_date": "API_ERROR",
            "items_services": "API_ERROR"
        }
    
    # Clean up amounts and ensure proper formatting
    if 'total_amount' in result:
        result['total_amount'] = clean_amount(result['total_amount'])
    if 'amount_due' in result:
        result['amount_due'] = clean_amount(result['amount_due'])
    
    return result

def process_attachment(attachment_content, filename, openai_api_key):
    """
    Process a single attachment (PDF, ZIP, or image file)
    No duplicate checking - treat every request as fresh
    """
    processed_results = []
    
    if filename.lower().endswith('.zip'):
        # Process ZIP file
        try:
            with zipfile.ZipFile(io.BytesIO(attachment_content), 'r') as zip_file:
                file_list = zip_file.namelist()
                print(f"Files in ZIP {filename}: {file_list}")
                
                for file_name in file_list:
                    if file_name.lower().endswith(('.pdf', '.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.webp')) and not file_name.startswith('__MACOSX/'):
                        print(f"Processing file from ZIP: {file_name}")
                        try:
                            file_data = zip_file.read(file_name)
                            if file_name.lower().endswith('.pdf') and file_data.startswith(b'%PDF'):
                                result = process_single_pdf(file_data, file_name, openai_api_key)
                                processed_results.append(result)
                                print(f"Successfully processed PDF from ZIP: {file_name}")
                            elif file_name.lower().endswith(('.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.webp')):
                                result = process_single_image(file_data, file_name, openai_api_key)
                                processed_results.append(result)
                                print(f"Successfully processed image from ZIP: {file_name}")
                            else:
                                print(f"Skipped {file_name} - not a valid PDF or image")
                        except Exception as e:
                            print(f"Error extracting {file_name}: {str(e)}")
                            processed_results.append({
                                "filename": file_name,
                                "status": "error",
                                "po_number": "ERROR",
                                "bill_to": "ERROR",
                                "bill_from": "ERROR",
                                "total_amount": "ERROR",
                                "amount_due": "ERROR",
                                "currency": "ERROR",
                                "bill_id": "ERROR",
                                "bill_date": "ERROR",
                                "items_services": "ERROR"
                            })
        except Exception as e:
            print(f"Error processing ZIP file {filename}: {str(e)}")
            processed_results.append({
                "filename": filename,
                "status": "error",
                "po_number": "ZIP_ERROR",
                "bill_to": "ZIP_ERROR",
                "bill_from": "ZIP_ERROR",
                "total_amount": "ZIP_ERROR",
                "amount_due": "ZIP_ERROR",
                "currency": "ZIP_ERROR",
                "bill_id": "ZIP_ERROR",
                "bill_date": "ZIP_ERROR",
                "items_services": "ZIP_ERROR"
            })
    
    elif filename.lower().endswith('.pdf'):
        # Process single PDF file
        print(f"Processing PDF: {filename}")
        result = process_single_pdf(attachment_content, filename, openai_api_key)
        processed_results.append(result)
    
    elif filename.lower().endswith(('.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.webp')):
        # Process single image file
        print(f"Processing image: {filename}")
        result = process_single_image(attachment_content, filename, openai_api_key)
        processed_results.append(result)
    
    return processed_results

def process_single_pdf(pdf_content, filename, openai_api_key):
    """
    Process a single PDF file and extract invoice data
    Updated with new field names including items_services
    """
    print(f"Processing PDF: {filename}")
    
    # Extract text from PDF
    text_result = extract_text_from_pdf(pdf_content)
    
    if not text_result["success"]:
        return {
            "filename": filename,
            "status": "error",
            "po_number": "ERROR",
            "bill_to": "ERROR",
            "bill_from": "ERROR",
            "total_amount": "ERROR",
            "amount_due": "ERROR",
            "currency": "ERROR",
            "bill_id": "ERROR",
            "bill_date": "ERROR",
            "items_services": "ERROR"
        }
    
    extracted_text = text_result["text"]
    
    if not extracted_text.strip():
        return {
            "filename": filename,
            "status": "error",
            "po_number": "NO_TEXT",
            "bill_to": "NO_TEXT",
            "bill_from": "NO_TEXT",
            "total_amount": "NO_TEXT",
            "amount_due": "NO_TEXT",
            "currency": "NO_TEXT",
            "bill_id": "NO_TEXT",
            "bill_date": "NO_TEXT",
            "items_services": "NO_TEXT"
        }
    
    # Check document type
    doc_classification = check_document_type(extracted_text, openai_api_key)
    
    if doc_classification["document_type"] != "BILL_INVOICE":
        return {
            "filename": filename,
            "status": "skipped",
            "po_number": "NOT_INVOICE",
            "bill_to": "NOT_INVOICE",
            "bill_from": "NOT_INVOICE",
            "total_amount": "NOT_INVOICE",
            "amount_due": "NOT_INVOICE",
            "currency": "NOT_INVOICE",
            "bill_id": "NOT_INVOICE",
            "bill_date": "NOT_INVOICE",
            "items_services": "NOT_INVOICE"
        }
    
    # Extract billing information
    billing_info = extract_billing_info_with_gpt(extracted_text, openai_api_key)
    
    result = {
        "filename": filename,
        "status": "success",
        **billing_info
    }
    
    print(f"Successfully processed PDF: {filename}")
    return result

def process_single_image(image_content, filename, openai_api_key):
    """
    Process a single image file and extract invoice data using Vision API
    """
    print(f"Processing image: {filename}")
    
    # Extract text from image using Vision API
    text_result = process_image_with_vision(image_content, filename, openai_api_key)
    
    if not text_result["success"]:
        return {
            "filename": filename,
            "status": "error",
            "po_number": "ERROR",
            "bill_to": "ERROR",
            "bill_from": "ERROR",
            "total_amount": "ERROR",
            "amount_due": "ERROR",
            "currency": "ERROR",
            "bill_id": "ERROR",
            "bill_date": "ERROR",
            "items_services": "ERROR"
        }
    
    extracted_text = text_result["text"]
    
    if not extracted_text.strip():
        return {
            "filename": filename,
            "status": "error",
            "po_number": "NO_TEXT",
            "bill_to": "NO_TEXT",
            "bill_from": "NO_TEXT",
            "total_amount": "NO_TEXT",
            "amount_due": "NO_TEXT",
            "currency": "NO_TEXT",
            "bill_id": "NO_TEXT",
            "bill_date": "NO_TEXT",
            "items_services": "NO_TEXT"
        }
    
    print(f"Extracted text from image {filename}: {extracted_text[:200]}...")
    
    # Check document type
    doc_classification = check_document_type(extracted_text, openai_api_key)
    
    if doc_classification["document_type"] != "BILL_INVOICE":
        return {
            "filename": filename,
            "status": "skipped",
            "po_number": "NOT_INVOICE",
            "bill_to": "NOT_INVOICE",
            "bill_from": "NOT_INVOICE",
            "total_amount": "NOT_INVOICE",
            "amount_due": "NOT_INVOICE",
            "currency": "NOT_INVOICE",
            "bill_id": "NOT_INVOICE",
            "bill_date": "NOT_INVOICE",
            "items_services": "NOT_INVOICE"
        }
    
    # Extract billing information
    billing_info = extract_billing_info_with_gpt(extracted_text, openai_api_key)
    
    result = {
        "filename": filename,
        "status": "success",
        **billing_info
    }
    
    print(f"Successfully processed image: {filename}")
    return result

def create_csv_from_results(all_results, bucket, output_key):
    """
    Create CSV file from all processing results and upload to S3.
    Updated with new field names including items_services column.
    """
    csv_buffer = io.StringIO()
    
    headers = [
        "filename",
        "po_number",
        "bill_to", 
        "bill_from",
        "total_amount",
        "amount_due",
        "currency",
        "bill_id",
        "bill_date",
        "items_services",
        "status"
    ]
    
    writer = csv.DictWriter(csv_buffer, fieldnames=headers)
    writer.writeheader()
    
    for result in all_results:
        row = {}
        for header in headers:
            row[header] = result.get(header, "")
        writer.writerow(row)
    
    csv_content = csv_buffer.getvalue()
    
    try:
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        print(f"CSV file uploaded to: s3://{bucket}/{output_key}")
        return csv_content.encode('utf-8')
    except Exception as e:
        print(f"Error uploading CSV: {str(e)}")
        return csv_content.encode('utf-8')

def send_no_attachments_email(sender_email, original_subject, from_email=None):
    """
    Send an email to the sender informing them that no valid attachments were found
    """
    try:
        # Use configured from_email or default
        if not from_email:
            from_email = os.environ.get('SES_FROM_EMAIL', 'noreply@yourdomain.com')
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = sender_email
        msg['Subject'] = f"No Valid Attachments Found - {original_subject}"
        
        # Email body
        body = f"""
Hello,

We received your email but could not find any valid PDF, ZIP, or image attachments to process.

Original Subject: {original_subject}
Processing Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC

Please ensure that:
1. Your email includes PDF files, ZIP files containing documents, or images of invoices/receipts
2. The files are properly attached (not embedded images in email body)
3. The PDF files contain readable text (not just scanned images without OCR)
4. Images are clear and readable (invoices, receipts, bills)

Supported file types:
- PDF files (.pdf)
- ZIP files (.zip) containing PDFs or images
- Image files (.jpg, .jpeg, .png, .bmp, .tiff, .webp)

If you need assistance, please reply to this email.

Best regards,
Invoice Processing System
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Send email
        response = ses.send_raw_email(
            Source=from_email,
            Destinations=[sender_email],
            RawMessage={'Data': msg.as_string()}
        )
        
        print(f"No attachments email sent to {sender_email}. MessageId: {response['MessageId']}")
        return True
        
    except Exception as e:
        print(f"Error sending no attachments email to {sender_email}: {str(e)}")
        return False

def send_csv_via_ses(sender_email, csv_content, original_subject, from_email=None):
    """
    Send the processed CSV file back to the original sender via SES
    """
    try:
        # Use configured from_email or default
        if not from_email:
            from_email = os.environ.get('SES_FROM_EMAIL', 'noreply@yourdomain.com')
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = sender_email
        msg['Subject'] = f"Invoice Processing Complete - {original_subject}"
        
        # Email body
        body = f"""
Hello,

Your invoice processing request has been completed successfully.

Original Subject: {original_subject}
Processing Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC

Please find the processed invoice data attached as a CSV file with the following columns:
- Filename: Name of the processed file
- PO Number: Purchase order number found in the invoice
- Bill To: Company/person the invoice is billed to
- Bill From: Company/vendor issuing the invoice  
- Total Amount: Total amount on the invoice (numbers only)
- Amount Due: Amount that needs to be paid (numbers only)
- Currency: Currency code (USD, INR, EUR, etc.)
- Bill ID: Invoice number or identifier
- Bill Date: Date of the invoice
- Items/Services: Products or services purchased/subscribed
- Status: Processing status

Our system now supports:
- PDF documents
- ZIP files containing multiple documents
- Image files (JPG, PNG, etc.) including handwritten receipts

Best regards,
Invoice Processing System
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Attach CSV file
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"invoice_data_{timestamp}.csv"
        
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(csv_content)
        encoders.encode_base64(attachment)
        attachment.add_header(
            'Content-Disposition',
            f'attachment; filename= {filename}'
        )
        msg.attach(attachment)
        
        # Send email
        response = ses.send_raw_email(
            Source=from_email,
            Destinations=[sender_email],
            RawMessage={'Data': msg.as_string()}
        )
        
        print(f"Email sent successfully to {sender_email}. MessageId: {response['MessageId']}")
        return True
        
    except Exception as e:
        print(f"Error sending email to {sender_email}: {str(e)}")
        return False

def lambda_handler(event, context):
    """
    Main Lambda handler with separate tracking for processed and sent emails
    Enhanced to support image processing
    """
    print(f"Lambda started. Event: {json.dumps(event, default=str)}")
    
    # Get OpenAI API key from environment variables
    openai_api_key = os.environ.get('OPENAI_API_KEY')
    if not openai_api_key:
        print("ERROR: OPENAI_API_KEY environment variable not set")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "OPENAI_API_KEY environment variable not set"
            })
        }
    
    # Use the correct bucket name and folder
    bucket = os.environ.get('S3_BUCKET', event.get('bucket', 'mailinvoices'))
    email_attachments_folder = "Emails/"
    
    print(f"Using bucket: {bucket}")
    print(f"Looking in folder: {email_attachments_folder}")
    
    processed_results = []
    failed_files = []
    emails_to_send = []
    
    # Get processed email records (Message-ID based tracking)
    processed_records = get_processed_emails(bucket)
    
    # Get sent email records (separate tracking for emails already sent)
    sent_records = get_sent_emails(bucket)
    
    try:
        # List all objects in the email_attachments folder
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=email_attachments_folder
        )
        
        if 'Contents' not in response:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": f"No files found in {email_attachments_folder}",
                    "bucket": bucket,
                    "folder": email_attachments_folder
                })
            }
        
        print(f"Found {len(response['Contents'])} objects in S3")
        
        # Filter for email files
        email_files = []
        for obj in response['Contents']:
            key = obj['Key']
            filename = key.split('/')[-1]
            
            # Skip the folder itself and empty files
            if key == email_attachments_folder or obj['Size'] == 0:
                continue
            
            # Skip obvious non-email files
            if filename.lower().endswith(('.csv', '.json', '.log')):
                continue
                
            email_files.append(obj)
        
        print(f"Found {len(email_files)} email files to check")
        
        if not email_files:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No email files found to process",
                    "bucket": bucket,
                    "folder": email_attachments_folder,
                    "total_objects": len(response['Contents'])
                })
            }
        
        # Process each email file with separate tracking for processing and sending
        new_emails_processed = 0
        emails_ready_to_send = 0
        
        for email_obj in email_files:
            email_key = email_obj['Key']
            print(f"=" * 50)
            print(f"Checking email: {email_key}")
            
            # First extract basic email info to create signature
            email_result = extract_attachments_from_email(bucket, email_key)
            
            if not email_result["success"]:
                print(f"Failed to extract email info from {email_key}: {email_result['error']}")
                failed_files.append({
                    "file": email_key,
                    "error": email_result["error"]
                })
                continue
            
            email_signature = create_email_signature(email_result)
            sender_email = email_result["sender_email"]
            subject = email_result["subject"]
            attachments = email_result["attachments"]
            
            print(f"Email from: {sender_email}")
            print(f"Subject: {subject}")
            print(f"Email signature: {email_signature}")
            print(f"Found {len(attachments)} attachments")
            
            # Check processing status
            already_processed = is_email_already_processed(email_result, processed_records)
            already_sent = is_email_already_sent(email_result, sent_records)
            
            if already_processed and already_sent:
                print(f"SKIPPING: Email already processed AND results already sent")
                continue
            elif already_processed and not already_sent:
                print(f"FOUND: Email processed but results NOT sent yet - will send results")
                # We need to get the results from the processed record or reprocess
                # For now, let's reprocess to ensure we have the results
            elif not already_processed:
                print(f"NEW EMAIL: Processing {email_key}")
                new_emails_processed += 1
            
            if not attachments:
                print(f"No valid attachments found")
                if sender_email and not already_sent:
                    send_no_attachments_email(sender_email, subject)
                    mark_email_as_sent(bucket, email_signature, sender_email, subject, 0)
                # Mark as processed even without attachments
                if not already_processed:
                    mark_email_as_processed(bucket, email_key, email_result, [])
                continue
            
            # Process attachments (reprocess if needed for sending)
            email_processed_results = []
            
            if not already_processed or not already_sent:
                for i, (attachment_content, filename, content_type) in enumerate(attachments):
                    print(f"Processing attachment {i+1}/{len(attachments)}: {filename}")
                    attachment_results = process_attachment(
                        attachment_content, 
                        filename, 
                        openai_api_key
                    )
                    email_processed_results.extend(attachment_results)
                    print(f"Got {len(attachment_results)} results from {filename}")
            
            if email_processed_results:
                processed_results.extend(email_processed_results)
                
                # Only add to send list if results haven't been sent yet
                if not already_sent:
                    emails_to_send.append({
                        "sender_email": sender_email,
                        "subject": subject,
                        "results": email_processed_results,
                        "email_signature": email_signature
                    })
                    emails_ready_to_send += 1
            
            # Mark as processed if it wasn't already
            if not already_processed:
                mark_email_as_processed(bucket, email_key, email_result, email_processed_results)
        
        print(f"=" * 50)
        print(f"PROCESSING SUMMARY:")
        print(f"Total email files found: {len(email_files)}")
        print(f"NEW emails processed: {new_emails_processed}")
        print(f"Previously processed: {len(processed_records)}")
        print(f"Already sent: {len(sent_records)}")
        print(f"Ready to send: {emails_ready_to_send}")
        
        # Send CSV files only for emails that haven't had results sent yet
        if emails_to_send:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            
            # Send individual CSVs to each sender
            for i, sender_info in enumerate(emails_to_send):
                print(f"Sending results to: {sender_info['sender_email']}")
                
                csv_key = f"processed_invoices/invoice_data_{timestamp}_{sender_info['sender_email'].split('@')[0]}.csv"
                csv_content = create_csv_from_results(sender_info['results'], bucket, csv_key)
                
                success = send_csv_via_ses(
                    sender_info['sender_email'], 
                    csv_content, 
                    sender_info['subject']
                )
                
                # Mark as sent only if email was successfully sent
                if success:
                    mark_email_as_sent(
                        bucket, 
                        sender_info['email_signature'], 
                        sender_info['sender_email'], 
                        sender_info['subject'], 
                        len(sender_info['results'])
                    )
        else:
            print("No emails ready to send results")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Processing complete. {new_emails_processed} NEW emails processed, {emails_ready_to_send} emails had results sent",
                "new_emails_processed": new_emails_processed,
                "total_emails_found": len(email_files),
                "previously_processed": len(processed_records),
                "previously_sent": len(sent_records),
                "emails_sent_this_run": len(emails_to_send),
                "summary": {
                    "successful_files": len([r for r in processed_results if r.get('status') == 'success']),
                    "error_files": len([r for r in processed_results if r.get('status') == 'error']),
                    "skipped_files": len([r for r in processed_results if r.get('status') == 'skipped']),
                    "total_files": len(processed_results)
                },
                "sample_results": processed_results[:3] if processed_results else [],
                "failed_files": failed_files,
                "note": f"Enhanced system now supports PDF, ZIP, and image files. Results have been emailed to {len(emails_to_send)} recipient(s) for emails that hadn't been sent yet."
            })
        }
        
    except Exception as e:
        error_message = f"Error in main processing: {str(e)}"
        print(error_message)
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": error_message,
                "processed_results": processed_results,
                "failed_files": failed_files,
                "debug_info": {
                    "bucket": bucket,
                    "folder": email_attachments_folder
                }
            })
        }