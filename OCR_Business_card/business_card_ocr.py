"""
Business Card OCR using EasyOCR
Install: pip install easyocr opencv-python pillow
"""

import easyocr
import cv2
import re
import json
from PIL import Image


# ─────────────────────────────────────────────
# Step 1: Initialize EasyOCR Reader
# ─────────────────────────────────────────────
# Languages: ['en'] for English only
# ['en', 'ta'] for English + Tamil (useful for Coimbatore!)
# gpu=False if you don't have a GPU
reader = easyocr.Reader(['en'], gpu=False)


# ─────────────────────────────────────────────
# Step 2: Preprocess Image (improves accuracy)
# ─────────────────────────────────────────────
def preprocess_image(image_path):
    img = cv2.imread(image_path)

    # Resize if too small
    h, w = img.shape[:2]
    if w < 800:
        scale = 800 / w
        img = cv2.resize(img, None, fx=scale, fy=scale)

    # Convert to grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # Denoise
    denoised = cv2.fastNlMeansDenoising(gray, h=10)

    # Increase contrast
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    enhanced = clahe.apply(denoised)

    # Save preprocessed image temporarily
    preprocessed_path = "preprocessed_card.jpg"
    cv2.imwrite(preprocessed_path, enhanced)

    return preprocessed_path


# ─────────────────────────────────────────────
# Step 3: Extract Raw Text using EasyOCR
# ─────────────────────────────────────────────
def extract_text(image_path):
    preprocessed = preprocess_image(image_path)

    # Returns list of [bounding_box, text, confidence]
    results = reader.readtext(preprocessed)

    # Filter low confidence results (below 30%)
    lines = [text for (_, text, confidence) in results if confidence > 0.3]

    print("Raw Extracted Lines:")
    for line in lines:
        print(f"  → {line}")

    return lines


# ─────────────────────────────────────────────
# Step 4: Map Text to Fields using Regex
# ─────────────────────────────────────────────
def map_fields(lines):
    contact = {
        "name":    None,
        "title":   None,
        "company": None,
        "email":   None,
        "phone":   None,
        "website": None,
        "address": None,
        "linkedin": None,
        "twitter": None,
    }

    # Regex patterns
    EMAIL_RE   = r'[\w\.-]+@[\w\.-]+\.\w+'
    PHONE_RE   = r'(\+?\d[\d\s\-().]{7,}\d)'
    WEBSITE_RE = r'(www\.[\w\.-]+\.\w+|https?://[\w\.-]+\.\w+[\w/.-]*)'
    LINKEDIN_RE = r'(linkedin\.com/in/[\w\-]+|linkedin\.com/[\w\-/]+)'
    TWITTER_RE  = r'@[\w]+'

    # Keywords that hint at job titles
    TITLE_KEYWORDS = [
        'ceo', 'cto', 'cfo', 'founder', 'co-founder', 'director',
        'manager', 'engineer', 'developer', 'designer', 'consultant',
        'analyst', 'officer', 'president', 'head', 'lead', 'senior',
        'associate', 'executive', 'specialist', 'architect', 'intern'
    ]

    unmatched = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Email
        email_match = re.search(EMAIL_RE, line, re.IGNORECASE)
        if email_match and not contact["email"]:
            contact["email"] = email_match.group()
            continue

        # LinkedIn (check before generic website)
        linkedin_match = re.search(LINKEDIN_RE, line, re.IGNORECASE)
        if linkedin_match and not contact["linkedin"]:
            contact["linkedin"] = linkedin_match.group()
            continue

        # Website
        website_match = re.search(WEBSITE_RE, line, re.IGNORECASE)
        if website_match and not contact["website"]:
            contact["website"] = website_match.group()
            continue

        # Phone
        phone_match = re.search(PHONE_RE, line)
        if phone_match and not contact["phone"]:
            contact["phone"] = phone_match.group().strip()
            continue

        # Twitter handle
        twitter_match = re.search(TWITTER_RE, line)
        if twitter_match and not contact["twitter"]:
            contact["twitter"] = twitter_match.group()
            continue

        # Job Title (keyword-based)
        if any(kw in line.lower() for kw in TITLE_KEYWORDS):
            if not contact["title"]:
                contact["title"] = line
                continue

        # Address (has number + street keywords)
        address_keywords = ['street', 'st.', 'road', 'rd.', 'avenue', 'ave',
                            'lane', 'blvd', 'nagar', 'colony', 'district',
                            'city', 'state', 'pin', 'zip', 'floor', 'suite']
        if any(kw in line.lower() for kw in address_keywords):
            if not contact["address"]:
                contact["address"] = line
                continue

        # Collect unmatched for name / company guessing
        unmatched.append(line)

    # ── Guess name and company from unmatched lines ──
    # Heuristic: ALL CAPS short line = company name
    # Mixed case 2-3 word line = person name
    for line in unmatched:
        words = line.split()

        if not contact["name"]:
            # Name: 2–4 words, mixed case, no digits
            if 2 <= len(words) <= 4 and not any(c.isdigit() for c in line):
                if not line.isupper():  # Not all caps (likely not a company)
                    contact["name"] = line
                    continue

        if not contact["company"]:
            # Company: could be all caps or contain Inc/Ltd/Pvt/Corp etc.
            company_hints = ['inc', 'ltd', 'llc', 'pvt', 'corp', 'co.', 'group',
                             'technologies', 'tech', 'solutions', 'services',
                             'systems', 'consulting', 'enterprise', 'digital']
            if any(kw in line.lower() for kw in company_hints) or line.isupper():
                contact["company"] = line
                continue

    # Remove None values for cleaner output
    contact = {k: v for k, v in contact.items() if v}

    return contact


# ─────────────────────────────────────────────
# Step 5: Main Function — Run Everything
# ─────────────────────────────────────────────
def scan_business_card(image_path):
    print(f"\n📇 Scanning: {image_path}")
    print("─" * 40)

    # Extract text
    lines = extract_text(image_path)

    # Map to fields
    contact = map_fields(lines)

    print("\n✅ Extracted Contact Info:")
    print(json.dumps(contact, indent=2))

    return contact


# ─────────────────────────────────────────────
# Run it
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import os
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Path to the business card image in the same directory
    image_file = os.path.join(current_dir, "business_card.png")
    
    if os.path.exists(image_file):
        result = scan_business_card(image_file)
    else:
        print(f"❌ Error: Could not find {image_file}")
