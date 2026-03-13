import easyocr
import cv2
import os

reader = easyocr.Reader(['en'], gpu=False)

def scan_business_card(image_path):
    # Debug: check if file exists
    print(f"File exists: {os.path.exists(image_path)}")
    print(f"Current working directory: {os.getcwd()}")

    img = cv2.imread(image_path)

    if img is None:
        print(f"❌ Could not load image: {image_path}")
        return

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    cv2.imwrite("temp.jpg", gray)

    results = reader.readtext("temp.jpg")

    print(f"\n📇 Extracted Text from: {image_path}")
    print("─" * 40)
    for (_, text, confidence) in results:
        if confidence > 0.3:
            print(text)

if __name__ == "__main__":
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Path to the business card image in the same directory
    image_file = os.path.join(current_dir, "business_card.png")
    
    scan_business_card(image_file)