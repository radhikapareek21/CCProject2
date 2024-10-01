from flask import Flask, request
import pandas as pd
import os

# Initialize the Flask application
app = Flask(__name__)

# Load the classification lookup table from the CSV file
classification_df = pd.read_csv('face_images_1000.csv')
# Create a dictionary for quick lookup
classification_lookup = dict(zip(classification_df['Image'], classification_df['Results']))

@app.route("/", methods=["POST"])
def handle_image():
    # Check if an image file is part of the request
    if 'inputFile' not in request.files:
        return "No file part", 400
    
    file = request.files['inputFile']
    
    # Get the filename without extension
    filename = os.path.splitext(file.filename)[0]
    
    # Perform the lookup for the filename in the classification table
    prediction_result = classification_lookup.get(filename, "Unknown")
    
    # Format the response as "<filename>:<prediction_result>"
    response_text = f"{filename}:{prediction_result}"
    
    return response_text, 200

if __name__ == "__main__":
    # Run the Flask server on port 8000
    app.run(host="0.0.0.0", port=8000)
