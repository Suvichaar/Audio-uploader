import streamlit as st
import requests
import boto3
import gspread
from google.oauth2.service_account import Credentials

# Function to initialize Google Sheets client
def init_google_sheets_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(credentials)

# Initialize S3 client with region
def init_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=st.secrets["AWS"]["aws_access_key_id"],
        aws_secret_access_key=st.secrets["AWS"]["aws_secret_access_key"],
        region_name=st.secrets["AWS"]["aws_region"]
    )

# Convert column letter to index (e.g., A -> 1, B -> 2)
def column_letter_to_index(letter):
    return ord(letter.upper()) - ord('A') + 1

# Generate TTS and upload to S3
def generate_and_upload_tts(text, s3_client, bucket_name, file_name):
    API_URL = st.secrets["AWS"]["azure_tts_api_url"]
    API_KEY = st.secrets["AWS"]["azure_api_key"]
    headers = {"Content-Type": "application/json", "api-key": API_KEY}
    payload = {
        "model": "gpt-4",
        "input": text,
        "voice": "nova",
        "output_format": "audio-24khz-48kbitrate-mono-mp3"
    }
    response = requests.post(API_URL, headers=headers, json=payload)
    
    if response.status_code == 200:
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=response.content)
        return f"https://{bucket_name}.s3.{st.secrets['AWS']['aws_region']}.amazonaws.com/{file_name}"
    else:
        st.error(f"TTS generation failed: {response.status_code}, {response.text}")
        return None

# Process Google Sheet and update with S3 URLs
def process_google_sheet(spreadsheet_id, source_column_letter, target_column_letter):
    google_sheets_client = init_google_sheets_client()
    s3_client = init_s3_client()
    bucket_name = st.secrets["AWS"]["s3_bucket_name"]
    
    # Convert column letters to indices
    source_column = column_letter_to_index(source_column_letter)
    target_column = column_letter_to_index(target_column_letter)
    
    # Access the Google Sheet
    sheet = google_sheets_client.open_by_key(spreadsheet_id).sheet1  # Assumes the first sheet
    texts = sheet.col_values(source_column)
    
    with st.spinner("Processing rows..."):
        for row_num, text in enumerate(texts[1:], start=2):  # Skip header row
            if text:
                text = str(text)  # Ensure text is a string
                file_name = f"tts_audio_row{row_num}.mp3"
                
                # Generate TTS audio and upload to S3
                s3_url = generate_and_upload_tts(text, s3_client, bucket_name, file_name)
                
                # Check if S3 URL is returned successfully
                if s3_url:
                    try:
                        # Update Google Sheet with the S3 URL
                        sheet.update_cell(row_num, target_column, s3_url)
                    except Exception as e:
                        st.error(f"Failed to update row {row_num} in Google Sheet: {e}")
            else:
                st.warning(f"Skipping empty cell at row {row_num}.")

# Streamlit App Interface
st.title("Google Sheets Text-to-Speech and S3 Upload")

spreadsheet_id = st.text_input("Enter the Google Spreadsheet ID:")
source_column = st.text_input("Enter the source column (e.g., A, B, C):").upper()
target_column = st.text_input("Enter the target column for S3 URLs (e.g., D, E, F):").upper()

if st.button("Run Text-to-Speech and Upload"):
    if spreadsheet_id and source_column and target_column:
        try:
            process_google_sheet(spreadsheet_id, source_column, target_column)
            st.success("Processing completed and S3 URLs updated in the Google Sheet.")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please provide all inputs.")
