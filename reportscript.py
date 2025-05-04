import os
import re
import sys
import json
import pandas as pd
from datetime import datetime
from deep_translator import GoogleTranslator
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import (
    CellFormat,
    Color,
    TextFormat,
    format_cell_range,
    set_column_width,
)
import tkinter as tk
from tkinter import filedialog
from concurrent.futures import ThreadPoolExecutor

CONFIG_FILE = "user_config.json"  # This file stores the user's configuration
# FILE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "extract"))

# File pattern
file_pattern = r"project_(\d{4}-\d{2}-\d{2})\.csv"

# Find the latest file
def get_latest_file(folder_path):
    if not os.path.exists(folder_path):
        print(f"Folder not found: {folder_path}")
        return None

    latest_file = None
    latest_date = None

    for filename in os.listdir(folder_path):
        match = re.search(file_pattern, filename)
        if match:
            file_date_str = match.group(1)  # Extract date part from filename
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")  # Convert to datetime
            
            # Check if this file is the latest
            if latest_date is None or file_date > latest_date:
                latest_date = file_date
                latest_file = latest_file = os.path.join(folder_path, filename)

    if not latest_file:
        print(f"No matching files found in folder: {folder_path}")

    return latest_file


def load_config():
    """Load user configuration from a file."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as file:
            return json.load(file)
    return {"email": None, "credential_file": None}


def save_config(email=None, credential_file=None):
    """Save user email and credential file path to a configuration file."""
    config = load_config()
    if email:
        config["email"] = email
    if credential_file:
        config["credential_file"] = credential_file
    with open(CONFIG_FILE, "w") as file:
        json.dump(config, file, indent=4)
    print("Configuration saved successfully!")


def ask_for_file(file_type):
    """Prompt the user to select a file using a file dialog."""
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(title=f"Select {file_type} File")
    return file_path


def get_email():
    """Retrieve the user's email from the configuration file or prompt for input."""
    config = load_config()
    email = config.get("email")
    if not email:
        email = input("Please enter your email address: ").strip()
        save_config(email=email)
    return email


def get_credential_file():
    """Retrieve the Google Sheets credential file path."""
    config = load_config()
    credential_file = config.get("credential_file")
    if not credential_file or not os.path.exists(credential_file):
        print("Please select the credential file.")
        credential_file = ask_for_file("Credential")
        if credential_file:
            save_config(credential_file=credential_file)
        else:
            print("No file selected. Exiting.")
            return None
    return credential_file


class Translator:
    _cache = {}

    @staticmethod
    def translate_text(text, src_lang="en", target_lang="ja"):
        """Translate text using GoogleTranslator with caching."""
        if pd.notna(text) and str(text).strip() != "":
            key = (text, src_lang, target_lang)
            if key in Translator._cache:
                return Translator._cache[key]
            try:
                translated = GoogleTranslator(source=src_lang, target=target_lang).translate(text)
                Translator._cache[key] = translated
                return translated
            except Exception as e:
                print(f"Error translating text: {e}")
                return text
        return text


class DataProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data_frame = pd.DataFrame()

    def load_data(self):
        """Load data from the provided CSV file."""
        try:
            self.data_frame = pd.read_csv(self.file_path, delimiter=";", encoding="utf-8")
        except Exception as e:
            print(f"Error loading CSV file: {e}")
            sys.exit(1)

    def _batch_translate(self, texts, src_lang="en", target_lang="ja"):
        """Translate a list of texts in parallel using threads."""
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(
                lambda text: Translator.translate_text(text, src_lang, target_lang),
                [text if pd.notna(text) and str(text).strip() != "" else "" for text in texts]
            ))
        return results

    def process_data(self):
        """Process and translate data."""
        name_replacements = {
            "dhanukakarunasena": "Dhanuka",
            "malcolmSansen": "Malcom",
            "shimizu39": "Prashanti",
            "shimizuSarun": "Saran",
            "SonSansen": "Son",
            "HtetSansen": "Htet"
        }
        self.data_frame["Assignees"] = self.data_frame["Assignees"].replace(name_replacements)

        formatted_df = pd.DataFrame()
        formatted_df["Assignees"] = self.data_frame["Assignees"]
        formatted_df["Title"] = self.data_frame["Title"]
        formatted_df["タイトル"] = self._batch_translate(self.data_frame["Title"].tolist())
        formatted_df["body"] = self.data_frame["body"]
        formatted_df["ボディー"] = self._batch_translate(self.data_frame["body"].tolist())
        formatted_df["Repository"] = self.data_frame["Repository"]
        formatted_df["Status"] = self.data_frame["Status"]
        formatted_df["plan start"] = self.data_frame["plan start"]
        formatted_df["plan finish"] = self.data_frame["plan finish"]
        formatted_df["real start"] = self.data_frame["real start"]
        formatted_df["real finish"] = self.data_frame["real finish"]

        self.data_frame = formatted_df.fillna("").sort_values(by="Assignees").reset_index(drop=True)

class GoogleSheetManager:
    def __init__(self, creds_file, spreadsheet_name):
        self.creds_file = creds_file
        self.spreadsheet_name = spreadsheet_name
        self.client = None
        self.spreadsheet = None

    def authenticate(self):
        """Authenticate using Google Sheets credentials."""
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_file, scope)
            self.client = gspread.authorize(creds)
        except Exception as e:
            print(f"Error during authentication: {e}")
            sys.exit(1)

    def get_or_create_spreadsheet(self):
        """Retrieve or create the Google Sheets spreadsheet."""
        try:
            self.spreadsheet = self.client.open(self.spreadsheet_name)
            print(f"Spreadsheet '{self.spreadsheet_name}' already exists.")
        except gspread.exceptions.SpreadsheetNotFound:
            self.spreadsheet = self.client.create(self.spreadsheet_name)
            print(f"Spreadsheet '{self.spreadsheet_name}' created.")

    def share_spreadsheet(self, email, role="writer"):
        """Share the spreadsheet with the given email."""
        self.spreadsheet.share(email, perm_type="user", role=role)

    def get_or_create_worksheet(self, worksheet_name):
        """Retrieve or create a worksheet."""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            print(f"Worksheet '{worksheet_name}' already exists.")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")
            print(f"Worksheet '{worksheet_name}' created.")
        return worksheet

    @staticmethod
    def update_data(worksheet, data_frame):
        """Upload data to the worksheet."""
        worksheet.clear()
        data = [data_frame.columns.values.tolist()] + data_frame.values.tolist()
        worksheet.update(values=data, range_name="A1")
        print("Data uploaded successfully!")

    @staticmethod
    def apply_formatting(worksheet, header_range):
        """Apply formatting to worksheet headers."""
        header_format = CellFormat(
            backgroundColor=Color(0, 0, 0),
            textFormat=TextFormat(foregroundColor=Color(1, 1, 1), bold=True),
            horizontalAlignment="CENTER",
            verticalAlignment="MIDDLE",
            wrapStrategy="WRAP",
        )
        format_cell_range(worksheet, header_range, header_format)

    @staticmethod
    def set_column_widths(worksheet, column_widths):
        """Set the column widths in the worksheet."""
        for col, width in column_widths.items():
            set_column_width(worksheet, col, width)


class WeeklyProjectReport:
    def __init__(self, csv_file, creds_file, spreadsheet_name, email):
        self.data_processor = DataProcessor(csv_file)
        self.sheet_manager = GoogleSheetManager(creds_file, spreadsheet_name)
        self.email = email

    def generate_report(self):
        """Generate and upload the weekly project report."""
        self.data_processor.load_data()
        self.data_processor.process_data()

        self.sheet_manager.authenticate()
        self.sheet_manager.get_or_create_spreadsheet()
        self.sheet_manager.share_spreadsheet(self.email)

        worksheet_name = f"project_{datetime.now().strftime('%Y-%m-%d')}"
        worksheet = self.sheet_manager.get_or_create_worksheet(worksheet_name)

        self.sheet_manager.update_data(worksheet, self.data_processor.data_frame)
        self.sheet_manager.apply_formatting(worksheet, "A1:K1")
        column_widths = {"A": 150, "B": 400, "C": 400, "D": 600, "E": 600, "F": 150, "G": 150, "H": 100, "I": 100, "J": 100, "K": 100,}
        self.sheet_manager.set_column_widths(worksheet, column_widths)


# Main execution
if __name__ == "__main__":
    # Get the latest file
    exe_path = os.path.dirname(sys.argv[0])  # Get the path of the executable
    folder_path = os.path.join(exe_path, "..", "extract") 
    csv_file_path = get_latest_file(folder_path)
    if not csv_file_path:
        print("CSV file is required.")
        sys.exit(1)

    credentials_file = get_credential_file()
    if not credentials_file:
        sys.exit(1)

    spreadsheet_name = "project_report"
    user_email = get_email()

    report = WeeklyProjectReport(csv_file_path, credentials_file, spreadsheet_name, user_email)
    report.generate_report()
