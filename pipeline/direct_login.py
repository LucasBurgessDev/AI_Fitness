import os
import getpass
from dotenv import load_dotenv
from garminconnect import Garmin

load_dotenv()

TOKEN_DIR = os.getenv("GARMIN_TOKENSTORE", ".garminconnect")

def main():
    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")

    if not email:
        email = input("Enter Garmin email: ")
    else:
        print(f"Using email from .env: {email}")

    if not password:
        password = getpass.getpass("Enter Garmin password: ")
    else:
        print("Using password from .env")

    try:
        print(f"Attempting login for {email}...")
        api = Garmin(email, password)
        api.login(tokenstore=TOKEN_DIR)
        print(f"Login SUCCESS! Tokens saved to {TOKEN_DIR}")
        print(f"Display name: {getattr(api, 'display_name', '(unknown)')}")
    except Exception as e:
        print("--- LOGIN FAILED ---")
        print(e)

if __name__ == "__main__":
    main()
