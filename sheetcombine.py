import statsapi
import pandas as pd
import os
import time
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables (for secure API credentials)
load_dotenv()

# === Editable Inputs ===
season = 2025

# === Get User Input for Dates with Validation ===
def get_valid_date(prompt):
    while True:
        user_input = input(f"{prompt} (DD/MM/YYYY): ")
        try:
            return datetime.strptime(user_input, "%d/%m/%Y").strftime("%m/%d/%Y")
        except ValueError:
            print("❌ Invalid date format. Please use DD/MM/YYYY.")

start_date = get_valid_date("Enter the start date")
end_date = get_valid_date("Enter the end date")

# === Secure Google Sheets ID ===
sheet_id = os.getenv("SHEET_ID")

# === Function to get player ID from name (with caching & optimized API calls) ===
cache_player_ids = {}

def get_player_id(player_name, season):
    if player_name in cache_player_ids:
        return cache_player_ids[player_name]
    try:
        players = statsapi.get('sports_players', {'season': season, 'gameType': 'W'})['people']
        for player in players:
            if player['fullName'] == player_name:
                cache_player_ids[player_name] = player['id']
                return player['id']
    except Exception as e:
        print(f"⚠ Error getting player ID for {player_name}: {e}")
    return None

# === Function to get trimmed pitching stats (with retry mechanism) ===
def get_player_stats(player_id, season, retries=3):
    for attempt in range(retries):
        try:
            data = statsapi.player_stat_data(
                player_id,
                group="pitching",
                type="season",
                sportId=1,
                season=season
            )
            stats = data['stats'][0]['stats']
            relevant_keys = [
                'era', 'whip', 'strikeoutsPer9Inn', 'walksPer9Inn',
                'hitsPer9Inn', 'runsScoredPer9', 'homeRunsPer9',
                'inningsPitched', 'gamesStarted'
            ]
            return {key: stats.get(key, "") for key in relevant_keys}
        except Exception as e:
            print(f"⚠ Error getting stats for player ID {player_id} (attempt {attempt+1}): {e}")
            time.sleep(2)  # Wait before retrying
    return {key: '' for key in relevant_keys}  # Return empty if all retries fail

# === Function to fetch schedule from StatsAPI ===
def fetch_schedule(start_date, end_date):
    return statsapi.schedule(start_date=start_date, end_date=end_date)

# === Function to process each game & enrich with pitcher stats ===
def process_game_data(games):
    data = []
    for game in games:
        away_pitcher_name = game.get('away_probable_pitcher', '')
        home_pitcher_name = game.get('home_probable_pitcher', '')

        away_stats = {}
        home_stats = {}

        if away_pitcher_name:
            away_id = get_player_id(away_pitcher_name, season)
            if away_id:
                away_stats = get_player_stats(away_id, season)

        if home_pitcher_name:
            home_id = get_player_id(home_pitcher_name, season)
            if home_id:
                home_stats = get_player_stats(home_id, season)

        row = [
            game['game_id'],
            game['away_name'],
            game['home_name'],
            away_pitcher_name,
            home_pitcher_name
        ]

        # Add pitching stats
        for key in ['era', 'whip', 'strikeoutsPer9Inn', 'walksPer9Inn', 'hitsPer9Inn', 'runsScoredPer9', 'homeRunsPer9', 'inningsPitched', 'gamesStarted']:
            row.append(away_stats.get(key, ''))
        for key in ['era', 'whip', 'strikeoutsPer9Inn', 'walksPer9Inn', 'hitsPer9Inn', 'runsScoredPer9', 'homeRunsPer9', 'inningsPitched', 'gamesStarted']:
            row.append(home_stats.get(key, ''))

        data.append(row)
        time.sleep(0.5)  # Prevent rate limiting
    return data

# === Function to clear range A to W in Google Sheets ===
def clear_google_sheet_range(service, sheet_id):
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range='nrsi_confidence!A:W',
            body={}
        ).execute()
        print("🧹 Cleared range A:W in the Google Sheet.")
    except Exception as e:
        print(f"❌ Failed to clear sheet: {e}")

# === Function to Write Data to Google Sheets ===
def write_to_google_sheets(data):
    credentials = service_account.Credentials.from_service_account_file(
        os.getenv("GOOGLE_SHEET_CREDENTIALS"),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    service = build('sheets', 'v4', credentials=credentials)

    # Clear previous data in columns A to W
    clear_google_sheet_range(service, sheet_id)

    headers = [
        "Game ID", "Away Team", "Home Team", "Away Pitcher", "Home Pitcher",
        "Away ERA", "Away WHIP", "Away K/9", "Away BB/9", "Away H/9", "Away R/9", "Away HR/9", "Away IP", "Away GS",
        "Home ERA", "Home WHIP", "Home K/9", "Home BB/9", "Home H/9", "Home R/9", "Home HR/9", "Home IP", "Home GS"
    ]

    # Convert data to DataFrame
    df = pd.DataFrame(data, columns=headers)

    # Convert numeric columns to proper numbers
    numeric_columns = [
        "Away ERA", "Away WHIP", "Away K/9", "Away BB/9", "Away H/9", "Away R/9", "Away HR/9", "Away IP", "Away GS",
        "Home ERA", "Home WHIP", "Home K/9", "Home BB/9", "Home H/9", "Home R/9", "Home HR/9", "Home IP", "Home GS"
    ]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    # Replace NaN values with empty strings
    df.fillna("", inplace=True)

    # Convert DataFrame back to list format
    values = [df.columns.tolist()] + df.values.tolist()
    body = {'values': values}

    try:
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='nrsi_confidence!A1',
            valueInputOption="RAW",
            body=body
        ).execute()
        print("✅ Successfully updated Google Sheet with numeric values.")
    except Exception as e:
        print(f"❌ Failed to update Google Sheet: {e}")

# === MAIN EXECUTION ===
if __name__ == "__main__":
    games = fetch_schedule(start_date, end_date)
    enriched_data = process_game_data(games)
    write_to_google_sheets(enriched_data)
