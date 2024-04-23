import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import datetime as dt
from datetime import timezone

##########################################################
# Global Variable Declaration
##########################################################
calendly_api_url = "https://api.calendly.com"
endpoint = "/scheduled_events"
url = calendly_api_url + endpoint
##########################################################
# Global function Declaration
##########################################################
def get_event(event_type,headers):
    response = requests.get(event_type, headers=headers)
    return response.json()["resource"]["scheduling_url"]
def list_of_access_token():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(os.path.join(os.getcwd(), "google-credentials.json"), scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open('Calendly')
    worksheet = spreadsheet.worksheet('access_token')
    data = worksheet.get_all_records()
    return pd.DataFrame(data)
def save_to_google_sheet(data):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(os.path.join(os.getcwd(), "google-credentials.json"), scope)
        client = gspread.authorize(credentials)
        spreadsheet = client.open('Calendly')
        worksheet = spreadsheet.worksheet("scheduled_events")
        existing_data = worksheet.get_all_records()
        df_existing = pd.DataFrame(existing_data)
        df_combined = pd.concat([df_existing, data], ignore_index=True, keys=['', ''])
        worksheet.update([df_combined.columns.values.tolist()] + df_combined.values.tolist())

    except Exception as e:
        print(f'Error saving to Google Sheet: {e}')
def fetch_scheduled_events(payload,headers,Event):
    while True:
        all_scheduled_events = []
        next_continue = True
        response = requests.get(url, headers=headers, params=payload)
        if response.status_code == 200:
            scheduled_events = response.json().get("collection", [])
            for index, item in pd.DataFrame(scheduled_events).iterrows():
                Created = pd.to_datetime(item['created_at'])
                data = {
                    "Account Name": item['name'],
                    "Date of Event Created": Created.strftime('%Y-%m-%d %H:%M:%S'),
                    "Email": item['event_memberships'][0]['user_email']
                }
                
                last_12_hours = dt.datetime.now(timezone.utc) + dt.timedelta(hours=-12)
                if dt.datetime.now(timezone.utc) >= Created.replace(tzinfo=timezone.utc) >= last_12_hours:
                    if get_event(item['event_type'],headers) == Event:
                        all_scheduled_events.append(data)
                else:
                    next_continue = False
                    break
            if all_scheduled_events:
                df = pd.DataFrame(all_scheduled_events)
                save_to_google_sheet(df)
            if next_continue:
                next_page = response.json().get("pagination", {}).get("next_page")
                if next_page:
                    payload["page"] = next_page
                else:
                    break
            else:
                break
        else:
            print(f"Error: {response.status_code}, {response.text}")
            break
##########################################################
########################## MAIN ##########################
##########################################################
if __name__ == "__main__":
    try:
        print(f"-------------Application Started At: {dt.datetime.now()}-------------")
        app_exec_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Get List of access Token
        access_token_list = list_of_access_token()
        print(f"Access Token Count: {len(access_token_list.index)}")

        for index,token in access_token_list.iterrows():
            # Update Calendly data
            payload = {
                "organization": f"https://api.calendly.com/organizations/{token['Organizations Id']}",
                "sort": "start_time:desc",
            }
            headers = {
                "Authorization": f"Bearer {token['Access Token']}",
                "Content-Type": "application/json",
                }
            fetch_scheduled_events(payload,headers,token['Event Link'])
            print(f"Data has been Updated for user : {token['Name']}")
    except Exception as e:
        raise Exception(e)
    finally:
        print(f"-------------Application Completed At: {dt.datetime.now()}-------------")