"""
One-off diagnostic: ask Drive where files are actually landing.
Prints the configured root folder's metadata and its top-level children.
"""
import json
import yaml
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

with open("config.yaml", "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

creds = Credentials.from_authorized_user_file(
    cfg["drive"]["oauth_token_cache"],
    ["https://www.googleapis.com/auth/drive"],
)
svc = build("drive", "v3", credentials=creds, cache_discovery=False)

rid = cfg["drive"]["root_folder_id"]
print(f"Configured root_folder_id: {rid}\n")

info = svc.files().get(
    fileId=rid,
    fields="id,name,trashed,parents,webViewLink,owners(emailAddress)",
).execute()
print("ROOT FOLDER METADATA:")
print(json.dumps(info, indent=2))
print()

kids = svc.files().list(
    q=f"'{rid}' in parents and trashed=false",
    fields="files(id,name,mimeType)",
    pageSize=100,
    orderBy="name",
).execute().get("files", [])
print(f"DIRECT CHILDREN OF ROOT ({len(kids)}):")
for k in kids:
    mt = k["mimeType"].split(".")[-1]
    print(f"  - [{mt:<10}] {k['name']}")

# Also: who am I authenticated as?
about = svc.about().get(fields="user(emailAddress,displayName)").execute()
print(f"\nAUTHENTICATED AS: {about['user']['emailAddress']}")
