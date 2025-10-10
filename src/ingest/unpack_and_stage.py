import zipfile, os, shutil, pathlib

ZIP_PATH = "data/downloads/Data-Engineering-Workshop.zip"
TMP_EXTRACT = "data/downloads/extract"
SFTP_UPLOAD = "sftp-upload"            # maps to the SFTP container volume
EXT_LAKE_DIR = "externallake-staging"  # we'll push this into MinIO bucket

# Clean & prepare
for p in [TMP_EXTRACT, SFTP_UPLOAD, EXT_LAKE_DIR]:
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

# 1) Unzip fresh
if os.path.exists(TMP_EXTRACT):
    shutil.rmtree(TMP_EXTRACT)
os.makedirs(TMP_EXTRACT, exist_ok=True)

with zipfile.ZipFile(ZIP_PATH, 'r') as z:
    z.extractall(TMP_EXTRACT)

# 2) Find the files we need (from that repo):
# Expect: sd254_cards.csv   (CSV -> SFTP)
#         sd254_users.json  (JSON -> external lake)
# Adjust names if different.
cards_csv = None
users_json = None

for root, _, files in os.walk(TMP_EXTRACT):
    for f in files:
        if f.lower() == "sd254_cards.csv":
            cards_csv = os.path.join(root, f)
        if f.lower() == "sd254_users.json":
            users_json = os.path.join(root, f)

if not cards_csv or not users_json:
    raise FileNotFoundError("Could not find 'sd254_cards.csv' or 'sd254_users.json' in the zip.")

# 3) Stage CSV into SFTP drop
shutil.copy(cards_csv, os.path.join(SFTP_UPLOAD, "sd254_cards.csv"))

# 4) Stage JSON into a local folder; we'll upload to MinIO 'externallake' shortly
shutil.copy(users_json, os.path.join(EXT_LAKE_DIR, "sd254_users.json"))

print("[OK] Staged -> SFTP:", os.path.join(SFTP_UPLOAD, "sd254_cards.csv"))
print("[OK] Staged -> externallake-staging:", os.path.join(EXT_LAKE_DIR, "sd254_users.json"))
