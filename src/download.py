import requests, zipfile, io, os

BASE_URL = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

def download_dataset(start_year=2018, end_year=2025):
    os.makedirs("data/raw", exist_ok=True)
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            if y == 2025 and m > 1:
                break
            folder = f"data/raw/{y}_{m:02d}"
            # idempotency check to avoid redownloading if already exists (useful during development)
            if os.path.exists(folder) and len(os.listdir(folder)) > 0:
                print(f"Skipping {y}-{m:02d} (already exists)")
                continue
            print(f"Downloading {y}-{m:02d}")
            url = BASE_URL.format(year=y, month=m)
            try:
                r = requests.get(url, timeout=60)
                z = zipfile.ZipFile(io.BytesIO(r.content))
                os.makedirs(folder, exist_ok=True)
                z.extractall(folder)
            except Exception as e:
                print(f"Failed {y}-{m:02d}: {e}")


if __name__ == "__main__":
    download_dataset()