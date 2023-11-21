import os, csv, time, datetime, asyncio
import sys

import requests
from tqdm import tqdm



base_link = "https://opendata.trudvsem.ru/csv/company.csv"
api_link = f"https://trudvsem.ru/iblocks/prr_public_company_profile?companyId="

def donwload_file(url:str, filename:str):
    with open(filename, "wb") as file_for_download:
        with requests.get(url, stream=True) as r:

            r.raise_for_status()
            total = int(r.headers.get('content-lenght', 0))

            tqdm_params = {
                'desc': url,
                'total': total,
                'miniters': 1,
                'unit': 'bit',
                'unit_scale': True,
                'unit_divisor': 1024
            }

            with tqdm(**tqdm_params) as pb:
                for chunk in r.iter_content(chunk_size=8192):
                    pb.update(len(chunk))
                    file_for_download.write(chunk)

if not os.path.exists("data.csv"):
    donwload_file(base_link, "data.csv")

companies_set = set()
total_rows = 0
checked_companies = 0


async def collet_data(companyID, ogrn, retry=2):
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
    }
    req = requests.get(f"{api_link} + {str(companyID)}", headers=headers)
    hitechComplex = None

    try:
        response = req.json()
        hitechComplex = response['data']['hiTechComplex']

        if req.status_code != 200:
            time.sleep(3)
            if retry:
                await collet_data(companyID, ogrn, retry=(retry-1))
            else:
                pass

    except KeyError as ex:
        hitechComplex = f'KeyError {ex}'

    finally:
        await update_file(companyID, ogrn, req.status_code, hitechComplex)



async def update_file(companyID, ogrn, status_code, hitechComplex):
    with open("restructured_data.csv", "a") as add:
        writer = csv.writer(add)
        writer.writerow(
            (
                companyID,
                ogrn,
                datetime.datetime.now(),
                status_code,
                hitechComplex
            )
        )


if not os.path.exists("restructured_data.csv"):
    with open("restructured_data.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            ("Company_ID",
             "OGRN",
             "Date added",
             "Status code while checked",
             "hitechComplex")
        )

        f.close()

else:
    with open("restructured_data.csv", "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            companies_set.add(str(row['Company_ID']))

        f.close()


with open("data.csv", "r") as count_rows:
    reader = csv.reader(count_rows)
    total_rows = sum(1 for row in reader)

with open("data.csv", "r") as datafile:
    reader = csv.reader(datafile, delimiter="|")

    next(reader)

    with tqdm(
            total=total_rows,
            unit="rows"
              ) as pb:
        for row in reader:
            if str(row[0]) not in companies_set:
                asyncio.run(collet_data(row[0], row[9]))
            else:
                pass
            pb.update()

print(f"Process finished: checked {checked_companies} companies")
os.remove("data.csv")