import csv, os, datetime

n = 0
if not os.path.exists("hitech.csv"):
    with open("hitech.csv", "w") as hf:
        writer = csv.writer(hf)
        writer.writerow(
            (
                "OGRN",
                "Date added"
            )
        )
        hf.close()

def update_hitech(ogrn):
    with open("hitech.csv", "a") as file:
        writer = csv.writer(file)
        writer.writerow(
            (
                ogrn,
                datetime.datetime.now()
            )
        )

if not os.path.exists("restructured_data.csv"):
    print("File doesnt exist, run parser first")
    exit()

else:
    with open("restructured_data.csv", "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if row['hitechComplex'] == 'True':
                update_hitech(row['OGRN'])
                n += 1
            else:
                pass

print(f"Process finished, added: {n} companies")
