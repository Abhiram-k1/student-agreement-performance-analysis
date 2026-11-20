"""Download UCI dataset 697 and adapt measured grades to the required Student schema."""
import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen
from zipfile import BadZipFile, ZipFile

ROOT = Path(__file__).resolve().parents[1]
URL = "https://archive.ics.uci.edu/static/public/697/predict+students+dropout+and+academic+success.zip"
COLUMNS = ["StudentID", "Name", "Department", "Semester", "Marks"]


def adapt(source: bytes, semester: int):
    records = csv.DictReader(io.StringIO(source.decode("utf-8-sig")), delimiter=";")
    grade_column = f"Curricular units {semester}st sem (grade)" if semester == 1 else "Curricular units 2nd sem (grade)"
    evaluations_column = f"Curricular units {semester}st sem (evaluations)" if semester == 1 else "Curricular units 2nd sem (evaluations)"
    required = {"Course", grade_column, evaluations_column}
    if not required.issubset(records.fieldnames or []):
        raise ValueError(f"Unexpected UCI schema, missing {sorted(required - set(records.fieldnames or []))}")
    students = []
    omitted = 0
    source_count = 0
    for row_number, row in enumerate(records, start=1):
        source_count += 1
        grade = float(row[grade_column])
        evaluations = int(row[evaluations_column])
        if evaluations == 0:
            omitted += 1
            continue
        if not 0 <= grade <= 20:
            raise ValueError(f"Grade outside [0,20] at source record {row_number}")
        students.append({
            "StudentID": f"UCI697-{row_number:05d}",
            "Name": f"Anonymous student {row_number:05d}",
            "Department": f"Course_{row['Course']}",
            "Semester": semester,
            "Marks": format(grade * 5.0, ".10g"),
        })
    if not students:
        raise ValueError("No evaluated semester records remain")
    return students, source_count, omitted


def write_csv(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(records)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--semester", type=int, choices=(1, 2), default=1)
    parser.add_argument("--source", type=Path, help="Use an already downloaded original UCI data.csv")
    parser.add_argument("--retrieval-url", help="Record the actual public mirror URL when using --source")
    parser.add_argument("--sample-size", type=int, default=100)
    args = parser.parse_args()
    if args.sample_size <= 0:
        parser.error("--sample-size must be positive")
    raw_path = ROOT / "data/raw/uci_students.csv"
    try:
        if args.source:
            source = args.source.read_bytes()
        else:
            with urlopen(URL, timeout=60) as response:
                archive = response.read()
            with ZipFile(io.BytesIO(archive)) as zipped:
                matches = [name for name in zipped.namelist() if name.rsplit("/", 1)[-1] == "data.csv"]
                if len(matches) != 1:
                    raise ValueError("UCI archive did not contain exactly one data.csv")
                source = zipped.read(matches[0])
        records, source_count, omitted = adapt(source, args.semester)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(source)
        processed_path = ROOT / "data/processed/03_Dataset.csv"
        sample_path = ROOT / "data/sample/03_Dataset.csv"
        write_csv(processed_path, records)
        write_csv(sample_path, records[:args.sample_size])
        metadata = {
            "source_url": URL,
            "retrieval_url": args.retrieval_url or (URL if not args.source else None),
            "license": "CC BY 4.0",
            "source_sha256": hashlib.sha256(source).hexdigest(),
            "semester": args.semester,
            "source_records": source_count,
            "processed_records": len(records),
            "omitted_no_evaluations": omitted,
            "sample_records": min(args.sample_size, len(records)),
            "marks_transformation": "source semester grade multiplied by 5",
            "department_transformation": "Course_ followed by the source Course code",
            "identity_transformation": "deterministic anonymous source row labels",
        }
        (ROOT / "data/processed/provenance.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        print(f"Prepared {len(records)} records from {source_count} source students. Omitted {omitted} students without evaluations.")
        print(f"Dataset saved to {processed_path}")
    except (OSError, URLError, BadZipFile, ValueError, KeyError) as error:
        parser.exit(1, f"Dataset acquisition failed: {error}\nDownload the UCI archive manually and rerun with --source path/to/data.csv.\n")


if __name__ == "__main__":
    main()
