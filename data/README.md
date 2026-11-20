# Dataset source and schema

The source is UCI dataset 697, **Predict Students' Dropout and Academic Success**, by Realinho, Vieira Martins, Machado, and Baptista (2021), DOI 10.24432/C5MC89. It contains records from a Portuguese higher education institution and measured academic performance at the end of the first and second semesters. UCI licenses it under CC BY 4.0, allowing redistribution and adaptation with attribution.

- Source page: https://archive.ics.uci.edu/dataset/697/predict+students+dropout+and+academic+success
- Original archive: https://archive.ics.uci.edu/static/public/697/predict+students+dropout+and+academic+success.zip
- License: https://creativecommons.org/licenses/by/4.0/

Run `python scripts/acquire_data.py` from the project root. The original `data.csv` extracted from the archive is saved unchanged as `data/raw/uci_students.csv`. The archive itself is not retained. The adapter saves the required format to `data/processed/03_Dataset.csv` and the first 100 eligible records to `data/sample/03_Dataset.csv`. The latter is a genuine source-derived test subset.

The packaged source CSV was acquired from the public repository mirror at https://raw.githubusercontent.com/gilljaskaran/mlops-student-dropout-pipeline/main/data/raw/data.csv with Git blob `367fe76c9292c9c02ae5c823e883926c075f33f2`. It contains 4,424 source records. The first-semester adaptation retains 4,075 evaluated records and omits 349 records without evaluations. The original UCI page remains the dataset citation and the downloader's primary source. The exact local source checksum is in `data/processed/provenance.json`. When manually supplying another public mirror through `--source`, use `--retrieval-url` to record its actual acquisition URL.

| Output column | Type | Provenance and interpretation |
| --- | --- | --- |
| `StudentID` | string | Stable `UCI697-` plus original row number. This is a project identifier for an anonymous source record. |
| `Name` | string | `Anonymous student` plus original row number. It is an anonymized display label. |
| `Department` | string | `Course_` plus the source `Course` code. It groups students by course as the available proxy for department. |
| `Semester` | integer | 1 by default, or 2 when selected with `--semester 2`. |
| `Marks` | double | Source `Curricular units 1st sem (grade)` or `Curricular units 2nd sem (grade)` multiplied by 5. This expresses the measured grade on a 0–100 scale. |

The source is anonymous and provides course codes. Its schema supports a transparent adaptation to the assignment's entity attributes. IDs/names are row labels, and course groups are the documented interpretation of Department. The adapted CSV is an educational analysis input derived from the original source.

Rows with zero in the selected semester's `evaluations` field are omitted because there is no evaluated semester record. A measured zero grade with positive evaluations is retained. This decision prevents averaging unavailable grades into academic performance. `provenance.json` records actual omissions and counts. It includes a SHA-256 of the exact original source for traceability.

The CSV uses UTF-8, a header, and comma separators. StudentID/Semester pairs must be unique. The loader rejects missing fields, empty text, nonpositive semesters, nonnumeric values and marks outside 0–100.

If downloading fails, download the source archive manually, extract its `data.csv`, and run `python scripts/acquire_data.py --source path/to/data.csv`. Expected original filename inside the archive is `data.csv`. Expected adapted project path is `data/processed/03_Dataset.csv`.

For the Google Drive dataset field, upload the adapted CSV and identify it as the derived assignment dataset. Keep the original UCI link in the original-source field. Verify the file is below 10 MB as required by the form.
