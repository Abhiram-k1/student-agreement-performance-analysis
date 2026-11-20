# Student Agreement Performance Analysis

Assignment 1 implements a Scala application that analyzes university academic records through Apache Spark RDDs. It defines a `Student` entity, creates a Scala collection of those objects, distributes it across RDD partitions, and calculates every requested result.

The university scenario asks for student performance across departments. The chosen public higher education dataset records measured semester grades and course codes. The adaptation uses course codes as department groupings and anonymous source-row labels as names and IDs. These interpretations are documented in `docs/requirements.md` and `data/README.md`.

## Technologies and prerequisites

- Java 17 JDK
- Scala 2.12.18
- Apache Spark Core 3.5.3
- sbt 1.10.2
- Apache Commons CSV 1.11.0 for input/output parsing
- Python 3.10 or newer for downloading and adapting the dataset
- Internet access for first-time dependency resolution and dataset acquisition

Spark performs the analyses through RDD transformations and actions. Python prepares the public source into the five required attributes.

## Project architecture

```text
UCI archive or original data.csv
    -> scripts/acquire_data.py
    -> data/processed/03_Dataset.csv + provenance.json + genuine sample
    -> StudentLoader creates a Vector[Student]
    -> SparkContext.parallelize creates a partitioned RDD
    -> lazy filter/map/reduceByKey chain and action evidence
    -> StudentAnalysis calculates required statistics
    -> CSV tables, summary, lineage, partition sizes and Spark event logs
```

```text
Assignment_1/
  build.sbt                 Scala/Spark dependency versions and Java options
  project/build.properties  sbt version
  requirements.txt          Python dependency note
  Dockerfile                Optional Linux container execution
  scripts/acquire_data.py   Download and source-preserving data adaptation
  src/main/scala/edu/bigdata/
    Student.scala           Required entity class
    StudentLoader.scala     CSV validation and entity construction
    StudentAnalysis.scala   RDD transformations and actions
    StudentAnalysisApp.scala End-to-end main entry point and output generation
  src/test/scala/edu/bigdata/StudentAnalysisChecks.scala
  tests/test_acquire_data.py
  data/                     Dataset, genuine sample, and provenance
  outputs/                  Run output guidance and generated results
  docs/                     Requirements, methodology and evidence instructions
  submission.md             Submission fields for you to complete
```

## Dataset

Use UCI **Predict Students' Dropout and Academic Success**, published by Realinho, Vieira Martins, Machado, and Baptista in 2021. The original source contains 4,424 student records from a Portuguese higher education institution and first/second semester academic performance.

The source is licensed under CC BY 4.0. The adaptation retains genuine measured grades. `Marks = semester grade × 5` converts the source 0–20 scale to 0–100. Students with no recorded evaluations in the selected semester are excluded. `provenance.json` records the actual source count, omissions, sample count, source SHA-256, and transformations. The genuine sample is the first 100 eligible source rows, suitable for functional testing. It has no random or invented observations.

Source page and download are available at the following links.

- [UCI dataset page](https://archive.ics.uci.edu/dataset/697/predict+students+dropout+and+academic+success)
- [Original source ZIP](https://archive.ics.uci.edu/static/public/697/predict+students+dropout+and+academic+success.zip)
- [Dataset DOI](https://doi.org/10.24432/C5MC89)
- [CC BY 4.0 license](https://creativecommons.org/licenses/by/4.0/)

The package includes the genuine original-source CSV obtained from a public UCI mirror, its first-semester adaptation with 4,075 evaluated records from 4,424 source students, and a 100-record genuine sample. Actual mirror acquisition details and the 349 no-evaluation omissions appear in `data/README.md` and `provenance.json`. You can execute the included dataset immediately after resolving Scala dependencies.

## Installation and environment setup

Extract the ZIP and open `Assignment_1` as the IDE project root. Install Java 17 and sbt. Check the tools with `java -version`, `sbt --version`, and `python --version`. Use `python3` where that is your Python executable name.

All following commands run from the `Assignment_1` root.

```bash
python scripts/acquire_data.py
sbt compile
```

The downloader writes `data/raw/uci_students.csv`, `data/processed/03_Dataset.csv`, `data/processed/provenance.json`, and `data/sample/03_Dataset.csv`. It uses Python's standard library, so `requirements.txt` has no installable packages.

To use second-semester grades, run the adapter with `--semester 2`. To prepare an original UCI CSV downloaded manually, use the following command.

```bash
python scripts/acquire_data.py --source path/to/data.csv --semester 1
```

The configured source grade/evaluation columns must exist. Consult `data/README.md` before supplying another dataset.

## Execute the pipeline

The main entry point is `edu.bigdata.StudentAnalysisApp`.

```bash
sbt "run"
```

Run the genuine sample with four partitions on two local worker threads.

```bash
sbt "run --input data/sample/03_Dataset.csv --master local[2] --partitions 4"
```

Use a custom output root or keep the Spark UI open for evidence collection.

```bash
sbt "run --input data/processed/03_Dataset.csv --output outputs/runs --master local[2] --partitions 4 --ui-wait-seconds 300"
```

Every option requires a value. `--input` selects the validated five-column CSV, `--output` selects the parent run directory, `--master` selects the Spark execution master, `--partitions` sets the input RDD partition count, and `--ui-wait-seconds` keeps the SparkContext alive after producing results.

The default local mode runs genuine Spark jobs on one machine. For a Spark standalone cluster, ensure Spark 3.5.3 and Scala 2.12 are compatible across the cluster, build the JAR and submit it with the Commons CSV dependency.

```bash
sbt package
spark-submit --class edu.bigdata.StudentAnalysisApp \
  --packages org.apache.commons:commons-csv:1.11.0 \
  --master spark://MASTER_HOST:7077 \
  target/scala-2.12/student-rdd-analysis_2.12-1.0.0.jar \
  --master spark://MASTER_HOST:7077 --input data/processed/03_Dataset.csv --partitions 8
```

The CSV is read on the driver to satisfy the required Scala-object collection step. Ensure the input path is available on the driver. Outputs are written on the driver. For this supplied small dataset, collecting all records supports the requested display. Very large collections and full record display require driver memory and should be reconsidered when extending the project.

## Optional Docker execution

The Docker route supplies a Linux Java/sbt environment. Docker must be installed and running, and the first build needs Internet access. The dataset should already be prepared, or use the packaged genuine dataset if present.

```bash
docker build -t assignment1 .
docker run --rm -p 4040:4040 -v "${PWD}/outputs:/project/outputs" assignment1 "run --ui-wait-seconds 300"
```

In PowerShell, `${PWD}` expands to the current project path. In a POSIX shell, `${PWD}` is also the current directory. Use the actual absolute project path if your shell formats it differently. The bind mount retains run outputs after the container exits.

## Pipeline behavior and expected outputs

The application loads and validates every Student attribute, creates all `Student` objects, and calls `parallelize`. It builds a `filter → map → reduceByKey` lineage before calling an action. A job-start listener records that building this chain starts no jobs and that `collect` starts jobs. The main analysis then displays every student record and calculates the requested results.

Each run creates `outputs/runs/<UTC timestamp>/` with these files.

| File | Content |
| --- | --- |
| `all_students.csv` | Every Student record also displayed in the console |
| `students_above_75.csv` | Students whose recorded marks are strictly greater than 75 |
| `department_counts.csv` | Unique student count in each department/course-code group |
| `department_average_marks.csv` | Additional group averages to interpret performance |
| `highest_marks_students.csv` | Every student tied for the highest mark |
| `lazy_high_scorer_counts.csv` | Output of the chained transformation demonstration |
| `partition_sizes.csv` | Actual records assigned to each input RDD partition |
| `summary.md` | Overall average, maximum, counts, execution context and lazy evidence |
| `rdd_lineage.txt` | Spark `toDebugString` dependency lineage |
| `run_metadata.json` | Actual Spark version, master and job-start observations |
| `event_logs/` | Actual Spark events for the History Server and DAG inspection |

Output values depend on the selected input and are calculated during execution. See `docs/spark_evidence.md` for observing the DAG and executor/task distribution. The use of timestamped run directories preserves earlier runs.

## Validation

```bash
python -m unittest discover -s tests
sbt "Test / runMain edu.bigdata.StudentAnalysisChecks"
```

The Python check verifies the adaptation against the genuine original source. The Scala checks compare RDD results with independent local calculations for counts, averages, threshold filtering, and all maximum ties. They also verify an empty RDD fails clearly. Supply another dataset path to the Scala checker after the class name if needed.

Read `docs/validation.md` for the validation actually completed while packaging. A command listed here describes how to run a check and does not itself assert that it has passed.

## Troubleshooting

- A missing CSV error names the expected path. Run the downloader or use `--input`.
- A UCI download error can be resolved by manually downloading and extracting `data.csv`, then running the adapter with `--source`.
- `sbt` requires network access during first dependency resolution. Check your proxy and Maven access if resolution fails.
- Use Java 17. Java module flags needed by Spark are included for forked sbt runs.
- Native Windows Spark can require compatible Hadoop native binaries. WSL2, Linux, or the supplied Dockerfile provide a Linux execution route when native Windows filesystem operations fail.
- Port 4040 is used by the first Spark UI. Spark may choose 4041 or another free port when 4040 is busy. Read the actual URL printed by the program.
- The Spark UI disappears when the context stops. Use `--ui-wait-seconds` or the History Server with saved event logs.
- A large driver-memory error can result from the assignment's required object collection and full display. Use the genuine sample while developing, or increase driver memory before processing much larger inputs.

## Submission

Use `submission.md` for the GitHub repository, source ZIP and dataset submission links. The dataset-upload field requires a file below 10 MB. The supplied adaptation and genuine sample are small. Check the chosen upload size before submission. The exact screenshot requirements and interpretation decisions appear in `docs/requirements.md`.
