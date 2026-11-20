# Validation status

Validation commands are documented in the README. This file records only checks actually run while packaging.

The dataset adapter ran successfully on the genuine source CSV acquired through the documented public mirror. It produced 4,075 evaluated semester-one records from 4,424 source students and omitted 349 records without evaluations. The package includes the 100-record genuine sample.

`python -s -m unittest discover -s tests` passed one source-adaptation test under Python 3.12 on 20 November 2026. It verifies every retained grade/course grouping against the original source, source row identifiers, evaluation eligibility and retained/omitted counts.

Scala compilation, the Spark RDD main pipeline and distributed integration checks have not yet run in the packaging environment. Java 17 is available, but sbt/Spark dependencies are absent. An attempted Maven launcher download failed with a socket permission error (Windows error 10013). Python and Spark dependency installation was also unavailable. This file will be updated if the packaging environment later obtains the required dependencies.

Cluster deployment and live Spark UI screenshot collection require the user's runtime infrastructure and visual observation. Those actions have not been asserted as completed here.

## Publication preparation checks

On 20 November 2026, Python 3.12.14 parsed all 2 Python source and test files without syntax errors. `python -s -m unittest discover -s tests -v` passed 1 tests against the publication project.

The submitted `03_Dataset.csv` contains 4,075 records and is 240,589 bytes, below 10 MB. Its bytes match the standalone dataset prepared for Drive upload. The project input path and documented filenames were checked after renaming. Spark execution and its infrastructure checks retain the unverified status documented above.
