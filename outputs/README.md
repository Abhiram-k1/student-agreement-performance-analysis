# Generated outputs

Run `sbt "run"` from the project root after preparing the dataset. Each execution writes a new timestamped directory under `outputs/runs/`.

The run contains complete/filtered/highest-score CSV tables, department counts and averages, partition sizes, summary, lineage, metadata and actual Spark event logs. The README explains each file. Read `summary.md` with the documented course-code and grade-scale interpretations.

Run directories are ignored by Git so repeated experiments do not clutter the source repository. You can deliberately add selected evidence when you prepare your submission. Generated results are created only from an executed input dataset.
