package edu.bigdata

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import org.apache.commons.csv.CSVFormat
import scala.collection.JavaConverters._

object StudentLoader {
  val columns = Seq("StudentID", "Name", "Department", "Semester", "Marks")

  def load(path: Path): Vector[Student] = {
    require(Files.isRegularFile(path),
      s"Dataset missing at $path. Run python scripts/acquire_data.py first or supply --input.")
    val reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)
    val parser = CSVFormat.DEFAULT.builder()
      .setHeader().setSkipHeaderRecord(true).build().parse(reader)
    try {
      require(columns.forall(parser.getHeaderMap.containsKey),
        s"Expected CSV columns ${columns.mkString(", ")}. See data/README.md.")
      val students = parser.iterator().asScala.map { record =>
        try {
          val student = Student(
            record.get("StudentID").trim,
            record.get("Name").trim,
            record.get("Department").trim,
            record.get("Semester").trim.toInt,
            record.get("Marks").trim.toDouble
          )
          require(student.studentId.nonEmpty && student.name.nonEmpty && student.department.nonEmpty,
            "StudentID, Name and Department must be nonempty")
          require(student.semester >= 1, "Semester must be positive")
          require(!student.marks.isNaN && !student.marks.isInfinity &&
            student.marks >= 0.0 && student.marks <= 100.0, "Marks must be finite and within [0, 100]")
          student
        } catch {
          case error: IllegalArgumentException =>
            throw new IllegalArgumentException(s"Invalid CSV record ${record.getRecordNumber}: ${error.getMessage}", error)
        }
      }.toVector
      require(students.nonEmpty, "No student records found in the dataset")
      require(students.map(s => (s.studentId, s.semester)).distinct.size == students.size,
        "Duplicate StudentID/Semester pairs found")
      students
    } finally {
      parser.close()
      reader.close()
    }
  }
}
