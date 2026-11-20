package edu.bigdata

/** One anonymized student's measured semester grade on a 0 to 100 scale. */
final case class Student(
    studentId: String,
    name: String,
    department: String,
    semester: Int,
    marks: Double
) extends Serializable
