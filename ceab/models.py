from sqlalchemy import Column, String, Float, ForeignKey, Integer, Date, CheckConstraint, PrimaryKeyConstraint
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Instructor(Base):
    __tablename__ = "instructors"
    instructorID = Column(String, primary_key=True)
    firstName = Column(String, nullable=False)
    lastName = Column(String, nullable=False)

    courses = relationship("Course", back_populates="instructor", cascade="all, delete-orphan")

class Course(Base):
    __tablename__ = "courses"
    courseID = Column(String, primary_key=True)
    instructorID = Column(String, ForeignKey("instructors.instructorID", ondelete="CASCADE"))
    prefix = Column(String, nullable=False)
    number = Column(Integer, nullable=False)
    suffix = Column(String, nullable=True)
    academicYear = Column(String, nullable=False)
    yearInProgram = Column(Integer, nullable=False)

    instructor = relationship("Instructor", back_populates="courses")
    measurements = relationship("Measurement", back_populates="course", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint(
            "prefix IN ('MME', 'ECE', 'ES', 'ELI', 'MSE')",
            name="check_valid_prefix"
        ),
        CheckConstraint(
            "number BETWEEN 1000 AND 9999",
            name="check_valid_number"
        ),
        CheckConstraint(
            "suffix IN ('A', 'B', 'F', 'none')", 
            name="check_valid_suffix"
        ),
        CheckConstraint(
            "length(academicYear) = 7 AND substr(academicYear, 5, 1) = '/'",
            name="check_academic_year_format"
        ),
        CheckConstraint(
            "yearInProgram BETWEEN 1 AND 4",
            name="check_valid_year_in_program"
        ),
    )

class Measurement(Base):
    __tablename__ = "measurements"
    measurementID = Column(String, primary_key=True)
    courseID = Column(String, ForeignKey("courses.courseID", ondelete="CASCADE"))
    attribute = Column(String, nullable=False)
    indicator = Column(Integer, nullable=False)
    deliverableType = Column(String, nullable=False)
    deliverableName = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    improvementTheme = Column(String)

    course = relationship("Course", back_populates="measurements")
    data = relationship("Data", back_populates="measurement", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint(
            "attribute IN ('KB', 'PA', 'I', 'DES', 'ITW', 'ET', 'CS', 'PR', 'IES', 'EE', 'EPM', 'LL')",
            name="check_valid_attribute"
        ),
        CheckConstraint(
            "indicator BETWEEN 1 AND 4",
            name="check_valid_indicator"
        ),
        CheckConstraint(
            "deliverableType IN ('Assignment', 'Final Exam', 'Lab', 'Midterm Exam', 'Presentation', 'Project', 'Quiz', 'Test')",
            name="check_valid_deliverable_type"
        ),
    )

class Data(Base):
    __tablename__ = "data"
    measurementID = Column(String, ForeignKey("measurements.measurementID", ondelete="CASCADE"))
    studentID = Column(String, nullable=False)
    score = Column(Integer)

    measurement = relationship("Measurement", back_populates="data")

    __table_args__ = (
        PrimaryKeyConstraint('measurementID', 'studentID'),
        CheckConstraint(
            "score BETWEEN 1 AND 4",
            name="check_valid_score"
        ),
    )