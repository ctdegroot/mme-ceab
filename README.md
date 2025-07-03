# MME-CEAB: A Tool for Managing CEAB Data for the Mechanical and Materials Engineering Program at Western University

**MME-CEAB** is a command-line tool and Python package for managing, analyzing, and visualizing accreditation data for engineering programs under the Canadian Engineering Accreditation Board (CEAB) framework. This tool was developed to support academic departments in organizing complex outcomes-based assessment data and generating reports for accreditation review.

---

## 🚀 Features

- Ingest Excel-based course and program data into a local SQLite database
- Query, summarize, and plot data
- Plot aggregate program-level indicators over time
- Generate reports and feedback forms for instructors

---

## ⚙️ Installation

```bash
git clone https://github.com/ctdegroot/mme-ceab.git
cd mme-ceab
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt

---

## 🛠 Usage

Scripts for ingesting data, generating reports, and creating plots are located in the `scripts/` directory. Core logic is implemented in the `ceab/` package.

### 1. Ingest Excel files into the database

```
add example usage here
```

### 2. Generate instructor narrative reports

```
add example usage here
```

This produces instructor-specific `.xlsx` reports using a pre-defined Excel template.

### 3. Plot aggregate scores

```
add example usage here
```

This creates plots summarizing student scores for a particular academic year, organized by attribute and indicator.

---

## 📦 Dependencies

- Python 3.9+
- `pandas`
- `sqlalchemy`
- `openpyxl`
- `matplotlib`
- `jinja2`

All dependencies are listed in `requirements.txt`.

---

## 👩‍🏫 Intended Audience

This tool is designed for engineering department faculty and staff responsible for:
- Managing CEAB accreditation data
- Generating assessment reports
- Performing data-informed curriculum review

---

## 🤝 Contributing

Contributions are welcome! To get started:

1. **Fork** this repository.
2. Create a new **branch** for your feature or fix:
   ```
   git checkout -b feature-name
   ```
3. **Commit** your changes and push to your fork:
   ```
   git push origin feature-name
   ```
4. **Open a pull request** with a clear description of your changes.

Please follow these conventions when contributing:
- Use **PEP8** for code style.
- Use the **[NumPy docstring format](https://numpydoc.readthedocs.io/en/latest/format.html)** for all functions and classes to ensure consistency and readability.
- Write clear and concise commit messages.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and distribute this software with attribution.

---

## ✍️ Acknowledgements

Developed by [Christopher DeGroot](https://github.com/ctdegroot) and collaborators in the Department of Mechanical & Materials Engineering at Western University.