# 🏦 ETL Pipeline World's Largest Banks
### *Automated Market Capitalisation Report in Multi-Currency*

> **Role:** Junior Data Engineer — Research Organisation  
> **Tools:** Python · BeautifulSoup · pandas · NumPy · SQLite3  
> **Data Source:** Wikipedia (via Internet Archive)

---

## What This Project Does

Every financial quarter, the organisation needs a fresh report of the **top 10 largest banks in the world by market capitalisation** expressed in four currencies so that offices in different countries can read it in their local currency.

This pipeline automates that entire process end-to-end:

```
Wikipedia (HTML)  ──→  extract()  ──→  transform()  ──→  load_to_csv()
                                                     ──→  load_to_db()
                                                     ──→  run_query()
                                                     ──→  code_log.txt
```

> All implementation details, function logic, and task breakdowns live inside the source file:  
> 👉 **[`banks_project.py`](./banks_project.py)**

---

## 📁 Project Structure

```
etl-bank-pipeline/
│
├── README.md                  ← Project overview & setup guide
├── ETL_Bank_Pipeline.py       ← Full ETL pipeline source code
├── exchange_rate.csv          ← Currency exchange rates (download before running)
├── requirements.txt           ← Python dependencies
├── .gitignore                 ← Files excluded from version control
│
└── outputs/  (generated on run — git ignored)
    ├── Largest_banks_data.csv ← Transformed data export
    ├── Banks.db               ← SQLite database
    └── code_log.txt           ← Timestamped execution log
```

---

## 🛠️ Setup & Installation

### 1. Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/etl-bank-pipeline.git
cd etl-bank-pipeline
```

### 2. Create a Virtual Environment *(Recommended)*
```bash
python3 -m venv venv
source venv/bin/activate        # Mac / Linux
venv\Scripts\activate           # Windows
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Download the Exchange Rate File
```bash
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
```

### 5. Run the Pipeline
```bash
python ETL_Bank_Pipeline.py
```

---

## 📤 Outputs Generated

| File | Description |
|------|-------------|
| `Largest_banks_data.csv` | Top 10 banks with MC in USD, GBP, EUR and INR |
| `Banks.db` | SQLite database with table `Largest_banks` |
| `code_log.txt` | Timestamped log of every pipeline stage |

---

## 🌍 Currency Coverage

The pipeline transforms market capitalisation from USD into three additional currencies using the exchange rates in `exchange_rate.csv`:

| Column | Currency | Target Office |
|--------|----------|--------------|
| `MC_USD_Billion` | US Dollar | Headquarters |
| `MC_GBP_Billion` | British Pound | London |
| `MC_EUR_Billion` | Euro | Berlin |
| `MC_INR_Billion` | Indian Rupee | New Delhi |

---

## 📦 Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `requests` | ≥ 2.26.0 | Fetch HTML from the data source URL |
| `beautifulsoup4` | ≥ 4.10.0 | Parse and scrape the HTML table |
| `pandas` | ≥ 1.3.0 | Data transformation and CSV/DB loading |
| `numpy` | ≥ 1.21.0 | Rounding exchange rate calculations |
| `lxml` | ≥ 4.6.0 | HTML parser backend for BeautifulSoup |

> `sqlite3` and `datetime` are part of the Python standard library — no installation needed.

---

## Author

**Sabelo Thandolwethu Mnikathi**  
IBM Data Science Professional Certificate · Python Project for Data Engineering

---

## License
This project is for educational purposes as part of the IBM Data Science Professional Certificate program.
