# EWB Portfolio Monitor — Setup Guide

## Files Included
| File | Purpose |
|---|---|
| `main.py` | The entire Streamlit dashboard app |
| `database.py` | Database connection (pymysql, no ODBC needed) |
| `queries.py` | All SQL queries |
| `requirements.txt` | Python packages to install |
| `.env.example` | Template for your DB credentials |

---

## Step-by-Step Setup in VS Code

### Step 1 — Extract the ZIP
1. Download and extract the ZIP file
2. You should have a folder called `ewb_dashboard_v2` with all the files inside

### Step 2 — Open in VS Code
1. Open VS Code
2. Click **File → Open Folder**
3. Select the `ewb_dashboard_v2` folder
4. Click **Select Folder**

### Step 3 — Open the Terminal
Press `` Ctrl + ` `` (backtick key, top-left of keyboard)
Or go to **Terminal → New Terminal** from the top menu

### Step 4 — Create a Virtual Environment
Type this in the terminal and press Enter:
```
python -m venv .venv
```
Then activate it:
```
.venv\Scripts\activate
```
You should see `(.venv)` at the start of the line.

If you get a scripts error, run this first:
```
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```
Then try the activate command again.

### Step 5 — Install All Required Packages
```
pip install -r requirements.txt
```
Wait for everything to install. This installs:
- streamlit, pymysql, pandas, plotly
- python-dotenv, sqlalchemy, xlwt, openpyxl

### Step 6 — Set Up Your Database Credentials
1. In VS Code file explorer (left sidebar), find `.env.example`
2. Right-click it → **Rename** → change name to `.env`
3. Open `.env` and fill in your details:
```
DB_HOST=192.168.15.197
DB_PORT=3306
DB_NAME=bcrm
DB_USER=pgsantos
DB_PASSWORD="$P7q24DutdARe"
```
Note: If your password has special characters like $ wrap it in double quotes.

### Step 7 — Select the Python Interpreter
1. Press `Ctrl + Shift + P`
2. Type: `Python: Select Interpreter`
3. Choose the one that shows `.venv` (looks like `.\.venv\Scripts\python.exe`)

### Step 8 — Run the Dashboard
```
streamlit run main.py
```
A browser tab will open automatically at http://localhost:8501

---

## How to Stop and Restart
- Stop: Press `Ctrl + C` in the terminal
- Start again: `streamlit run main.py`

## How to Update (if you get a new version)
Just replace `main.py` and `queries.py` with the new files.
Keep your `.env` file — never replace it.

---

## Troubleshooting

| Error | Fix |
|---|---|
| `ModuleNotFoundError` | Run `pip install -r requirements.txt` again |
| `Database error` | Check your `.env` file credentials |
| `RangeError: Invalid interval` | Date filter issue — fixed in this version |
| `python not recognized` | Try `python3` instead of `python` |
| Scripts disabled error | Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser` |
