from flask import Flask, request, jsonify, send_file, render_template
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os, io, json, zipfile
import base64

from datetime import date




app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SHEET_ID = "1wrM7E9qGKcWJvN4mBwYMpkgp31jlxPGgEYCDsHn0bkc"


import base64

def get_google_creds():
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS_B64")
    if not creds_b64:
        raise ValueError("GOOGLE_CREDENTIALS_B64 not set.")
    creds_json = base64.b64decode(creds_b64).decode("utf-8")
    info = json.loads(creds_json)
    return Credentials.from_service_account_info(info, scopes=SCOPES)

# def get_google_creds():
#     """Load Google credentials from environment variable GOOGLE_CREDENTIALS_JSON."""
#     creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
#     if not creds_json:
#         raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable not set.")
#     info = json.loads(creds_json)
#     # Fix: Render stores \n as literal text, restore real newlines in the private key
#     info["private_key"] = info["private_key"].replace("\\n", "\n")
#     return Credentials.from_service_account_info(info, scopes=SCOPES)
# def get_google_creds():
#     """Load Google credentials from environment variable GOOGLE_CREDENTIALS_JSON."""
#     try:
#         creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")

#         if not creds_json:
#             print("❌ ENV ERROR: GOOGLE_CREDENTIALS_JSON not set")
#             raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable not set.")

#         print("✅ ENV FOUND: Length =", len(creds_json))

#         # Try parsing JSON
#         try:
#             info = json.loads(creds_json)
#             print("✅ JSON PARSED SUCCESSFULLY")
#         except Exception as e:
#             print("❌ JSON PARSE ERROR:", str(e))
#             raise

#         # Check required keys
#         required_keys = ["type", "project_id", "private_key", "client_email"]
#         for key in required_keys:
#             if key not in info:
#                 print(f"❌ MISSING KEY: {key}")
#                 raise ValueError(f"Missing key: {key}")

#         print("✅ REQUIRED KEYS PRESENT")

#         # Debug private key format (SAFE)
#         pk = info.get("private_key", "")
#         print("🔍 PRIVATE KEY CHECK:")
#         print("   Starts with BEGIN:", pk.startswith("-----BEGIN"))
#         print("   Ends with END:", pk.strip().endswith("-----END PRIVATE KEY-----"))
#         print("   Contains \\n:", "\\n" in pk)
#         print("   Contains real newline:", "\n" in pk)
#         print("   Length:", len(pk))

#         # Fix newline issue safely
#         if "\\n" in pk:
#             info["private_key"] = pk.replace("\\n", "\n")
#             print("✅ Replaced \\n with real newlines")
#         else:
#             print("ℹ️ No \\n replacement needed")

#         # Final credential creation test
#         try:
#             creds = Credentials.from_service_account_info(info, scopes=SCOPES)
#             print("✅ GOOGLE CREDS CREATED SUCCESSFULLY")
#             return creds
#         except Exception as e:
#             print("❌ GOOGLE CREDS CREATION FAILED:", str(e))
#             raise

#     except Exception as e:
#         print("🔥 FINAL ERROR in get_google_creds:", str(e))
#         raise


def get_flagged_customers():
    """Fetch OFFICE and POLICE customer lists from Google Sheet."""
    office_customers = set()
    police_customers = set()
    try:
        creds = get_google_creds()
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)

        # OFFICE tab
        try:
            office_ws = sh.worksheet("OFFICE")
            office_data = office_ws.get_all_values()
            for row in office_data[1:]:  # skip header
                if row and row[0].strip():
                    office_customers.add(row[0].strip().upper())
        except Exception as e:
            print(f"OFFICE tab error: {e}")

        # POLICE tab
        try:
            police_ws = sh.worksheet("POLICE")
            police_data = police_ws.get_all_values()
            for row in police_data[1:]:  # skip header
                if row and row[0].strip():
                    police_customers.add(row[0].strip().upper())
        except Exception as e:
            print(f"POLICE tab error: {e}")

    except Exception as e:
        print(f"Google Sheets error: {e}")

    return office_customers, police_customers


def parse_quickbooks(file):
    """Parse QuickBooks XLS export and return clean DataFrame."""
    df = pd.read_excel(file, engine="xlrd", header=None)

    # Find the actual header row (the one containing 'Customer')
    header_row = None
    for i, row in df.iterrows():
        if any(str(cell).strip() == "Customer" for cell in row):
            header_row = i
            break

    if header_row is None:
        raise ValueError("Could not find header row in the file.")

    df.columns = df.iloc[header_row]
    df = df.iloc[header_row + 1:].reset_index(drop=True)

    # Keep only rows that have a Customer value
    df = df[df["Customer"].notna() & (df["Customer"].astype(str).str.strip() != "")]

    # Parse Customer column: BRANCH:AGENT:SUBAGENT:CUSTOMER
    def parse_customer(val):
        parts = str(val).split(":")
        agent = parts[1].strip() if len(parts) > 1 else ""
        customer = parts[3].strip() if len(parts) > 3 else (parts[-1].strip() if parts else "")
        return agent, customer

    df[["Agent", "CustomerName"]] = df["Customer"].apply(
        lambda x: pd.Series(parse_customer(x))
    )

    # Clean Balance column
    df["Balance"] = pd.to_numeric(
        df["Balance"].astype(str).str.replace(",", "").str.strip(), errors="coerce"
    ).fillna(0)

    return df[["Agent", "CustomerName", "Balance"]]


def build_summary(df, office_customers, police_customers):
    """Group by Agent+Customer, sum Balance, add Status."""
    summary = (
        df.groupby(["Agent", "CustomerName"], as_index=False)["Balance"]
        .sum()
        .rename(columns={"Balance": "Total Debt"})
    )

    def get_status(name):
        n = name.strip().upper()
        if n in office_customers:
            return "Bike in Office"
        if n in police_customers:
            return "Bike at Police"
        return ""

    summary["Status"] = summary["CustomerName"].apply(get_status)
    summary["Date"] = date.today().strftime("%d %B %Y")
    summary = summary.sort_values("Total Debt", ascending=False)
    return summary


def build_comparison(df_morning, df_evening, office_customers, police_customers):
    """Build morning vs evening comparison per agent."""
    morning = (
        df_morning.groupby(["Agent", "CustomerName"], as_index=False)["Balance"]
        .sum()
        .rename(columns={"Balance": "Morning Amount"})
    )
    evening = (
        df_evening.groupby(["Agent", "CustomerName"], as_index=False)["Balance"]
        .sum()
        .rename(columns={"Balance": "Evening Amount"})
    )

    merged = morning.merge(evening, on=["Agent", "CustomerName"], how="left")

    def get_status(row):
        n = row["CustomerName"].strip().upper()
        if n in office_customers:
            return "Bike in Office"
        if n in police_customers:
            return "Bike at Police"
        if pd.isna(row.get("Evening Amount")):
            return "Paid"
        return ""

    merged["Status"] = merged.apply(get_status, axis=1)
    # Where customer is not in evening (paid), show 0 instead of blank
    merged["Evening Amount"] = merged["Evening Amount"].apply(
        lambda x: 0 if pd.isna(x) else x
    )
    merged["Date"] = date.today().strftime("%d %B %Y")
    merged = merged.sort_values("Morning Amount", ascending=False)
    return merged


def write_agent_excels(summary_df, columns, today_str):
    """Write one Excel per agent, return dict {agent_name: bytes}."""
    files = {}
    for agent, group in summary_df.groupby("Agent"):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            group[columns].to_excel(writer, index=False, sheet_name="Report")
            workbook = writer.book
            worksheet = writer.sheets["Report"]

            # Formatting
            header_fmt = workbook.add_format({
                "bold": True, "bg_color": "#1F4E79", "font_color": "white",
                "border": 1, "align": "center"
            })
            paid_fmt = workbook.add_format({"bg_color": "#C6EFCE", "font_color": "#276221"})
            office_fmt = workbook.add_format({"bg_color": "#FFEB9C", "font_color": "#9C5700"})
            police_fmt = workbook.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
            money_fmt = workbook.add_format({"num_format": "#,##0.00"})

            # Write header manually with formatting
            for col_num, col_name in enumerate(columns):
                worksheet.write(0, col_num, col_name, header_fmt)
                worksheet.set_column(col_num, col_num, 25)

            # Write data rows with conditional formatting
            for row_num, (_, row) in enumerate(group[columns].iterrows(), start=1):
                status = str(row.get("Status", ""))
                row_fmt = None
                if status == "Paid":
                    row_fmt = paid_fmt
                elif status == "Bike in Office":
                    row_fmt = office_fmt
                elif status == "Bike at Police":
                    row_fmt = police_fmt

                for col_num, col_name in enumerate(columns):
                    val = row[col_name]
                    if col_name in ("Total Debt", "Morning Amount") and val != "":
                        worksheet.write_number(row_num, col_num, float(val) if val != "" else 0, money_fmt)
                    elif col_name == "Evening Amount" and val != "":
                        try:
                            worksheet.write_number(row_num, col_num, float(val), money_fmt)
                        except Exception:
                            worksheet.write(row_num, col_num, val)
                    else:
                        if row_fmt:
                            worksheet.write(row_num, col_num, val, row_fmt)
                        else:
                            worksheet.write(row_num, col_num, val)

            # Add date header above table
            worksheet.write(0, 0, f"Date: {today_str}", header_fmt)

        files[agent] = output.getvalue()
    return files


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/generate-debt-reports", methods=["POST"])
def generate_debt_reports():
    """Function 1: Single QuickBooks export → per-agent debt reports."""
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file uploaded"}), 400

        file = request.files["file"]
        office_customers, police_customers = get_flagged_customers()
        df = parse_quickbooks(file)
        summary = build_summary(df, office_customers, police_customers)

        today_str = date.today().strftime("%d %B %Y")
        summary = summary.rename(columns={"CustomerName": "Customer Name"})
        columns = ["Date", "Agent", "Customer Name", "Total Debt", "Status"]

        files = write_agent_excels(summary, columns, today_str)

        agents = list(files.keys())

        for agent, data in files.items():
            safe_name = agent.replace("/", "-").replace("\\", "-")
            path = os.path.join(OUTPUT_FOLDER, f"{safe_name}_{today_str}.xlsx")
            with open(path, "wb") as f:
                f.write(data)

        return jsonify({"success": True, "agents": agents, "mode": "debt"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/generate-comparison-reports", methods=["POST"])
def generate_comparison_reports():
    """Function 2: Morning + Evening QuickBooks exports → comparison reports."""
    try:
        if "morning" not in request.files or "evening" not in request.files:
            return jsonify({"error": "Both morning and evening files required"}), 400

        morning_file = request.files["morning"]
        evening_file = request.files["evening"]

        office_customers, police_customers = get_flagged_customers()
        df_morning = parse_quickbooks(morning_file)
        df_evening = parse_quickbooks(evening_file)

        comparison = build_comparison(df_morning, df_evening, office_customers, police_customers)
        comparison = comparison.rename(columns={"CustomerName": "Customer Name"})

        today_str = date.today().strftime("%d %B %Y")
        columns = ["Date", "Agent", "Customer Name", "Morning Amount", "Evening Amount", "Status"]

        files = write_agent_excels(comparison, columns, today_str)

        agents = list(files.keys())
        for agent, data in files.items():
            safe_name = agent.replace("/", "-").replace("\\", "-")
            path = os.path.join(OUTPUT_FOLDER, f"COMPARISON_{safe_name}_{today_str}.xlsx")
            with open(path, "wb") as f:
                f.write(data)

        return jsonify({"success": True, "agents": agents, "mode": "comparison"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/download/<mode>/<agent_name>")
def download_agent_file(mode, agent_name):
    """Download a specific agent's file."""
    today_str = date.today().strftime("%d %B %Y")
    safe_name = agent_name.replace("/", "-").replace("\\", "-")

    if mode == "comparison":
        filename = f"COMPARISON_{safe_name}_{today_str}.xlsx"
    else:
        filename = f"{safe_name}_{today_str}.xlsx"

    path = os.path.join(OUTPUT_FOLDER, filename)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404

    return send_file(path, as_attachment=True, download_name=filename)


@app.route("/api/download-all/<mode>")
def download_all(mode):
    """Download all agent files as a ZIP."""
    today_str = date.today().strftime("%d %B %Y")
    prefix = "COMPARISON_" if mode == "comparison" else ""

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        for fname in os.listdir(OUTPUT_FOLDER):
            if fname.startswith(prefix) and fname.endswith(".xlsx") and today_str in fname:
                fpath = os.path.join(OUTPUT_FOLDER, fname)
                zf.write(fpath, fname)

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"all_reports_{today_str}.zip"
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)