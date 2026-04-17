from flask import Flask, request, jsonify, send_file, render_template
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os, io, json, zipfile
import base64
import time

from datetime import date


app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SHEET_ID = "1wrM7E9qGKcWJvN4mBwYMpkgp31jlxPGgEYCDsHn0bkc"


def get_google_creds():
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS_B64")
    if not creds_b64:
        raise ValueError("GOOGLE_CREDENTIALS_B64 not set.")
    creds_json = base64.b64decode(creds_b64).decode("utf-8")
    info = json.loads(creds_json)
    return Credentials.from_service_account_info(info, scopes=SCOPES)


@app.route("/debug-creds")
def debug_creds():
    result = {}

    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS_B64")
    result["1_env_var_found"] = bool(creds_b64)
    result["1_env_var_length"] = len(creds_b64) if creds_b64 else 0

    if not creds_b64:
        return jsonify(result)

    try:
        creds_json = base64.b64decode(creds_b64).decode("utf-8")
        info = json.loads(creds_json)
        result["2_json_parsed"] = True
    except Exception as e:
        result["2_json_parsed"] = False
        result["2_json_error"] = str(e)
        return jsonify(result)

    required = ["type", "project_id", "private_key", "client_email"]
    for k in required:
        result[f"3_has_{k}"] = k in info

    pk = info.get("private_key", "")
    result["4_pk_length"] = len(pk)
    result["4_starts_with_BEGIN"] = pk.startswith("-----BEGIN")
    result["4_ends_with_END"] = pk.strip().endswith("-----END PRIVATE KEY-----")

    try:
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        result["5_creds_created"] = True
    except Exception as e:
        result["5_creds_created"] = False
        result["5_error"] = str(e)
        return jsonify(result)

    try:
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)
        sheets = [ws.title for ws in sh.worksheets()]
        result["6_sheets_connected"] = True
        result["6_sheet_tabs"] = sheets
    except Exception as e:
        result["6_sheets_connected"] = False
        result["6_error"] = str(e)

    return jsonify(result)


_flagged_cache = None
_flagged_cache_time = 0
CACHE_TTL = 300  # refresh every 5 minutes


def get_flagged_customers():
    global _flagged_cache, _flagged_cache_time
    if _flagged_cache and (time.time() - _flagged_cache_time) < CACHE_TTL:
        return _flagged_cache

    office_customers = set()
    police_customers = set()
    try:
        creds = get_google_creds()
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)
        try:
            office_ws = sh.worksheet("OFFICE")
            for row in office_ws.get_all_values()[1:]:
                if row and row[0].strip():
                    office_customers.add(row[0].strip().upper())
        except Exception as e:
            print(f"OFFICE tab error: {e}")
        try:
            police_ws = sh.worksheet("POLICE")
            for row in police_ws.get_all_values()[1:]:
                if row and row[0].strip():
                    police_customers.add(row[0].strip().upper())
        except Exception as e:
            print(f"POLICE tab error: {e}")
    except Exception as e:
        print(f"Google Sheets error: {e}")

    _flagged_cache = (office_customers, police_customers)
    _flagged_cache_time = time.time()
    return _flagged_cache


def parse_quickbooks(file):
    """Parse QuickBooks XLS export. Returns DataFrame with invoice details."""
    df = pd.read_excel(file, engine="xlrd", header=None)

    header_row = None
    for i, row in df.iterrows():
        if any(str(cell).strip() == "Customer" for cell in row):
            header_row = i
            break

    if header_row is None:
        raise ValueError("Could not find header row in the file.")

    df.columns = df.iloc[header_row]
    df = df.iloc[header_row + 1:].reset_index(drop=True)

    df = df[df["Customer"].notna() & (df["Customer"].astype(str).str.strip() != "")]

    def parse_customer(val):
        parts = str(val).split(":")
        agent = parts[1].strip() if len(parts) > 1 else ""
        customer = parts[3].strip() if len(parts) > 3 else (parts[-1].strip() if parts else "")
        return agent, customer

    df[["Agent", "CustomerName"]] = df["Customer"].apply(
        lambda x: pd.Series(parse_customer(x))
    )

    df["Balance"] = pd.to_numeric(
        df["Balance"].astype(str).str.replace(",", "").str.strip(), errors="coerce"
    ).fillna(0)

    # Invoice Date = column index 0, Invoice Number = column index 2
    col_names = df.columns.tolist()
    invoice_date_col = col_names[0]
    invoice_num_col  = col_names[2]

    df["InvoiceDate"]   = pd.to_datetime(df[invoice_date_col], errors="coerce")
    df["InvoiceNumber"] = df[invoice_num_col].astype(str).str.strip()

    return df[["Agent", "CustomerName", "Balance", "InvoiceNumber", "InvoiceDate"]]


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


def build_invoice_details(df):
    """
    Build invoice details DataFrame:
    - Customers sorted by total debt (highest first)
    - Within each customer, invoices sorted oldest → newest
    - Alternating row banding handled at write time
    """
    customer_totals = (
        df.groupby(["Agent", "CustomerName"])["Balance"]
        .sum()
        .reset_index()
        .rename(columns={"Balance": "CustomerTotal"})
    )

    df_detail = df.merge(customer_totals, on=["Agent", "CustomerName"])

    df_detail = df_detail.sort_values(
        ["Agent", "CustomerTotal", "CustomerName", "InvoiceDate"],
        ascending=[True, False, True, True]
    )

    df_detail = df_detail.rename(columns={
        "CustomerName":  "Customer Name",
        "InvoiceNumber": "Invoice Number",
        "InvoiceDate":   "Invoice Date",
        "Balance":       "Amount"
    })

    return df_detail[["Agent", "Customer Name", "Invoice Number", "Invoice Date", "Amount"]]


def build_comparison(df_morning, df_evening):
    """Build morning vs evening comparison per agent WITHOUT status."""
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
    merged["Evening Amount"] = merged["Evening Amount"].fillna(0)
    merged["Date"] = date.today().strftime("%d %B %Y")
    merged = merged.sort_values("Morning Amount", ascending=False)

    return merged


def write_agent_excels(summary_df, columns, today_str, invoice_df=None):
    """Write one Excel per agent, return dict {agent_name: bytes}."""
    files = {}
    for agent, group in summary_df.groupby("Agent"):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            group[columns].to_excel(writer, index=False, sheet_name="Report")
            workbook = writer.book
            worksheet = writer.sheets["Report"]

            # ── Shared formats ────────────────────────────────────────
            header_fmt = workbook.add_format({
                "bold": True, "bg_color": "#1F4E79", "font_color": "white",
                "border": 1, "align": "center"
            })
            paid_fmt   = workbook.add_format({"bg_color": "#C6EFCE", "font_color": "#276221"})
            office_fmt = workbook.add_format({"bg_color": "#FFEB9C", "font_color": "#9C5700"})
            police_fmt = workbook.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
            money_fmt  = workbook.add_format({"num_format": "#,##0.00"})
            date_fmt   = workbook.add_format({"num_format": "dd/mm/yyyy"})
            band_fmt   = workbook.add_format({"bg_color": "#EBF3FB"})

            # Write header row with formatting
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

            # ── Comparison summary rows (only when Morning/Evening columns exist) ──
            if "Morning Amount" in columns and "Evening Amount" in columns:
                morning_fmt = workbook.add_format({
                    "bold": True, "bg_color": "#2563EB", "font_color": "white",
                    "num_format": "#,##0.00", "border": 1,
                })
                evening_fmt = workbook.add_format({
                    "bold": True, "bg_color": "#7C3AED", "font_color": "white",
                    "num_format": "#,##0.00", "border": 1,
                })
                collected_fmt = workbook.add_format({
                    "bold": True, "bg_color": "#059669", "font_color": "white",
                    "num_format": "#,##0.00", "border": 1,
                })
                debt_fmt = workbook.add_format({
                    "bold": True, "bg_color": "#DC2626", "font_color": "white",
                    "num_format": "#,##0.00", "border": 1,
                })

                morning_total = group["Morning Amount"].sum()
                evening_total = group["Evening Amount"].sum()
                collected = morning_total - evening_total

                summary_start = len(group) + 3  # leave a blank gap

                worksheet.write(summary_start,     2, "Morning Arrear",  morning_fmt)
                worksheet.write_number(summary_start,     3, morning_total,  morning_fmt)

                worksheet.write(summary_start + 1, 2, "Evening Arrear",  evening_fmt)
                worksheet.write_number(summary_start + 1, 3, evening_total,  evening_fmt)

                worksheet.write(summary_start + 2, 2, "Total Collected", collected_fmt)
                worksheet.write_number(summary_start + 2, 3, collected,      collected_fmt)

                worksheet.write(summary_start + 3, 2, "Total Debted",    debt_fmt)
                worksheet.write_number(summary_start + 3, 3, evening_total,  debt_fmt)

            # ── TAB 2: Invoice Details (debt reports only) ────────────
            if invoice_df is not None:
                agent_invoices = invoice_df[invoice_df["Agent"] == agent].copy()
                detail_cols = ["Customer Name", "Invoice Number", "Invoice Date", "Amount"]

                agent_invoices[detail_cols].to_excel(
                    writer, index=False, sheet_name="Invoice Details"
                )
                ws2 = writer.sheets["Invoice Details"]

                # Header row
                for col_num, col_name in enumerate(detail_cols):
                    ws2.write(0, col_num, col_name, header_fmt)
                    ws2.set_column(col_num, col_num, 30)

                # Data rows with alternating band per customer
                current_customer = None
                band = False

                for row_num, (_, row) in enumerate(agent_invoices[detail_cols].iterrows(), start=1):
                    cust = row["Customer Name"]
                    if cust != current_customer:
                        current_customer = cust
                        band = not band

                    for col_num, col_name in enumerate(detail_cols):
                        val = row[col_name]
                        if col_name == "Amount":
                            ws2.write_number(
                                row_num, col_num,
                                float(val) if pd.notna(val) else 0,
                                money_fmt
                            )
                        elif col_name == "Invoice Date":
                            if pd.notna(val):
                                ws2.write_datetime(
                                    row_num, col_num,
                                    val.to_pydatetime(),
                                    date_fmt
                                )
                            else:
                                ws2.write(row_num, col_num, "")
                        else:
                            fmt = band_fmt if band else None
                            if fmt:
                                ws2.write(row_num, col_num, str(val) if pd.notna(val) else "", fmt)
                            else:
                                ws2.write(row_num, col_num, str(val) if pd.notna(val) else "")

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

        # Build invoice details for Tab 2
        invoice_details = build_invoice_details(df)

        files = write_agent_excels(summary, columns, today_str, invoice_df=invoice_details)

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

        df_morning = parse_quickbooks(morning_file)
        df_evening = parse_quickbooks(evening_file)

        comparison = build_comparison(df_morning, df_evening)
        comparison = comparison.rename(columns={"CustomerName": "Customer Name"})

        today_str = date.today().strftime("%d %B %Y")
        columns = ["Date", "Agent", "Customer Name", "Morning Amount", "Evening Amount"]

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
        download_name=f"all_reports_{today_str}.zip",
    )


if __name__ == "__main__":
    app.run(debug=True, host="1.0.0.0", port=5000)