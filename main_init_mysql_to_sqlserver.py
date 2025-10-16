import os
from dotenv import load_dotenv
import time
import datetime
import mysql.connector
import pyodbc

load_dotenv()

MYSQL_CONN = {
    'host': os.getenv("MYSQL_HOST"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASS"),
    'database': os.getenv("MYSQL_DB")
}

SQLSERVER_CONN = {
    'server': os.getenv("SQLSERVER_HOST"),
    'database': os.getenv("SQLSERVER_DB"),
    'username': os.getenv("SQLSERVER_USER"),
    'password': os.getenv("SQLSERVER_PASS")
}

MYSQL_TABLE = os.getenv("MYSQL_TABLE")
SQLSRV_TABLE = os.getenv("SQLSRV_TABLE")
WB_TAG = os.getenv("WB_TAG")

def get_shift_date(dt):
    if not dt:
        return None
    try:
        trim_date = dt.date()
        trim_time = dt.time()
        if datetime.time(7, 0, 0) <= trim_time <= datetime.time(18, 59, 59):
            return datetime.datetime.combine(trim_date, datetime.time(0, 0, 1))
        elif datetime.time(19, 0, 0) <= trim_time <= datetime.time(23, 59, 59):
            return datetime.datetime.combine(trim_date, datetime.time(0, 0, 2))
        else:
            return datetime.datetime.combine(trim_date - datetime.timedelta(days=1), datetime.time(0, 0, 2))
    except Exception as e:
        print("Error get_shift_date:", e)
        return None

def initial_sync():
    print("=== [Initial Sync] Pemeriksaan awal... ===")

    sqlsrv_conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQLSERVER_CONN['server']};"
        f"DATABASE={SQLSERVER_CONN['database']};"
        f"UID={SQLSERVER_CONN['username']};"
        f"PWD={SQLSERVER_CONN['password']}",
        autocommit=False
    )
    sqlsrv_cur = sqlsrv_conn.cursor()
    sqlsrv_cur.fast_executemany = True  # 🚀 aktifkan mode cepat

    mysql_conn = mysql.connector.connect(**MYSQL_CONN)
    mysql_cur = mysql_conn.cursor(dictionary=True)

    try:
        sqlsrv_cur.execute(f"SELECT COUNT(*) FROM {SQLSRV_TABLE} WHERE wb_tag = ?", (WB_TAG,))
        count_sqlsrv = sqlsrv_cur.fetchone()[0]

        if count_sqlsrv > 0:
            print(f"❌ Data dengan WB_TAG={WB_TAG} sudah ada di tabel {SQLSRV_TABLE} ({count_sqlsrv} baris).")
            return

        mysql_cur.execute(f"SELECT * FROM {MYSQL_TABLE}")
        rows = mysql_cur.fetchall()
        total_rows = len(rows)
        if total_rows == 0:
            print("⚠️ Tidak ada data untuk disalin.")
            return

        print(f"Menyalin {total_rows} baris dari MySQL ke SQL Server...")
        
        batch_data = []
        for row in rows:
            tanggal_shift = get_shift_date(row.get("TANGGAL2"))
            row["tanggal_shift"] = tanggal_shift
            row["date_sync"] = datetime.datetime.now()
            row["wb_tag"] = WB_TAG
            row["deleted"] = 0
            batch_data.append(row)

        col_names = list(batch_data[0].keys())
        col_list_sql = ", ".join(f"[{c}]" for c in col_names)
        placeholders = ", ".join("?" for _ in col_names)
        insert_sql = f"INSERT INTO {SQLSRV_TABLE} ({col_list_sql}) VALUES ({placeholders})"

        params_list = [[row[c] for c in col_names] for row in batch_data]
        sqlsrv_cur.executemany(insert_sql, params_list)
        sqlsrv_conn.commit()

        print(f"✅ Selesai! Total {total_rows} baris disalin ke SQL Server.")
    except Exception as e:
        print(f"❌ Error fatal: {e}")
        sqlsrv_conn.rollback()
    finally:
        mysql_cur.close()
        mysql_conn.close()
        sqlsrv_cur.close()
        sqlsrv_conn.close()

if __name__ == "__main__":
    try:
        start = time.time()
        start_time = datetime.datetime.now()

        print("=== Program Initial Sync MySQL → SQL Server ===")
        print(f"🕒 Mulai pada: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        initial_sync()
        
        durasi = round(time.time() - start)
        selesai_pada = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"🕔 Selesai pada: {selesai_pada}")
        print(f"⏱️ Durasi total: {durasi}s")
    
    except Exception as e:
        print("\n‼️ Terjadi error:")
        print(e)

    finally:
        input("\nTekan Enter untuk keluar...")