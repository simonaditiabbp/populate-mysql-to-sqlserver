import os
from dotenv import load_dotenv
import logging
import sys
import re
import requests
import threading
import time
import datetime
import mysql.connector
import pyodbc
from mysql.connector.locales.eng import client_error

load_dotenv()

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = os.path.join(LOG_DIR, f"log_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt")

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Redirect semua print() ke logging ===
class LoggerWriter:
    def __init__(self, level):
        self.level = level
    def write(self, message):
        if message.strip():
            self.level(message.strip())
    def flush(self):
        pass

sys.stdout = LoggerWriter(logging.info)
sys.stderr = LoggerWriter(logging.error)

logging.info("=== Program started ===")

# === Konfigurasi koneksi ===
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

MYSQL_LOG = os.getenv("MYSQL_TABLE_LOG")
SQLSERVER_LOG = os.getenv("SQLSERVER_TABLE_LOG")

MYSQL_TABLE = os.getenv("MYSQL_TABLE")
SQLSRV_TABLE = os.getenv("SQLSERVER_TABLE")
WB_TAG = os.getenv("WB_TAG", "DEFAULT_WB")
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", 20))

PC_NAME = os.getenv("PC_NAME")

LAST_MAIN_ERROR_NORMALIZED = None
LAST_STATUS_LOG = None
LAST_SYNC_STATUS_LOG = None
LAST_HEARTBEAT_ERROR_NORMALIZED = None
LAST_LOGGED_SYNC = set()
LAST_LOGGED_ERROR_PROCESSING = set()

def normalize_error_exception_utama(err_text: str) -> str:
    err_text = re.sub(r'0x[0-9A-Fa-f]+', '0xADDR', err_text)
    err_text = re.sub(r'\s+', ' ', err_text.strip())
    return err_text

def normalize_error(err_text: str) -> str:
    err = err_text.lower()
    err = re.sub(r"0x[0-9a-f]+", "0xADDR", err)  # hilangkan alamat memori
    err = re.sub(r"\s+", " ", err)               # normalisasi spasi
    err = re.sub(r"httpsconnectionpool\(host='[^']*'\)", "httpsconnectionpool(host='X')", err)
    err = re.sub(r"timeout=[0-9]+", "timeout=X", err)
    return err.strip()

def normalize_error_already_sync(msg: str) -> str:
    msg = msg.strip().lower()
    msg = re.sub(r"0x[0-9a-f]+", "0xADDR", msg)   # hapus alamat memori
    msg = re.sub(r"\s+", " ", msg)
    return msg

def send_heartbeat(pc_name):
    heartbeat_ip = os.getenv("MONITORING_IP")
    heartbeat_url = f"{heartbeat_ip}/api/heartbeat"
    
    global LAST_HEARTBEAT_ERROR_NORMALIZED
    
    while True:
        try:
            requests.post(heartbeat_url, json={"pc_name": pc_name}, timeout=3)
            
            if LAST_HEARTBEAT_ERROR_NORMALIZED is not None:
                LAST_HEARTBEAT_ERROR_NORMALIZED = None
                
        except Exception as e:
            raw_err = str(e)
            norm_err = normalize_error(raw_err)

            if norm_err != LAST_HEARTBEAT_ERROR_NORMALIZED:
                print(f"Gagal kirim heartbeat: {raw_err}")
                LAST_HEARTBEAT_ERROR_NORMALIZED = norm_err
        time.sleep(20)
        
# === Fungsi bantu ===
def get_shift_date(dt):
    """Mengembalikan tanggal shift berdasarkan jam kerja."""
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


def sync_data_timbang():
    # --- koneksi ke SQL Server ---
    sqlsrv_conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQLSERVER_CONN['server']};"
        f"DATABASE={SQLSERVER_CONN['database']};"
        f"UID={SQLSERVER_CONN['username']};"
        f"PWD={SQLSERVER_CONN['password']}"
    )
    sqlsrv_cur = sqlsrv_conn.cursor()

    # --- koneksi ke MySQL ---
    mysql_conn = mysql.connector.connect(**MYSQL_CONN)
    mysql_cur = mysql_conn.cursor(dictionary=True)
    
    global LAST_STATUS_LOG

    try:
        mysql_cur.execute(f"SELECT NOURUT1, AKSI, PLANT_ID FROM {MYSQL_LOG} WHERE STATUS = 'PENDING' ORDER BY log_time")
        logs = mysql_cur.fetchall()
        if not logs:
            STATUS_LOG = "Tidak ada log baru di DB PC untuk diproses"
            if STATUS_LOG != LAST_STATUS_LOG:
                print(STATUS_LOG)
                LAST_STATUS_LOG = STATUS_LOG
            return

        print(f"Menemukan {len(logs)} log; memproses...")

        for entry in logs:
            NOURUT1 = entry.get('NOURUT1')
            PLANT_ID = entry.get('PLANT_ID')
            aksi = (entry.get('AKSI') or 'UPDATE').upper()
            
            try:
                MESSAGE_LOG_WANT_TO_CLEAN = entry.get('MESSAGE') or ""
                MESSAGE_LOG = re.sub(r"\s*\|\s*\[Error Populate Data\].*", "", MESSAGE_LOG_WANT_TO_CLEAN).strip()
                
                mysql_cur.execute(
                    f"SELECT * FROM {MYSQL_LOG} WHERE NOURUT1 = %s AND PLANT_ID = %s order by COUNTER_DONE desc LIMIT 1",
                    (NOURUT1, PLANT_ID)
                )
                row_counter_done = mysql_cur.fetchone()
                row_counter_done_old = row_counter_done['COUNTER_DONE']                                                 
                row_counter_done_update = row_counter_done_old + 1
                
                if aksi in ('INSERT', 'UPDATE'):                    
                    # ambil row dari MySQL
                    mysql_cur.execute(
                        f"SELECT * FROM {MYSQL_TABLE} WHERE NOURUT1 = %s AND PLANT_ID = %s",
                        (NOURUT1, PLANT_ID)
                    )
                    row = mysql_cur.fetchone()

                    if not row:
                        error_notfound_mysql = f"Baris {NOURUT1}-{PLANT_ID} tidak ditemukan di MySQL (skip)."
                        print(error_notfound_mysql)
                        
                        MESSAGE_LOG = error_notfound_mysql
                        
                        mysql_cur.execute(
                            f"UPDATE {MYSQL_LOG} SET STATUS = 'FAILED', MESSAGE = %s, PC_NAME = %s WHERE NOURUT1 = %s AND PLANT_ID = %s",
                            (MESSAGE_LOG, PC_NAME, NOURUT1, PLANT_ID)
                        )
                        mysql_conn.commit()
                        continue
                    
                    # Tambahkan kolom buatan
                    tanggal_shift = get_shift_date(row.get("TANGGAL2"))
                    row["TANGGAL_SHIFT"] = tanggal_shift
                    row["DATE_SYNC"] = datetime.datetime.now()
                    row["WB_TAG"] = WB_TAG
                    row["DELETED"] = 0

                    col_names = list(row.keys())

                    # cek apakah sudah ada di SQL Server
                    sqlsrv_cur.execute(
                        f"SELECT * FROM {SQLSRV_TABLE} WHERE NOURUT1 = ? AND PLANT_ID = ?",
                        (row['NOURUT1'], row['PLANT_ID'])
                    )
                    old_row = sqlsrv_cur.fetchone()
                    exists = old_row is not None

                    if exists:
                        columns = [col[0] for col in sqlsrv_cur.description]
                        old_dict = dict(zip(columns, old_row))
                                            

                        # cari kolom yang berubah (selain PK & manual field)
                        skip_fields = ('NOURUT1', 'PLANT_ID', 'DATE_SYNC')
                        changed_cols = [
                            c for c in col_names
                            if c not in skip_fields and str(row[c]) != str(old_dict.get(c))
                        ]
                        
                        if changed_cols:
                            set_clause = ", ".join(f"[{c}] = ?" for c in changed_cols)
                            params = [row[c] for c in changed_cols] + [row['NOURUT1'], row['PLANT_ID']]
                            update_sql = f"""
                                UPDATE {SQLSRV_TABLE}
                                SET {set_clause}, [DATE_SYNC] = ?
                                WHERE NOURUT1 = ? AND PLANT_ID = ?
                            """
                            params = [row[c] for c in changed_cols] + [row["DATE_SYNC"], row['NOURUT1'], row['PLANT_ID']]
                            sqlsrv_cur.execute(update_sql, params)
                            sqlsrv_conn.commit()
                            print(f"UPDATE parsial ({len(changed_cols)} kolom, {', '.join(changed_cols)}): {NOURUT1}-{PLANT_ID}")
                            
                            # cek deleted flag
                            aksi = 'UPDATE'
                            if old_dict.get('DELETED') != row.get('DELETED'):
                                aksi = 'INSERT'
                            
                            MESSAGE_LOG = "Data updated successfully"
                            mysql_cur.execute(
                                f"UPDATE {MYSQL_LOG} SET STATUS = 'SUCCESS', MESSAGE = '{MESSAGE_LOG}', COUNTER_DONE = %s, PC_NAME = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND AKSI = %s AND COUNTER_DONE = %s",
                                (row_counter_done_update, PC_NAME, NOURUT1, PLANT_ID, aksi, 0)
                            )
                            mysql_conn.commit()
                            LAST_STATUS_LOG = None
                            
                        else:
                            MESSAGE_LOG = f"Tidak ada perubahan untuk {NOURUT1}-{PLANT_ID}- WantCounterDone: {row_counter_done_update}."
                            normalized = normalize_error_already_sync(MESSAGE_LOG)
                            if normalized not in LAST_LOGGED_SYNC:
                                print(MESSAGE_LOG)
                                LAST_LOGGED_SYNC.add(normalized)
                                
                                mysql_cur.execute(
                                f"UPDATE {MYSQL_LOG} SET STATUS = 'FAILED', MESSAGE = CONCAT(COALESCE(MESSAGE, ''), ' | [Error Populate Data] : ', %s), COUNTER_DONE = %s, PC_NAME = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND AKSI = 'UPDATE' AND COUNTER_DONE = %s",
                                    (MESSAGE_LOG, row_counter_done_update, NOURUT1, PLANT_ID, 0)
                                )
                                mysql_conn.commit()                            
                                                
                    else:
                        # INSERT baru
                        col_list_sql = ", ".join(f"[{c}]" for c in col_names)
                        placeholders = ", ".join("?" for _ in col_names)
                        params = [row[c] for c in col_names]
                        insert_sql = f"INSERT INTO {SQLSRV_TABLE} ({col_list_sql}) VALUES ({placeholders})"

                        try:
                            sqlsrv_cur.execute(insert_sql, params)
                            sqlsrv_conn.commit()
                            print(f"INSERT sukses: {NOURUT1}-{PLANT_ID}")
                            
                            MESSAGE_LOG = "Data inserted successfully"
                            mysql_cur.execute(
                                f"UPDATE {MYSQL_LOG} SET STATUS = 'SUCCESS', MESSAGE = '{MESSAGE_LOG}', COUNTER_DONE = %s, PC_NAME = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND AKSI = 'INSERT' AND COUNTER_DONE = %s",
                                (row_counter_done_update, PC_NAME, NOURUT1, PLANT_ID, 0)
                            )
                            mysql_conn.commit()
                            LAST_STATUS_LOG = None
                            
                        except Exception as e:
                            MESSAGE_LOG = f"Gagal INSERT {NOURUT1}-{PLANT_ID}: {e}"
                            print(MESSAGE_LOG)
                            mysql_cur.execute(
                                f"UPDATE {MYSQL_LOG} SET STATUS = 'FAILED', MESSAGE = CONCAT(COALESCE(MESSAGE, ''), ' | [Error Populate Data] : ', {MESSAGE_LOG}), COUNTER_DONE = %s, PC_NAME = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND AKSI = 'INSERT' AND COUNTER_DONE = %s",
                                (row_counter_done_update, PC_NAME, NOURUT1, PLANT_ID, 0)
                            )
                            mysql_conn.commit()
                            continue

                elif aksi == 'DELETE':
                    try:
                        sqlsrv_cur.execute(
                            f"UPDATE {SQLSRV_TABLE} SET deleted = 1 WHERE NOURUT1 = ? AND PLANT_ID = ?",
                            (NOURUT1, PLANT_ID)
                        )
                        sqlsrv_conn.commit()
                                                
                        MESSAGE_LOG = "Data deleted successfully"
                        mysql_cur.execute(
                            f"UPDATE {MYSQL_LOG} SET STATUS = 'SUCCESS', MESSAGE = '{MESSAGE_LOG}', COUNTER_DONE = %s, PC_NAME = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND AKSI = 'DELETE' AND COUNTER_DONE = %s",
                            (row_counter_done_update, PC_NAME, NOURUT1, PLANT_ID, 0)
                        )
                        mysql_conn.commit()
                        LAST_STATUS_LOG = None
                        
                        print(f"DELETE flag sukses: {NOURUT1}-{PLANT_ID}")
                    except Exception as e:
                        MESSAGE_LOG = f"Gagal update deleted flag {NOURUT1}-{PLANT_ID}: {e}"
                        print(MESSAGE_LOG)
                        mysql_cur.execute(
                            f"UPDATE {MYSQL_LOG} SET STATUS = 'FAILED', MESSAGE = CONCAT(COALESCE(MESSAGE, ''), ' | [Error Populate Data] : ', {MESSAGE_LOG}), COUNTER_DONE = %s, PC_NAME = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND AKSI = 'DELETE' AND COUNTER_DONE = %s",
                            (row_counter_done_update, PC_NAME, NOURUT1, PLANT_ID, 0)
                        )
                        mysql_conn.commit()
                        continue

                else:
                    MESSAGE_LOG = f"Aksi tidak dikenal ({aksi})"
                    print(MESSAGE_LOG)
                    mysql_cur.execute(
                        f"UPDATE {MYSQL_LOG} SET STATUS = 'FAILED', MESSAGE = CONCAT(COALESCE(MESSAGE, ''), ' | [Error Populate Data] : ', {MESSAGE_LOG}), COUNTER_DONE = %s, PC_NAME = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND AKSI = '{aksi}' AND COUNTER_DONE = %s",
                        (row_counter_done_update, PC_NAME, NOURUT1, PLANT_ID, 0)
                    )
                    mysql_conn.commit()
                        
                    # mysql_cur.execute(
                    #     f"DELETE FROM {MYSQL_LOG} WHERE NOURUT1 = %s AND PLANT_ID = %s",
                    #     (NOURUT1, PLANT_ID)
                    # )
                    # mysql_conn.commit()

            except Exception as e:
                MESSAGE_LOG = f"ERROR processing {NOURUT1}-{PLANT_ID}: {e}"
                normalized = normalize_error_already_sync(MESSAGE_LOG)
                if normalized not in LAST_LOGGED_ERROR_PROCESSING:
                    print(MESSAGE_LOG)
                    LAST_LOGGED_ERROR_PROCESSING.add(normalized)
                    mysql_cur.execute(
                        f"UPDATE {MYSQL_LOG} SET MESSAGE = CONCAT(COALESCE(MESSAGE, ''), ' | [Error Populate Data] : ', %s), PC_NAME = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND COUNTER_DONE = %s",
                        (MESSAGE_LOG, PC_NAME, NOURUT1, PLANT_ID, 0)
                    )
                    mysql_conn.commit()
                
                continue

        print("=== Sinkronisasi selesai ===")

    finally:
        try:
            mysql_cur.close()
            mysql_conn.close()
        except:
            pass
        try:
            sqlsrv_cur.close()
            sqlsrv_conn.close()
        except:
            pass

def sync_data_timbang_log():
    global LAST_SYNC_STATUS_LOG
    
    try:
        mysql_conn = mysql.connector.connect(**MYSQL_CONN)
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        mysql_cursor.execute(f"""
            SELECT * FROM {MYSQL_LOG} 
            WHERE (SYNC_STATUS IS NULL OR SYNC_STATUS != 'SENT')
            ORDER BY LOG_TIME ASC
            LIMIT 100
        """)
        logs = mysql_cursor.fetchall()

        if not logs:
            SYNC_STATUS_LOG = "Tidak ada log baru untuk dikirim ke SQL Server"
            if SYNC_STATUS_LOG != LAST_SYNC_STATUS_LOG:
                print(SYNC_STATUS_LOG)
                LAST_SYNC_STATUS_LOG = SYNC_STATUS_LOG

            mysql_conn.close()
            return

        sqlsrv_cur = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQLSERVER_CONN['server']};"
            f"DATABASE={SQLSERVER_CONN['database']};"
            f"UID={SQLSERVER_CONN['username']};PWD={SQLSERVER_CONN['password']}"
        )
        sql_cursor = sqlsrv_cur.cursor()

        for log in logs:
            try:
                message = log.get("MESSAGE") or ""
                message_clean = re.sub(r"\s*\|\s*\[Error Sync to Log SqlServer\].*", "", message).strip()
        
                sql_cursor.execute(f"""
                    INSERT INTO {SQLSERVER_LOG} (NOURUT1, PLANT_ID, AKSI, COUNTER_DONE, PC_NAME, STATUS, MESSAGE, LOG_TIME)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    log.get("NOURUT1"),
                    log.get("PLANT_ID"),
                    log.get("AKSI"),
                    log.get("COUNTER_DONE"),
                    log.get("PC_NAME"),
                    log.get("STATUS"),
                    message_clean,
                    log.get("LOG_TIME"),
                ))

                # update status di mysql
                query = f"""
                    UPDATE {MYSQL_LOG} 
                    SET SYNC_STATUS='SENT' 
                    WHERE NOURUT1=%s AND LOG_TIME=%s
                """
                mysql_cursor.execute(query, (log["NOURUT1"], log["LOG_TIME"]))

            except Exception as e:
                print(f"[ERROR] Gagal kirim log: {e}")
                query = f"""
                    UPDATE {MYSQL_LOG}
                    SET SYNC_STATUS='PENDING',
                        MESSAGE = CONCAT(COALESCE(MESSAGE, ''), ' | [Error Sync to Log SqlServer] : ', %s)
                    WHERE NOURUT1=%s AND LOG_TIME=%s
                """
                mysql_cursor.execute(query, (str(e), log["NOURUT1"], log["LOG_TIME"]))
                continue

        sqlsrv_cur.commit()
        mysql_conn.commit()

        print(f"{len(logs)} log berhasil dikirim ke SQL Server")

        sqlsrv_cur.close()
        mysql_conn.close()
        
        LAST_SYNC_STATUS_LOG = None
        print("=== Sinkronisasi Log selesai ===")

    except Exception as e:
        print(f"[FATAL SYNC Data Timbang Log] {e}")


# === Main loop ===
if __name__ == "__main__":
    threading.Thread(target=send_heartbeat, args=(PC_NAME,), daemon=True).start()
    
    while True:
        try:
            sync_data_timbang()
            sync_data_timbang_log()
            
             # reset error jika sudah normal
            if LAST_MAIN_ERROR_NORMALIZED is not None:
                print("Koneksi kembali normal.")
                LAST_MAIN_ERROR_NORMALIZED = None
                
        except Exception as e:
            raw_err = str(e)
            norm_err = normalize_error_exception_utama(raw_err)

            # hanya cetak error baru
            if norm_err != LAST_MAIN_ERROR_NORMALIZED:
                print(f"Terjadi Error Utama: {raw_err}")
                LAST_MAIN_ERROR_NORMALIZED = norm_err
                
        time.sleep(SYNC_INTERVAL)
