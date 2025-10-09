import os
from dotenv import load_dotenv
import requests
import threading
import time
import datetime
import mysql.connector
import pyodbc

load_dotenv()

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
MYSQL_TABLE = os.getenv("MYSQL_TABLE")
SQLSRV_TABLE = os.getenv("SQLSRV_TABLE")
WB_TAG = os.getenv("WB_TAG", "DEFAULT_WB")
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", 10))

def send_heartbeat(pc_name):
    while True:
        try:
            requests.post("http://127.0.0.1:5000/api/heartbeat", json={"pc_name": pc_name}, timeout=3)
        except Exception as e:
            print("⚠️ Gagal kirim heartbeat:", e)
        time.sleep(10)
        
# === Fungsi bantu ===
def get_shift_date(dt):
    """Mengembalikan tanggal shift berdasarkan jam kerja."""
    if not dt:
        return None
    try:
        trim_date = dt.date()
        trim_time = dt.time()
        if datetime.time(7, 0, 0) <= trim_time <= datetime.time(18, 59, 0):
            return datetime.datetime.combine(trim_date, datetime.time(0, 0, 1))
        elif datetime.time(19, 0, 0) <= trim_time <= datetime.time(23, 59, 59):
            return datetime.datetime.combine(trim_date, datetime.time(0, 0, 2))
        else:
            return datetime.datetime.combine(trim_date - datetime.timedelta(days=1), datetime.time(0, 0, 2))
    except Exception as e:
        print("Error get_shift_date:", e)
        return None


def sync_data():
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

    try:
        mysql_cur.execute(f"SELECT NOURUT1, AKSI, PLANT_ID FROM {MYSQL_LOG} WHERE STATUS = 'PENDING' ORDER BY log_time")
        logs = mysql_cur.fetchall()
        if not logs:
            print("Tidak ada log baru di", MYSQL_LOG)
            return

        print(f"Menemukan {len(logs)} log; memproses...")

        for entry in logs:
            NOURUT1 = entry.get('NOURUT1')
            PLANT_ID = entry.get('PLANT_ID')
            aksi = (entry.get('AKSI') or 'UPDATE').upper()

            try:                                
                mysql_cur.execute(
                    f"SELECT * FROM {MYSQL_LOG} WHERE NOURUT1 = %s AND PLANT_ID = %s order by COUNTER_DONE desc LIMIT 1",
                    (NOURUT1, PLANT_ID)
                )
                row_counter_done = mysql_cur.fetchone()
                row_counter_done_old = row_counter_done['COUNTER_DONE']                                                 
                row_counter_done_update = row_counter_done_old + 1
                
                print("row_counter_done_old: ", row_counter_done_old)
                print("row_counter_done_update: ", row_counter_done_update)
                                
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
                        
                        mysql_cur.execute(
                            f"UPDATE {MYSQL_LOG} SET STATUS = 'FAILED', MESSAGE = %s WHERE NOURUT1 = %s AND PLANT_ID = %s",
                            (error_notfound_mysql, NOURUT1, PLANT_ID)
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
                        skip_fields = ('NOURUT1', 'PLANT_ID', 'DELETED', 'DATE_SYNC', 'WB_TAG')
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
                            print(f"UPDATE parsial ({len(changed_cols)} kolom): {NOURUT1}-{PLANT_ID}")
                        else:
                            print(f"Tidak ada perubahan untuk {NOURUT1}-{PLANT_ID}.")
                        
                        # hapus log
                        # mysql_cur.execute(
                        #     f"DELETE FROM {MYSQL_LOG} WHERE NOURUT1 = %s AND PLANT_ID = %s",
                        #     (NOURUT1, PLANT_ID)
                        # )
                        # mysql_conn.commit()
                        
                        mysql_cur.execute(
                            f"UPDATE {MYSQL_LOG} SET STATUS = 'SUCCESS', MESSAGE = 'Data updated successfully', COUNTER_DONE = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND AKSI = 'UPDATE' AND COUNTER_DONE = %s",
                            (row_counter_done_update, NOURUT1, PLANT_ID, 0)
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
                            # mysql_cur.execute(
                            #     f"DELETE FROM {MYSQL_LOG} WHERE NOURUT1 = %s AND PLANT_ID = %s",
                            #     (NOURUT1, PLANT_ID)
                            # )
                            # mysql_conn.commit()
                            
                            mysql_cur.execute(
                                f"UPDATE {MYSQL_LOG} SET STATUS = 'SUCCESS', MESSAGE = 'Data inserted successfully', COUNTER_DONE = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND AKSI = 'INSERT' AND COUNTER_DONE = %s",
                                (row_counter_done_update, NOURUT1, PLANT_ID, 0)
                            )
                            mysql_conn.commit()
                            
                        except Exception as e:
                            print(f"Gagal INSERT {NOURUT1}-{PLANT_ID}: {e}")
                            continue

                elif aksi == 'DELETE':
                    try:
                        sqlsrv_cur.execute(
                            f"UPDATE {SQLSRV_TABLE} SET deleted = 1 WHERE NOURUT1 = ? AND PLANT_ID = ?",
                            (NOURUT1, PLANT_ID)
                        )
                        sqlsrv_conn.commit()
                        # mysql_cur.execute(
                        #     f"DELETE FROM {MYSQL_LOG} WHERE NOURUT1 = %s AND PLANT_ID = %s",
                        #     (NOURUT1, PLANT_ID)
                        # )
                        # mysql_conn.commit()
                        
                        print("#DELETE row_counter_done_update: ", row_counter_done_update)
                        print("#DELETE row_counter_done_old: ", row_counter_done_old)
                        mysql_cur.execute(
                            f"UPDATE {MYSQL_LOG} SET STATUS = 'SUCCESS', MESSAGE = 'Data deleted successfully', COUNTER_DONE = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND AKSI = 'DELETE' AND COUNTER_DONE = %s",
                            (row_counter_done_update, NOURUT1, PLANT_ID, 0)
                        )
                        mysql_conn.commit()
                        
                        print(f"DELETE flag sukses: {NOURUT1}-{PLANT_ID}")
                    except Exception as e:
                        print(f"Gagal update deleted flag {NOURUT1}-{PLANT_ID}: {e}")
                        continue

                else:
                    print(f"Aksi tidak dikenal ({aksi}) -> hapus log")
                    mysql_cur.execute(
                        f"DELETE FROM {MYSQL_LOG} WHERE NOURUT1 = %s AND PLANT_ID = %s",
                        (NOURUT1, PLANT_ID)
                    )
                    mysql_conn.commit()

            except Exception as e:
                error_message = f"ERROR processing {NOURUT1}-{PLANT_ID}: {e}"
                print(error_message)
                mysql_cur.execute(
                    f"UPDATE {MYSQL_LOG} SET MESSAGE = %s WHERE NOURUT1 = %s AND PLANT_ID = %s AND COUNTER_DONE = %s",
                    (error_message, NOURUT1, PLANT_ID, 0)
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


# === Main loop ===
if __name__ == "__main__":
    while True:
        try:
            PC_NAME = os.getenv("PC_NAME")
            threading.Thread(target=send_heartbeat, args=(PC_NAME,), daemon=True).start()
            sync_data()
        except Exception as e:
            print("Terjadi error utama:", e)
        time.sleep(SYNC_INTERVAL)
