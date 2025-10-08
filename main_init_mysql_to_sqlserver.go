package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

// Fungsi menentukan tanggal_shift seperti di Python
func getShiftDate(t time.Time) *time.Time {
	h := t.Hour()
	if h >= 7 && h <= 18 {
		d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 1, 0, t.Location())
		return &d
	} else if h >= 19 && h <= 23 {
		d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 2, 0, t.Location())
		return &d
	} else {
		prev := t.AddDate(0, 0, -1)
		d := time.Date(prev.Year(), prev.Month(), prev.Day(), 0, 0, 2, 0, t.Location())
		return &d
	}
}

func normalizeValue(v interface{}) interface{} {
	switch val := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(val)
	default:
		return val
	}
}

func main() {
	fmt.Println("=== 🚀 Initial Sync MySQL → SQL Server (Bulk Mode) ===")

	startTime := time.Now()
	fmt.Printf("🕒 Mulai pada: %s\n", startTime.Format("2006-01-02 15:04:05"))

	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env: %v", err)
	}

	// --- Konfigurasi ---
	mysqlHost := os.Getenv("MYSQL_HOST")
	mysqlUser := os.Getenv("MYSQL_USER")
	mysqlPass := os.Getenv("MYSQL_PASS")
	mysqlDB := os.Getenv("MYSQL_DB")

	sqlsrvHost := os.Getenv("SQLSERVER_HOST")
	sqlsrvDB := os.Getenv("SQLSERVER_DB")
	sqlsrvUser := os.Getenv("SQLSERVER_USER")
	sqlsrvPass := os.Getenv("SQLSERVER_PASS")

	mysqlTable := os.Getenv("MYSQL_TABLE")
	sqlsrvTable := os.Getenv("SQLSRV_TABLE")
	wbTag := os.Getenv("WB_TAG")
	if wbTag == "" {
		wbTag = "DEFAULT_WB"
	}

	// --- Koneksi database ---
	mysqlDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", mysqlUser, mysqlPass, mysqlHost, mysqlDB)
	mysqlConn, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatalf("Koneksi MySQL gagal: %v", err)
	}
	defer mysqlConn.Close()

	sqlsrvDSN := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", sqlsrvUser, sqlsrvPass, sqlsrvHost, sqlsrvDB)
	sqlsrvConn, err := sql.Open("sqlserver", sqlsrvDSN)
	if err != nil {
		log.Fatalf("Koneksi SQL Server gagal: %v", err)
	}
	defer sqlsrvConn.Close()

	fmt.Println("=== [Initial Sync] Pemeriksaan awal... ===")

	// --- Cek tabel SQL Server kosong ---
	var count int
	err = sqlsrvConn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", sqlsrvTable)).Scan(&count)
	if err != nil {
		log.Fatalf("Gagal menghitung data SQL Server: %v", err)
	}
	if count > 0 {
		fmt.Printf("❌ Inisialisasi dibatalkan: Tabel %s sudah berisi %d data.\n", sqlsrvTable, count)
		return
	}

	// --- Ambil semua data MySQL ---
	rows, err := mysqlConn.Query(fmt.Sprintf("SELECT * FROM %s", mysqlTable))
	if err != nil {
		log.Fatalf("Gagal ambil data MySQL: %v", err)
	}
	defer rows.Close()

	mysqlCols, _ := rows.Columns()
	allCols := append(mysqlCols, "TANGGAL_SHIFT", "DATE_SYNC", "WB_TAG", "DELETED")

	fmt.Printf("Menyalin data dari %s → %s menggunakan Bulk Insert...\n", mysqlTable, sqlsrvTable)

	tx, err := sqlsrvConn.Begin()
	if err != nil {
		log.Fatalf("Gagal mulai transaksi SQL Server: %v", err)
	}

	stmt, err := tx.Prepare(mssql.CopyIn(sqlsrvTable, mssql.BulkOptions{}, allCols...))
	if err != nil {
		log.Fatalf("Gagal siapkan bulk insert: %v", err)
	}

	totalInserted := 0
	mysqlVals := make([]interface{}, len(mysqlCols))
	mysqlPtrs := make([]interface{}, len(mysqlCols))
	for i := range mysqlVals {
		mysqlPtrs[i] = &mysqlVals[i]
	}

	for rows.Next() {
		if err := rows.Scan(mysqlPtrs...); err != nil {
			log.Printf("⚠️ Gagal membaca row: %v", err)
			continue
		}

		insertVals := make([]interface{}, 0, len(allCols))
		for i := range mysqlVals {
			insertVals = append(insertVals, normalizeValue(mysqlVals[i]))
		}

		// Hitung tanggal_shift
		var tanggal2 time.Time
		for i, col := range mysqlCols {
			if col == "TANGGAL2" {
				if val, ok := mysqlVals[i].(time.Time); ok {
					tanggal2 = val
				}
				break
			}
		}

		var tanggalShift *time.Time
		if !tanggal2.IsZero() {
			tanggalShift = getShiftDate(tanggal2)
		}

		insertVals = append(insertVals, tanggalShift)
		insertVals = append(insertVals, time.Now())
		insertVals = append(insertVals, wbTag)
		insertVals = append(insertVals, 0)

		_, err = stmt.Exec(insertVals...)
		if err != nil {
			log.Printf("⚠️ Gagal bulk insert row: %v", err)
			continue
		}

		totalInserted++
		if totalInserted%10000 == 0 {
			fmt.Printf("Progress: %d baris...\n", totalInserted)
		}
	}

	// Tutup Bulk Insert
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("Gagal eksekusi akhir bulk insert: %v", err)
	}
	if err := stmt.Close(); err != nil {
		log.Fatalf("Gagal menutup bulk statement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatalf("Gagal commit bulk insert: %v", err)
	}

	endTime := time.Now()
	fmt.Printf("✅ Selesai. Total %d baris disalin.\n", totalInserted)
	fmt.Printf("🕔 Selesai pada: %s\n", endTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("⏱️ Durasi total: %s\n", endTime.Sub(startTime).Round(time.Second))
	fmt.Println("=== Proses selesai ===")
}
