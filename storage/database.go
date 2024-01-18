package storage

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/sec-data-pipeline/db-init/request"
)

type Database struct {
	DB *sql.DB
}

type DBConnParams struct {
	DBHost string
	DBPort string
	DBName string
	DBUser string
	DBPass string
	SSL    string
}

func New(
	connParams *DBConnParams,
) (*Database, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s dbname=%s password=%s sslmode=%s",
		connParams.DBHost,
		connParams.DBPort,
		connParams.DBUser,
		connParams.DBName,
		connParams.DBPass,
		connParams.SSL,
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return &Database{DB: db}, nil
}

func (s *Database) createCompanyTable() error {
	stmt := `CREATE TABLE company (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    cik VARCHAR(10) UNIQUE NOT NULL
  );`
	_, err := s.DB.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (s *Database) createTickerTable() error {
	stmt := `CREATE TABLE ticker (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES company(id) ON DELETE CASCADE,
    value VARCHAR(20) UNIQUE NOT NULL,
    exchange VARCHAR(50)
  );`
	_, err := s.DB.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (s *Database) createFilingTable() error {
	stmt := `CREATE TABLE filing (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES company(id) ON DELETE CASCADE,
    sec_id VARCHAR(50) UNIQUE NOT NULL,
		form VARCHAR(20) NOT NULL,
		original_file VARCHAR(200) NOT NULL,
		filing_date TIMESTAMP DEFAULT NULL,
		report_date TIMESTAMP DEFAULT NULL,
		acceptance_date TIMESTAMP DEFAULT NULL,
		last_modified_date TIMESTAMP DEFAULT NULL
  );`
	_, err := s.DB.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (db *Database) CreateTables() error {
	err := db.createCompanyTable()
	if err != nil {
		return err
	}
	err = db.createTickerTable()
	if err != nil {
		return err
	}
	err = db.createFilingTable()
	if err != nil {
		return err
	}
	return nil
}

func (s *Database) InsertCompany(company request.Company) error {
	if company.Tickers == nil ||
		company.Exchanges == nil ||
		len(company.Tickers) != len(company.Exchanges) {
		return errors.New("Corrupted data")
	}
	stmt := `INSERT INTO company (name, cik) VALUES ($1, $2) RETURNING id;`
	row := s.DB.QueryRow(stmt, company.Name, company.CIK)
	var id int
	if err := row.Scan(&id); err != nil {
		return err
	}
	stmt = `INSERT INTO ticker (company_id, value, exchange) VALUES ($1, $2, $3);`
	for i, v := range company.Tickers {
		_, err := s.DB.Exec(stmt, id, v, company.Exchanges[i])
		if err != nil {
			return err
		}
	}
	return nil
}
