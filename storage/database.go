package storage

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/sec-data-pipeline/db-init/request"
)

type Database interface {
	CreateTables() error
	InsertCompany(company *request.Company) error
	InsertHeader(header []string) error
}

type postgresDB struct {
	*sql.DB
}

type postgresParams struct {
	DBHost string `json:"DB_HOST"`
	DBPort string `json:"DB_PORT"`
	DBName string `json:"DB_NAME"`
	DBUser string `json:"DB_USER"`
	DBPass string `json:"DB_PASS"`
	ssl    string
}

func NewPostgres(params *postgresParams) (*postgresDB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s dbname=%s password=%s sslmode=%s",
		params.DBHost,
		params.DBPort,
		params.DBUser,
		params.DBName,
		params.DBPass,
		params.ssl,
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &postgresDB{db}, nil
}

func (db *postgresDB) createCompanyTable() error {
	stmt := `CREATE TABLE company (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    cik VARCHAR(10) UNIQUE NOT NULL
  );`
	_, err := db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (db *postgresDB) createTickerTable() error {
	stmt := `CREATE TABLE ticker (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES company(id) ON DELETE CASCADE,
    value VARCHAR(20) UNIQUE NOT NULL,
    exchange VARCHAR(50)
  );`
	_, err := db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (db *postgresDB) createFilingTable() error {
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
	_, err := db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (db *postgresDB) createHeaderTable() error {
	stmt := `CREATE TABLE header (
    id SERIAL PRIMARY KEY,
    values VARCHAR(100)[] UNIQUE NOT NULL
  );`
	_, err := db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (db *postgresDB) createTableTable() error {
	stmt := `CREATE TABLE "table" (
    id SERIAL PRIMARY KEY,
    filing_id INTEGER REFERENCES filing(id) ON DELETE CASCADE,
    index INTEGER,
		header_id INTEGER REFERENCES header(id) ON DELETE CASCADE,
		header_leaves INTEGER DEFAULT NULL,
		header_letters INTEGER DEFAULT NULL,
		UNIQUE(filing_id, index)
  );`
	_, err := db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (db *postgresDB) createKeywordTable() error {
	stmt := `CREATE TABLE keyword (
    id SERIAL PRIMARY KEY,
    value TEXT UNIQUE NOT NULL
  );`
	_, err := db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (db *postgresDB) createTableKeywordMapping() error {
	stmt := `CREATE TABLE table_keyword_mapping(
    table_id INTEGER REFERENCES "table"(id) ON DELETE CASCADE,
    keyword_id INTEGER REFERENCES keyword(id) ON DELETE CASCADE,
		UNIQUE(table_id, keyword_id)
  );`
	_, err := db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (db *postgresDB) CreateTables() error {
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
	err = db.createHeaderTable()
	if err != nil {
		return err
	}
	err = db.createTableTable()
	if err != nil {
		return err
	}
	err = db.createKeywordTable()
	if err != nil {
		return err
	}
	err = db.createTableKeywordMapping()
	if err != nil {
		return err
	}
	return nil
}

func (db *postgresDB) InsertCompany(company *request.Company) error {
	if company == nil || company.Tickers == nil ||
		company.Exchanges == nil ||
		len(company.Tickers) != len(company.Exchanges) {
		return errors.New("Corrupted data")
	}
	stmt := `INSERT INTO company (name, cik) VALUES ($1, $2) RETURNING id;`
	row := db.QueryRow(stmt, company.Name, company.CIK)
	var id int
	if err := row.Scan(&id); err != nil {
		return err
	}
	stmt = `INSERT INTO ticker (company_id, value, exchange) VALUES ($1, $2, $3);`
	for i, v := range company.Tickers {
		_, err := db.Exec(stmt, id, v, company.Exchanges[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *postgresDB) InsertHeader(header []string) error {
	stmt := `INSERT INTO header (values) VALUES ($1);`
	_, err := db.Exec(stmt, append(pq.StringArray{}, header...))
	if err != nil {
		return err
	}
	return nil
}
