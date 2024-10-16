package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) VALUES (?, ?, ?, ?)", p.Client, p.Status, p.Address, p.CreatedAt)
	if err != nil {
		return 0, err
	}
	// верните идентификатор последней добавленной записи
	number, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return int(number), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	query := "SELECT client, status, address, created_at FROM parcel WHERE number = ?"
	row := s.db.QueryRow(query, number)
	// заполните объект Parcel данными из таблицы
	p := Parcel{}
	err := row.Scan(&p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return Parcel{}, fmt.Errorf("no parcel found with number %d", number)
		}
		return Parcel{}, err
	}
	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	query := "SELECT number, client, status, address, created_at FROM parcel WHERE client = ?"
	rows, err := s.db.Query(query, client)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// заполните срез Parcel данными из таблицы
	var res []Parcel
	for rows.Next() {
		var p Parcel
		if err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel
	query := "UPDATE parcel SET status = ? WHERE number = ?"
	result, err := s.db.Exec(query, status, number)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("no parcel found with number %d", number)
	}
	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	var currentStatus string
	statusQuery := "SELECT status FROM parcel WHERE number = ?"
	err := s.db.QueryRow(statusQuery, number).Scan(&currentStatus)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no parcel found with number %d", number)
		}
		return err
	}
	if currentStatus != "registered" {
		return fmt.Errorf("cannot change address: parcel status is '%s', must be 'registered'", currentStatus)
	}
	updateQuery := "UPDATE parcel SET address = ? WHERE number = ?"
	_, err = s.db.Exec(updateQuery, address, number)
	if err != nil {
		return err
	}
	return nil
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	var currentStatus string
	statusQuery := "SELECT status FROM parcel WHERE number = ?"
	err := s.db.QueryRow(statusQuery, number).Scan(&currentStatus)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no parcel found with number %d", number)
		}
		return err
	}
	if currentStatus != "registered" {
		return fmt.Errorf("cannot delete parcel: status is '%s', must be 'registered'", currentStatus)
	}
	deleteQuery := "DELETE FROM parcel WHERE number = ?"
	_, err = s.db.Exec(deleteQuery, number)
	if err != nil {
		return err
	}
	return nil
}
