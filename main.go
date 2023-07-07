package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

const (
	host     = "your-db-host"
	port     = 5432 // or your PostgreSQL port
	user     = "your-db-username"
	password = "your-db-password"
	dbname   = "your-db-name"
)

type Contact struct {
	ID                  int            `json:"id"`
	PhoneNumber         sql.NullInt64  `json:"phoneNumber"`
	Email               sql.NullString `json:"email"`
	PrimaryContactID    int            `json:"primaryContactId"`
	SecondaryContactIDs []int          `json:"secondaryContactIds"`
}

type ConsolidatedContact struct {
	PrimaryContactID    int      `json:"primaryContactId"`
	Emails              []string `json:"emails"`
	PhoneNumbers        []int    `json:"phoneNumbers"`
	SecondaryContactIDs []int    `json:"secondaryContactIds"`
}

func main() {
	lambda.Start(IdentifyContact)
}

func IdentifyContact(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var contact Contact

	err := json.Unmarshal([]byte(request.Body), &contact)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       err.Error(),
		}, nil
	}

	db := ConnectDB()
	defer db.Close()

	consolidatedContact, err := ConsolidateContacts(db, contact)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       err.Error(),
		}, nil
	}

	responseBody, err := json.Marshal(map[string]interface{}{"contact": consolidatedContact})
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       err.Error(),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(responseBody),
		Headers:    map[string]string{"Content-Type": "application/json"},
	}, nil
}

func ConnectDB() *sql.DB {
	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func ConsolidateContacts(db *sql.DB, contact Contact) (ConsolidatedContact, error) {
	var consolidated ConsolidatedContact

	query := `SELECT * FROM contact WHERE email = $1 OR phoneNumber = $2`
	rows, err := db.Query(query, contact.Email.String, contact.PhoneNumber.Int64)
	if err != nil {
		return consolidated, err
	}
	defer rows.Close()

	var contacts []Contact
	for rows.Next() {
		var c Contact
		err := rows.Scan(&c.ID, &c.PhoneNumber, &c.Email, &c.PrimaryContactID)
		if err != nil {
			return consolidated, err
		}
		contacts = append(contacts, c)
	}

	if err := rows.Err(); err != nil {
		return consolidated, err
	}

	if len(contacts) == 0 {
		err := createPrimaryContact(db, &contact)
		if err != nil {
			return consolidated, err
		}

		consolidated.PrimaryContactID = contact.ID
		consolidated.Emails = append(consolidated.Emails, contact.Email.String)
		consolidated.PhoneNumbers = append(consolidated.PhoneNumbers, int(contact.PhoneNumber.Int64))
		return consolidated, nil
	}

	consolidated.PrimaryContactID = contacts[0].PrimaryContactID
	consolidated.Emails = extractUniqueEmails(contacts)
	consolidated.PhoneNumbers = extractUniquePhoneNumbers(contacts)
	consolidated.SecondaryContactIDs = extractAllSecondaryContactIDs(contacts)

	if shouldCreateSecondaryContact(contact, consolidated) {
		err := createSecondaryContact(db, &contact, consolidated.PrimaryContactID)
		if err != nil {
			return consolidated, err
		}
		consolidated.SecondaryContactIDs = append(consolidated.SecondaryContactIDs, contact.ID)
	}

	return consolidated, nil
}

func createPrimaryContact(db *sql.DB, contact *Contact) error {
	query := `INSERT INTO contact (phoneNumber, email, linkPrecedence) VALUES ($1, $2, 'primary') RETURNING id`
	err := db.QueryRow(query, contact.PhoneNumber.Int64, contact.Email.String).Scan(&contact.ID)
	if err != nil {
		return err
	}
	return nil
}

func shouldCreateSecondaryContact(contact Contact, consolidated ConsolidatedContact) bool {
	for _, email := range consolidated.Emails {
		if email == contact.Email.String {
			return false
		}
	}

	for _, phoneNumber := range consolidated.PhoneNumbers {
		if phoneNumber == int(contact.PhoneNumber.Int64) {
			return false
		}
	}

	return true
}

func createSecondaryContact(db *sql.DB, contact *Contact, primaryContactID int) error {
	query := `INSERT INTO contact (phoneNumber, email, linkPrecedence, linkedId) VALUES ($1, $2, 'secondary', $3) RETURNING id`
	err := db.QueryRow(query, contact.PhoneNumber.Int64, contact.Email.String, primaryContactID).Scan(&contact.ID)
	if err != nil {
		return err
	}
	return nil
}

func extractUniqueEmails(contacts []Contact) []string {
	emailSet := make(map[string]bool)
	var emails []string

	for _, contact := range contacts {
		if contact.Email.Valid && !emailSet[contact.Email.String] {
			emailSet[contact.Email.String] = true
			emails = append(emails, contact.Email.String)
		}
	}

	return emails
}

func extractUniquePhoneNumbers(contacts []Contact) []int {
	phoneSet := make(map[int]bool)
	var phoneNumbers []int

	for _, contact := range contacts {
		if contact.PhoneNumber.Valid && !phoneSet[int(contact.PhoneNumber.Int64)] {
			phoneSet[int(contact.PhoneNumber.Int64)] = true
			phoneNumbers = append(phoneNumbers, int(contact.PhoneNumber.Int64))
		}
	}

	return phoneNumbers
}

func extractAllSecondaryContactIDs(contacts []Contact) []int {
	var secondaryContactIDs []int

	for _, contact := range contacts {
		secondaryContactIDs = append(secondaryContactIDs, contact.SecondaryContactIDs...)
	}

	return secondaryContactIDs
}

/*```go
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

const (
	host     = "your-db-host"
	port     = 5432 // or your PostgreSQL port
	user     = "your-db-username"
	password = "your-db-password"
	dbname   = "your-db-name"
)

type Contact struct {
	ID                   int            `json:"id"`
	PhoneNumber          sql.NullInt64  `json:"phoneNumber"`
	Email                sql.NullString `json:"email"`
	PrimaryContactID     int            `json:"primaryContactId"`
	SecondaryContactIDs  []int          `json:"secondaryContactIds"`
}

type ConsolidatedContact struct {
	PrimaryContactID    int      `json:"primaryContactId"`
	Emails              []string `json:"emails"`
	PhoneNumbers        []int    `json:"phoneNumbers"`
	SecondaryContactIDs []int    `json:"secondaryContactIds"`
}

func main() {
	lambda.Start(IdentifyContact)
}

func IdentifyContact(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var contact Contact

	err := json.Unmarshal([]byte(request.Body), &contact)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       err.Error(),
		}, nil
	}

	db := ConnectDB()
	defer db.Close()

	consolidatedContact, err := ConsolidateContacts(db, contact)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       err.Error(),
		}, nil
	}

	responseBody, err := json.Marshal(map[string]interface{}{"contact": consolidatedContact})
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       err.Error(),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(responseBody),
		Headers:    map[string]string{"Content-Type": "application/json"},
	}, nil
}

func ConnectDB() *sql.DB {
	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func ConsolidateContacts(db *sql.DB, contact Contact) (ConsolidatedContact, error) {
	var consolidated ConsolidatedContact

	query := `SELECT * FROM contact WHERE email = $1 OR phoneNumber = $2`
	rows, err := db.Query(query, contact.Email.String, contact.PhoneNumber.Int64)
	if err != nil {
		return consolidated, err
	}
	defer rows.Close()

	var contacts []Contact
	for rows.Next() {
		var c Contact
		err := rows.Scan(&c.ID, &c.PhoneNumber, &c.Email, &c.PrimaryContactID, &c.SecondaryContactIDs)
		if err != nil {
			return consolidated, err
		}
		contacts = append(contacts, c)
	}

	if err := rows.Err(); err != nil {
		return consolidated, err
	}

	if len(contacts) == 0 {
		err := createPrimaryContact(db, &contact)
		if err != nil {
			return consolidated, err
		}

		consolidated.PrimaryContactID = contact.ID
		consolidated.Emails = append(consolidated.Emails, contact.Email.String)
		consolidated.PhoneNumbers = append(consolidated.PhoneNumbers, int(contact.PhoneNumber.Int64))
		return consolidated, nil
	}

	consolidated.PrimaryContactID = contacts[0].PrimaryContactID
	consolidated.Emails = extractUniqueEmails(contacts)
	consolidated.PhoneNumbers = extractUniquePhoneNumbers(contacts)
	consolidated.SecondaryContactIDs = extractAllSecondaryContactIDs(contacts)

	if shouldCreateSecondaryContact(contact, consolidated) {
		err := createSecondaryContact(db, &contact, consolidated.PrimaryContactID)
		if err != nil {
			return consolidated, err
		}
		consolidated.SecondaryContactIDs = append(consolidated.SecondaryContactIDs, contact.ID)
	}

	return consolidated, nil
}

func createPrimaryContact(db *sql.DB, contact *Contact) error {
	query := `INSERT INTO contact (phoneNumber, email, linkPrecedence) VALUES ($1, $2, 'primary') RETURNING id`
	err := db.QueryRow(query, contact.PhoneNumber.Int64, contact.Email.String).Scan(&contact.ID)
	if err != nil {
		return err
	}
	return nil
}

func shouldCreateSecondaryContact(contact Contact, consolidated ConsolidatedContact) bool {
	for _, email := range consolidated.Emails {
		if email == contact.Email.String {
			return false
		}
	}

	for _, phoneNumber := range consolidated.PhoneNumbers {
		if phoneNumber == int(contact.PhoneNumber.Int64) {
			return false
		}
	}

	return true
}

func createSecondaryContact(db *sql.DB, contact *Contact, primaryContactID int) error {
	query := `INSERT INTO contact (phoneNumber, email, linkPrecedence, linkedId) VALUES ($1, $2, 'secondary', $3) RETURNING id`
	err := db.QueryRow(query, contact.PhoneNumber.Int64, contact.Email.String, primaryContactID).Scan(&contact.ID)
	if err != nil {
		return err
	}
	return nil
}

func extractUniqueEmails(contacts []Contact) []string {
	emailSet := make(map[string]bool)
	var emails []string

	for _, contact := range contacts {
		if contact.Email.Valid && !emailSet[contact.Email.String] {
			emailSet[contact.Email.String] = true
			emails = append(emails, contact.Email.String)
		}
	}

	return emails
}

func extractUniquePhoneNumbers(contacts []Contact) []int {
	phoneSet := make(map[int]bool)
	var phoneNumbers []int

	for _, contact := range contacts {
		if contact.PhoneNumber.Valid && !phoneSet[int(contact.PhoneNumber.Int64)] {
			phoneSet[int(contact.PhoneNumber.Int64)] = true
			phoneNumbers = append(phoneNumbers, int(contact.PhoneNumber.Int64))
		}
	}

	return phoneNumbers
}

func extractAllSecondaryContactIDs(contacts []Contact) []int {
	var secondaryContactIDs []int

	for _, contact := range contacts {
		secondaryContactIDs = append(secondaryContactIDs, contact.SecondaryContactIDs...)
	}

	return secondaryContactIDs
}
*/
