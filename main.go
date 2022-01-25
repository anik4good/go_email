package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/smtp"
	"os"
	"time"

	"github.com/bxcodec/faker/v3"
	"github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v2"
	"gopkg.in/yaml.v2"
)

type Config struct {
	DB_USERNAME string `yaml:"DB_USERNAME"`
	DB_PASSWORD string `yaml:"DB_PASSWORD"`
	DB_IP       string `yaml:"DB_IP"`
	DB_PORT     string `yaml:"DB_PORT"`
	DB_NAME     string `yaml:"DB_NAME"`
}

type QueuedEmail struct {
	ID     uint64
	Name   string
	Email  string
	Status uint64
}

type Seed struct {
	db *sql.DB
}

var database *sql.DB
var logger *log.Logger
var confg Config

func main() {

	configFile, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		log.Fatalln("error reading yaml file", err)
	}
	err = yaml.Unmarshal(configFile, &confg)
	if err != nil {
		log.Fatalln("error writting yaml file to struct: ", err)
	}

	logger = InitLogger()
	logger.Println("Starting application")

	database = InitDatabase()

	//	UserSeed()

	app := fiber.New()

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello, World 👋!")
	})

	// GET /api/register
	app.Post("/api/create", func(c *fiber.Ctx) error {

		//	data, _ := json.MarshalIndent(app.Stack(), "", "  ")
		//	fmt.Println(string(data))
		requestBody := c.Body()
		var email QueuedEmail
		json.Unmarshal(requestBody, &email)
		_, err := database.Exec(`INSERT INTO users(name, email,status) VALUES (?,?,?)`, email.Name, email.Email, email.Status)
		if err != nil {

			//	panic(err)

			fmt.Println("error creating user:", email.Name)
			json.NewEncoder(c).Encode("error creating user:")
			return nil
			//	json.NewEncoder(c).Encode("error creating user:")
		}

		json.NewEncoder(c).Encode("received Email: " + email.Email)
		return nil
	}).Name("api")

	go app.Listen(":3000")

	// do a forever for loop here that repetedly query the database and check for any record that has

	for {
		//	UserSeed()

		newRecords := checkForNewRecords()

		for newRecords.Next() {
			var q QueuedEmail
			err := newRecords.Scan(&q.ID, &q.Name, &q.Email)
			if err != nil {
				logger.Println("Error writting new records to queued sms struct", err)
				continue
			}

			changeStatusToPending(q.ID)

			go processEmail(q)
			logger.Println("Status change for id ", q.ID)

			fmt.Println("Status change for id ", q.ID)

		}

		time.Sleep(2 * time.Second)
		fmt.Println("No data found")
	}
}

func processEmail(queuedEmail QueuedEmail) {
	err := sendEmail(queuedEmail)
	if err != nil {
		logger.Println(err)
		return
	}

	changeStatusToSuccess(queuedEmail.ID)

}

func sendEmail(queuedEmail QueuedEmail) error {
	//	logger.Println("Sending sms to", queuedEmail.Email)
	fmt.Println("Sending Email to", queuedEmail.Email)

	//	send_email(queuedEmail)

	return nil
}

func checkForNewRecords() *sql.Rows {
	rows, err := database.Query("select id, name, email from users WHERE status = 0 LIMIT 500")
	if err != nil {
		logger.Println("Error on new records checking ..", err)
	}
	return rows
}

func changeStatusToPending(id uint64) {
	_, err := database.Exec("UPDATE users SET status = ? WHERE id = ?", 2, id)
	if err != nil {
		logger.Println("Error updating status of "+string(rune(id))+" in users", (err), " to pending")
		return
	}
}

func changeStatusToSuccess(id uint64) {
	_, err := database.Exec("UPDATE users SET status = ? WHERE id = ?", 3, id)
	if err != nil {
		logger.Println("Error updating status of "+string(rune(id))+" in users", (err), " to pending")
		return
	}
}

func send_email(queuedEmail QueuedEmail) {
	// Configuration
	from := "anik4nobody@gmail.com"
	password := "kfxmwyvzhcifoylj"
	//	to := []queuedEmail.Email
	to := []string{queuedEmail.Email}
	smtpHost := "smtp.gmail.com"
	smtpPort := "587"

	//message := []byte("My super secret message.")

	message := []byte(
		"Subject: discount Gophers!\r\n" +
			"\r\n" +
			"This is the email body.\r\n")

	// Create authentication
	auth := smtp.PlainAuth("", from, password, smtpHost)

	// Send actual message
	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, from, to, message)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Message sent to: ", queuedEmail.Email)

}

// InitLogger initialize a new logger with specific file to log and return it
func InitLogger() *log.Logger {
	logFileName := "logs/" + time.Now().Format("2006-01-02") + ".log"

	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	return log.New(file, "", log.LstdFlags)
}

func InitDatabase() *sql.DB {

	cfg := mysql.Config{
		User:                 confg.DB_USERNAME,
		Passwd:               confg.DB_PASSWORD,
		Net:                  "tcp",
		Addr:                 confg.DB_IP + ":" + confg.DB_PORT,
		DBName:               confg.DB_NAME,
		AllowNativePasswords: true,
		ParseTime:            true,
	}

	// open the database connssection with the config. if encounter any error print the error and exit from program
	database, error := sql.Open("mysql", cfg.FormatDSN())
	if error != nil {
		logger.Fatalln("Error connecting to database", error)
	}

	// ping the database to make sure connection is successfull
	error = database.Ping()
	if error != nil {
		logger.Fatalln("Error on ping the database", error)
	}

	return database
}

func UserSeed() {

	for i := 0; i < 100; i++ {
		//prepare the statement
		//	stmt, _ := s.db.Prepare(`INSERT INTO users(name, email) VALUES (?,?)`)
		// execute query
		//	_, err := stmt.Exec(faker.Name(), faker.Email())

		_, err := database.Exec(`INSERT INTO users(name, email,status) VALUES (?,?,?)`, faker.Name(), faker.Email(), 0)
		if err != nil {
			panic(err)
		}

	}
}
