package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	stan "github.com/nats-io/stan.go"
)

func getJsonData(file string) []byte {
	jsonFile, err := os.Open(file)
	if err != nil {
		log.Fatalf("Ошибка открытия JSON-файла: %v", err)
	}
	defer jsonFile.Close()

	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		log.Fatalf("Ошибка чтения JSON-файла: %v", err)
	}

	// Проверка валидности JSON
	var jsonData map[string]interface{}
	if err := json.Unmarshal(byteValue, &jsonData); err != nil {
		log.Fatalf("Ошибка распаковки JSON-файла: %v", err)
	}
	//fmt.Println(byteValue)
	return byteValue

}

const (
	clusterID = "test-cluster"
	clientID  = "sender"
	subject   = "orders.new"
)

func main() {
	// Подключение к NATS Streaming
	sc, err := stan.Connect(clusterID, clientID)
	if err != nil {
		log.Fatalf("Ошибка соединения с NATS Streaming: %v", err)
	}
	defer sc.Close()

	// Публикация JSON данных в канал NATS Streaming
	if err := sc.Publish(subject, getJsonData(os.Args[1])); err != nil {
		log.Fatalf("Ошибка публикации сообщения в NATS Streaming: %v", err)
	}

	fmt.Println("Сообщение опубликовано в NATS Streaming")
}
