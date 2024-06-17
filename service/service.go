package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
	stan "github.com/nats-io/stan.go"
	"github.com/patrickmn/go-cache"
)

var (
	db       *sql.DB
	memCache *cache.Cache
	mu       sync.Mutex
)

type Order struct {
	ID   int                    `json:"id"`
	Data map[string]interface{} `json:"data"`
}

func initDB() {
	var err error
	data, err := os.ReadFile("cred.txt")
	if err != nil {
		log.Fatalf("Ошибка чтения файла: %s", err)
	}
	connStr := fmt.Sprintf("user=postgres dbname=Orders sslmode=disable password=%s", data)
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
}

func initCache() {
	memCache = cache.New(5*time.Minute, 10*time.Minute)

	rows, err := db.Query("SELECT id, data FROM orders")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var jsonData []byte
		if err := rows.Scan(&id, &jsonData); err != nil {
			log.Fatal(err)
		}

		if jsonData == nil {
			continue
		}

		var orderData map[string]interface{}
		if err := json.Unmarshal(jsonData, &orderData); err != nil {
			log.Fatal(err)
		}

		order := Order{
			ID:   id,
			Data: orderData,
		}

		memCache.Set(fmt.Sprintf("%d", id), order, cache.DefaultExpiration)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func saveOrderToDB(order Order) (int, error) {
	jsonData, err := json.Marshal(order.Data)
	if err != nil {
		return 0, err
	}

	var id int
	err = db.QueryRow("INSERT INTO orders (data) VALUES ($1) RETURNING id", jsonData).Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

func handleNATSMessage(m *stan.Msg) {
	var orderData map[string]interface{}
	if err := json.Unmarshal(m.Data, &orderData); err != nil {
		log.Printf("Ошибка распаковки JSON: %v\n", err)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	order := Order{
		Data: orderData,
	}

	id, err := saveOrderToDB(order)
	if err != nil {
		log.Printf("Ошибка сохранения в БД: %v\n", err)
		return
	}

	order.ID = id
	memCache.Set(fmt.Sprintf("%d", order.ID), order, cache.DefaultExpiration)

	log.Printf("Заказ получен и сохранен: %v\n", order)
}

func getOrderFromCache(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Отсутствует параметр запроса (id)", http.StatusBadRequest)
		return
	}

	if x, found := memCache.Get(id); found {
		if order, ok := x.(Order); ok {
			json.NewEncoder(w).Encode(order)
		} else {
			http.Error(w, "Заказ не найден", http.StatusNotFound)
		}
	} else {
		http.Error(w, "Заказ не найден", http.StatusNotFound)
	}
}

const (
	clusterID = "test-cluster"
	clientID  = "subscriber"
	subject   = "orders.new"
)

func main() {
	initDB()
	defer db.Close()

	initCache()

	sc, err := stan.Connect(clusterID, clientID)
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()
	_, err = sc.Subscribe(subject, func(m *stan.Msg) {
		handleNATSMessage(m)
	})
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/order", getOrderFromCache)
	http.Handle("/", http.FileServer(http.Dir("../static")))

	log.Println("Сервер запущен на :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

	select {}
}
