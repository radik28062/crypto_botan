package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

var stopChans map[string]chan struct{}
var doneChans map[string]chan struct{}
var reloadChans map[string]chan struct{}

const readTimeout = 30 * time.Second

type task struct {
	id        int
	exchanges []string
	min       float64
	max       float64
	name      string
	userId    int
	chatId    int
}
type coin struct {
	exch     string
	symbol   string
	askPrice float64
	bidPrice float64
}

var coinsMap sync.Map

type NotificationEvent struct {
	TaskId       int     `json:"task_id"`
	UserId       int     `json:"user_id"`
	ChatId       int     `json:"chat_id"`
	Name         string  `json:"name"`
	SpreadPct    float64 `json:"spread_pct"`
	BestAskExch  string  `json:"best_ask_exch"`
	BestAskPrice float64 `json:"best_ask_price"`
	BestBidExch  string  `json:"best_bid_exch"`
	BestBidPrice float64 `json:"best_bid_price"`
	CreatedAt    int64   `json:"created_at"`
	Type         string  `json:"type"`
}
type Config struct {
	dbURL         string
	TGToken       string
	RedisURL      string
	RedisPassword string
	MinWorker     int
	MaxWorker     int
	KafkaBrokers  []string
	KafkaTopic    string
	exchanges     []string
}

func LoadConfig() (Config, error) {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found")
	}
	minW, err := strconv.Atoi(os.Getenv("MIN_WORKER"))
	if err != nil {
		return Config{}, fmt.Errorf("MIN_WORKER: %w", err)
	}
	maxW, err := strconv.Atoi(os.Getenv("MAX_WORKER"))
	if err != nil {
		return Config{}, fmt.Errorf("MAX_WORKER: %w", err)
	}
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is empty")
	}
	topic := os.Getenv("KAFKA_TOPIC_NOTIFICATIONS")
	if topic == "" {
		return Config{}, fmt.Errorf("KAFKA_TOPIC_NOTIFICATIONS is empty")
	}
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		return Config{}, fmt.Errorf("DB_URL is empty")
	}
	tgToken := os.Getenv("TG_TOKEN")
	if tgToken == "" {
		return Config{}, fmt.Errorf("TG_TOKEN is empty")
	}
	rdbURL := os.Getenv("REDIS_URL")
	if rdbURL == "" {
		return Config{}, fmt.Errorf("REDIS_URL is empty")
	}
	rdbPassword := os.Getenv("REDIS_PASSWORD")

	exchanges := os.Getenv("EXCHANGES")
	if exchanges == "" {
		return Config{}, fmt.Errorf("EXCHANGES is empty")
	}

	cfg := Config{
		dbURL:         dbURL,
		TGToken:       tgToken,
		RedisURL:      rdbURL,
		RedisPassword: rdbPassword,
		MinWorker:     minW,
		MaxWorker:     maxW,
		KafkaBrokers:  strings.Split(brokersEnv, ","),
		KafkaTopic:    topic,
		exchanges:     strings.Split(exchanges, ","),
	}
	return cfg, nil

}

func main() {
	stopChans = make(map[string]chan struct{})
	doneChans = make(map[string]chan struct{})
	reloadChans = make(map[string]chan struct{})

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	var wg sync.WaitGroup

	conn, err := pgx.Connect(ctx, cfg.dbURL)
	if err != nil {
		log.Fatal("Ошибка при подключения к базе", err)
	}
	defer conn.Close(ctx)
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisURL,
		Password: cfg.RedisPassword,
		DB:       0,
	})
	_, err = rdb.Ping(ctx).Result()
	defer rdb.Close()
	if err != nil {
		log.Fatalf("Не удалось подключиться к Redis: %v", err)
	}
	kafkaWriter := kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 30 * time.Millisecond,
		WriteTimeout: 30 * time.Second,
		ReadTimeout:  30 * time.Second,
		Async:        false,
	}

	updateMonitoringTasks(ctx, conn, rdb, cfg, &wg)

	for _, exchange := range cfg.exchanges {
		stopChans[exchange] = make(chan struct{}, 1)
		doneChans[exchange] = make(chan struct{}, 1)
		reloadChans[exchange] = make(chan struct{}, 1)
	}

	startMonitoring(ctx, rdb, gateMonitoring, "gate", &wg)
	startMonitoring(ctx, rdb, binanceMonitoring, "binance", &wg)
	startMonitoring(ctx, rdb, mexcMonitoring, "mexc", &wg)

	go func(ctx context.Context) {
		wg.Add(1)
		defer wg.Done()

		ticker := time.NewTicker(time.Minute * 5)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				updateMonitoringTasks(ctx, conn, rdb, cfg, &wg)

				needUpdate, err := rdb.Get(ctx, "need_update").Result()
				if err != nil {
					log.Printf("Failed to get need_update: %v", err)
				}
				if needUpdate == "Y" {
					for ch := range reloadChans {
						select {
						case reloadChans[ch] <- struct{}{}:
							if err := rdb.Set(ctx, "need_update", "N", 0).Err(); err != nil {
								log.Printf("Failed to set need_update: %v", err)
							}

						default:
							log.Printf("Channel %s is full or not ready, skipping", ch)
						}
					}

				}

			}
		}
	}(ctx)

	//Запускаем поиск спредов
	log.Println("============")

	go findSpread(ctx, &kafkaWriter, conn, rdb, &wg)

	sig := <-sigCh
	log.Printf("received signal: %v, shutting down...", sig)

	cancel()
	wg.Wait()

	log.Println("service stopped")

}

/*
	func loadTestData() {
		kafkaWriter := newKafkaWriter()
		defer kafkaWriter.Close()
		event := &NotificationEvent{
			TaskId:       int(rand.Intn(1_000_000)),
			UserId:       int(33),
			ChatId:       int(353225274),
			Name:         "ETH",
			SpreadPct:    4.5,
			BestAskExch:  "gate",
			BestAskPrice: 3.9,
			BestBidExch:  "binance",
			BestBidPrice: 3.8,
			CreatedAt:    time.Now().Unix(),
			Type:         "spread_detected",
		}

		for i := 0; i < 2000; i++ {

			event.TaskId = int(rand.Intn(1_000_000))
			payload, _ := json.Marshal(event)

			msg := kafka.Message{
				Key:   []byte("spread_detected:" + strconv.Itoa(i)),
				Value: payload,
				Time:  time.Now(),
			}
			kafkaWriter.WriteMessages(ctx, msg)
		}
	}
*/
func startMonitoring(
	ctx context.Context,
	rdb *redis.Client,
	run func(stop chan struct{}, done chan struct{}, symbols []string),
	name string,
	wg *sync.WaitGroup,
) {

	redisKey := fmt.Sprintf("monitor_tasks.exchanges.%s.coins", name)
	Symbols, _ := rdb.SMembers(ctx, redisKey).Result()

	go run(stopChans[name], doneChans[name], Symbols)
	go func() {
		wg.Add(1)
		defer wg.Done()

		ticker := time.NewTicker(45 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				stopChans[name] <- struct{}{}
				<-doneChans[name]
				return

			case <-ticker.C:
				log.Printf("%s restart by ticker\n", name)
				reloadChans[name] <- struct{}{}
			case <-reloadChans[name]:
				log.Printf("%s restart by reloadChans\n", name)

				stopChans[name] <- struct{}{}
				<-doneChans[name]
				Symbols, _ := rdb.SMembers(ctx, redisKey).Result()
				go run(stopChans[name], doneChans[name], Symbols)
				ticker.Reset(45 * time.Minute)
			}
		}
	}()

}

func findSpread(ctx context.Context, kafkaWriter *kafka.Writer, conn *pgx.Conn, rdb *redis.Client, wg *sync.WaitGroup) {

	wg.Add(1)
	defer wg.Done()

	var taskIds, oldTaskIds string

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	tickerUpdateTasks := time.NewTicker(3 * time.Minute)

	tasks := map[int]task{}

	for {
		select {
		case <-ctx.Done():
			log.Println("findSpread stopping...")
			return

		case <-tickerUpdateTasks.C:
			tasksInWork, err := rdb.SMembers(ctx, "tasks_in_work").Result()

			if err != nil {
				log.Printf("get tasks_in_work in updateMonitoringTasks : %v", err)
				continue
			}

			// Нет тасков
			if len(tasksInWork) == 0 {
				continue
			}
			taskIds = strings.Join(tasksInWork, ",")

			// если не было обновления выходим
			if oldTaskIds == taskIds {
				continue
			}

			oldTaskIds = taskIds
			clear(tasks)
			rows, err := conn.Query(ctx, `
				select 
				    monitor_tasks.id, exchanges, min, max, user_id, users.chatid, monitor_tasks.name 
				from 
				    monitor_tasks 
					join users on users.id = monitor_tasks.user_id 
				where status = 0 AND monitor_tasks.id = ANY($1)`, tasksInWork)

			if err != nil {
				log.Printf("get monitor_tasks in findSpread: %v\n", err)
				continue
			}

			var exchanges []string
			var id, userId, chatId int
			var minSpread, maxSpread float64
			var name string
			for rows.Next() {
				err := rows.Scan(&id, &exchanges, &minSpread, &maxSpread, &userId, &chatId, &name)
				if err != nil {
					log.Println("Ошибка получения бирж: ", err)
					continue
				}
				tasks[id] = task{
					id:        id,
					exchanges: exchanges,
					min:       minSpread,
					max:       maxSpread,
					name:      name,
					userId:    userId,
					chatId:    chatId,
				}

			}
			rows.Close()

		case <-ticker.C:
			for id, task := range tasks {
				var bestAskPrice, bestBidPrice float64
				var bestAskExch, bestBidExch string

				found := false

				for _, t := range task.exchanges {
					if value, ok := coinsMap.Load(t); ok {
						c := value.(coin)
						if c.bidPrice == 0 || c.askPrice == 0 {
							continue // Защита от нулевых котировок, если на бирже нет указанной монеты
						}
						if !found || c.askPrice < bestAskPrice {
							bestAskPrice = c.askPrice
							bestAskExch = c.exch
						}
						if !found || c.bidPrice > bestBidPrice {
							bestBidPrice = c.bidPrice
							bestBidExch = c.exch
						}
						found = true
					}
				}
				if !found || bestBidPrice == 0 {
					continue
				}

				spread := bestBidPrice - bestAskPrice
				spreadPrc := spread / bestAskPrice * 100

				if task.min == 0.0 || spreadPrc > task.min {
					if task.max == 0.0 || spreadPrc < task.max {

						fmt.Printf("=== %s (%f) === (%v)\n", task.name, spreadPrc, id)
						fmt.Printf("Лонг на %s по %.6f\n", bestAskExch, bestAskPrice)
						fmt.Printf("Шорт на %s по %.6f\n", bestBidExch, bestBidPrice)
						// записываем в кафка
						spreadPrc = math.Round(spreadPrc*100) / 100

						event := NotificationEvent{
							TaskId:       task.id,
							UserId:       task.userId,
							ChatId:       task.chatId,
							Name:         task.name,
							SpreadPct:    spreadPrc,
							BestAskExch:  bestAskExch,
							BestAskPrice: bestAskPrice,
							BestBidExch:  bestBidExch,
							BestBidPrice: bestBidPrice,
							CreatedAt:    time.Now().Unix(),
							Type:         "spread_detected",
						}
						if err := sendNotificationEvent(ctx, kafkaWriter, event); err != nil {
							log.Printf("kafka send error: %v", err)
						}

					}
				}

			}
		}
	}
}

func sendNotificationEvent(ctx context.Context, writer *kafka.Writer, event NotificationEvent) error {

	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("marshal protobuf error: %v", err)
		return nil
	}
	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("%s:%d", event.Type, event.TaskId)),
		Value: payload,
		Time:  time.Now(),
	}
	if err := writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("sendNotificationEvent write kafka message: %w", err)
	}

	return nil
}

func updateMonitoringTasks(ctx context.Context, conn *pgx.Conn, rdb *redis.Client, cfg Config, wg *sync.WaitGroup) {

	wg.Add(1)
	defer wg.Done()

	// Задания, которые уже в работе
	tasksInWork, err := rdb.SMembers(ctx, "tasks_in_work").Result()
	if err != nil {
		log.Printf("get tasks_in_work in updateMonitoringTasks : %v", err)
		return
	}
	fmt.Println("updateMonitoringTasks tasksInWork", tasksInWork)
	if len(tasksInWork) > 0 {
		var count int
		// таски с отрицательным статусом удалённые или на паузе
		err = conn.QueryRow(ctx, `
				select 
				    COUNT(ID)
				from 
				    monitor_tasks 
				where status < 0 AND id = ANY($1)`, tasksInWork).Scan(&count)
		if err != nil {
			log.Printf("Query error: %v", err)

		}
		if count > 0 {
			if err := rdb.Set(ctx, "need_update", "Y", 0).Err(); err != nil {
				log.Printf("Failed to set need_update: %v", err)
			}
		}
	}

	//Проверяем, появились ли новые задания
	rows, err := conn.Query(ctx, `
				select 
				    id, exchanges
				from 
				    monitor_tasks 
				where status = 0 AND id != ALL($1)`, tasksInWork)

	if err != nil {
		fmt.Printf("Ошибка запроса: %v", err)
		return
	}

	ids := make([]string, 0)
	coins := make(map[string][]string) // [gate] = [BTC,ETH,SOL...]

	for rows.Next() {
		var exchanges []string
		var id int

		err := rows.Scan(&id, &exchanges)
		if err != nil {
			log.Printf("Failed to get task from DB %v\n", err)
			continue
		}
		ids = append(ids, strconv.Itoa(id))
		fmt.Println("ids ", ids, exchanges)

		for _, exchange := range exchanges {
			parts := strings.Split(exchange, ":")
			coins[parts[0]] = append(coins[parts[0]], parts[1])
		}
	}
	rows.Close()

	if len(ids) == 0 {
		return
	}

	if err := rdb.Set(ctx, "need_update", "Y", 0).Err(); err != nil {
		log.Printf("Failed to set need_update: %v", err)
	}

	// Обновляем .coins
	pipe := rdb.Pipeline()
	for _, exchange := range cfg.exchanges {
		rKey := fmt.Sprintf("monitor_tasks.exchanges.%s.coins", exchange)

		pipe.Unlink(ctx, rKey)
		if len(coins[exchange]) > 0 {
			for _, v := range coins[exchange] {
				pipe.SAdd(ctx, rKey, v)
			}
		}
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("failed to update monitor_tasks.exchanges.*.coins: %v", err)
	}

	if len(ids) > 0 {
		_, err = conn.Exec(ctx, `
			UPDATE 
				monitor_tasks 
			SET 
			    get_to_work_at = CURRENT_TIMESTAMP 
			WHERE id = ANY ($1)`,
			ids)
		if err != nil {
			log.Println("failed to set CURRENT_TIMESTAMP to monitor_tasks", err)
		}
		args := make([]interface{}, len(ids))
		for i, v := range ids {
			args[i] = v
		}
		_, err := rdb.SAdd(ctx, "tasks_in_work", args...).Result()
		if err != nil {
			log.Printf("rdb SAdd in updateMonitoringTasks: %v", err)
		}
	}
}
func gateMonitoring(stopChan chan struct{}, doneChan chan struct{}, symbols []string) {
	defer func() {
		doneChan <- struct{}{}
	}()

	type BookTickerEvent struct {
		Time    int64  `json:"time"`    // Unix время в секундах
		TimeMs  int64  `json:"time_ms"` // Unix время в миллисекундах
		Channel string `json:"channel"` // futures.book_ticker
		Event   string `json:"event"`   // update
		Result  struct {
			T     int64       `json:"t"` // timestamp генерации в мс
			U     uint64      `json:"u"` // order book update ID
			S     string      `json:"s"` // имя контракта, например BTC_USDT
			B     string      `json:"b"` // best bid price, "" если нет бидов
			BSize interface{} `json:"B"` // размер лучшего бида, 0 если нет
			A     string      `json:"a"` // best ask price, "" если нет асков
			ASize interface{} `json:"A"` // размер лучшего аска, 0 если нет
		} `json:"result"`
	}
	type BookTickerSubscribeEvent struct {
		Time    int64    `json:"time"`    // unix время в секундах
		TimeMs  int64    `json:"time_ms"` // unix время в миллисекундах
		ConnID  string   `json:"conn_id"` // идентификатор соединения
		TraceID string   `json:"trace_id"`
		Channel string   `json:"channel"` // futures.book_ticker
		Event   string   `json:"event"`   // subscribe
		Payload []string `json:"payload"` // список контрактов
		Result  struct {
			Status string `json:"status"` // "success" или "fail"
		} `json:"result"`
	}
	var event BookTickerEvent
	var msg BookTickerSubscribeEvent

	wsURL := "wss://fx-ws.gateio.ws/v4/ws/usdt"
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)

	if err == nil {
		defer wsConn.Close()

		subscribeMsg := map[string]interface{}{
			"time":    time.Now().Unix(),
			"channel": "futures.book_ticker",
			"event":   "subscribe",
			"payload": []string{},
		}
		for _, symbol := range symbols {
			subscribeMsg["payload"] = append(subscribeMsg["payload"].([]string), symbol+"_USDT")
		}
		if err := wsConn.WriteJSON(subscribeMsg); err != nil {
			log.Println("ошибка подписки ", err)
		}
	} else {
		log.Println("ошибка подключения gate", err)
		return
	}

	_, message, err := wsConn.ReadMessage()
	if err != nil {
		log.Println("Gate ReadMessage:", err)
		return
	}

	err = json.Unmarshal(message, &msg)

	if err != nil {
		log.Println("Ошибка Unmarshal:", err)
	}
	if msg.Result.Status != "success" {
		log.Println("Ошибка подписки:", msg)
	}
	type lastQuote struct {
		ask string
		bid string
	}

	lastPrices := make(map[string]lastQuote, len(symbols))

	for {
		select {
		case <-stopChan:
			log.Println("Gate WS stop signal")
			_ = wsConn.Close()
			return
		default:
			_ = wsConn.SetReadDeadline(time.Now().Add(readTimeout))
			_, message, err := wsConn.ReadMessage()

			if err != nil {
				log.Println("Gate ReadMessage:", err)
				return
			}

			err = json.Unmarshal(message, &event)
			if err != nil {
				log.Println("Unmarshal:", err)
				continue
			}
			// пропускаем сообщение, если цена не изменилась
			if lastPrices[event.Result.S].ask == event.Result.A && lastPrices[event.Result.S].bid == event.Result.B {
				continue
			}
			aPrice, err := strconv.ParseFloat(event.Result.A, 64)
			if err != nil {
				log.Println("ParseFloat aPrice ", err)
				continue
			}
			bPrice, err := strconv.ParseFloat(event.Result.B, 64)
			if err != nil {
				log.Println("ParseFloat bPrice ", err)
				continue
			}

			c := coin{
				symbol:   event.Result.S,
				exch:     "gate",
				askPrice: aPrice,
				bidPrice: bPrice,
			}

			coinsMap.Store(c.exch+":"+c.symbol, c)

			lastPrices[event.Result.S] = lastQuote{
				ask: event.Result.A,
				bid: event.Result.B,
			}
		}
	}
}

func mexcMonitoring(stopChan chan struct{}, doneChan chan struct{}, symbols []string) {
	defer func() {
		doneChan <- struct{}{}
	}()

	type MexcTickerEvent struct {
		Channel string `json:"channel"`
		Symbol  string `json:"symbol"`
		Data    struct {
			Ask1 float64 `json:"ask1"`
			Bid1 float64 `json:"bid1"`
		} `json:"data"`
	}

	type MexcPong struct {
		Channel string `json:"channel"`
		Data    int64  `json:"data"`
	}

	type MexcSubAck struct {
		Channel string `json:"channel"`
		Data    string `json:"data"`
		Ts      int64  `json:"ts"`
	}
	var env MexcSubAck
	wsURL := "wss://contract.mexc.com/edge"
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Printf("ошибка подключения MEXC: %v", err)
		return
	}
	defer wsConn.Close()

	for _, symbol := range symbols {
		subscribeMsg := map[string]interface{}{
			"method": "sub.ticker",
			"param": map[string]interface{}{
				"symbol": symbol + "_USDT",
			},
		}

		if err := wsConn.WriteJSON(subscribeMsg); err != nil {
			log.Printf("subscribe mexc error for %s: %v", symbol, err)
		}
	}

	pingTicker := time.NewTicker(15 * time.Second)
	defer pingTicker.Stop()

	var event MexcTickerEvent
	var pong MexcPong
	var ack MexcSubAck

	for {
		select {
		case <-stopChan:
			log.Println("MEXC WS stop signal")
			_ = wsConn.Close()
			return

		case <-pingTicker.C:
			err := wsConn.WriteJSON(map[string]string{
				"method": "ping",
			})
			if err != nil {
				log.Println("MEXC ping error:", err)
				return
			}

		default:
			_ = wsConn.SetReadDeadline(time.Now().Add(readTimeout))

			_, rawMessage, err := wsConn.ReadMessage()
			if err != nil {
				log.Println("MEXC ReadMessage:", err)
				return
			}

			message, err := decodeMexcMessage(rawMessage)

			if err != nil {
				log.Println("MEXC decode message:", err)
				continue
			}

			if err := json.Unmarshal(message, &pong); err == nil && pong.Channel == "pong" {
				continue
			}
			err = json.Unmarshal(message, &env)
			if err == nil {
				if env.Channel == "rs.error" {
					log.Println("MEXC rs.error:", err, "message:", string(message))
				}
				continue
			}

			if err := json.Unmarshal(message, &ack); err == nil && ack.Channel == "rs.sub.ticker" {
				if ack.Data != "success" {
					log.Println("subscribe failed:", ack)
				}
				continue
			}

			err = json.Unmarshal(message, &event)

			if err != nil {
				log.Println("MEXC Unmarshal:", err, "message:", string(message))
				continue
			}

			if event.Channel != "push.ticker" {
				continue
			}

			c := coin{
				symbol:   event.Symbol,
				exch:     "mexc",
				askPrice: event.Data.Ask1,
				bidPrice: event.Data.Bid1,
			}

			coinsMap.Store(c.exch+":"+c.symbol, c)
		}
	}
}

func binanceMonitoring(stopChan chan struct{}, doneChan chan struct{}, symbols []string) {
	defer func() {
		doneChan <- struct{}{}
	}()

	type BinanceBookTickerEvent struct {
		EventType string `json:"e"`
		UpdateID  uint64 `json:"u"`
		EventTime int64  `json:"E"`
		TransTime int64  `json:"T"`
		Symbol    string `json:"s"`
		BidPrice  string `json:"b"`
		BidSize   string `json:"B"`
		AskPrice  string `json:"a"`
		AskSize   string `json:"A"`
	}

	type BinanceCombinedEvent struct {
		Stream string                 `json:"stream"`
		Data   BinanceBookTickerEvent `json:"data"`
	}

	streams := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		streams = append(streams, strings.ToLower(symbol)+"usdt@bookTicker")
	}

	wsURL := "wss://fstream.binance.com/stream?streams=" + strings.Join(streams, "/")
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Printf("ошибка подключения Binance: %v", err)
		return
	}
	defer wsConn.Close()

	var wrapper BinanceCombinedEvent

	for {
		select {
		case <-stopChan:
			log.Println("Binance WS stop signal")
			_ = wsConn.Close()
			return
		default:
			_ = wsConn.SetReadDeadline(time.Now().Add(readTimeout))
			_, message, err := wsConn.ReadMessage()

			if err != nil {
				log.Println("Binance ReadMessage:", err)
				return
			}

			err = json.Unmarshal(message, &wrapper)
			if err != nil {
				log.Println("Binance Unmarshal:", err)
				continue
			}

			aPrice, err := strconv.ParseFloat(wrapper.Data.AskPrice, 64)
			if err != nil {
				log.Println("Binance ask price parse:", err)
				continue
			}

			bPrice, err := strconv.ParseFloat(wrapper.Data.BidPrice, 64)
			if err != nil {
				log.Println("Binance bid price parse:", err)
				continue
			}

			c := coin{
				symbol:   normalizeBinanceSymbol(wrapper.Data.Symbol),
				exch:     "binance",
				askPrice: aPrice,
				bidPrice: bPrice,
			}

			coinsMap.Store(c.exch+":"+c.symbol, c)
		}
	}
}

func normalizeBinanceSymbol(s string) string {
	s = strings.ToUpper(s)
	if strings.HasSuffix(s, "USDT") {
		base := strings.TrimSuffix(s, "USDT")
		return base + "_USDT"
	}
	return s
}

func decodeMexcMessage(message []byte) ([]byte, error) {
	if len(message) > 0 && (message[0] == '{' || message[0] == '[') {
		return message, nil
	}

	r, err := gzip.NewReader(bytes.NewReader(message))
	if err != nil {
		return message, nil
	}
	defer r.Close()

	decoded, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}
