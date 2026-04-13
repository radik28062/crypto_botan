package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"log"
	"os"
	"strings"

	"github.com/go-telegram/bot/models"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"

	"github.com/go-telegram/bot"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

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
}

type KafkaReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}
type TelegramSender interface {
	SendMessage(ctx context.Context, params *bot.SendMessageParams) (*models.Message, error)
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

	cfg := Config{
		dbURL:         dbURL,
		TGToken:       tgToken,
		RedisURL:      rdbURL,
		RedisPassword: rdbPassword,
		MinWorker:     minW,
		MaxWorker:     maxW,
		KafkaBrokers:  strings.Split(brokersEnv, ","),
		KafkaTopic:    topic,
	}
	return cfg, nil

}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	conn, err := pgx.Connect(ctx, cfg.dbURL)
	if err != nil {
		log.Fatal("Ошибка при подключения к базе", err)
	}
	defer conn.Close(ctx)

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisURL,
		Password: cfg.RedisPassword,
		DB:       4,
	})
	_, err = rdb.Ping(ctx).Result()
	defer rdb.Close()
	if err != nil {
		log.Fatalf("Не удалось подключиться к Redis: %v", err)
	}

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: "notifications-service",
	})

	defer kafkaReader.Close()
	b, err := bot.New(cfg.TGToken)
	if err != nil {
		log.Fatal("Ошибка при инициализации бота", err)
	}

	log.Println("============")
	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		kafkaToRedis(ctx, kafkaReader, rdb)

	}()

	go func() {
		defer wg.Done()
		workerManager(ctx, rdb, cfg, b)

	}()

	sig := <-sigCh
	log.Printf("received signal: %v, shutting down...", sig)
	cancel()
	wg.Wait()

	log.Println("service stopped")
}

func kafkaToRedis(ctx context.Context, kafkaReader KafkaReader, rdb *redis.Client) {
	var processedCount int64
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	log.Println("🚀 kafkaToRedis started")

	go func() {
		for range ticker.C {
			queueLen, _ := rdb.LLen(ctx, "tasks:queue").Result()
			log.Printf("📊 [STATS Kafka] Processed: %d | Queue size: %d",
				processedCount, queueLen)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("kafkaToRedis stopped")
			return
		default:
			var err error
			processedCount, err = sendTaskFromKafkaToRedis(ctx, kafkaReader, rdb, processedCount)
			if err != nil {
				log.Printf("❌ sendTaskFromKafkaToRedis: %v", err)
			}
		}

	}
}

func sendTaskFromKafkaToRedis(ctx context.Context, kafkaReader KafkaReader, rdb *redis.Client, processedCount int64) (int64, error) {
	msg, err := kafkaReader.ReadMessage(ctx)
	if err != nil {
		log.Printf("❌ [KAFKA] Read error: %v", err)
		return processedCount, err
	}
	err = rdb.RPush(ctx, "tasks:queue", msg.Value).Err()
	if err != nil {
		log.Printf("❌ [REDIS] RPUSH error: %v", err)
		return processedCount, err
	}
	if err = kafkaReader.CommitMessages(ctx, msg); err != nil {
		log.Printf("❌ [KAFKA] Commit error: %v", err)
		return processedCount, err
	}
	processedCount++
	return processedCount, nil
}

func workerManager(ctx context.Context, rdb *redis.Client, cfg Config, b TelegramSender) {
	log.Println("🚀 workerManager started")
	var taskCount int
	var err error

	var workerCancels []context.CancelFunc
	parentCtx := ctx

	ticker := time.NewTicker(100 * time.Millisecond)
	writerConsole := time.NewTicker(5 * time.Second)
	defer writerConsole.Stop()
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("workerManager stopping...")
			for _, cancel := range workerCancels {
				cancel()
			}
			log.Println("workerManager stopped")
			return

		case <-writerConsole.C:
			workerCount := len(workerCancels)
			howManyNeed := (taskCount + 99) / 100

			fmt.Println("\n" + strings.Repeat("═", 50))
			fmt.Printf("📊 СТАТИСТИКА РАБОТЫ Воркеров\n")
			fmt.Println(strings.Repeat("─", 50))
			fmt.Printf("📦 Задач в очереди:    %d\n", taskCount)
			fmt.Printf("👷 Воркеров активно:   %d\n", workerCount)
			fmt.Printf("🎯 Требуется воркеров: %d\n", howManyNeed)

			fmt.Println(strings.Repeat("═", 50))

		case <-ticker.C:

			taskCount, err = getTaskCount(ctx, rdb)
			if err != nil {
				log.Println("getTaskCount", err)
			}
			workerCount := getWorkerCount(workerCancels)
			delta := getWorkersDelta(taskCount, workerCount, 100, cfg)

			//создаёи/убиваем воркеров
			if delta > 0 {
				fmt.Printf("🟢 Масштабирование: +%d воркеров (было %d)\n", delta, workerCount)
				for i := 0; i < delta; i++ {
					workerCtx, cancel := context.WithCancel(parentCtx)
					workerId := len(workerCancels) + 1
					go worker(workerCtx, rdb, workerId, b)
					workerCancels = append(workerCancels, cancel)
				}
			}
			if delta < 0 {
				fmt.Printf("🔴 Масштабирование: %d воркеров (было %d)\n", delta, workerCount)
				for i := 0; i > delta; i-- {
					workerCancels[0]()
					workerCancels = workerCancels[1:]
				}
			}

		}
	}

}

func getWorkerCount(cancels []context.CancelFunc) int {
	return len(cancels)
}

func getWorkersDelta(taskCount int, workerCount int, perWorker int, cfg Config) int {
	howManyNeed := (taskCount + perWorker - 1) / perWorker
	delta := 0

	if workerCount > cfg.MaxWorker {
		delta = cfg.MaxWorker - workerCount
	}

	if workerCount < cfg.MinWorker {
		delta = cfg.MinWorker - workerCount
	}

	if howManyNeed > workerCount {
		if howManyNeed > cfg.MaxWorker {
			delta = cfg.MaxWorker - workerCount
		} else {
			delta = howManyNeed - workerCount
		}
	}

	if howManyNeed < workerCount {
		if howManyNeed < cfg.MinWorker {
			delta = cfg.MinWorker - workerCount
		} else {
			delta = howManyNeed - workerCount
		}
	}

	return delta
}

func getTaskCount(ctx context.Context, rdb *redis.Client) (int, error) {
	val, err := rdb.LLen(ctx, "tasks:queue").Result()
	if err != nil {
		return 0, err
	}
	taskCount := int(val)
	return taskCount, nil
}

/*
	func worker(ctx context.Context, rdb *redis.Client) {
		fmt.Println("Worker started ")
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Worker done ")
				return
			default:
				var event NotificationEvent
				taskTxt, err := rdb.LPop(ctx, "tasks:queue").Result()
				if err != nil {
					continue
				}
				fmt.Println("получили задание ", taskTxt)

				err = json.Unmarshal([]byte(taskTxt), &event)
				if err != nil {
					log.Println("json.Unmarshal", err)
				}
				fmt.Println("распаковка ")
				spew.Dump(event)

				lnKey := "NotificationTime:" + strconv.Itoa(event.TaskId)
				lastNotificationTimeStr, err := rdb.Get(ctx, lnKey).Result()

				if err == nil {
					t, _ := strconv.ParseInt(lastNotificationTimeStr, 10, 64)
					if time.Since(time.Unix(t, 0)) < 15*time.Minute {
						log.Printf("Пропускаем уведомление для task %d по таймауту", event.TaskId)
						continue
					}
				}

				mggHTML := getTgHtml(event)
				fmt.Println("Сгенерировали сообщение ", mggHTML)
				if mggHTML == "" {
					log.Println("Не удалось сгенерировать сообщение", event)
				}

				params := &bot.SendMessageParams{
					ChatID:    event.ChatId, // ID чата
					Text:      mggHTML,      // Текст с HTML-разметкой
					ParseMode: "HTML",       // Режим форматирования (HTML или Markdown)
				}

				_, err = b.SendMessage(ctx, params)
				if err != nil {
					log.Printf("Ошибка отправки сообщения: %v", err)
					continue
				}

				fmt.Println("отправили в ТГ ", mggHTML)

				err = rdb.Set(ctx, lnKey, time.Now().Unix(), 0).Err() // обновлем время последнего уведомления
				if err != nil {
					log.Println("rdb.Set NotificationTime", err)
				}
				fmt.Println("обновили время")
			}
		}
	}
*/

func worker(ctx context.Context, rdb *redis.Client, workerID int, b TelegramSender) {
	log.Printf("🟢 [Worker %d] Started", workerID)

	var processedCount int64

	for {
		select {
		case <-ctx.Done():
			log.Printf("🔴 [Worker %d] Stopped (processed %d tasks)", workerID, processedCount)
			return
		default:
			event, err := getTaskForWork(ctx, rdb)

			if err != nil {
				log.Println("getTaskForWork", err)
				continue
			}
			if event.TaskId == 0 {
				continue
			}
			processedCount++

			lnKey := getLnKey(event.TaskId)
			if isDuplicate(ctx, rdb, lnKey) {
				continue
			}

			msgHTML := getTgHtml(event)
			if msgHTML == "" {
				continue
			}
			err = sendMsgToTG(ctx, b, event.ChatId, msgHTML)
			if err != nil {
				continue
			}
			err = rdb.Set(ctx, lnKey, time.Now().Unix(), 15*time.Minute).Err()
			if err != nil {
				log.Printf("⚠️ [Worker %d] Failed to update notification time: %v", workerID, err)
			}
		}
	}
}

func sendMsgToTG(ctx context.Context, b TelegramSender, id int, html string) error {
	params := &bot.SendMessageParams{
		ChatID:    int64(id),
		Text:      html,
		ParseMode: "HTML",
	}
	_, err := b.SendMessage(ctx, params)
	return err

}

func getLnKey(id int) string {
	return "NotificationTime:" + strconv.Itoa(id)
}

func isDuplicate(ctx context.Context, rdb *redis.Client, lnKey string) bool {

	lastNotificationTimeStr, err := rdb.Get(ctx, lnKey).Result()

	if err == nil {
		t, _ := strconv.ParseInt(lastNotificationTimeStr, 10, 64)
		timeSince := time.Since(time.Unix(t, 0))
		if timeSince < 15*time.Minute {
			return true
		}

	}
	return false
}

func getTaskForWork(ctx context.Context, rdb *redis.Client) (NotificationEvent, error) {
	var event NotificationEvent

	res, err := rdb.BRPop(ctx, 5*time.Second, "tasks:queue").Result()

	if err != nil {
		return event, err
	}
	if err = json.Unmarshal([]byte(res[1]), &event); err != nil {
		return event, err
	}
	return event, nil
}

func getTgHtml(event NotificationEvent) string {
	var txt string

	switch event.Type {
	case "spread_detected":

		txt = fmt.Sprintf(
			"<b>Обнаружен спред %s %.2f%%</b>\n\n"+
				"🟢 LONG (%s): %.8f\n"+
				"🔴 SHORT (%s): %.8f",
			event.Name, event.SpreadPct,
			event.BestBidExch, event.BestBidPrice,
			event.BestAskExch, event.BestAskPrice,
		)
	default:
		txt = fmt.Sprintf("Неизвестный тип события: %s", event.Type)
	}

	return txt
}
