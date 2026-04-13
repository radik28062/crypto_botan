package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

var conn *pgx.Conn

type spreadMonitor struct {
	id        int
	name      string
	min       float64
	max       float64
	userId    int
	exchanges map[string]string
}

func (m *spreadMonitor) save(ctx context.Context, conn *pgx.Conn) {

	exch := make([]string, 0, len(m.exchanges))
	for k, v := range m.exchanges {
		exch = append(exch, k+":"+v)
	}
	query := `INSERT INTO monitor_tasks (name, min, max, exchanges,user_id,created_at) VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) RETURNING id`
	err := conn.QueryRow(ctx, query, m.name, m.min, m.max, exch, m.userId).Scan(&m.id)
	if err != nil {
		log.Printf("Ошибка при отправке сообщения: %v \n", err)
	}

}

type Config struct {
	DBURL   string
	TGToken string
}

func LoadConfig() (Config, error) {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found")
	}

	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		return Config{}, fmt.Errorf("DB_URL is empty")
	}

	tgToken := os.Getenv("TG_TOKEN")
	if tgToken == "" {
		return Config{}, fmt.Errorf("TG_TOKEN is empty")
	}

	return Config{
		DBURL:   dbURL,
		TGToken: tgToken,
	}, nil
}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	b, err := bot.New(cfg.TGToken)
	if err != nil {
		log.Fatal("Ошибка при инициализации бота", err)
	}
	conn, err = pgx.Connect(context.Background(), cfg.DBURL)
	if err != nil {
		log.Fatal("Ошибка при подключения к базе", err)
	}
	defer conn.Close(context.Background())

	b.RegisterHandler(bot.HandlerTypeMessageText, "/start", bot.MatchTypeExact, startHandler)
	b.RegisterHandler(bot.HandlerTypeMessageText, "/help", bot.MatchTypeExact, helpHandler)

	b.RegisterHandler(bot.HandlerTypeMessageText, "монитор", bot.MatchTypePrefix, monitorHandler)
	b.RegisterHandler(bot.HandlerTypeMessageText, "Монитор", bot.MatchTypePrefix, monitorHandler)

	b.RegisterHandler(bot.HandlerTypeMessageText, "%", bot.MatchTypePrefix, textHandler)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	fmt.Println("Бот запущен. Нажмите Ctrl+C для остановки.")

	b.Start(ctx)
}

func monitorHandler(ctx context.Context, b *bot.Bot, update *models.Update) {
	userMsg := update.Message.Text
	fmt.Println("Пришло сообщение ", userMsg)
	monitor := &spreadMonitor{}
	chatId := update.Message.Chat.ID
	var userID int
	query := "SELECT id FROM users WHERE chatid = $1"
	err := conn.QueryRow(ctx, query, chatId).Scan(&userID)

	if err != nil {
		log.Printf("Пользователь не найден: %v", err)
		return
	}
	monitor.userId = userID
	fmt.Println("Пользователь найден ", userMsg)

	monitor.exchanges = make(map[string]string)
	lines := strings.Split(userMsg, "\n")

	re := regexp.MustCompile(`https?://(?:[^@\n]+@)?([^:/?\n]+)`)
	exchanges := map[string]*regexp.Regexp{
		"binance.com": regexp.MustCompile(`/futures/([A-Z]+)USDT`),
		"gate.com":    regexp.MustCompile(`/futures/USDT/([A-Z]+)_USDT`),
		"mexc.com":    regexp.MustCompile(`/futures/([A-Z]+)_USDT`),
	}

	for i, line := range lines {
		if i == 0 {
			firstLineParts := strings.Split(line, " ")
			for _, part := range firstLineParts {

				tempText := strings.TrimSpace(part)
				if tempText == "" {
					continue
				} else if tempText[0] == 60 { // меньше

					numStr := strings.TrimPrefix(tempText, "<")
					numStr = strings.Replace(numStr, ",", ".", 1)
					numValue, err := strconv.ParseFloat(numStr, 64)
					monitor.max = numValue
					if err != nil {
						log.Println("Ошибка парсинга задания для монитора, меньше", err)
						return
					}
				} else if tempText[0] == 62 { // больше

					numStr := strings.TrimPrefix(tempText, ">")
					numStr = strings.Replace(numStr, ",", ".", 1)
					numValue, err := strconv.ParseFloat(numStr, 64)
					monitor.min = numValue
					if err != nil {
						log.Println("Ошибка парсинга задания для монитора, больше", err)
						return
					}
				} else if strings.ToLower(part) != "монитор" {
					monitor.name = strings.TrimSpace(part)
				}
			}

		} else { //ссылки
			matches := re.FindStringSubmatch(line)

			if len(matches) > 1 {
				domainParts := strings.Split(strings.TrimPrefix(matches[1], "www."), ".")
				domain := domainParts[0]

				reg, ok := exchanges[domain]
				if ok {
					coinMatches := reg.FindStringSubmatch(line)
					if len(coinMatches) > 1 {
						monitor.exchanges[domain] = coinMatches[1]
					} else {
						log.Println("Ошибка парсинга монеты для монитора:", line)
					}

				} else {
					log.Println("Ошибка парсинга задания для монитора, не поддерживается биржа: ", domain)
				}
			}
		}

	}

	if len(monitor.exchanges) > 0 && (monitor.min > 0 || monitor.max > 0) {
		monitor.save(ctx, conn)
	} else {
		send_msg_to_user(ctx, b, update, "Неверный формат!", "error")
	}
}

func send_msg_to_user(ctx context.Context, b *bot.Bot, update *models.Update, msg string, msgType string) {

	if msgType == "error" {
		msg = "❌ " + msg
	}
	_, err := b.SendMessage(ctx, &bot.SendMessageParams{
		ChatID:    update.Message.Chat.ID,
		Text:      msg,
		ParseMode: models.ParseModeHTML,
	})
	if err != nil {
		log.Println("Ошибка при отправке сообщения: ", err)
	}
}

func textHandler(ctx context.Context, b *bot.Bot, update *models.Update) {
	msg := update.Message.Text
	if strings.HasPrefix(msg, "%") {
		// высчитываем спред цены по запросу пользователя.
		msg = strings.TrimSpace(strings.TrimPrefix(msg, "%"))
		msg = strings.Replace(msg, ",", ".", -1)
		parts := strings.Fields(msg)
		if len(parts) != 3 {

			send_msg_to_user(ctx, b, update, "Неверный формат!", "error")
		} else {
			min, err := strconv.ParseFloat(parts[0], 64)
			if err != nil {
				send_msg_to_user(ctx, b, update, fmt.Sprintf("Некорректное число: %s", parts[0]), "error")
				return
			}

			max, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				send_msg_to_user(ctx, b, update, fmt.Sprintf("Некорректное число: %s", parts[1]), "error")
				return
			}

			if min > max {
				min, max = max, min
			}
			count, err := strconv.ParseFloat(parts[2], 64)
			if err != nil {
				send_msg_to_user(ctx, b, update, fmt.Sprintf("Некорректное число: %s", parts[2]), "error")
				return
			}
			prc := 100 * (max - min) / min
			profit := count * (max - min)

			send_msg_to_user(ctx, b, update, fmt.Sprintf("Spread: <b>%.2f%%</b> Profit: <b>$%.2f</b>", prc, profit), "successes")
		}

	}
}
func getHelpMessage() string {
	return `🌟 <b>Крипто Ботан</b> — твой помощник в мире криптотрейдинга! 🌟

📊 <b>Расчёт спреда</b>
Отправь сообщение в формате:
<code>% цена1 цена2 количество</code>

<b>Пример:</b>
<code>% 0.2 0.3 500</code>

Где:
• цена1 и цена2 — цены для сравнения
• количество — объём позиции в монетах

📈 <b>Мониторинг спреда</b>
Чтобы добавить мониторинг, отправь:

<code>Монитор название &gt;мин &lt;макс
ссылка1
ссылка2
ссылка3</code>

<b>Пример:</b>
<code>Монитор PTB &gt;0.5 &lt;5
https://www.binance.com/ru/futures/PTBUSDT
https://www.gate.com/ru/futures/USDT/PTB_USDT
https://www.mexc.com/uk-UA/futures/PTB_USDT</code>

• название — придумай сам, например PTB или BTC
• <code>&gt;мин</code> — нижняя граница спреда, например <code>&gt;2</code>
• <code>&lt;макс</code> — верхняя граница спреда, например <code>&lt;5</code>

💡 <b>Как это работает</b>
Бот будет отслеживать спред на указанных биржах и пришлёт уведомление, когда значение войдёт в заданный диапазон.

✅ <b>Готово!</b> Теперь ты всегда будешь в курсе выгодных возможностей 🚀`
}

func helpHandler(ctx context.Context, b *bot.Bot, update *models.Update) {

	helpMessage := getHelpMessage()

	_, err := b.SendMessage(ctx, &bot.SendMessageParams{
		ChatID:    update.Message.Chat.ID,
		Text:      helpMessage,
		ParseMode: models.ParseModeHTML,
	})
	if err != nil {
		log.Println("Ошибка при отправке сообщения: ", err)
	}
}

func startHandler(ctx context.Context, b *bot.Bot, update *models.Update) {
	if update == nil || update.Message == nil {
		log.Println("startHandler: update/message/chat is nil")
		return
	}

	_, err := b.SendMessage(ctx, &bot.SendMessageParams{
		ChatID: update.Message.Chat.ID,
		Text:   "Привет! Я крипто ботан 👋.\nВышли мне сообщение в формате % 0,2 0,3 500, где 0,2 и 0,3 цены, а 500 - это объем позиции в монетах.",
	})
	if err != nil {
		log.Println("Ошибка при отправке сообщения: ", err)
	}

	var username, firstName, lastName string
	if update.Message.From != nil {
		username = update.Message.From.Username
		firstName = update.Message.From.FirstName
		lastName = update.Message.From.LastName
	} else {
		log.Println("startHandler: update.Message.From is nil")
	}

	query := `INSERT INTO users (chatid, username, name, last_name, last_active)
		VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
		ON CONFLICT (chatid)
		DO UPDATE SET username = $2, last_active = CURRENT_TIMESTAMP`

	_, err = conn.Exec(ctx, query, update.Message.Chat.ID, username, firstName, lastName)
	if err != nil {
		log.Println("Ошибка при сохранении пользователя: ", err)
	}
}
