package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeKafkaReader struct {
	msgs          []kafka.Message
	readErr       error
	commitErr     error
	committedMsgs []kafka.Message
	readCalls     int
	commitCalls   int
}

func (f *fakeKafkaReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	f.readCalls++
	if f.readErr != nil {
		return kafka.Message{}, f.readErr
	}
	if len(f.msgs) == 0 {
		return kafka.Message{}, errors.New("no msgs")
	}
	msg := f.msgs[0]
	f.msgs = f.msgs[1:]
	return msg, nil
}
func (f *fakeKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.commitCalls++
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committedMsgs = append(f.committedMsgs, msgs...)
	return nil
}
func TestSendTaskFromKafkaToRedis_Success(t *testing.T) {
	ctx := context.Background()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()
	reader := &fakeKafkaReader{
		msgs: []kafka.Message{
			{
				Value: []byte("task_id:125"),
			},
			{
				Value: []byte(""),
			},
			{
				Value: []byte(`Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
`),
			},
		},
	}
	processedCount, err := sendTaskFromKafkaToRedis(ctx, reader, rdb, 0)
	require.NoError(t, err)
	processedCount, err = sendTaskFromKafkaToRedis(ctx, reader, rdb, processedCount)
	require.NoError(t, err)
	processedCount, err = sendTaskFromKafkaToRedis(ctx, reader, rdb, processedCount)
	require.NoError(t, err)

	assert.Equal(t, int64(3), processedCount)
	assert.Equal(t, 3, reader.readCalls)
	assert.Equal(t, 3, reader.commitCalls)
	assert.Len(t, reader.committedMsgs, 3)

	queueLen, err := rdb.LLen(ctx, "tasks:queue").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(3), queueLen)

	taskTxt, err := rdb.LPop(ctx, "tasks:queue").Result()
	require.NoError(t, err)
	assert.Equal(t, "task_id:125", taskTxt)

	taskTxt, err = rdb.LPop(ctx, "tasks:queue").Result()
	require.NoError(t, err)
	assert.Equal(t, ``, taskTxt)

	taskTxt, err = rdb.LPop(ctx, "tasks:queue").Result()
	require.NoError(t, err)
	assert.Equal(t, `Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum
`, taskTxt)

}

func Test_getWorkersDelta(t *testing.T) {

	cfg := Config{
		MinWorker: 1,
		MaxWorker: 5,
	}

	tests := []struct {
		name        string
		taskCount   int
		workerCount int
		perWorker   int
		want        int
	}{
		{
			name:        "нужно добавить одного воркера",
			taskCount:   150,
			workerCount: 1,
			perWorker:   100,
			want:        1,
		},
		{
			name:        "ничего менять не нужно",
			taskCount:   200,
			workerCount: 2,
			perWorker:   100,
			want:        0,
		},
		{
			name:        "нужно убрать одного воркера",
			taskCount:   100,
			workerCount: 2,
			perWorker:   100,
			want:        -1,
		},
		{
			name:        "не опускаться ниже MinWorker",
			taskCount:   0,
			workerCount: 3,
			perWorker:   100,
			want:        -2,
		},
		{
			name:        "подняться до MinWorker если воркеров слишком мало",
			taskCount:   0,
			workerCount: 0,
			perWorker:   100,
			want:        1,
		},
		{
			name:        "не превышать MaxWorker",
			taskCount:   1000,
			workerCount: 2,
			perWorker:   100,
			want:        3,
		},
		{
			name:        "уменьшить если воркеров уже больше MaxWorker",
			taskCount:   1000,
			workerCount: 7,
			perWorker:   100,
			want:        -2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getWorkersDelta(tt.taskCount, tt.workerCount, tt.perWorker, cfg)
			assert.Equal(t, tt.want, got)
		})
	}
}
func TestGetWorkerCount(t *testing.T) {
	cancel1 := func() {}
	cancel2 := func() {}
	cancel3 := func() {}

	tests := []struct {
		name    string
		cancels []context.CancelFunc
		want    int
	}{
		{
			name:    "Пустой слайс",
			cancels: nil,
			want:    0,
		},
		{
			name:    "1",
			cancels: []context.CancelFunc{cancel1},
			want:    1,
		},
		{
			name:    "3",
			cancels: []context.CancelFunc{cancel1, cancel2, cancel3},
			want:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getWorkerCount(tt.cancels)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_getLnKey(t *testing.T) {

	tests := []struct {
		name string
		id   int
		want string
	}{
		{
			name: "1",
			id:   1,
			want: "NotificationTime:1",
		},
		{
			name: "852145628452369",
			id:   852145628452369,
			want: "NotificationTime:852145628452369",
		},
		{
			name: "-9999",
			id:   -9999,
			want: "NotificationTime:-9999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getLnKey(tt.id)
			assert.Equal(t, tt.want, got)
		})
	}
}
func TestGetTgHtml(t *testing.T) {
	tests := []struct {
		name  string
		event NotificationEvent
		want  string
	}{
		{
			name: "spread detected",
			event: NotificationEvent{
				Name:         "BTC",
				SpreadPct:    1.25,
				BestBidExch:  "binance",
				BestBidPrice: 65000.12345678,
				BestAskExch:  "gate",
				BestAskPrice: 64900.87654321,
				Type:         "spread_detected",
			},
			want: "<b>Обнаружен спред BTC 1.25%</b>\n\n🟢 LONG (binance): 65000.12345678\n🔴 SHORT (gate): 64900.87654321",
		},
		{
			name: "unknown type",
			event: NotificationEvent{
				Type: "unknown_event",
			},
			want: "Неизвестный тип события: unknown_event",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getTgHtml(tt.event)
			assert.Equal(t, tt.want, got)
		})
	}
}
func TestGetTaskCount(t *testing.T) {
	ctx := context.Background()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()

	err = rdb.RPush(ctx, "tasks:queue", "0", "1", "2").Err()
	require.NoError(t, err)

	got, err := getTaskCount(ctx, rdb)
	require.NoError(t, err)
	assert.Equal(t, 3, got)
}

func TestIsDuplicate(t *testing.T) {
	ctx := context.Background()

	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()

	t.Run("Ключ не существует", func(t *testing.T) {
		got := isDuplicate(ctx, rdb, "NotificationTime:1")
		assert.False(t, got)
	})

	t.Run("Дубль", func(t *testing.T) {
		err := rdb.Set(ctx, "NotificationTime:2", time.Now().Unix(), 0).Err()
		require.NoError(t, err)

		got := isDuplicate(ctx, rdb, "NotificationTime:2")
		assert.True(t, got)
	})

	t.Run("Уже не дубль", func(t *testing.T) {
		oldTime := time.Now().Add(-16 * time.Minute).Unix()
		err := rdb.Set(ctx, "NotificationTime:3", oldTime, 0).Err()
		require.NoError(t, err)

		got := isDuplicate(ctx, rdb, "NotificationTime:3")
		assert.False(t, got)
	})
}

type fakeTelegramSender struct {
	calls  int
	params *bot.SendMessageParams
	err    error
}

func (f *fakeTelegramSender) SendMessage(ctx context.Context, params *bot.SendMessageParams) (*models.Message, error) {
	f.calls++
	f.params = params

	if params.ChatID == int64(9999) {
		return nil, errors.New("chat not exist")

	}
	return nil, nil
}

func TestSendMsgToTG(t *testing.T) {
	ctx := context.Background()

	fakeBot := &fakeTelegramSender{}
	t.Run("success", func(t *testing.T) {

		err := sendMsgToTG(ctx, fakeBot, 12345, "<b>Hello</b>")

		require.NoError(t, err)
		assert.Equal(t, 1, fakeBot.calls)
		require.NotNil(t, fakeBot.params)
		assert.Equal(t, int64(12345), fakeBot.params.ChatID)
		assert.Equal(t, "<b>Hello</b>", fakeBot.params.Text)
		assert.Equal(t, models.ParseMode("HTML"), fakeBot.params.ParseMode)
	})

	t.Run("send error chat not exist ", func(t *testing.T) {

		err := sendMsgToTG(ctx, fakeBot, 9999, "test message")

		require.Error(t, err)
		assert.Equal(t, 2, fakeBot.calls)
	})
}

func TestGetTaskForWork(t *testing.T) {
	ctx := context.Background()

	t.Run("valid json", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		rdb := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})
		defer rdb.Close()

		taskJSON := `{
			"task_id": 11,
			"user_id": 22,
			"chat_id": 33,
			"name": "BTC",
			"spread_pct": 1.5,
			"best_ask_exch": "gate",
			"best_ask_price": 65000.12,
			"best_bid_exch": "binance",
			"best_bid_price": 65100.34,
			"created_at": 1710000000,
			"type": "spread_detected"
		}`

		err = rdb.RPush(ctx, "tasks:queue", taskJSON).Err()
		require.NoError(t, err)

		event, err := getTaskForWork(ctx, rdb)

		require.NoError(t, err)
		assert.Equal(t, 11, event.TaskId)
		assert.Equal(t, 22, event.UserId)
		assert.Equal(t, 33, event.ChatId)
		assert.Equal(t, "BTC", event.Name)
		assert.Equal(t, 1.5, event.SpreadPct)
		assert.Equal(t, "gate", event.BestAskExch)
		assert.Equal(t, 65000.12, event.BestAskPrice)
		assert.Equal(t, "binance", event.BestBidExch)
		assert.Equal(t, 65100.34, event.BestBidPrice)
		assert.Equal(t, int64(1710000000), event.CreatedAt)
		assert.Equal(t, "spread_detected", event.Type)
	})

	t.Run("invalid json", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		rdb := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})
		defer rdb.Close()

		err = rdb.RPush(ctx, "tasks:queue", "not-json").Err()
		require.NoError(t, err)

		event, err := getTaskForWork(ctx, rdb)

		require.Error(t, err)
		assert.Equal(t, NotificationEvent{}, event)
	})

	t.Run("empty queue", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		rdb := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})
		defer rdb.Close()

		shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		event, err := getTaskForWork(shortCtx, rdb)

		require.Error(t, err)
		assert.Equal(t, NotificationEvent{}, event)
	})
}
