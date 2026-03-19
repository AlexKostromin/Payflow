package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Модели ---

// Payment — структура платежа, соответствует таблице payments в PostgreSQL
type Payment struct {
	ID          string    `json:"id"`
	FromWallet  string    `json:"from_wallet"`
	ToWallet    string    `json:"to_wallet"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

// --- Server: все зависимости в одном месте ---

// Server хранит пул соединений к БД; на нём определены все HTTP-обработчики
type Server struct {
	pool *pgxpool.Pool
}

// --- Хелпер: все ответы через JSON ---

// writeJSON устанавливает Content-Type, HTTP-статус и кодирует data в JSON
func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// --- Обработчики ---

// health — GET /health, проверка что сервер жив
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

// createPayment — POST /payments, создание нового платежа
func (s *Server) createPayment(w http.ResponseWriter, r *http.Request) {
	// Структура входящего запроса — только поля от клиента, без id/status/created_at
	var req struct {
		FromWallet  string `json:"from_wallet"`
		ToWallet    string `json:"to_wallet"`
		AmountCents int64  `json:"amount_cents"`
		Currency    string `json:"currency"`
	}

	// Парсим JSON из тела запроса
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}

	// Валидация: оба кошелька обязательны
	if req.FromWallet == "" || req.ToWallet == "" {
		writeJSON(w, 400, map[string]string{"error": "from_wallet and to_wallet required"})
		return
	}
	// Нельзя отправить платёж самому себе
	if req.FromWallet == req.ToWallet {
		writeJSON(w, 422, map[string]string{"error": "cannot pay yourself"})
		return
	}
	// Сумма должна быть положительной
	if req.AmountCents <= 0 {
		writeJSON(w, 422, map[string]string{"error": "amount_cents must be positive"})
		return
	}

	// Вставляем платёж в БД; RETURNING возвращает все поля включая сгенерированные (id, status, created_at)
	var p Payment
	err := s.pool.QueryRow(r.Context(),
		`INSERT INTO payments (from_wallet, to_wallet, amount_cents, currency)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id, from_wallet, to_wallet, amount_cents, currency, status, created_at`,
		req.FromWallet, req.ToWallet, req.AmountCents, req.Currency,
	).Scan(&p.ID, &p.FromWallet, &p.ToWallet, &p.AmountCents, &p.Currency, &p.Status, &p.CreatedAt)

	if err != nil {
		slog.Error("create payment", "error", err)
		writeJSON(w, 500, map[string]string{"error": "failed to create payment"})
		return
	}

	slog.Info("payment created", "id", p.ID, "amount", p.AmountCents, "currency", p.Currency)
	writeJSON(w, 201, p)
}

// getPayment — GET /payments/{id}, получение платежа по UUID
func (s *Server) getPayment(w http.ResponseWriter, r *http.Request) {
	// Извлекаем id из URL-пути (например, /payments/550e8400-... → id = "550e8400-...")
	id := r.PathValue("id")

	// Ищем платёж в БД
	var p Payment
	err := s.pool.QueryRow(r.Context(),
		`SELECT id, from_wallet, to_wallet, amount_cents, currency, status, created_at
		 FROM payments WHERE id = $1`, id,
	).Scan(&p.ID, &p.FromWallet, &p.ToWallet, &p.AmountCents, &p.Currency, &p.Status, &p.CreatedAt)

	// Если строка не найдена — 404
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "payment not found"})
		return
	}
	// Любая другая ошибка — 500
	if err != nil {
		slog.Error("get payment", "error", err, "id", id)
		writeJSON(w, 500, map[string]string{"error": "internal error"})
		return
	}

	slog.Info("payment fetched", "id", p.ID)
	writeJSON(w, 200, p)
}

// --- main ---

func main() {
	ctx := context.Background()

	// DSN для подключения к PostgreSQL: берём из переменной окружения или используем дефолт
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:secret@localhost:5432/payflow?sslmode=disable"
	}

	// Создаём пул соединений к БД
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	// Проверяем, что БД доступна
	if err := pool.Ping(ctx); err != nil {
		log.Fatal("cannot connect to postgres:", err)
	}
	slog.Info("connected to postgres")

	// Создаём сервер с пулом соединений и регистрируем маршруты
	server := &Server{pool: pool}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.health)
	mux.HandleFunc("POST /payments", server.createPayment)
	mux.HandleFunc("GET /payments/{id}", server.getPayment)

	// Порт из переменной окружения или дефолт 8080
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Запускаем HTTP-сервер
	slog.Info("starting server", "port", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
