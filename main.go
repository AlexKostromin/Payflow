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
	ID          string    `json:"id" db:"id"`
	FromWallet  string    `json:"from_wallet" db:"from_wallet"`
	ToWallet    string    `json:"to_wallet" db:"to_wallet"`
	AmountCents int64     `json:"amount_cents" db:"amount_cents"`
	Currency    string    `json:"currency" db:"currency"`
	Status      string    `json:"status" db:"status"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
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
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// listPayments — GET /payments, список платежей с опциональной фильтрацией по статусу (?status=completed)
func (s *Server) listPayments(w http.ResponseWriter, r *http.Request) {
	// Читаем query-параметр status из URL (если не передан — вернём все платежи)
	status := r.URL.Query().Get("status")

	// Собираем SQL-запрос динамически: если status задан — добавляем WHERE
	query := `Select id, from_wallet, to_wallet, amount_cents, currency, status, created_at from payments`
	args := []any{}
	if status != "" {
		query += " where status = $1"
		args = append(args, status)
	}
	// Сортировка по дате (новые первые), лимит 20 записей
	query += ` order by created_at desc limit 20`

	// Выполняем запрос и собираем результат через CollectRows (автоматический маппинг по именам колонок)
	rows, err := s.pool.Query(r.Context(), query, args...)
	if err != nil {
		slog.Error("list payments", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	payments, err := pgx.CollectRows(rows, pgx.RowToStructByName[Payment])
	if err != nil {
		slog.Error("collect payments", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	writeJSON(w, http.StatusOK, payments)
}

func (s *Server) cancelPayment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var p Payment
	err := s.pool.QueryRow(r.Context(),
		`UPDATE payments SET status = 'cancelled'
		 WHERE id = $1 AND status = 'created'
		 RETURNING id, from_wallet, to_wallet, amount_cents, currency, status, created_at`, id).
		Scan(&p.ID, &p.FromWallet, &p.ToWallet, &p.AmountCents, &p.Currency, &p.Status, &p.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Проверяем: платёж не существует или уже не в статусе 'created'
		var currentStatus string
		err2 := s.pool.QueryRow(r.Context(), `SELECT status FROM payments WHERE id = $1`, id).Scan(&currentStatus)
		if err2 != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "payment not found"})
			return
		}
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "payment is already " + currentStatus})
		return
	}

	if err != nil {
		slog.Error("cancel payment", "error", err, "id", id)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	slog.Info("payment cancelled", "id", id)
	writeJSON(w, http.StatusOK, p)
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
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	// Валидация: оба кошелька обязательны
	if req.FromWallet == "" || req.ToWallet == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "from_wallet and to_wallet required"})
		return
	}
	// Нельзя отправить платёж самому себе
	if req.FromWallet == req.ToWallet {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "cannot pay yourself"})
		return
	}
	// Сумма должна быть положительной
	if req.AmountCents <= 0 {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "amount_cents must be positive"})
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
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create payment"})
		return
	}

	slog.Info("payment created", "id", p.ID, "amount", p.AmountCents, "currency", p.Currency)
	writeJSON(w, http.StatusCreated, p)
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
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "payment not found"})
		return
	}
	// Любая другая ошибка — 500
	if err != nil {
		slog.Error("get payment", "error", err, "id", id)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	slog.Info("payment fetched", "id", p.ID)
	writeJSON(w, http.StatusOK, p)
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
	mux.HandleFunc("GET /payments", server.listPayments)
	mux.HandleFunc("PUT /payments/{id}/cancel", server.cancelPayment)

	// Порт из переменной окружения или дефолт 8080
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Запускаем HTTP-сервер
	slog.Info("starting server", "port", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
