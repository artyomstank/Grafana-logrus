# Используем официальный образ Go
FROM golang:1.24-alpine

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем go.mod и go.sum для кеширования зависимостей
COPY go.mod go.sum ./

# Скачиваем зависимости
RUN go mod download

# Копируем весь код
COPY . .

# Собираем бинарник продьюсера
RUN go build -o producer ./cmd

# Команда по умолчанию
CMD ["./producer"]
