# PubSub

`PubSub` поддерживает:
- Подписку/отписку, закрытие
- Graceful shutdown
- Логирование через `log/slog`
- Можно безопасно конкурентно много раз закрывать `PubSub`, отписываться от событий, вызывать `Publish/Subscribe` на закрытом `PubSub`
- Api по Grpc

---

## Как работает
### 1. Создание подписки
При `ps.Subscribe(subject, cb)`:
1. Создаётся `subscriptionImpl` с двумя каналами:
   * `messages` (buffered, размер `queueSize`)
   * `stream` (unbuffered)
2. Добавляем подписку в `data[subject]` под защитой `mutex`.
3. Запускаем две горутины:
   * **Reader** (`start()`):
     * Читает из `messages`, накапливает сообщения в срез `buffer`, либо отправляет сообщения в `stream`
     * При `close(messages)` выгребает остаток, логирует и закрывает `stream`.
     * Благодаря такому подходу, даже если подписчик медленный, публикатор долго ждать не будет (даже если канал переполнен), так как из канала сообщения будут попадать в буфер.
   * **Handler**: читает `for msg := range stream`, вызывает `cb(msg)`.
4. Увеличиваем `sync.WaitGroup` на 2, чтобы поддержать в close подождать завершение всех воркеров.

### 2. Публикация
Метод `Publish(subject, msg)`:
* Под RLock берёт слайс подписок `data[subject]`.
* Для каждой подписки вызывает `send(msg)`:
  * Внутри rw-блокировка, проверяет флаг `unsubscribed`.
  * Шлёт в `messages` сообщение.
* Если `Close` уже вызван, возвращает `ErrSubPubAlreadyClosed`.

### 3. Отписка
`subscriptionImpl.Unsubscribe()`:
* С эксклюзивной блокировкой выставляет `unsubscribed = true` и **закрывает** канал `messages`.
* Это триггерит окончание `start()`, он очистит буфер и закроет `stream`.

### 4. Закрытие
`subPubImpl.Close(ctx)`:
1. Под блокировкой проверяет и выставляет `closed = true`; вызывает `Unsubscribe()` для всех подписок.
2. Запускает горутину, которая ждёт `wg.Wait()` (завершения всех `start` + handler).
3. В `select` блокируется до `<-done` (успех) или `ctx.Done()` (таймаут).
4. Новые вызовы `Publish`/`Subscribe` после этого вернут ошибку.

---

## Конфигурация

```yaml
grpc_port: 0.0.0.0:50051 //по умолчанию :50051
queue_size: 256 //размер канала, в который отпрявляет сообщения публикатор (если не указывать, то 256)
log_level: debug //по умолчанию INFO
```

---

## Запуск

- Собрать докер образ:
    ```bash
    $ docker build -t pubsub:latest .
    ```
- Запустить контейнер:
    ```bash
    $ docker run --rm -p 50051:50051 -v ./config:/app/config -e CONFIG_PATH=./config/local.yaml pubsub:latest
    ```
- Либо если установлен `task` (https://taskfile.dev), то:
    ```bash
    $ task docker-build
    $ task docker-run
    ```