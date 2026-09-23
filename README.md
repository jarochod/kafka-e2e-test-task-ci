## Wymagania

- Docker + Docker Compose v2 (`docker compose`, nie `docker-compose`).
- Wolny port `9092` lokalnie (Kafka).
- Do pracy lokalnej nad testami (poza Dockerem): Node.js 20+.

## Uruchomienie środowiska

```bash
# Zbuduj obrazy i uruchom Kafkę + inicjalizację topiców + konsumenta
docker compose up --build -d

# Sprawdź logi konsumenta (powinien czekać na wiadomości)
docker compose logs -f consumer
```

## Ręczna demonstracja działania aplikacji (opcjonalnie, nie jest wymagane do zadania)

```bash
docker compose --profile demo run --rm producer
docker compose logs -f consumer
```

## Uruchomienie testów (tu pracujesz)

Po zmianach w kodzie testów lub helperach przebuduj obraz i uruchom testy jednym poleceniem:

```bash
docker compose --profile test build tests && docker compose --profile test run --rm tests
```

Przy ponownym uruchomieniu testów bez zmian w kodzie wystarczy:

```bash
docker compose --profile test run --rm tests
```

Na start wszystkie testy w `tests/__tests__/kafkaFlow.test.ts` są oznaczone
`it.skip(...)` — po zaimplementowaniu każdego testu zamień `it.skip` na `it`.

### Uruchomienie testów lokalnie (bez Dockera dla samych testów)

Przydatne przy pisaniu/debugowaniu — szybszy cykl niż przebudowa obrazu Dockera:

```bash
cd tests
npm install
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 npm test
```

Sprawdzenie typów bez uruchamiania testów:

```bash
cd tests
npx tsc --noEmit
```

## Zatrzymanie i sprzątanie środowiska

```bash
docker compose down -v
```

(`-v` czyści też dane Kafki — przydatne, gdy chcesz zacząć testy od zera).


---

## Twoje notatki (uzupełnij przed oddaniem zadania)

### Uwagi do uruchomienia

* Po zmianach w `tests/__tests__` lub `tests/src` obraz testowy należy przebudować przed uruchomieniem testów. Przy kolejnych uruchomieniach bez zmian można użyć istniejącego obrazu.
* Nie używam `--no-cache` przy normalnym cyklu pracy — standardowy Docker build poprawnie wykorzystuje cache.

### Podejście do testowania

* Testy sprawdzają pełny przepływ E2E: wysyłają wiadomości na `orders` i weryfikują wynik na `orders-processed` lub `orders-dlq`.
* Do synchronizacji używam `waitForMessage()` i `collectMessages()` z timeoutem zamiast stałych opóźnień. Matchery wykorzystują unikalne `orderId`, dzięki czemu testy nie dopasowują przypadkowo starszych wiadomości.
* Helpery są typowane generycznie (`waitForMessage<T>`, `collectMessages<T>`), a dla wiadomości przetworzonych i DLQ zdefiniowane są dedykowane typy. Granicę parsowania JSON pozostawiam jako `unknown`, a konkretny typ wiadomości określany jest po stronie wywołania helpera.
* Poprawiłem kolejność cleanupu w `waitForMessage()` i `collectMessages()`, tak aby `consumer.disconnect()` nie blokował zwrócenia wyniku do asercji.
* Testowe consumery mają unikalne `groupId` i czytają topic od początku, co zapewnia izolację testów.

### Znane ograniczenia / co zrobiłbym/zrobiłabym inaczej mając więcej czasu

* Negatywne testy, które sprawdzają brak wiadomości na topicu, muszą odczekać do końca ustawionego timeoutu. Dodatkowo graceful disconnect KafkaJS może wydłużyć rzeczywisty czas takiego testu.
* Testy są uruchamiane sekwencyjnie (`--runInBand`), co upraszcza izolację i ogranicza potencjalne problemy związane z równoległym dostępem do Kafki, ale nie maksymalizuje szybkości wykonania.
* Przy większej liczbie testów rozważyłbym dedykowane mechanizmy przygotowania/czyszczenia danych testowych, aby ograniczyć zależność od historii wiadomości na topicach.
