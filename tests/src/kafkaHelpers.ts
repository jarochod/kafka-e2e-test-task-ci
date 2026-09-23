/**
 * Helpery do testów Kafki.
 *
 * Ten plik jest już GOTOWY — nie musisz go modyfikować (choć oczywiście
 * możesz, jeśli uważasz, że coś warto poprawić lub rozszerzyć — to też
 * będzie pozytywnie ocenione, jeśli uzasadnisz zmianę w README).
 *
 * Do Twojej dyspozycji:
 *   - getProducer() / disconnectProducer() — połączony klient producenta.
 *   - makeTestOrder(overrides) — buduje przykładowe, poprawne zamówienie.
 *   - sendOrder(producer, order, topic?) — wysyła zamówienie (obiekt) na topic.
 *   - sendRaw(producer, rawValue, key?, topic?) — wysyła surowe dane
 *     (przydatne np. do testowania niepoprawnego JSON-a).
 *   - waitForMessage(topic, matchFn, timeoutSeconds?) — czeka aż na danym
 *     topicu pojawi się wiadomość spełniająca matchFn(value) -> boolean.
 *     Zwraca sparsowany obiekt albo null, jeśli nie znaleziono w limicie czasu.
 *   - collectMessages(topic, matchFn, expectedCount, timeoutSeconds?)
 *     — zbiera do expectedCount wiadomości spełniających matchFn.
 *
 * Każde wywołanie waitForMessage / collectMessages tworzy konsumenta
 * z UNIKALNYM groupId i czyta topic od początku (fromBeginning: true)
 * — dzięki temu testy są od siebie izolowane i nie interferują ze sobą,
 * niezależnie od kolejności uruchomienia.
 */
import { Kafka, Producer, Consumer, EachMessagePayload } from "kafkajs";
import { randomUUID } from "crypto";

export const BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092";
export const ORDERS_TOPIC = "orders";
export const ORDERS_PROCESSED_TOPIC = "orders-processed";
export const ORDERS_DLQ_TOPIC = "orders-dlq";

const DEFAULT_POLL_TIMEOUT_SECONDS = 20;

const kafka = new Kafka({
  clientId: "kafka-qa-tests",
  brokers: [BOOTSTRAP_SERVERS],
  retry: {
    initialRetryTime: 2000,
    retries: 30,
  },
});

export interface Order {
  orderId: string;
  customer: string;
  amount: number;
  status: string;
}

export interface ProcessedOrder extends Order {
  status: "PROCESSED";
}

export interface DlqMessage {
  error: string;
  originalMessage?: Partial<Order>;
  raw?: string;
}

export type MatchFn<T> = (value: T) => boolean;

let sharedProducer: Producer | null = null;

/** Zwraca połączonego, współdzielonego producenta (tworzy go przy pierwszym wywołaniu). */
export async function getProducer(): Promise<Producer> {
  if (!sharedProducer) {
    sharedProducer = kafka.producer();
    await sharedProducer.connect();
  }
  return sharedProducer;
}

/** Rozłącza współdzielonego producenta — wywołaj w afterAll(). */
export async function disconnectProducer(): Promise<void> {
  if (sharedProducer) {
    await sharedProducer.disconnect();
    sharedProducer = null;
  }
}

/** Buduje przykładowe, poprawne zamówienie testowe z możliwością nadpisania pól. */
export function makeTestOrder(overrides: Partial<Order> = {}): Order {
  return {
    orderId: randomUUID(),
    customer: "Test Testowski",
    amount: 123.45,
    status: "NEW",
    ...overrides,
  };
}

/** Wysyła zamówienie (obiekt) jako JSON na wskazany topic (domyślnie 'orders'). */
export async function sendOrder(
  producer: Producer,
  order: Partial<Order>,
  topic: string = ORDERS_TOPIC
): Promise<void> {
  await producer.send({
    topic,
    messages: [{ key: order.orderId, value: JSON.stringify(order) }],
  });
}

/**
 * Wysyła surowe (potencjalnie niepoprawne) dane, np. zepsuty JSON.
 * Przydatne do testowania obsługi błędnych wiadomości.
 */
export async function sendRaw(
  producer: Producer,
  rawValue: string | Buffer,
  key?: string,
  topic: string = ORDERS_TOPIC
): Promise<void> {
  await producer.send({
    topic,
    messages: [{ key, value: rawValue }],
  });
}

function newConsumer(): Consumer {
  return kafka.consumer({ groupId: `test-${randomUUID()}` });
}

function tryParse(value: Buffer | null): unknown | null {
  if (!value) return null;
  try {
    return JSON.parse(value.toString("utf-8"));
  } catch {
    return null;
  }
}

/**
 * Czeka aż w danym topicu pojawi się wiadomość spełniająca matchFn(value) -> boolean.
 * Zwraca sparsowaną wiadomość albo null, jeśli nie znaleziono w limicie czasu.
 */
export async function waitForMessage<T>(
  topic: string,
  matchFn: MatchFn<T>,
  timeoutSeconds: number = DEFAULT_POLL_TIMEOUT_SECONDS
): Promise<T | null> {
  const consumer = newConsumer();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  return new Promise((resolve, reject) => {
    let settled = false;

    const finish = (result: T | null) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);

      setImmediate(() => {
        consumer
          .disconnect()
          .then(() => resolve(result))
          .catch(reject);
      });
    };

    const timer = setTimeout(() => {
      finish(null);
    }, timeoutSeconds * 1000);

    consumer
      .run({
        eachMessage: async ({ message }: EachMessagePayload) => {
          if (settled) return;

          const parsed = tryParse(message.value);
          if (parsed !== null && matchFn(parsed as T)) {
            finish(parsed as T);
          }
        },
      })
      .catch((err) => {
        if (!settled) {
          clearTimeout(timer);
          reject(err);
        }
      });
  });
}

/** Zbiera do expectedCount wiadomości spełniających matchFn w limicie czasu. */
export async function collectMessages<T>(
  topic: string,
  matchFn: MatchFn<T>,
  expectedCount: number,
  timeoutSeconds: number = DEFAULT_POLL_TIMEOUT_SECONDS
): Promise<T[]> {
  const consumer = newConsumer();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  const found: T[] = [];

  return new Promise((resolve, reject) => {
    let settled = false;

    const finish = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);

      setImmediate(() => {
        consumer
          .disconnect()
          .then(() => resolve(found))
          .catch(reject);
      });
    };

    const timer = setTimeout(() => {
      finish();
    }, timeoutSeconds * 1000);

    consumer
      .run({
        eachMessage: async ({ message }: EachMessagePayload) => {
          if (settled) return;

          const parsed = tryParse(message.value);
          if (parsed !== null && matchFn(parsed as T)) {
            found.push(parsed as T);
            if (found.length >= expectedCount) {
              finish();
            }
          }
        },
      })
      .catch((err) => {
        if (!settled) {
          clearTimeout(timer);
          reject(err);
        }
      });
  });
}