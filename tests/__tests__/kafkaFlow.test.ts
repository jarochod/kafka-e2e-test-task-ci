/**
 * TWOJE ZADANIE: zaimplementuj testy.
 *
 * Aplikacja (producer + consumer) jest już gotowa i działająca — Twoim
 * zadaniem jest napisanie testów E2E weryfikujących jej zachowanie,
 *
 * Masz do dyspozycji gotowe helpery z `src/kafkaHelpers.ts` (opisane
 * w komentarzach tego pliku) — możesz z nich korzystać, ale nie musisz:
 * jeśli uważasz, że lepiej napisać coś inaczej, zrób to i uzasadnij w README.
 *
 */
import { Producer } from "kafkajs";
import {
  ORDERS_DLQ_TOPIC,
  ORDERS_PROCESSED_TOPIC,
  ORDERS_TOPIC,
  Order,
  collectMessages,
  disconnectProducer,
  getProducer,
  makeTestOrder,
  sendOrder,
  sendRaw,
  waitForMessage,
} from "../src/kafkaHelpers";

describe("Przepływ przetwarzania zamówień przez Kafkę", () => {
  let producer: Producer;

  beforeAll(async () => {
    producer = await getProducer();
  });

  afterAll(async () => {
    await disconnectProducer();
  });

  it("przetwarza poprawną wiadomość (happy path)", async () => {
    const order = makeTestOrder();

    await sendOrder(producer, order);

    const result = await waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      (message) => message.orderId === order.orderId,
    );

    expect(result).not.toBeNull();
    expect(result.orderId).toBe(order.orderId);
    expect(result.status).toBe("PROCESSED");
  });

  it("zachowuje oryginalne dane w przetworzonej wiadomości", async () => {
    const order = makeTestOrder({
      customer: "Jan Kowalski",
      amount: 456.78,
    });

    await sendOrder(producer, order);

    const result = await waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      (message) => message.orderId === order.orderId,
    );

    expect(result).not.toBeNull();
    expect(result.orderId).toBe(order.orderId);
    expect(result.customer).toBe(order.customer);
    expect(result.amount).toBe(order.amount);
    expect(result.status).toBe("PROCESSED");
  });

  it("przetwarza wiele wiadomości bez utraty i duplikacji", async () => {
    const orders = Array.from({ length: 10 }, () => makeTestOrder());

    for (const order of orders) {
      await sendOrder(producer, order);
    }

    const orderIds = new Set(orders.map((order) => order.orderId));

    const results = await collectMessages(
      ORDERS_PROCESSED_TOPIC,
      (message) => orderIds.has(message.orderId),
      orders.length,
    );

    expect(results).toHaveLength(orders.length);

    const processedOrderIds = results.map((message) => message.orderId);

    expect(new Set(processedOrderIds).size).toBe(orders.length);

    for (const order of orders) {
      const result = results.find(
        (message) => message.orderId === order.orderId,
      );

      expect(result).toBeDefined();
      expect(result.status).toBe("PROCESSED");
    }
  });

  it("wysyła wiadomość z brakującym polem do DLQ", async () => {
  const order = makeTestOrder();
  const { amount: _amount, ...invalidOrder } = order;

  await sendRaw(producer, JSON.stringify(invalidOrder), order.orderId);

  const dlqResult = await waitForMessage(
    ORDERS_DLQ_TOPIC,
    (message) =>
      message.originalMessage?.orderId === order.orderId,
  );

  expect(dlqResult).not.toBeNull();
  expect(dlqResult.error).toBe("Brakujące pola: [amount]");
  expect(dlqResult.originalMessage).toEqual(invalidOrder);

  const processedResult = await waitForMessage(
    ORDERS_PROCESSED_TOPIC,
    (message) => message.orderId === order.orderId,
    2,
  );

  expect(processedResult).toBeNull();
});

it("wysyła niepoprawny JSON do DLQ", async () => {
  const invalidJson = "{not-a-valid-json";
  const key = `invalid-json-${Date.now()}`;

  await sendRaw(producer, invalidJson, key);

  const dlqResult = await waitForMessage(
    ORDERS_DLQ_TOPIC,
    (message) =>
      message.error?.startsWith("Niepoprawny JSON:") &&
      message.raw === invalidJson,
  );

  expect(dlqResult).not.toBeNull();
  expect(dlqResult.error).toContain("Niepoprawny JSON:");
  expect(dlqResult.raw).toBe(invalidJson);
});

});
