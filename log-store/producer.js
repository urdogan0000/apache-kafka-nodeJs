const { Kafka } = require("kafkajs");
const log_data = require("./logs.json");

createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_store_client",
      brokers: ["localhost:39092"],
    });

    const producer = kafka.producer();

    console.log("Producer'a bağlanılıyor....");
    await producer.connect();
    console.log("Producer'a bağlantı başarılı");
    let messages = log_data.map((item) => {
      return {
        value:JSON.stringify(item),
        partition: item.type=="system" ? 0:1,
      };
    });

    const message_result = await producer.send({
      topic: "LogStoreTopic",
      messages: messages,
    });

    console.log("gÖNDERİM İŞLEMİ BAŞARILIDIR", JSON.stringify(message_result));

    await producer.disconnect();
  } catch (error) {
    console.log(error);
  } finally {
    process.exit(0);
  }
}
