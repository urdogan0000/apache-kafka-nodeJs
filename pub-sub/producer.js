const { Kafka } = require("kafkajs");


createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: ["localhost:39092"],
    });

    const producer = kafka.producer();

    console.log("Producer'a bağlanılıyor....");
    await producer.connect();
    console.log("Producer'a bağlantı başarılı");
  

    const message_result = await producer.send({
      topic: "raw_video_topic",
      messages: [{
        value: "Yeni video İçeriği",
        partition:0
      }],
    });

    console.log("gÖNDERİM İŞLEMİ BAŞARILIDIR", JSON.stringify(message_result));

    await producer.disconnect();
  } catch (error) {
    console.log(error);
  } finally {
    process.exit(0);
  }
}
