const { Kafka } = require("kafkajs");

createTopic();

async function  createTopic() {
  try {
    //Admin Stuff..
    const kafka = new Kafka({
      clientId: "kafka_ornek_1",
      brokers: ["localhost:39092"],
    });

    const admin = kafka.admin();

    console.log("Kafka Broker'a bağlanılıyor....");
    await admin.connect();
    console.log("Kafka Broker'abağlantı başarılı,Topic üretilecek....");

    await admin.createTopics({
      topics: [
        {
          topic: "Logs",
          numPartitions: 1,
        },
        {
          topic: "Logs2",
          numPartitions: 2,
        },
      ],
    });
    console.log("Topic üretildi");
    await admin.disconnect();
  } catch (error) {
    console.log(error);
  } finally {
    process.exit(0);
  }
}
