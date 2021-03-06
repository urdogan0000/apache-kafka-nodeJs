const { Kafka } = require("kafkajs");

const topic_name=process.argv[2] || "Logs2"
createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_ornek_1",
      brokers: ["localhost:39092"],
    });

    const consumer = kafka.consumer({
      groupId: "ornek_1_cg_1",
    });

    console.log("Consumer'a bağlanılıyor....");
    await consumer.connect();
    console.log("Consumer'a bağlantı başarılı");

    //Consumer subscribe
    await consumer.subscribe({
      topic: topic_name,
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async (result) => {
        console.log(
          `Gelen mesaj ${result.message.value} :Partition :=> ${result.partition}`
        );
      },
    });
  } catch (error) {
    console.log(error);
  }
}
