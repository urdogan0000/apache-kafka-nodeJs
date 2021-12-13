const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: ["localhost:39092"],
    });

    const consumer = kafka.consumer({
      groupId: "hd_4k_8k-encoder_consumer_group",
    });

    console.log("Consumer'a bağlanılıyor....");
    await consumer.connect();
    console.log("Consumer'a bağlantı başarılı");

    //Consumer subscribe
    await consumer.subscribe({
      topic: "raw_video_topic",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async (result) => {
        console.log(
          `Gelen mesaj ${result.message.value}_4k_8k_encoder`
        );
      },
    });
  } catch (error) {
    console.log(error);
  }
}
