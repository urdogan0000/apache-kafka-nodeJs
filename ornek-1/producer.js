const { Kafka } = require("kafkajs");
const topic_name=process.argv[2] || "Logs2"
const partition=process.argv[3] || 0
createProducer();

async function  createProducer() {
  try {
   
    const kafka = new Kafka({
      clientId: "kafka_ornek_1",
      brokers: ["localhost:39092"],
    });

    const producer = kafka.producer();

    console.log("Producer'a bağlanılıyor....");
    await producer.connect();
    console.log("Producer'a bağlantı başarılı");

    const message_result=await producer.send({
      topic : topic_name,
      messages :[{
        value:"Bu bir test log mesajıdır",
        partition:partition
      }]
    })

    console.log("gÖNDERİM İŞLEMİ BAŞARILIDIR",JSON.stringify(message_result))

    await producer.disconnect()
    
  } catch (error) {
    console.log(error);
  } finally {
    process.exit(0);
  }
}
