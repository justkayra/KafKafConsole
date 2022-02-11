package dev.kaira.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    static String bootstrapServers = "localhost:29092"; // usually of the form cell-1.streaming.<region>.oci.oraclecloud.com:9092 ;
    static String tenancyName = "kai";
    static String username = "oci";
    static String streamPoolId = "pool_id";
    static String authToken = "auth_token"; // from step 8 of Prerequisites section
    static String streamOrKafkaTopicName = "topic_stream"; // from step 2 of Prerequisites section

    private static Properties getKafkaProperties() {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("ssl.client.auth", "none");
       /* properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");*/
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final String value = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + tenancyName + "/"
                + username + "/"
                + streamPoolId + "\" "
                + "password=\""
                + authToken + "\";";
        properties.put("sasl.jaas.config", value);
        properties.put("retries", 3); // retries on transient errors and load balancing disconnection
        properties.put("max.request.size", 1024 * 1024); // limit request size to 1MB
        return properties;
    }

    public static void main(String args[]) {
        try {
            Properties properties = getKafkaProperties();
            KafkaProducer producer = new KafkaProducer<>(properties);

            for(int i=0;i<10;i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(streamOrKafkaTopicName, "messageKey" + i, "messageValue" + i);
                producer.send(record, (md, ex) -> {
                    if (ex != null) {
                        System.err.println("exception occurred in producer for review :" + record.value()
                                + ", exception is " + ex);
                        ex.printStackTrace();
                    } else {
                        System.err.println("Sent msg to " + md.partition() + " with offset " + md.offset() + " at " + md.timestamp());
                    }
                });
            }
            // producer.send() is async, to make sure all messages are sent we use producer.flush()
            producer.flush();
            producer.close();
        } catch (Exception e) {
            System.err.println("Error: exception " + e);
        }
    }
}
