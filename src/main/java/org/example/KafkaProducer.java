package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.bean.AuditBean;


import java.util.Properties;

/**
 * @Author : Suri Aravind @Creation Date : 21/02/24
 */
public class KafkaProducer {
    public static void main(String[] args){
        sendIngestionUpdate();
    }
    public static void sendIngestionUpdate() {
        try {
            String bootstrapServers = "localhost:9092";
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

      AuditBean auditBean = AuditBean.builder()
              .context("Application")
              .category("Searches")
              .build();

        ProducerRecord<String, String> producerRecord =
          new ProducerRecord<>("P360_AUDIT_MESSAGE", new ObjectMapper().writeValueAsString(auditBean));
            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    System.out.println("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    System.err.println("Error while producing"+ e);
                }
            });
            producer.flush();
            producer.close();
        }catch (Exception exception){
            System.err.println(" Error :"+ exception.getMessage()+ exception);
        }
    }
}
