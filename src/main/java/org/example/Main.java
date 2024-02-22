package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("Suriya");
        String topic="P360_AUDIT_MESSAGE";


        Properties consumerConfig = new Properties();
        consumerConfig.put(AWSConfigConstants.AWS_REGION, "ap-south-1");
        consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "AKIA6GBLMM7VK4CCDEUF");
        consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "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");
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
        consumerConfig.setProperty(ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS, "true");

        FlinkKinesisConsumer<KinesisiBean> kinesisConsumer =
                new FlinkKinesisConsumer<>("Flink-POC-ASH", new JSONStringValueDeserializationSchema(), consumerConfig);
        //kinesisConsumer.setPeriodicWatermarkAssigner(new CustomAssignerWithPeriodicWatermarks());
        DataStream<KinesisiBean> stream = env.addSource(kinesisConsumer);
                //env.addSource(kinesisConsumer);
        stream.print();

        // DataStream<AuditBean> auditBeanDataStream = env.addSource(kinesisConsumer);
        // auditBeanDataStream.print();
       // updatePostgresqlUsingKafka(topic, env);

        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }

    private static void updatePostgresqlUsingKafka(String topic, StreamExecutionEnvironment env) {
        Properties kafkaProperties = new Properties();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        KafkaSource<AuditBean> source = KafkaSource.<AuditBean>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId(topic)
                .setProperties(kafkaProperties)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();
        System.out.println("Before print stream");
        DataStream<AuditBean> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        dataStream.print();
        System.out.println("After print stream");

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("org.postgresql.Driver")
                .withUrl("jdbc:postgresql://192.168.1.230:5432/ads")
                .withUsername("adsuser")
                .withPassword("AdS@3421")
                .build();


        dataStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS public.audit("+
                        "context varchar(255),"+
                        "category varchar(255) "+
                        ")",
                        (JdbcStatementBuilder<AuditBean>) (preparedStatement, auditBean) -> {

                },
                executionOptions,
                connectionOptions
        )).name(" Create table if doesn't exits into database");
        dataStream.addSink(JdbcSink.sink(
                "INSERT INTO public.audit (context,category) VALUES (?,?)",
                (JdbcStatementBuilder<AuditBean>) (preparedStatement, auditBean) -> {
                    preparedStatement.setString(1,auditBean.getContext());
                    preparedStatement.setString(2,auditBean.getCategory());
                },
                executionOptions,
                connectionOptions

        )).name(" Insert into audit table");
    }
}