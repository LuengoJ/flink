package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Jobkafka {

    // Kafka values
    String topic = "input-topic";
    String bootstrapServer = "localhost:9092";
    String groupID = "my-group";
    
    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(bootstrapServer)
        .setTopics(topic)
        .setGroupId(groupID)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    public static void main(String[] args) throws Exception{
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Jobkafka job = new Jobkafka();

        job.consumejob(env);

        env.execute("ConsumeKafka");

    }

    private void consumejob(StreamExecutionEnvironment env){

        DataStream<String> numbers = env.fromSource(
            source, 
            WatermarkStrategy.noWatermarks(), 
            "Kakfa Source");

        numbers.print();
    }

}
