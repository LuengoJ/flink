package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class JobKafkaConsume {

    // Kafka values
    String inputTopic = "input-topic";
    String outputTopic = "output-topic";
    String bootstrapServers = "localhost:9092";
    String groupID = "my-group";
    
    // Constructing instance of KafkaSource
    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(bootstrapServers)
        .setTopics(inputTopic)
        .setGroupId(groupID)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
			.setValueSerializationSchema(new SimpleStringSchema())
			.setTopic(outputTopic)
			.build();

    KafkaSink<String> sink = KafkaSink.<String>builder()
			.setBootstrapServers(bootstrapServers)
			.setRecordSerializer(serializer)
			.build();

    // Main
    public static void main(String[] args) throws Exception{
        
        // Streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        JobKafkaConsume job = new JobKafkaConsume();

        // Kafka's data consumption process and the processing that will be performed on that data.
        job.consume(env);

        // Starts the application execution and starts processing data streams
        env.execute("ConsumeKafka");

    }

    /**
     * This method defines the logic to consume data from a Kafka source and print it.
     * @param env The StreamExecutionEnvironment to use for creating the data stream.
     */

    private void consume(StreamExecutionEnvironment env){


    // Define a DataStream to consume data from a Kafka source
    DataStream<Integer> numbers = env.fromSource(
        source, 
        WatermarkStrategy.noWatermarks(), 
        "Kakfa Source").map(new MapFunction<String,Integer>(){

            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }})
        ;

    DataStream<Integer> doubledNumbers = numbers.map(num -> num * 2);

    // DataStream<Integer> sumOfValues = numbers
    //         .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
    //         .sum(0);

    numbers.print("Original: ");
    doubledNumbers.print("Transformado: ");
    // sumOfValues.print();
    }
}
