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


public class JobkafkaSimpleConsume {

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

        JobkafkaSimpleConsume job = new JobkafkaSimpleConsume();

        // Kafka's data consumption process and the processing that will be performed on that data.
        job.reducejob(env);

        // Starts the application execution and starts processing data streams
        env.execute("ConsumeKafka");

    }

    /**
     * This method defines the logic to consume data from a Kafka source and print it.
     * @param env The StreamExecutionEnvironment to use for creating the data stream.
     */


    
    void reducejob(StreamExecutionEnvironment env){

        // Define a DataStream to consume data from a Kafka source
        DataStream<Integer> numbers = env.fromSource(
            source, 
            WatermarkStrategy.noWatermarks(), 
            "Kakfa Source").map(new MapFunction<String,Integer>(){

                @Override
                public Integer map(String value) throws Exception {
                    // TODO Auto-generated method stub
                    return Integer.valueOf(value);
                }})
            ;

        // // Definir la ventana de tiempo y la operación de reducción
        // numbers.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
        //     .reduce(new ReduceFunction<Integer>() {
        //        @Override
        //         public Integer reduce(Integer value1, Integer value2) throws Exception{
        //             // Realizar la operación de suma
        //             return value1 + value2;
        //         }
        //     });
            
        // Enviar los resultados al KafkaSink
        
        // numbers.map(new MapFunction<Integer,String>(){

        //         @Override
        //         public String map(Integer value) throws Exception {
        //             // TODO Auto-generated method stub
        //             return String.valueOf(value);
        //         }});
        
        // numbers.sinkTo(sink);

        numbers.print();
    }

}
