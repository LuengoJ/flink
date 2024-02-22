package com.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App 
{
    public static void main( String[] args ) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        App job = new App();

        job.newjob(env);

        env.execute("prueba");

    }

    private void newjob(StreamExecutionEnvironment env){

        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2));

        flintstones.print();

    }
}
