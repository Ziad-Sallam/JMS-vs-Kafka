package com.example;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        try {
            KafkaProducerBenchmark.main(args);
            KafkaConsumerBenchmark.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
