package com.kafka_stream_skeleton.producer;

import com.cellwize.model.MeasResults;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class Application {
    public static void main(String[] args) throws InterruptedException {

        String topic = System.getenv("INPUT_TOPIC");
        System.out.println(topic);
        LoginProducer loginProducer = new LoginProducer();
        List<String> countersName;


        countersName = new ArrayList<>();
        countersName.add("counter1");
        countersName.add("counter2");
        countersName.add("counter3");
        countersName.add("counter4");
        countersName.add("counter5");
        countersName.add("counter6");
        countersName.add("counter7");
        countersName.add("counter8");
        countersName.add("counter9");
        countersName.add("counter10");


        while (true) {
            Thread.sleep(1000);
            Random rand = new Random();
            int cellId = rand.nextInt(9);
            int counter = rand.nextInt(9);
            int value = rand.nextInt(100);
            MeasResults measResults = new MeasResults();
            measResults.setCellGuid("h_guid:" + cellId);
            measResults.setCounterName(countersName.get(counter));
            measResults.setTimestamp(System.currentTimeMillis());
            measResults.setValue(value);
            loginProducer.produce(topic, "bla", "bla", "bla", measResults);
        }
    }
}
