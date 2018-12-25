package com.kafka_stream_skeleton;

import com.cellwize.model.KPIDataPoint;
import com.cellwize.model.MeasResults;
import com.cellwize.model.Pair;
import com.kafka_stream_skeleton.serialization.SerdeBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

public class Application {

    private static final String APPLICATION_ID = System.getenv("APPLICATION_ID");
    private static final String INPUT_TOPIC = System.getenv("INPUT_TOPIC");
    private static final String OUTPUT_TOPIC = System.getenv("OUTPUT_TOPIC");
    private static final String BOOTSTRAP_SERVER = System.getenv("KAFKA_URL");

    public static void main(final String[] args) {


        final KafkaStreams streams = buildStream();
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    private static KafkaStreams buildStream() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        final long INTERVAL_SECONDS = 60L;

        Serde<MeasResults> measResultsSerde = SerdeBuilder.buildSerde(MeasResults.class);
        Serde<Pair> pairSerde = SerdeBuilder.buildSerde(Pair.class);
        Serde<Map<String, Long>> mapSerde = Serdes.serdeFrom(new MapSerializer(), new MapDeserializer());


        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, MeasResults> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), measResultsSerde));


        System.out.println("start streaming processing on topic " + INPUT_TOPIC);

        final LongUnaryOperator rounder = (long timestamp) -> timestamp - (timestamp % (INTERVAL_SECONDS * 1000L));

        KTable<Windowed<Pair>, Map<String, Long>> counts = source
                .filter((key, value) -> value != null)
                .map((key, value) -> {
                    final Pair pair = new Pair();
                    pair.setGuid(value.getCellGuid());
                    pair.setTimestamp(rounder.applyAsLong(value.getTimestamp()));
                    return new KeyValue<>(pair, value);
                })
                .groupByKey(Serialized.with(pairSerde, measResultsSerde))
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(INTERVAL_SECONDS)))
                .aggregate(HashMap::new, (key, value, aggregate) -> {
                    aggregate.putIfAbsent(value.getCounterName(), 0L);
                    long sum = aggregate.get(value.getCounterName()) + value.getValue();
                    aggregate.put(value.getCounterName(), sum);
                    return aggregate;
                }, Materialized.<Pair, Map<String, Long>, WindowStore<Bytes, byte[]>>as("counts-store").withValueSerde(mapSerde));


        final Serde<String> stringSerde = Serdes.String();

        final Serde<KPIDataPoint> kpiDataPointSerde = SerdeBuilder.buildSerde(KPIDataPoint.class);

        counts
                .toStream().print(Printed.toSysOut());
//                .filter((windowed, counters) -> {
//                    return counters.get("counter1") != null || counters.get("counter2") != null;
//                })
//                .map((windowed, counters) -> {
//                    long kpiValue = counters.get("counter1") / counters.get("counter2");
//                    final KPIDataPoint kpiDataPoint = new KPIDataPoint("generic-kpi", windowed.key().getTimestamp(), kpiValue);
//                    return new KeyValue<>(windowed.key(), kpiDataPoint);
//                })
//                .to(OUTPUT_TOPIC, Produced.with(pairSerde, kpiDataPointSerde));


        System.out.println("Streaming processing will produce results to topic " + OUTPUT_TOPIC);

        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        return kafkaStreams;
    }

}