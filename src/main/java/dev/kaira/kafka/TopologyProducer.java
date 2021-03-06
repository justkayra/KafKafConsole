package dev.kaira.kafka;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.time.Instant;

@ApplicationScoped
public class TopologyProducer {

    static final String PRICE_POOL_STORE = "price-pool-store";

    private static final String PRICE_POOL_TOPIC = "price-pool";
    private static final String PRICE_VALUES_TOPIC = "price-values";
    private static final String PRICES_AGGREGATED_TOPIC = "prices-aggregated";

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapperSerde<Price> weatherStationSerde = new ObjectMapperSerde<>(Price.class);
        ObjectMapperSerde<Aggregation> aggregationSerde = new ObjectMapperSerde<>(Aggregation.class);
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(PRICE_POOL_STORE);
        GlobalKTable<Integer, Price> stations = builder.globalTable(PRICE_POOL_TOPIC,Consumed.with(Serdes.Integer(), weatherStationSerde));

        builder.stream(
                PRICE_VALUES_TOPIC,
                        Consumed.with(Serdes.Integer(), Serdes.String())
                )
                .join(
                        stations,
                        (stationId, timestampAndValue) -> stationId,
                        (timestampAndValue, station) -> {
                            String[] parts = timestampAndValue.split(";");
                            return new PriceMeasurement(station.id, station.name,
                                    Instant.parse(parts[0]), Double.valueOf(parts[1]));
                        }
                )
                .groupByKey()
                .aggregate(
                        Aggregation::new,
                        (stationId, value, aggregation) -> aggregation.updateFrom(value),
                        Materialized.<Integer, Aggregation> as(storeSupplier)
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(aggregationSerde)
                )
                .toStream()
                .to(
                        PRICES_AGGREGATED_TOPIC,
                        Produced.with(Serdes.Integer(), aggregationSerde)
                );

        return builder.build();
    }
}
