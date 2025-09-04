package io.kas.streamservice.config;

import io.kas.streamservice.dto.event.PromoEvent;
import io.kas.streamservice.dto.event.PromoInfo;
import io.kas.streamservice.services.BookProcessor;
import io.kas.streamservice.services.CustomSerdes;
import io.kas.streamservice.services.PromoProcessor;
import org.apache.kafka.common.serialization.ListDeserializer;
import org.apache.kafka.common.serialization.ListSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import java.util.List;

@Configuration
public class PromoTopologyConfig {

  @Bean
  public Topology promoTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    // State store: bookId -> list of PromoInfo
    StoreBuilder<KeyValueStore<String, List<PromoInfo>>> promoStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("active-promos-store"),
            Serdes.String(),
            CustomSerdes.promoInfoListSerde()
        );

    builder.addStateStore(promoStoreBuilder);

    // Process promo-topic: update promos in state store
    builder.stream("promo-topic", Consumed.with(Serdes.String(), CustomSerdes.promoEventSerde()))
        .process(() -> new PromoProcessor("active-promos-store"), "active-promos-store");

    // Process book-topic: enrich with promos + max discount
    builder.stream("book-topic", Consumed.with(Serdes.String(), CustomSerdes.bookEventSerde()))
        .process(() -> new BookProcessor("active-promos-store"), "active-promos-store")
        .to("book-with-promo-topic", Produced.with(Serdes.String(), CustomSerdes.bookEventSerde()));

    return builder.build();
  }
}
