package io.kas.streamservice.services;

import io.kas.streamservice.dto.event.BookEvent;
import io.kas.streamservice.dto.event.PromoEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
@Service
public class StreamProcessingService {

  private KafkaStreams streams;
  private final Properties streamsConfig;


  @PostConstruct
  public void startStreamProcessing() {
    StreamsBuilder builder = new StreamsBuilder();

    buildProcessingTopology(builder);

    streams = new KafkaStreams(builder.build(), streamsConfig);

    streams.setUncaughtExceptionHandler(exception -> {
      log.error("Uncaught exception: {}", exception.getMessage());
      exception.printStackTrace();
      return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
    });

    streams.start();

    log.info("Kafka Streams started for book and promo processing");
  }

  private void buildProcessingTopology(StreamsBuilder builder) {

    KStream<String, BookEvent> bookStream = builder.stream(
        "book-topic",
        Consumed.with(Serdes.String(), CustomSerdes.bookEventSerde())
    );

    KStream<String, PromoEvent> promoStream = builder.stream(
        "promo-topic",
        Consumed.with(Serdes.String(), CustomSerdes.promoEventSerde())
    );

    KTable<String, Integer> maxPromoByBook = promoStream
        .filter((key, promoEvent) ->
            "PROMO_ACTIVATED".equals(promoEvent.getEventType()) ||
                "PROMO_UPDATED".equals(promoEvent.getEventType()))
        .map((key, promoEvent) ->
            KeyValue.pair(
                promoEvent.getPayload().getBookId().toString(),
                promoEvent.getPayload().getPromoValue()
            ))
        .groupByKey()
        .reduce(
            (oldValue, newValue) -> Math.max(oldValue, newValue),
            Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>
                    as("max-promo-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
        );

    KStream<String, BookEvent> enrichedBookStream = bookStream
        .leftJoin(
            maxPromoByBook,
            (bookEvent, maxDiscount) -> {
              if (maxDiscount != null) {
                BookEvent enrichedEvent = new BookEvent();
                enrichedEvent.setEventId(bookEvent.getEventId());
                enrichedEvent.setEventType(bookEvent.getEventType());
                enrichedEvent.setOccurredAt(bookEvent.getOccurredAt());

                BookEvent.Payload enrichedPayload = new BookEvent.Payload();
                BookEvent.Payload originalPayload = bookEvent.getPayload();

                enrichedPayload.setBookId(originalPayload.getBookId());
                enrichedPayload.setTitle(originalPayload.getTitle());
                enrichedPayload.setAuthors(originalPayload.getAuthors());
                enrichedPayload.setDescription(originalPayload.getDescription());
                enrichedPayload.setCoverImage(originalPayload.getCoverImage());
                enrichedPayload.setCategory(originalPayload.getCategory());
                enrichedPayload.setPublisher(originalPayload.getPublisher());
                enrichedPayload.setPublishedYear(originalPayload.getPublishedYear());
                enrichedPayload.setLanguage(originalPayload.getLanguage());
                enrichedPayload.setPageCount(originalPayload.getPageCount());
                enrichedPayload.setPrice(originalPayload.getPrice());
                enrichedPayload.setDiscount(maxDiscount);
                enrichedPayload.setStatus(originalPayload.getStatus());
                enrichedPayload.setTimestamp(originalPayload.getTimestamp());

                enrichedEvent.setPayload(enrichedPayload);
                return enrichedEvent;
              }
              return bookEvent;
            }
        );

    enrichedBookStream.to(
        "book-with-promo-topic",
        Produced.with(Serdes.String(), CustomSerdes.bookEventSerde())
    );

    enrichedBookStream.foreach((key, value) -> {
      log.info("Processed book: {}  with discount: {}",
          value.getPayload().getBookId(),
          value.getPayload().getDiscount());
    });
  }

  @PreDestroy
  public void stopStreamProcessing() {
    if (streams != null) {
      streams.close();
      log.info("Kafka Streams stopped");
    }
  }
}
