package io.kas.streamservice.services;

import io.kas.streamservice.dto.event.BookEvent;
import io.kas.streamservice.dto.event.PromoInfo;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class BookProcessor implements Processor<String, BookEvent, String, BookEvent> {

  private final String storeName;
  private KeyValueStore<String, List<PromoInfo>> store;
  private ProcessorContext<String, BookEvent> context;

  public BookProcessor(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public void init(ProcessorContext<String, BookEvent> context) {
    this.context = context;
    this.store = context.getStateStore(storeName);
  }

  @Override
  public void process(Record<String, BookEvent> record) {
    BookEvent bookEvent = record.value();
    String bookId = bookEvent.getPayload().getBookId().toString();

    List<PromoInfo> promos = store.get(bookId);
    int maxDiscount = 0;
    if (promos != null && !promos.isEmpty()) {
      maxDiscount = promos.stream()
          .mapToInt(PromoInfo::getValue)
          .max()
          .orElse(0);
    }

    bookEvent.getPayload().setActivePromos(promos != null ? promos : new ArrayList<>());
    bookEvent.getPayload().setDiscount(maxDiscount);

    context.forward(record.withValue(bookEvent));
  }

  @Override
  public void close() {
  }
}
