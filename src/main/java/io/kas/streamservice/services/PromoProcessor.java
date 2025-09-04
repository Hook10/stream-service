package io.kas.streamservice.services;

import io.kas.streamservice.dto.event.PromoEvent;
import io.kas.streamservice.dto.event.PromoInfo;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class PromoProcessor implements Processor<String, PromoEvent, Void, Void> {

  private final String storeName;
  private KeyValueStore<String, List<PromoInfo>> store;
  private ProcessorContext<Void, Void> context;

  public PromoProcessor(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public void init(ProcessorContext<Void, Void> context) {
    this.context = context;
    this.store = context.getStateStore(storeName);
  }

  @Override
  public void process(Record<String, PromoEvent> record) {
    PromoEvent event = record.value();
    String bookId = event.getPayload().getBookId().toString();

    List<PromoInfo> promos = store.get(bookId);
    if (promos == null) {
      promos = new ArrayList<>();
    }

    switch (event.getEventType()) {
      case "PROMO_ACTIVATED":
      case "PROMO_UPDATED":
        promos.removeIf(p -> p.getId().equals(event.getPayload().getPromoId()));
        promos.add(new PromoInfo(event.getPayload().getPromoId(), event.getPayload().getPromoValue()));
        break;

      case "PROMO_DELETED":
      case "PROMO_ENDED":
        promos.removeIf(p -> p.getId().equals(event.getPayload().getPromoId()));
        break;
    }

    store.put(bookId, promos);
  }

  @Override
  public void close() {
    // nothing to clean up
  }
}