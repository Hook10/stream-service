package io.kas.streamservice.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.kas.streamservice.dto.event.BookEvent;
import io.kas.streamservice.dto.event.PromoEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class CustomSerdes {

  private static final ObjectMapper objectMapper = new ObjectMapper()
      .registerModule(new JavaTimeModule());

  public static Serde<BookEvent> bookEventSerde() {
    return new Serde<BookEvent>() {
      @Override
      public Serializer<BookEvent> serializer() {
        return new Serializer<BookEvent>() {
          @Override
          public void configure(Map<String, ?> configs, boolean isKey) {}

          @Override
          public byte[] serialize(String topic, BookEvent data) {
            try {
              return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
              throw new RuntimeException("Error serializing BookEvent", e);
            }
          }

          @Override
          public void close() {}
        };
      }

      @Override
      public Deserializer<BookEvent> deserializer() {
        return new Deserializer<BookEvent>() {
          @Override
          public void configure(Map<String, ?> configs, boolean isKey) {}

          @Override
          public BookEvent deserialize(String topic, byte[] data) {
            try {
              return objectMapper.readValue(data, BookEvent.class);
            } catch (IOException e) {
              throw new RuntimeException("Error deserializing BookEvent", e);
            }
          }

          @Override
          public void close() {}
        };
      }

      @Override
      public void configure(Map<String, ?> configs, boolean isKey) {}

      @Override
      public void close() {}
    };
  }

  public static Serde<PromoEvent> promoEventSerde() {
    return new Serde<PromoEvent>() {
      @Override
      public Serializer<PromoEvent> serializer() {
        return new Serializer<PromoEvent>() {
          @Override
          public void configure(Map<String, ?> configs, boolean isKey) {}

          @Override
          public byte[] serialize(String topic, PromoEvent data) {
            try {
              return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
              throw new RuntimeException("Error serializing PromoEvent", e);
            }
          }

          @Override
          public void close() {}
        };
      }

      @Override
      public Deserializer<PromoEvent> deserializer() {
        return new Deserializer<PromoEvent>() {
          @Override
          public void configure(Map<String, ?> configs, boolean isKey) {}

          @Override
          public PromoEvent deserialize(String topic, byte[] data) {
            try {
              return objectMapper.readValue(data, PromoEvent.class);
            } catch (IOException e) {
              throw new RuntimeException("Error deserializing PromoEvent", e);
            }
          }

          @Override
          public void close() {}
        };
      }

      @Override
      public void configure(Map<String, ?> configs, boolean isKey) {}

      @Override
      public void close() {}
    };
  }
}
