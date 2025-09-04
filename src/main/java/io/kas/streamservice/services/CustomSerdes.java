package io.kas.streamservice.services;

import io.kas.streamservice.dto.event.BookEvent;
import io.kas.streamservice.dto.event.PromoEvent;
import io.kas.streamservice.dto.event.PromoInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;

public class CustomSerdes {

  private static final ObjectMapper objectMapper = new ObjectMapper()
      .registerModule(new JavaTimeModule());

  public static Serde<List<PromoInfo>> promoInfoListSerde() {
    Serializer<List<PromoInfo>> serializer = (topic, data) -> {
      try {
        return data == null ? null : objectMapper.writeValueAsBytes(data);
      } catch (Exception e) {
        throw new RuntimeException("Error serializing PromoInfo list", e);
      }
    };

    Deserializer<List<PromoInfo>> deserializer = (topic, data) -> {
      try {
        if (data == null) return new ArrayList<>();
        return objectMapper.readValue(data, new TypeReference<List<PromoInfo>>() {});
      } catch (Exception e) {
        throw new RuntimeException("Error deserializing PromoInfo list", e);
      }
    };

    return Serdes.serdeFrom(serializer, deserializer);
  }

  public static Serde<BookEvent> bookEventSerde() {
    Serializer<BookEvent> serializer = (topic, data) -> {
      try {
        return data == null ? null : objectMapper.writeValueAsBytes(data);
      } catch (Exception e) {
        throw new RuntimeException("Error serializing BookEvent", e);
      }
    };

    Deserializer<BookEvent> deserializer = (topic, data) -> {
      try {
        if (data == null) return null;
        return objectMapper.readValue(data, BookEvent.class);
      } catch (Exception e) {
        throw new RuntimeException("Error deserializing BookEvent", e);
      }
    };

    return Serdes.serdeFrom(serializer, deserializer);
  }

  public static Serde<PromoEvent> promoEventSerde() {
    Serializer<PromoEvent> serializer = (topic, data) -> {
      try {
        return data == null ? null : objectMapper.writeValueAsBytes(data);
      } catch (Exception e) {
        throw new RuntimeException("Error serializing PromoEvent", e);
      }
    };

    Deserializer<PromoEvent> deserializer = (topic, data) -> {
      try {
        if (data == null) return null;
        return objectMapper.readValue(data, PromoEvent.class);
      } catch (Exception e) {
        throw new RuntimeException("Error deserializing PromoEvent", e);
      }
    };

    return Serdes.serdeFrom(serializer, deserializer);
  }
}
