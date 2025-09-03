package io.kas.streamservice.dto.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PromoEvent {
  private UUID eventId;
  private String eventType;
  private Instant occurredAt;
  private Payload payload;

  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Payload {
    private UUID promoId;
    private UUID bookId;
    private Integer promoValue;
    private Instant startDate;
    private Instant endDate;
  }
}
