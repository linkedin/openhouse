package com.linkedin.openhouse.common.api.util;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/** Utility for Gson to have compatible serde with LocalDateTime */
public class LocalDateTimeSerializer extends TypeAdapter<LocalDateTime> {

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  @Override
  public void write(JsonWriter jsonWriter, LocalDateTime date) throws IOException {
    if (date == null) {
      jsonWriter.nullValue();
    } else {
      jsonWriter.value(date.format(FORMATTER));
    }
  }

  @Override
  public LocalDateTime read(JsonReader jsonReader) throws IOException {
    if (jsonReader.peek() == JsonToken.NULL) {
      jsonReader.nextNull();
      return null;
    } else {
      String dateString = jsonReader.nextString();
      try {
        return LocalDateTime.parse(dateString, FORMATTER);
      } catch (DateTimeParseException e) {
        throw new IOException("Failed to parse LocalDateTime: " + dateString, e);
      }
    }
  }
}
