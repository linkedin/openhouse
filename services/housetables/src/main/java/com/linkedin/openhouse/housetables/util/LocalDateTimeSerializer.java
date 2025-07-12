package com.linkedin.openhouse.housetables.util;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** Utility for Gson to have compatible serde with LocalDateTime */
public class LocalDateTimeSerializer extends TypeAdapter<LocalDateTime> {

  @Override
  public void write(JsonWriter jsonWriter, LocalDateTime date) throws IOException {
    if (date == null) {
      jsonWriter.nullValue();
    } else {
      jsonWriter.value(date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)); // "yyyy-mm-dd HH:mm:ss"
    }
  }

  @Override
  public LocalDateTime read(JsonReader jsonReader) throws IOException {
    if (jsonReader.peek() == JsonToken.NULL) {
      jsonReader.nextNull();
      return null;
    } else {
      return LocalDateTime.parse(jsonReader.nextString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }
  }
}
