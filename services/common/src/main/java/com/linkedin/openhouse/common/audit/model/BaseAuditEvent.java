package com.linkedin.openhouse.common.audit.model;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.time.Instant;

/** Base audit event model to be inherited by service audit event and table audit event. */
public class BaseAuditEvent {
  public String toJson() {
    return new GsonBuilder()
        .registerTypeAdapter(Instant.class, new InstantTypeAdapter())
        .serializeNulls()
        .disableHtmlEscaping()
        .create()
        .toJson(this);
  }

  /**
   * Java 16 started blocking reflection for JDK internals, https://openjdk.org/jeps/396 As a
   * result, Gson's default typeAdapter {@link
   * com.google.gson.internal.bind.ReflectiveTypeAdapterFactory} doesn't work anymore. Without the
   * adapter, failure occurs for {@link BaseAuditEvent#toJson()} with the error: "Failed making
   * field 'java.time.Instant#seconds' accessible" Therefore, we write a custom type adapter
   */
  static class InstantTypeAdapter extends TypeAdapter<Instant> {
    public void write(JsonWriter writer, Instant inst) throws IOException {
      if (inst == null) {
        writer.nullValue();
      } else {
        writer.value(String.valueOf(inst.toEpochMilli()));
      }
    }

    public Instant read(JsonReader reader) throws IOException {
      if (reader.peek() == JsonToken.NULL) {
        reader.nextNull();
        return null;
      }
      return Instant.ofEpochMilli(Long.parseLong(reader.nextString()));
    }
  }
}
