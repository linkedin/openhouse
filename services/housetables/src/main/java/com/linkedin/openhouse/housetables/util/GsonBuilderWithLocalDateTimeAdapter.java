package com.linkedin.openhouse.housetables.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.LocalDateTime;

/** Utility class to create a Gson instance with a custom serializer for LocalDateTime. */
public class GsonBuilderWithLocalDateTimeAdapter {

  public static final GsonBuilder GSON_BUILDER =
      new GsonBuilder().registerTypeAdapter(LocalDateTime.class, new LocalDateTimeSerializer());

  public static Gson createGson() {
    return GSON_BUILDER.create();
  }
}
