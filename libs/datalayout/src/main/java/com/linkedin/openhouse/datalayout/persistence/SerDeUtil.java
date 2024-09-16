package com.linkedin.openhouse.datalayout.persistence;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringEscapeUtils;

public final class SerDeUtil {
  private SerDeUtil() {}

  public static String toJsonString(List<DataLayoutStrategy> strategies) {
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<ArrayList<DataLayoutStrategy>>() {}.getType();
    return StringEscapeUtils.escapeJava(gson.toJson(strategies, type));
  }

  public static String toJsonString(DataLayoutStrategy strategy) {
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<DataLayoutStrategy>() {}.getType();
    return StringEscapeUtils.escapeJava(gson.toJson(strategy, type));
  }

  public static List<DataLayoutStrategy> fromJsonStringList(String jsonString) {
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<ArrayList<DataLayoutStrategy>>() {}.getType();
    return gson.fromJson(StringEscapeUtils.unescapeJava(jsonString), type);
  }

  public static DataLayoutStrategy fromJsonString(String jsonString) {
    Gson gson = new GsonBuilder().create();
    Type type = new TypeToken<DataLayoutStrategy>() {}.getType();
    return gson.fromJson(StringEscapeUtils.unescapeJava(jsonString), type);
  }
}
