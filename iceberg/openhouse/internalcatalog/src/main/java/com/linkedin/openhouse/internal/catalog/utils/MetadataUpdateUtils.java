package com.linkedin.openhouse.internal.catalog.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Utility class for updating metadata files in HDFS. Provides methods to read, modify, and write
 * JSON metadata files.
 */
@Slf4j
public class MetadataUpdateUtils {

  /**
   * Updates a specific field in a metadata file located at the given HDFS path.
   *
   * @param fs The Hadoop FileSystem instance
   * @param hdfsPath The path to the metadata file in HDFS
   * @param fieldName The name of the field to update
   * @param updatedValue The new value for the field
   * @throws IOException if there's an error reading or writing the file
   */
  public static void updateMetadataField(
      FileSystem fs, String hdfsPath, String fieldName, Long updatedValue) throws IOException {
    try {
      InputStream inputStream = fs.open(new Path(hdfsPath));
      String jsonString = readInputStream(inputStream);
      IOUtils.closeStream(inputStream);

      String updatedJsonString = updateJsonField(jsonString, fieldName, updatedValue);

      OutputStream outputStream = fs.create(new Path(hdfsPath), true);
      writeOutputStream(outputStream, updatedJsonString);
      IOUtils.closeStream(outputStream);

      log.info("Updated json metadata at path: {}", hdfsPath);
    } catch (IOException e) {
      String errMsg =
          String.format(
              "Failed to update metadata file at path: %s. Error: %s", hdfsPath, e.getMessage());
      log.error(errMsg);
      throw new IOException(errMsg);
    }
  }

  /**
   * Reads the content of an InputStream and returns it as a String.
   *
   * @param inputStream The InputStream to read from
   * @return The content as a String
   * @throws IOException if there's an error reading the stream
   */
  private static String readInputStream(InputStream inputStream) throws IOException {
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      StringBuilder content = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        content.append(line);
      }
      return content.toString();
    }
  }

  /**
   * Writes content to an OutputStream.
   *
   * @param outputStream The OutputStream to write to
   * @param content The content to write
   * @throws IOException if there's an error writing to the stream
   */
  private static void writeOutputStream(OutputStream outputStream, String content)
      throws IOException {
    try (BufferedWriter writer =
        new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
      writer.write(content);
    }
  }

  /**
   * Updates a specific field in a JSON string with a new value.
   *
   * @param jsonString The JSON string to modify
   * @param fieldName The name of the field to update
   * @param updatedValue The new value for the field
   * @return The updated JSON string with pretty printing
   * @throws IOException if there's an error parsing or writing the JSON
   */
  private static String updateJsonField(String jsonString, String fieldName, long updatedValue)
      throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    ((ObjectNode) jsonNode).put(fieldName, updatedValue);
    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
  }
}
