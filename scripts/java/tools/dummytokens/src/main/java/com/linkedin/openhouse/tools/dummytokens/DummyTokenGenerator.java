package com.linkedin.openhouse.tools.dummytokens;

import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public final class DummyTokenGenerator {

  private static final String[] USERS = {"openhouse", "u_tableowner"};

  private DummyTokenGenerator() {}

  public static void main(String[] args) throws org.apache.commons.cli.ParseException, IOException {
    System.out.println("IN MAIN OF DUMMYTOKENGENERATOR");
    Option optionDirPath =
        Option.builder("d")
            .required(true)
            .longOpt("dirPath")
            .argName("dirPath")
            .hasArg()
            .desc("Dir Path to dump Dummy Tokens for users")
            .build();
    Options options = new Options();
    options.addOption(optionDirPath);

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    if (!commandLine.hasOption("d")) {
      System.out.println("Missing dir path to dump dummy tokens");
      System.exit(1);
    }

    String dirPath = commandLine.getOptionValue("d");
    Files.createDirectories(Paths.get(dirPath));

    Map<String, String> collect =
        Arrays.stream(USERS)
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    string -> {
                      DummyTokenInterceptor.DummySecurityJWT dummySecurityJWT =
                          new DummyTokenInterceptor.DummySecurityJWT(string);
                      try {
                        return dummySecurityJWT.buildNoopJWT();
                      } catch (ParseException | org.codehaus.jettison.json.JSONException e) {
                        e.printStackTrace();
                        System.exit(1);
                      }
                      return "";
                    }));
    collect.forEach(
        (key, value) -> {
          String filePath = String.valueOf(Paths.get(dirPath, String.join(".", key, "token")));
          try {
            File file = new File(filePath);
            OutputStreamWriter outputStreamWriter =
                new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8);
            outputStreamWriter.write(value);
            outputStreamWriter.close();
          } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
          }
        });
  }
}
