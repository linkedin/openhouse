package com.linkedin.openhouse.jobs.util;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class SimpleRecord implements Serializable {
  // Getters and setters required
  private int id;
  private String data;

  // Constructor
  public SimpleRecord(int id, String data) {
    this.id = id;
    this.data = data;
  }
}
