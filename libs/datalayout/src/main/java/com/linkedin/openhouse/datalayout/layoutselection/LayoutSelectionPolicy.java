package com.linkedin.openhouse.datalayout.layoutselection;

import java.util.List;

public interface LayoutSelectionPolicy<T extends DataLayout> {
  // TODO: Determine what type this would need to be, do we want to rank?
  List<T> evaluate();
}
