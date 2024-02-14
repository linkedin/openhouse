package com.linkedin.openhouse.housetables.dto.model;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * The utilities class used across different DTO objects for common use case across different house
 * tables.
 *
 * <p>The suppression is needed, since code style check doesn't seem to be capture the access level
 * configuration and indicating there should be an explicit private constructor defined, although
 * they are equivalent.
 */
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Utilities {

  /**
   * Return true if the filtering object has null value. Use case of this method is mainly for
   * filtering. Given the following objects:
   *
   * <p>obj0 { a: Long(3) b: "no" c: "work" }
   *
   * <p>obj1 { a: Long(5), b: "hello" c: "world" }
   *
   * <p>obj2 { a: null b: "hello" c: null }
   *
   * <p>obj2 is considered as a container for predicate on each fields. Obj0 will be filtered given
   * field b doesn't match. Obj1 will pass the predicate since non-null field b matches and rest of
   * null fields are essentially alwaysTrue predicate.
   *
   * <p>This method serves as a supplement for {@link Object#equals(Object)} methods where only one
   * of two objects being null leads to false.
   */
  static boolean fieldMatch(Object repoObj, Object filterObj) {
    return filterObj == null || repoObj.equals(filterObj);
  }

  static boolean fieldMatchCaseInsensitive(Object repoObj, Object filterObj) {
    return fieldMatch(repoObj, filterObj) || compareIgnoreCaseOnlyIfStringType(repoObj, filterObj);
  }

  private static boolean compareIgnoreCaseOnlyIfStringType(Object a, Object b) {
    if (a instanceof String && b instanceof String) {
      return ((String) a).equalsIgnoreCase((String) b);
    }
    // If one or both are not strings, return false
    return false;
  }
}
