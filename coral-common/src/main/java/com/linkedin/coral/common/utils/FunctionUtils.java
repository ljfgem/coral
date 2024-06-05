/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;


public class FunctionUtils {
  public static final String CORAL_VERSIONED_UDF_PREFIX = "coralversionedudf_(\\d+|x)_(\\d+|x)_(\\d+|x)";

  /**
   * Checks if the given class name has a shading prefix.
   * A class name is considered shaded if the prefix before the first dot
   * follows {@link FunctionUtils#CORAL_VERSIONED_UDF_PREFIX} format
   */
  public static boolean isClassShaded(String className) {
    if (className != null && !className.isEmpty()) {
      int firstDotIndex = className.indexOf('.');
      if (firstDotIndex != -1) {
        String prefix = className.substring(0, firstDotIndex);
        return prefix.matches(CORAL_VERSIONED_UDF_PREFIX);
      }
    }
    return false;
  }

  /**
   * Removes the shading prefix from a given UDF class name if it is present.
   * A class name is considered shaded if the prefix before the first dot
   * follows {@link FunctionUtils#CORAL_VERSIONED_UDF_PREFIX} format
   */
  public static String removeShadingPrefix(String className) {
    if (isClassShaded(className)) {
      return className.substring(className.indexOf('.') + 1);
    } else {
      return className;
    }
  }

  /**
   * Generates a versioned function name based on the given function name and class name.
   * For example, if the function name is "myFunction" and the class name is "coralversionedudf_1_0_0.com.linkedin.MyClass",
   * the versioned function name will be "myFunction_1_0_0". If the class name is not shaded, such as "com.linkedin.MyClass",
   * the versioned function name will be "myFunction".
   */
  public static String getVersionedFunctionName(String functionName, String className) {
    String versionedPrefix = className.substring(0, className.indexOf('.'));
    Matcher matcher = Pattern.compile(CORAL_VERSIONED_UDF_PREFIX).matcher(versionedPrefix);
    if (matcher.find()) {
      return String.join("_", ImmutableList.of(functionName, matcher.group(1), matcher.group(2), matcher.group(3)));
    } else {
      return functionName;
    }
  }
}
