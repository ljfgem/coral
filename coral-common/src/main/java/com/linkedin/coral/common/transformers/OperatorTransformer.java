/**
 * Copyright 2017-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.transformers;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.linkedin.coral.common.ToRelConverter;


/**
 * Abstract class for generic transformations on SqlNodes
 */
public abstract class OperatorTransformer {
  protected SqlNode inputSqlNode;
  private ToRelConverter toRelConverter;
  private SqlSelect topSelectNode;
  private Boolean isTopSelectNodeConverted = false;

  protected abstract boolean condition();

  protected abstract SqlNode transform();

  public SqlNode apply(SqlNode inputSqlNode) {
    return apply(inputSqlNode, null);
  }

  public SqlNode apply(SqlNode inputSqlNode, ToRelConverter toRelConverter) {
    this.inputSqlNode = inputSqlNode;
    this.toRelConverter = toRelConverter;
    if (inputSqlNode instanceof SqlSelect && topSelectNode == null) {
      topSelectNode = (SqlSelect) inputSqlNode;
    }
    if (condition()) {
      return transform();
    } else {
      return inputSqlNode;
    }
  }

  /**
   * Get the RelDataType of the input SqlNode
   */
  protected RelDataType getRelDataType(SqlNode sqlNode) {
    if (toRelConverter == null) {
      throw new RuntimeException("Please provide toRelConverter to get the RelDataType of a SqlNode!");
    }
    if (!isTopSelectNodeConverted) {
      toRelConverter.toRel(topSelectNode);
      isTopSelectNodeConverted = true;
    }
    final SqlValidator sqlValidator = toRelConverter.getSqlValidator();
    final SqlValidatorScope selectScope = sqlValidator.getSelectScope(topSelectNode);
    return sqlValidator.deriveType(selectScope, sqlNode);
  }
}
