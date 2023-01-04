/**
 * Copyright 2017-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.transformers;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlValidator;


/**
 * Abstract class for generic transformations on SqlNodes
 */
public abstract class OperatorTransformer {
  protected SqlNode inputSqlNode;
  protected SqlValidator sqlValidator;
  protected List<SqlSelect> topSelectNodes = new ArrayList<>();

  protected abstract boolean condition();

  protected abstract SqlNode transform();

  public SqlNode apply(SqlNode inputSqlNode) {
    return apply(inputSqlNode, null);
  }

  /**
   * @param inputSqlNode input SqlNode to be transformed
   * @param sqlValidator Calcite SqlValidator, which is used to derive RelDataType of the SqlNode
   * @return transformed SqlNode
   */
  public SqlNode apply(SqlNode inputSqlNode, SqlValidator sqlValidator) {
    this.inputSqlNode = inputSqlNode;
    this.sqlValidator = sqlValidator;
    if (inputSqlNode instanceof SqlSelect) {
      // Store the top select SqlNodes, which will be used to construct the dummy SqlSelect to derive the RelDataType
      // of a SqlNode
      this.topSelectNodes.add((SqlSelect) inputSqlNode);
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
    if (sqlValidator == null) {
      throw new RuntimeException("Please provide sqlValidator to get the RelDataType of a SqlNode!");
    }
    // Traverse the stored top select SqlNodes from new to old to construct the dummy SqlSelect,
    // return the SqlNode's RelDataType directly if SqlValidator can validate the dummy SqlSelect
    for (int i = topSelectNodes.size() - 1; i >= 0; --i) {
      final SqlSelect topSelectNode = topSelectNodes.get(i);
      final SqlSelect dummySqlSelect = new SqlSelect(topSelectNode.getParserPosition(), null, SqlNodeList.of(sqlNode),
          topSelectNode.getFrom(), null, null, null, null, null, null, null);
      try {
        sqlValidator.validate(dummySqlSelect);
        return sqlValidator.getValidatedNodeType(dummySqlSelect).getFieldList().get(0).getType();
      } catch (Throwable ignored) {
      }
    }
    throw new RuntimeException("Failed to derive the RelDataType for SqlNode " + sqlNode);
  }
}
