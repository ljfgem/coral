/**
 * Copyright 2017-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.transformers;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;


/**
 * Abstract class for generic transformations on SqlCalls
 */
public abstract class OperatorTransformer {
  SqlNodeDataTypeUtil sqlNodeDataTypeUtil;

  public OperatorTransformer() {

  }

  public OperatorTransformer(SqlNodeDataTypeUtil sqlNodeDataTypeUtil) {
    this.sqlNodeDataTypeUtil = sqlNodeDataTypeUtil;
  }

  protected abstract boolean condition(SqlCall sqlCall);

  protected abstract SqlCall transform(SqlCall sqlCall);

  public SqlCall apply(SqlCall sqlCall) {
    if (condition(sqlCall)) {
      return transform(sqlCall);
    } else {
      return sqlCall;
    }
  }

  protected RelDataType getRelDataType(SqlNode sqlNode) {
    if (sqlNodeDataTypeUtil == null) {
      throw new RuntimeException("Please provide sqlNodeDataTypeUtil to get the RelDataType of a SqlNode!");
    }
    return sqlNodeDataTypeUtil.getRelDataType(sqlNode);
  }
}
