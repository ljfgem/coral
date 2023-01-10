/**
 * Copyright 2017-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.transformers;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;


/**
 * Abstract class for generic transformations on SqlCalls
 */
public abstract class OperatorTransformer {
  private SqlValidator sqlValidator;
  private SqlValidatorScope selectScope;

  public OperatorTransformer() {

  }

  public OperatorTransformer(SqlValidator sqlValidator, SqlValidatorScope selectScope) {
    this.sqlValidator = sqlValidator;
    this.selectScope = selectScope;
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
    if (sqlValidator == null) {
      throw new RuntimeException("Please provide sqlValidator to get the RelDataType of a SqlNode!");
    }
    return sqlValidator.deriveType(selectScope, sqlNode);
  }
}
