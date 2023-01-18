/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.transformers;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;


public class SqlNodeDataTypeUtil {
  private final SqlValidator sqlValidator;
  private final SqlValidatorScope topSqlSelectScope;

  public SqlNodeDataTypeUtil(SqlValidator sqlValidator, SqlSelect topSqlSelect) {
    this.sqlValidator = sqlValidator;
    sqlValidator.validate(topSqlSelect);
    this.topSqlSelectScope = sqlValidator.getSelectScope(topSqlSelect);
  }

  public RelDataType getRelDataType(SqlNode sqlNode) {
    return sqlValidator.deriveType(topSqlSelectScope, sqlNode);
  }
}
