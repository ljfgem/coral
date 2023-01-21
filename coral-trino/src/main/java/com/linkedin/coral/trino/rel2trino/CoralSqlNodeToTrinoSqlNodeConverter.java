/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.trino.rel2trino;

import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;

import com.linkedin.coral.common.transformers.SqlCallTransformers;
import com.linkedin.coral.trino.rel2trino.transformers.ConcatTransformer;


public class CoralSqlNodeToTrinoSqlNodeConverter extends SqlShuttle {
  private final SqlCallTransformers sqlCallTransformers;

  public CoralSqlNodeToTrinoSqlNodeConverter(SqlValidator sqlValidator, Map<String, Boolean> configs) {
    this.sqlCallTransformers = SqlCallTransformers.of(new ConcatTransformer(sqlValidator));
  }

  @Override
  public SqlNode visit(SqlCall call) {
    return super.visit(sqlCallTransformers.apply(call));
  }
}
