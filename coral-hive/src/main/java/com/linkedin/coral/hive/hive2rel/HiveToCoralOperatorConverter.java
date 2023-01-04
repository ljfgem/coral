/**
 * Copyright 2017-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.hive.hive2rel;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;

import com.linkedin.coral.com.google.common.collect.ImmutableList;
import com.linkedin.coral.common.transformers.OperatorTransformer;
import com.linkedin.coral.transformers.OneBasedArrayIndexTransformer;


/**
 * Converts Hive SqlNode to Coral SqlNode
 */
public class HiveToCoralOperatorConverter extends SqlShuttle {
  private final SqlValidator sqlValidator;
  private final List<OperatorTransformer> OPERATOR_TRANSFORMERS = ImmutableList.of(new OneBasedArrayIndexTransformer());

  public HiveToCoralOperatorConverter(SqlValidator sqlValidator) {
    this.sqlValidator = sqlValidator;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    for (OperatorTransformer operatorTransformer : OPERATOR_TRANSFORMERS) {
      call = (SqlCall) operatorTransformer.apply(call, sqlValidator);
    }
    return super.visit(call);
  }
}
