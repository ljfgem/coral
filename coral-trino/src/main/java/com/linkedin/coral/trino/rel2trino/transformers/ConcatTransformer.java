/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.trino.rel2trino.transformers;

import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

import com.linkedin.coral.common.transformers.SqlCallTransformer;

import static org.apache.calcite.sql.parser.SqlParserPos.*;


public class ConcatTransformer extends SqlCallTransformer {

  private static final String CONCAT_OPERATOR = "CONCAT";

  public ConcatTransformer(SqlValidator sqlValidator) {
    super(sqlValidator);
  }

  @Override
  protected boolean predicate(SqlCall sqlCall) {
    return CONCAT_OPERATOR.equalsIgnoreCase(sqlCall.getOperator().getName());
  }

  @Override
  protected SqlCall transform(SqlCall sqlCall) {
    for (int i = 0; i < sqlCall.getOperandList().size(); ++i) {
      final SqlNode operand = sqlCall.getOperandList().get(i);
      final SqlTypeName sqlTypeName = getRelDataType(operand).getSqlTypeName();
      if (sqlTypeName != SqlTypeName.VARCHAR && sqlTypeName != SqlTypeName.CHAR) {
        final SqlCall castCall = SqlStdOperatorTable.CAST.createCall(operand.getParserPosition(), operand,
            createBasicTypeSpec(SqlTypeName.VARCHAR));
        sqlCall.setOperand(i, castCall);
      }
    }
    return sqlCall;
  }

  private SqlDataTypeSpec createBasicTypeSpec(SqlTypeName type) {
    final SqlTypeNameSpec typeNameSpec = new SqlBasicTypeNameSpec(type, ZERO);
    return new SqlDataTypeSpec(typeNameSpec, ZERO);
  }
}
