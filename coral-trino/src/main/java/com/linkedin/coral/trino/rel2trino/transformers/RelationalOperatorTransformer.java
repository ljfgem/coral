/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.trino.rel2trino.transformers;

import java.util.Set;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlValidator;

import com.linkedin.coral.com.google.common.collect.ImmutableMultimap;
import com.linkedin.coral.com.google.common.collect.ImmutableSet;
import com.linkedin.coral.com.google.common.collect.Multimap;
import com.linkedin.coral.common.transformers.SqlCallTransformer;
import com.linkedin.coral.trino.rel2trino.TrinoTryCastFunction;

import static org.apache.calcite.sql.parser.SqlParserPos.*;


public class RelationalOperatorTransformer extends SqlCallTransformer {

  // SUPPORTED_TYPE_CAST_MAP is a static mapping that maps a SqlTypeFamily key to its set of
  // type-castable SqlTypeFamilies.
  private static final Multimap<SqlTypeFamily, SqlTypeFamily> SUPPORTED_TYPE_CAST_MAP =
      ImmutableMultimap.<SqlTypeFamily, SqlTypeFamily> builder()
          .putAll(SqlTypeFamily.CHARACTER, SqlTypeFamily.BOOLEAN, SqlTypeFamily.NUMERIC, SqlTypeFamily.DATE,
              SqlTypeFamily.TIME, SqlTypeFamily.TIMESTAMP)
          .putAll(SqlTypeFamily.NUMERIC, SqlTypeFamily.DATE, SqlTypeFamily.TIME, SqlTypeFamily.TIMESTAMP)
          .putAll(SqlTypeFamily.BOOLEAN, SqlTypeFamily.NUMERIC)
          .putAll(SqlTypeFamily.BINARY, SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC).build();
  private static final Set<SqlKind> RELATIONAL_OPERATOR_SQL_KIND = ImmutableSet.of(SqlKind.EQUALS, SqlKind.GREATER_THAN,
      SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.NOT_EQUALS);

  public RelationalOperatorTransformer(SqlValidator sqlValidator) {
    super(sqlValidator);
  }

  @Override
  protected boolean condition(SqlCall sqlCall) {
    final SqlOperator operator = sqlCall.getOperator();
    if (RELATIONAL_OPERATOR_SQL_KIND.contains(operator.getKind())) {
      SqlNode leftOperand = sqlCall.operand(0);
      final SqlNode rightOperand = sqlCall.operand(1);
      if (leftOperand.getKind() == SqlKind.CAST) {
        leftOperand = ((SqlCall) leftOperand).operand(0);
      }
      return SUPPORTED_TYPE_CAST_MAP.containsEntry(getRelDataType(leftOperand).getSqlTypeName().getFamily(),
          getRelDataType(rightOperand).getSqlTypeName().getFamily());
    }
    return false;
  }

  @Override
  protected SqlCall transform(SqlCall sqlCall) {
    SqlNode leftOperand = sqlCall.operand(0);
    final SqlNode rightOperand = sqlCall.operand(1);
    if (leftOperand.getKind() == SqlKind.CAST) {
      leftOperand = ((SqlCall) leftOperand).operand(0);
    }

    final SqlCall tryCastNode = TrinoTryCastFunction.INSTANCE.createCall(ZERO, leftOperand,
        getSqlDataTypeSpecForCasting(getRelDataType(rightOperand)));
    return sqlCall.getOperator().createCall(ZERO, tryCastNode, rightOperand);
  }

  private SqlDataTypeSpec getSqlDataTypeSpecForCasting(RelDataType relDataType) {
    final SqlTypeNameSpec typeNameSpec = new SqlBasicTypeNameSpec(relDataType.getSqlTypeName(), ZERO);
    return new SqlDataTypeSpec(typeNameSpec, ZERO);
  }
}
