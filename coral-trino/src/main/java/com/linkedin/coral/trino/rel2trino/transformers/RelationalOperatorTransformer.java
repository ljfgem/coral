/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.trino.rel2trino.transformers;

import java.util.List;
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

import com.linkedin.coral.com.google.common.collect.ImmutableList;
import com.linkedin.coral.com.google.common.collect.ImmutableMultimap;
import com.linkedin.coral.com.google.common.collect.ImmutableSet;
import com.linkedin.coral.com.google.common.collect.Multimap;
import com.linkedin.coral.common.transformers.SqlCallTransformer;
import com.linkedin.coral.trino.rel2trino.TrinoTryCastFunction;

import static org.apache.calcite.sql.SqlKind.*;
import static org.apache.calcite.sql.parser.SqlParserPos.*;
import static org.apache.calcite.sql.type.SqlTypeFamily.*;


/**
 * Coral IR allows implicit castings like casting from int to varchar for relational operators. However, Trino requires explicit casting.
 * This transformer adds explicit {@link TrinoTryCastFunction} to ensure operand inter-compatibility for Trino.
 *
 * For example, `"0" = 1` is transformed into `TRY_CAST("0" AS INT) = 1`
 */
public class RelationalOperatorTransformer extends SqlCallTransformer {

  // SUPPORTED_TYPE_CAST_MAP is a static mapping that maps a SqlTypeFamily key to its set of
  // type-castable SqlTypeFamilies.
  private static final Multimap<SqlTypeFamily, SqlTypeFamily> SUPPORTED_TYPE_CAST_MAP = ImmutableMultimap
      .<SqlTypeFamily, SqlTypeFamily> builder().putAll(CHARACTER, BOOLEAN, NUMERIC, DATE, TIME, TIMESTAMP)
      .putAll(NUMERIC, DATE, TIME, TIMESTAMP).putAll(BOOLEAN, NUMERIC).putAll(BINARY, CHARACTER, NUMERIC).build();
  private static final Set<SqlKind> RELATIONAL_OPERATOR_SQL_KIND = ImmutableSet.of(SqlKind.EQUALS, SqlKind.GREATER_THAN,
      SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.NOT_EQUALS);

  public RelationalOperatorTransformer(SqlValidator sqlValidator) {
    super(sqlValidator);
  }

  @Override
  protected boolean condition(SqlCall sqlCall) {
    final SqlOperator operator = sqlCall.getOperator();
    if (RELATIONAL_OPERATOR_SQL_KIND.contains(operator.getKind())) {
      final SqlNode leftOperand = sqlCall.operand(0);
      final SqlNode rightOperand = sqlCall.operand(1);
      return leftOperand.getKind() != CAST && rightOperand.getKind() != CAST;
    }
    return false;
  }

  @Override
  protected SqlCall transform(SqlCall sqlCall) {
    final SqlNode leftOperand = sqlCall.operand(0);
    final SqlNode rightOperand = sqlCall.operand(1);

    final RelDataType leftRelDataType = getRelDataType(leftOperand);
    final RelDataType rightRelDataType = getRelDataType(rightOperand);

    final SqlTypeFamily leftSqlTypeFamily = leftRelDataType.getSqlTypeName().getFamily();
    final SqlTypeFamily rightSqlTypeFamily = rightRelDataType.getSqlTypeName().getFamily();

    List<SqlNode> updatedOperands = sqlCall.getOperandList();

    if (SUPPORTED_TYPE_CAST_MAP.containsEntry(leftSqlTypeFamily, rightSqlTypeFamily)) {
      updatedOperands = ImmutableList.of(tryCastOperandToType(leftOperand, rightRelDataType), rightOperand);
    } else if (SUPPORTED_TYPE_CAST_MAP.containsEntry(rightSqlTypeFamily, leftSqlTypeFamily)) {
      updatedOperands = ImmutableList.of(leftOperand, tryCastOperandToType(rightOperand, leftRelDataType));
    }
    return sqlCall.getOperator().createCall(ZERO, updatedOperands);
  }

  private SqlCall tryCastOperandToType(SqlNode operand, RelDataType relDataType) {
    final SqlTypeNameSpec typeNameSpec = new SqlBasicTypeNameSpec(relDataType.getSqlTypeName(), ZERO);
    final SqlDataTypeSpec sqlDataTypeSpec = new SqlDataTypeSpec(typeNameSpec, ZERO);
    return TrinoTryCastFunction.INSTANCE.createCall(ZERO, operand, sqlDataTypeSpec);
  }
}
