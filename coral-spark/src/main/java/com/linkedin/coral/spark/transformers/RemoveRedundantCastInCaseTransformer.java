/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.spark.transformers;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;

import com.linkedin.coral.common.transformers.SqlCallTransformer;


public class RemoveRedundantCastInCaseTransformer extends SqlCallTransformer {
  private SqlDataTypeSpec castNullToType;

  @Override
  protected boolean condition(SqlCall sqlCall) {
    if (sqlCall instanceof SqlCase) {
      final List<SqlNode> thenAndElseOperands = new ArrayList<>(((SqlCase) sqlCall).getThenOperands().getList());
      thenAndElseOperands.add(((SqlCase) sqlCall).getElseOperand());
      for (SqlNode thenOrElseOperand : thenAndElseOperands) {
        if (thenOrElseOperand.getKind() == SqlKind.CAST) {
          final SqlNode firstOperandOfCast = ((SqlCall) thenOrElseOperand).getOperandList().get(0);
          if (firstOperandOfCast instanceof SqlLiteral
              && ((SqlLiteral) firstOperandOfCast).getTypeName() == SqlTypeName.NULL) {
            castNullToType = (SqlDataTypeSpec) ((SqlCall) thenOrElseOperand).getOperandList().get(1);
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  protected SqlCall transform(SqlCall sqlCall) {
    final SqlCase sqlCase = (SqlCase) sqlCall;
    for (int i = 0; i < sqlCase.getThenOperands().size(); ++i) {
      final SqlNode thenOperand = sqlCase.getThenOperands().get(i);
      sqlCase.getThenOperands().set(i, removeRedundantCast(thenOperand));
    }
    // 3 is the position of `else` operand
    sqlCase.setOperand(3, removeRedundantCast(sqlCase.getElseOperand()));
    return sqlCase;
  }

  private SqlNode removeRedundantCast(SqlNode sqlNode) {
    if (sqlNode.getKind() == SqlKind.CAST
        && castNullToType.equalsDeep(((SqlCall) sqlNode).getOperandList().get(1), Litmus.IGNORE)) {
      return ((SqlCall) sqlNode).getOperandList().get(0);
    } else {
      return sqlNode;
    }
  }
}
