/**
 * Copyright 2018-2022 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.hive.hive2rel;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlRexContext;

import com.linkedin.coral.com.google.common.collect.ImmutableList;
import com.linkedin.coral.common.CoralConvertletTable;
import com.linkedin.coral.hive.hive2rel.functions.HiveInOperator;
import com.linkedin.coral.hive.hive2rel.functions.HiveNamedStructFunction;


/**
 * ConvertletTable for Hive Operators
 * @see ReflectiveConvertletTable documentation for method naming and visibility rules
 */
public class HiveConvertletTable extends CoralConvertletTable {

  @SuppressWarnings("unused")
  public RexNode convertNamedStruct(SqlRexContext cx, HiveNamedStructFunction func, SqlCall call) {
    List<RexNode> operandExpressions = new ArrayList<>(call.operandCount() / 2);
    for (int i = 0; i < call.operandCount(); i += 2) {
      operandExpressions.add(cx.convertExpression(call.operand(i + 1)));
    }
    RelDataType retType = cx.getValidator().getValidatedNodeType(call);
    RexNode rowNode = cx.getRexBuilder().makeCall(retType, SqlStdOperatorTable.ROW, operandExpressions);
    return cx.getRexBuilder().makeCast(retType, rowNode);
  }

  @SuppressWarnings("unused")
  public RexNode convertHiveInOperator(SqlRexContext cx, HiveInOperator operator, SqlCall call) {
    List<SqlNode> operandList = call.getOperandList();
    Preconditions.checkState(operandList.size() == 2 && operandList.get(1) instanceof SqlNodeList);
    RexNode lhs = cx.convertExpression(operandList.get(0));
    SqlNodeList rhsNodes = (SqlNodeList) operandList.get(1);
    ImmutableList.Builder<RexNode> rexNodes = ImmutableList.<RexNode> builder().add(lhs);
    for (int i = 0; i < rhsNodes.size(); i++) {
      rexNodes.add(cx.convertExpression(rhsNodes.get(i)));
    }

    RelDataType retType = cx.getValidator().getValidatedNodeType(call);
    return cx.getRexBuilder().makeCall(retType, HiveInOperator.IN, rexNodes.build());
  }
}
