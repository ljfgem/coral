/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.spark;

import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlShuttle;

import com.linkedin.coral.common.calcite.CalciteUtil;
import com.linkedin.coral.common.transformers.OperatorBasedSqlCallTransformer;
import com.linkedin.coral.common.transformers.SqlCallTransformers;
import com.linkedin.coral.spark.containers.SparkUDFInfo;
import com.linkedin.coral.spark.transformers.AddExplicitAliasTransformer;
import com.linkedin.coral.spark.transformers.CastToNamedStructTransformer;
import com.linkedin.coral.spark.transformers.FallBackToHiveUDFTransformer;
import com.linkedin.coral.spark.transformers.FuzzyUnionGenericProjectTransformer;
import com.linkedin.coral.spark.transformers.RemoveRedundantCastInCaseTransformer;
import com.linkedin.coral.spark.transformers.SwapExtractUnionFunctionTransformer;
import com.linkedin.coral.spark.transformers.TransportableUDFTransformer;

import static com.linkedin.coral.spark.transformers.TransportableUDFTransformer.*;


/**
 * This class extends the class of SqlShuttle and initialize a SqlCallTransformers which containing a list of SqlCallTransformers
 * to traverse the hierarchy of a SqlCall and converts the functions from Coral operator to Spark operator if it is required
 */
public class CoralToSparkSqlCallConverter extends SqlShuttle {
  private final SqlCallTransformers sqlCallTransformers;

  public CoralToSparkSqlCallConverter(Set<SparkUDFInfo> sparkUDFInfos, List<String> aliases) {
    this.sqlCallTransformers = SqlCallTransformers.of(new CastToNamedStructTransformer(),
        new FuzzyUnionGenericProjectTransformer(sparkUDFInfos), new SwapExtractUnionFunctionTransformer(sparkUDFInfos),
        new RemoveRedundantCastInCaseTransformer(),
        // Transportable UDFs
        new TransportableUDFTransformer("com.linkedin.dali.udf.date.hive.DateFormatToEpoch",
            "com.linkedin.stdudfs.daliudfs.spark.DateFormatToEpoch", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.dali.udf.date.hive.EpochToDateFormat",
            "com.linkedin.stdudfs.daliudfs.spark.EpochToDateFormat", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.dali.udf.date.hive.EpochToEpochMilliseconds",
            "com.linkedin.stdudfs.daliudfs.spark.EpochToEpochMilliseconds", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.dali.udf.isguestmemberid.hive.IsGuestMemberId",
            "com.linkedin.stdudfs.daliudfs.spark.IsGuestMemberId", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.dali.udf.istestmemberid.hive.IsTestMemberId",
            "com.linkedin.stdudfs.daliudfs.spark.IsTestMemberId", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.dali.udf.maplookup.hive.MapLookup",
            "com.linkedin.stdudfs.daliudfs.spark.MapLookup", DALI_UDFS_IVY_URL_SPARK_2_11, DALI_UDFS_IVY_URL_SPARK_2_12,
            sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.dali.udf.sanitize.hive.Sanitize",
            "com.linkedin.stdudfs.daliudfs.spark.Sanitize", DALI_UDFS_IVY_URL_SPARK_2_11, DALI_UDFS_IVY_URL_SPARK_2_12,
            sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.dali.udf.watbotcrawlerlookup.hive.WATBotCrawlerLookup",
            "com.linkedin.stdudfs.daliudfs.spark.WatBotCrawlerLookup", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.daliudfs.hive.DateFormatToEpoch",
            "com.linkedin.stdudfs.daliudfs.spark.DateFormatToEpoch", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.daliudfs.hive.EpochToDateFormat",
            "com.linkedin.stdudfs.daliudfs.spark.EpochToDateFormat", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.daliudfs.hive.EpochToEpochMilliseconds",
            "com.linkedin.stdudfs.daliudfs.spark.EpochToEpochMilliseconds", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.daliudfs.hive.GetProfileSections",
            "com.linkedin.stdudfs.daliudfs.spark.GetProfileSections", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.stringudfs.hive.InitCap",
            "com.linkedin.stdudfs.stringudfs.spark.InitCap",
            "ivy://com.linkedin.standard-udfs-common-sql-udfs:standard-udfs-string-udfs:1.0.1?classifier=spark_2.11",
            "ivy://com.linkedin.standard-udfs-common-sql-udfs:standard-udfs-string-udfs:1.0.1?classifier=spark_2.12",
            sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.daliudfs.hive.IsGuestMemberId",
            "com.linkedin.stdudfs.daliudfs.spark.IsGuestMemberId", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.daliudfs.hive.IsTestMemberId",
            "com.linkedin.stdudfs.daliudfs.spark.IsTestMemberId", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.daliudfs.hive.MapLookup",
            "com.linkedin.stdudfs.daliudfs.spark.MapLookup", DALI_UDFS_IVY_URL_SPARK_2_11, DALI_UDFS_IVY_URL_SPARK_2_12,
            sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.daliudfs.hive.PortalLookup",
            "com.linkedin.stdudfs.daliudfs.spark.PortalLookup", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.daliudfs.hive.Sanitize",
            "com.linkedin.stdudfs.daliudfs.spark.Sanitize", DALI_UDFS_IVY_URL_SPARK_2_11, DALI_UDFS_IVY_URL_SPARK_2_12,
            sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.userinterfacelookup.hive.UserInterfaceLookup",
            "com.linkedin.stdudfs.userinterfacelookup.spark.UserInterfaceLookup",
            "ivy://com.linkedin.standard-udf-userinterfacelookup:userinterfacelookup-std-udf:0.0.27?classifier=spark_2.11",
            "ivy://com.linkedin.standard-udf-userinterfacelookup:userinterfacelookup-std-udf:0.0.27?classifier=spark_2.12",
            sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.daliudfs.hive.WatBotCrawlerLookup",
            "com.linkedin.stdudfs.daliudfs.spark.WatBotCrawlerLookup", DALI_UDFS_IVY_URL_SPARK_2_11,
            DALI_UDFS_IVY_URL_SPARK_2_12, sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.jemslookup.udf.hive.JemsLookup",
            "com.linkedin.jemslookup.udf.spark.JemsLookup",
            "ivy://com.linkedin.jobs-udf:jems-udfs:2.1.7?classifier=spark_2.11",
            "ivy://com.linkedin.jobs-udf:jems-udfs:2.1.7?classifier=spark_2.12", sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.parsing.hive.UserAgentParser",
            "com.linkedin.stdudfs.parsing.spark.UserAgentParser",
            "ivy://com.linkedin.standard-udfs-parsing:parsing-stdudfs:3.0.3?classifier=spark_2.11",
            "ivy://com.linkedin.standard-udfs-parsing:parsing-stdudfs:3.0.3?classifier=spark_2.12", sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.parsing.hive.Ip2Str",
            "com.linkedin.stdudfs.parsing.spark.Ip2Str",
            "ivy://com.linkedin.standard-udfs-parsing:parsing-stdudfs:3.0.3?classifier=spark_2.11",
            "ivy://com.linkedin.standard-udfs-parsing:parsing-stdudfs:3.0.3?classifier=spark_2.12", sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.stdudfs.lookup.hive.BrowserLookup",
            "com.linkedin.stdudfs.lookup.spark.BrowserLookup",
            "ivy://com.linkedin.standard-udfs-parsing:parsing-stdudfs:3.0.3?classifier=spark_2.11",
            "ivy://com.linkedin.standard-udfs-parsing:parsing-stdudfs:3.0.3?classifier=spark_2.12", sparkUDFInfos),

        new TransportableUDFTransformer("com.linkedin.jobs.udf.hive.ConvertIndustryCode",
            "com.linkedin.jobs.udf.spark.ConvertIndustryCode",
            "ivy://com.linkedin.jobs-udf:jobs-udfs:2.1.6?classifier=spark_2.11",
            "ivy://com.linkedin.jobs-udf:jobs-udfs:2.1.6?classifier=spark_2.12", sparkUDFInfos),

        // Transportable UDF for unit test
        new TransportableUDFTransformer("com.linkedin.coral.hive.hive2rel.CoralTestUDF",
            "com.linkedin.coral.spark.CoralTestUDF",
            "ivy://com.linkedin.coral.spark.CoralTestUDF?classifier=spark_2.11", null, sparkUDFInfos),

        // Built-in operator
        new OperatorBasedSqlCallTransformer("CARDINALITY", 1,
            CalciteUtil.createSqlOperatorOfFunction("size", ReturnTypes.INTEGER), null, null, null),

        // Fall back to the original Hive UDF defined in StaticHiveFunctionRegistry after failing to apply transformers above
        new FallBackToHiveUDFTransformer(sparkUDFInfos),

        new AddExplicitAliasTransformer(aliases));
  }

  @Override
  public SqlNode visit(SqlCall call) {
    final SqlCall transformedSqlCall = sqlCallTransformers.apply(call);
    return super.visit(transformedSqlCall);
  }
}
