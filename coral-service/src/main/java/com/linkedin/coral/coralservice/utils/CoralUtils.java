package com.linkedin.coral.coralservice.utils;

import com.linkedin.coral.common.HiveMetastoreClient;
import com.linkedin.coral.common.HiveMscAdapter;
import com.linkedin.coral.coralservice.metastore.MetastoreProvider;
import com.linkedin.coral.hive.hive2rel.HiveToRelConverter;
import com.linkedin.coral.trino.rel2trino.RelToTrinoConverter;
import com.linkedin.coral.trino.trino2rel.TrinoToRelConverter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.springframework.context.annotation.Configuration;

import static com.linkedin.coral.coralservice.CoralServiceApplication.*;


/**
 * Utility class to provide Coral functionality to Coral Service.
 */
@Configuration
public class CoralUtils {

  private static HiveMetastoreClient hiveMetastoreClient;

  // Converters
  public static HiveToRelConverter hiveToRelConverter;
  public static TrinoToRelConverter trinoToRelConverter;
  public static RelToTrinoConverter relToTrinoConverter = new RelToTrinoConverter();

  // Local Metastore
  public static Driver driver;
  public static HiveConf conf;
  public static final String CORAL_SERVICE_DIR = "coral.service.test.dir";

  public static void initHiveMetastoreClient() throws Exception{
    // Connect to remote production Hive Metastore Client
    hiveMetastoreClient  = MetastoreProvider.getMetastoreClient();
    hiveToRelConverter = new HiveToRelConverter(hiveMetastoreClient);
    trinoToRelConverter  = new TrinoToRelConverter(hiveMetastoreClient);
  }

  public static void initLocalMetastore() throws IOException, HiveException, MetaException {
    // Create a temporary local metastore
    conf = loadResourceHiveConf();

    try {
      // Delete existing local metastore if it exists
      String tempDir = conf.get(CORAL_SERVICE_DIR);
      LOGGER.info("Temp Workspace: " + tempDir);
      FileUtils.deleteDirectory(new File(tempDir));
    } catch (IOException e) {
      e.printStackTrace();
    }

    SessionState.start(conf);
    driver = new Driver(conf);

    hiveMetastoreClient = new HiveMscAdapter(Hive.get(conf).getMSC());
    hiveToRelConverter = new HiveToRelConverter(hiveMetastoreClient);
    trinoToRelConverter  = new TrinoToRelConverter(hiveMetastoreClient);
  }

  public static void run(Driver driver, String sql) {
    while (true) {
      try {
        CommandProcessorResponse result = driver.run(sql);
        if (result.getException() != null) {
          throw new RuntimeException("Execution failed for: " + sql, result.getException());
        }
      } catch (CommandNeedRetryException e) {
        continue;
      }
      break;
    }
  }

  public static HiveConf loadResourceHiveConf() {
    InputStream hiveConfStream = CoralUtils.class.getClassLoader().getResourceAsStream("hive.xml");
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(CORAL_SERVICE_DIR,
        System.getProperty("java.io.tmpdir") + "/coral/service/" + UUID.randomUUID());
    hiveConf.addResource(hiveConfStream);
    hiveConf.set("mapreduce.framework.name", "local");
    hiveConf.set("_hive.hdfs.session.path", "/tmp/coral/service");
    hiveConf.set("_hive.local.session.path", "/tmp/coral/service");
    return hiveConf;
  }
}