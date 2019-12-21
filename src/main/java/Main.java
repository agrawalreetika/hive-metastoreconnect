/**
 * Created by ragraw1 on 08/02/19.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;
import java.io.File;


import org.apache.hadoop.fs.Path;

import org.apache.hadoop.security.UserGroupInformation;

public class Main
{
    public static void  main(String [] args) throws MetaException,MalformedURLException,IOException
    {
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.addResource("/etc/hdfs-site.xml");

        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("ragraw1"+"@"+
                        "<Realm>",
                "/etc/.ragraw1.keytab");

        HiveConf hiveConf = new HiveConf(SessionState.class);
        hiveConf.addResource(new Path("/etc/hive-site.xml"));

        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);

        System.out.println("Metastore client : " + hiveMetaStoreClient);
        System.out.println(hiveMetaStoreClient.getAllTables("ragraw1"));
        
        hiveMetaStoreClient.close();
    }
}

