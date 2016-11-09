package com.cloudera.support.kms.keystore;

import java.net.URI;
import java.net.URL;
import java.security.KeyStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;


public class KeyStoreChecker {

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub

    Configuration conf = new Configuration(true);

    conf.addResource(new URL("file://" + new Path(args[0]).toUri()));

    System.out.println(">>>>" + args[1]);

    URI uri = new URI(args[1]);

    Path path = ProviderUtils.unnestUri(uri);

    FileSystem fs = path.getFileSystem(conf);

    FSDataInputStream in = fs.open(path);

    KeyStore keyStore = KeyStore.getInstance("jceks");

    keyStore.load(in, args[2].toCharArray());

    System.out.println(keyStore.size());

  }

}
