/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.catalyst.transport;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import io.atomix.catalyst.util.Assert;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty TLS.
 *
 * @author <a href="http://github.com/electrical">Richard Pijnenburg</a>
 */

public class NettyTls {

  private static final Logger LOGGER = LoggerFactory.getLogger(NettyTls.class);
  private static boolean ts_use_ks = true;
  private static KeyStore ts;
  private static NettyProperties properties;

  public NettyTls(NettyProperties properties) {
    this.properties = properties;
  }

  public SSLEngine InitSSLEngine(Boolean client) throws Exception {

    if (properties.sslTruststorePath() != null) {
      ts_use_ks = false;
    }

    // Load the keystore
    KeyStore ks = loadKeystore(properties.sslKeystorePath(), properties.sslKeystorePassword());

    // Setup the keyManager to use our keystore
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, KeyStoreKeyPass(properties));

    // Setup the Trust keystore
    if (ts_use_ks == false) {
      // Use the separate Trust keystore
      LOGGER.debug("Using separate Truststore");
      KeyStore ts = loadKeystore(properties.sslTruststorePath(), properties.sslTruststorePassword());
    } else {
      // Reuse the existing keystore
      ts = ks;
      LOGGER.debug("Using Keystore as Truststore");
    }

    TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmFactory.init(ts);

    KeyManager[] km = kmf.getKeyManagers();
    TrustManager[] tm = tmFactory.getTrustManagers();

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(km, tm, null);
    SSLEngine sslEngine = sslContext.createSSLEngine();
    sslEngine.setUseClientMode(client);
    sslEngine.setWantClientAuth(true);
    sslEngine.setEnabledProtocols(sslEngine.getSupportedProtocols());
    sslEngine.setEnabledCipherSuites(sslEngine.getSupportedCipherSuites());
    sslEngine.setEnableSessionCreation(true);

    return sslEngine;
  }

  private KeyStore loadKeystore(String path, String password) throws Exception {
    Assert.notNull(path, "Path");
    File file = new File(path);

    LOGGER.debug("Using JKS at {}", file.getCanonicalPath());
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(new FileInputStream(file.getCanonicalPath()), password.toCharArray());
    return ks;
  }

  private char[] KeyStoreKeyPass(NettyProperties properties) throws Exception {
    if (properties.sslKeystoreKeyPassword() != null) {
      return properties.sslKeystoreKeyPassword().toCharArray();
    } else {
      return properties.sslKeystorePassword().toCharArray();
    }
  }

}
