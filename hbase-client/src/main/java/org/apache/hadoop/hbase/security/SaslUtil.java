/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.security;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherOption;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.security.SaslRpcServer;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;

@InterfaceAudience.Private
public class SaslUtil {
  private static final Log log = LogFactory.getLog(SaslUtil.class);

  public static final String HBASE_RPC_SECURITY_CRYPTO_CIPHER_SUITES =
          "hbase.rpc.security.crypto.cipher.suites";
  public static final String HBASE_RPC_SECURITY_CRYPTO_CIPHER_KEY_BITLENGTH_KEY =
          "hbase.rpc.security.crypto.cipher.key.bitlength";
  public static final int HBASE_RPC_SECURITY_CRYPTO_CIPHER_KEY_BITLENGTH_DEFAULT = 128;
  public static final String SASL_DEFAULT_REALM = "default";
  public static final int SWITCH_TO_SIMPLE_AUTH = -88;
  public static final int USE_NEGOTIATED_CIPHER = -89;

  public enum QualityOfProtection {
    AUTHENTICATION("auth"),
    INTEGRITY("auth-int"),
    PRIVACY("auth-conf");

    private final String saslQop;

    QualityOfProtection(String saslQop) {
      this.saslQop = saslQop;
    }

    public String getSaslQop() {
      return saslQop;
    }

    public boolean matches(String stringQop) {
        if (saslQop.equals(stringQop)) {
          log.warn(
            "Use authentication/integrity/privacy as value for rpc protection " +
                    "configurations instead of auth/auth-int/auth-conf");
          return true;
        }
        return name().equalsIgnoreCase(stringQop);
    }
  }

  /** Splitting fully qualified Kerberos name into parts */
  public static String[] splitKerberosName(String fullName) {
    return fullName.split("[/@]");
  }

  static String encodeIdentifier(byte[] identifier) {
    return new String(Base64.encodeBase64(identifier));
  }

  static byte[] decodeIdentifier(String identifier) {
    return Base64.decodeBase64(identifier.getBytes());
  }

  static char[] encodePassword(byte[] password) {
    return new String(Base64.encodeBase64(password)).toCharArray();
  }

  /**
   * Returns {@link org.apache.hadoop.hbase.security.SaslUtil.QualityOfProtection}
   * corresponding to the given {@code stringQop} value. Returns null if the value is
   * invalid.
   * @param stringQop
   */
  public static QualityOfProtection getQop(String stringQop) {
    for (QualityOfProtection qop: QualityOfProtection.values()) {
      if (qop.matches(stringQop)) {
        return qop;
      }
    }
    throw new IllegalArgumentException("Invalid qop: " + stringQop +
      ". It must be one of :" + QualityOfProtection.values().toString());
  }
  static Map<String, String> initSaslProperties(String rpcProtection) {
    String saslQop;
    if (rpcProtection.isEmpty()) {
      saslQop = QualityOfProtection.AUTHENTICATION.getSaslQop();
    } else {
      String[] qops = rpcProtection.split(",");
      StringBuilder saslQopBuilder = new StringBuilder();
      for (int i = 0; i < qops.length; i++) {
        QualityOfProtection qop = getQop(qops[i]);
        saslQopBuilder.append(",").append(qop.getSaslQop());
      }
      saslQop = saslQopBuilder.substring(1);
    }
    Map<String, String> saslProps = new TreeMap<>();
    saslProps.put(Sasl.QOP, saslQop);
    saslProps.put(Sasl.SERVER_AUTH, "true");
    return saslProps;
  }
  /**
   * Check whether requested SASL Qop contains privacy.
   *
   * @param saslProps properties of SASL negotiation
   * @return boolean true if privacy exists
   */
  public static boolean requestedQopContainsPrivacy(
          Map<String, String> saslProps) {
    Set<String> requestedQop = ImmutableSet.copyOf(Arrays.asList(
            saslProps.get(Sasl.QOP).split(",")));
    return requestedQop.contains(
            SaslRpcServer.QualityOfProtection.PRIVACY.getSaslQop());
  }

  /**
   * After successful SASL negotiation, returns whether it's QOP privacy.
   *
   * @return boolean whether it's QOP privacy
   */
  public static boolean isNegotiatedQopPrivacy(SaslServer saslServer) {
    String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
    return qop != null && SaslUtil.QualityOfProtection.PRIVACY
            .getSaslQop().equalsIgnoreCase(qop);
  }

  /**
   * After successful SASL negotiation, returns whether it's QOP privacy.
   *
   * @return boolean whether it's QOP privacy
   */
  public static boolean isNegotiatedQopPrivacy(SaslClient saslClient) {
    String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
    return qop != null && SaslUtil.QualityOfProtection.PRIVACY
            .getSaslQop().equalsIgnoreCase(qop);
  }

  /**
   * Negotiate a cipher option which server supports.
   *
   * @param conf the configuration
   * @param options the cipher options which client supports
   * @return CipherOption negotiated cipher option
   */
  public static CipherOption negotiateCipherOption(Configuration conf,
                                                   List<CipherOption> options) throws IOException {
    // Negotiate cipher suites if configured.  Currently, the only supported
    // cipher suite is AES/CTR/NoPadding, but the protocol allows multiple
    // values for future expansion.
    String cipherSuites = conf.get(HBASE_RPC_SECURITY_CRYPTO_CIPHER_SUITES);
    if (cipherSuites == null || cipherSuites.isEmpty()) {
      return null;
    }
    if (!cipherSuites.equals(CipherSuite.AES_CTR_NOPADDING.getName())) {
      throw new IOException(String.format("Invalid cipher suite, %s=%s",
              HBASE_RPC_SECURITY_CRYPTO_CIPHER_SUITES, cipherSuites));
    }
    if (options != null) {
      for (CipherOption option : options) {
        CipherSuite suite = option.getCipherSuite();
        if (suite == CipherSuite.AES_CTR_NOPADDING) {
          int keyLen = conf.getInt(
                  HBASE_RPC_SECURITY_CRYPTO_CIPHER_KEY_BITLENGTH_KEY,
                  HBASE_RPC_SECURITY_CRYPTO_CIPHER_KEY_BITLENGTH_DEFAULT) / 8;
          CryptoCodec codec = CryptoCodec.getInstance(conf, suite);
          byte[] inKey = new byte[keyLen];
          byte[] inIv = new byte[suite.getAlgorithmBlockSize()];
          byte[] outKey = new byte[keyLen];
          byte[] outIv = new byte[suite.getAlgorithmBlockSize()];
          assert codec != null;
          codec.generateSecureRandom(inKey);
          codec.generateSecureRandom(inIv);
          codec.generateSecureRandom(outKey);
          codec.generateSecureRandom(outIv);
          return new CipherOption(suite, inKey, inIv, outKey, outIv);
        }
      }
    }
    return null;
  }

  /**
   * Encrypt the key of the negotiated cipher option.
   *
   * @param option negotiated cipher option
   * @param saslServer SASL server
   * @return CipherOption negotiated cipher option which contains the
   * encrypted key and iv
   * @throws IOException for any error
   */
  public static CipherOption wrap(CipherOption option, SaslServer saslServer)
          throws IOException {
    if (option != null) {
      byte[] inKey = option.getInKey();
      if (inKey != null) {
        inKey = saslServer.wrap(inKey, 0, inKey.length);
      }
      byte[] outKey = option.getOutKey();
      if (outKey != null) {
        outKey = saslServer.wrap(outKey, 0, outKey.length);
      }
      return new CipherOption(option.getCipherSuite(), inKey, option.getInIv(),
              outKey, option.getOutIv());
    }

    return null;
  }

  /**
   * Decrypt the key of the negotiated cipher option.
   *
   * @param option negotiated cipher option
   * @param saslClient SASL client
   * @return CipherOption negotiated cipher option which contains the
   * decrypted key and iv
   * @throws IOException for any error
   */
  public static CipherOption unwrap(CipherOption option, SaslClient saslClient)
          throws IOException {
    if (option != null) {
      byte[] inKey = option.getInKey();
      if (inKey != null) {
        inKey = saslClient.unwrap(inKey, 0, inKey.length);
      }
      byte[] outKey = option.getOutKey();
      if (outKey != null) {
        outKey = saslClient.unwrap(outKey, 0, outKey.length);
      }
      return new CipherOption(option.getCipherSuite(), inKey, option.getInIv(),
              outKey, option.getOutIv());
    }

    return null;
  }

  /**
   * Read the cipher options from the given string.
   *
   * @param cipherSuites the ciphers as a string
   * @return List<CipherOption> of the cipher options
   */
  public static List<CipherOption> getCipherOptions(String cipherSuites)
          throws IOException {
    List<CipherOption> cipherOptions = null;
    if (cipherSuites != null && !cipherSuites.isEmpty()) {
      cipherOptions = Lists.newArrayListWithCapacity(1);
      for (String cipherSuite : Splitter.on(',').trimResults().
              omitEmptyStrings().split(cipherSuites)) {
        CipherOption option = new CipherOption(
                CipherSuite.convert(cipherSuite));
        cipherOptions.add(option);
      }
    }
    return cipherOptions;
  }
}
