package org.apache.hadoop.hbase.security;

/**
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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseMultiRealmUserAuthentication {

  private static final Log LOG = LogFactory.getLog(HBaseMultiRealmUserAuthentication.class);
  // Configuration setttings copy-pasted from Hadoop
  public static String KERBEROS_USER_REALM_PRINCIPAL =
      "hadoop.security.authentication.userrealm.principal";
  public static String KERBEROS_USER_REALM ="hadoop.security.authentication.userrealm";

  // class variable used to store the Subject
  private static UserGroupInformation ugi;

  private static boolean isInitialized;
  private static boolean isEnabled;
  private static Method isAUserInADifferentRealmMethod;
  private static Method getServerUGIForUserRealmMethod;
  private static Method loginServerFromCurrentKeytabAndReturnUGIMethod;
  private static Method replaceRealmWithUserRealmMethod;

  private static synchronized void initialize(boolean isEnabled) throws IOException {
    if (!isEnabled) {
      return;
    }
    String className = "org.apache.hadoop.security.MultiRealmUserAuthentication";
    try {
      Class c = Class.forName(className, true, UserGroupInformation.class.getClassLoader());
      isAUserInADifferentRealmMethod = c.getDeclaredMethod(
          "isAUserInADifferentRealm", UserGroupInformation.class, Configuration.class);
      getServerUGIForUserRealmMethod = c.getDeclaredMethod(
          "getServerUGIForUserRealm", Configuration.class);
      replaceRealmWithUserRealmMethod = c.getDeclaredMethod(
          "replaceRealmWithUserRealm", String.class, Configuration.class);
      loginServerFromCurrentKeytabAndReturnUGIMethod = UserGroupInformation.class
          .getDeclaredMethod("loginServerFromCurrentKeytabAndReturnUGI", String.class);
    } catch (Throwable t) {
      LOG.warn("Failed to load the required class" + className + " or get methods: " + t);
      throw new IOException("Underlying Hadoop version doesn't support multi-realm " +
          "authentication", t);
    }
    return;
  }

  private static void ensureInitialized(Configuration conf) throws IOException {
    if (!isInitialized) {
      isEnabled = conf.get(KERBEROS_USER_REALM) == null ? false : true;
      initialize(isEnabled);
      isInitialized = true;
    }
  }

  /** Forwarding method, doesn't have to be synchronized. */
  public static boolean isAUserInADifferentRealm(UserGroupInformation ticket, Configuration conf)
      throws IOException {
    ensureInitialized(conf);
    if (!isEnabled) {
      return false;
    }
    try {
      return ((Boolean)isAUserInADifferentRealmMethod.invoke(null, ticket, conf)).booleanValue();
    } catch (IllegalAccessException iae) {
      throw new IOException("Hadoop version does not support multi-realm", iae);
    } catch (InvocationTargetException ite) {
      throw new IOException(ite.getTargetException());
    }
  }

  /**
   * return the subject for server Principal  in the user realm
   * This will be the same name as the server principal of the default realm with the
   *  realm name replaced with the user realm name.
   *  Once created, the the UGI is cached.
   * @param conf
   * @return UserGroupInformation
   */
  public static synchronized UserGroupInformation getServerUGIForUserRealm(Configuration conf)
    throws IOException {
    ensureInitialized(conf);
    if (ugi != null) return ugi;

    // In case of error before ugi is set, we fall thru and return null.
    String kurp = conf.get(KERBEROS_USER_REALM_PRINCIPAL);
    try {
      if (kurp != null) {
        try {
          ugi = (UserGroupInformation)
              loginServerFromCurrentKeytabAndReturnUGIMethod.invoke(null, kurp);
        } catch (InvocationTargetException ite) {
          LOG.warn("Current user information cannot be obtained", ite);
        }
      } else {
        try {
          ugi = (UserGroupInformation)getServerUGIForUserRealmMethod.invoke(null, conf);
        } catch (InvocationTargetException ite) {
          throw new IOException(ite.getTargetException());
        }
      }
    } catch (IllegalAccessException iae) {
      throw new IOException("Hadoop version does not support multi-realm", iae);
    }
    return ugi;
  }

  /**
   * replaces the realm part of the principal name with the user realm
   * This method will be invoked by client side
   * @param principalName
   * @param conf
   * @return string value containing server principal in user realm
   */
  public static String replaceRealmWithUserRealm(String principalName, Configuration conf)
      throws IOException {
    ensureInitialized(conf);
    String kurp = conf.get(KERBEROS_USER_REALM_PRINCIPAL);
    if (kurp != null) return kurp;
    try {
      return (String)replaceRealmWithUserRealmMethod.invoke(null, principalName, conf);
    } catch (IllegalAccessException iae) {
      throw new IOException("Hadoop version does not support multi-realm", iae);
    } catch (InvocationTargetException ite) {
      throw new IOException(ite.getTargetException());
    }
  }
}
