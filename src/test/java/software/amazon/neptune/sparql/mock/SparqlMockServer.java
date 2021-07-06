/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// TODO: update this class before release & ensure licensing done correctly

package software.amazon.neptune.sparql.mock;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.web.WebLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.riot.web.HttpOp;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.modify.request.Target;
import org.apache.jena.sparql.modify.request.UpdateDrop;
import org.apache.jena.system.Txn;
import org.apache.jena.update.Update;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateProcessor;
import java.util.concurrent.atomic.AtomicInteger;

import static software.amazon.neptune.sparql.mock.SparqlMockServer.ServerScope.CLASS;
import static software.amazon.neptune.sparql.mock.SparqlMockServer.ServerScope.SUITE;
import static software.amazon.neptune.sparql.mock.SparqlMockServer.ServerScope.TEST;

/**
 * Manage a single server for use with tests. It supports three modes:
 * <ul>
 * <li>One server for a whole test suite
 * <li>One server per test class
 * <li>One server per individual test
 * </ul>
 * One server per individual test can be troublesome due to connections not closing down
 * fast enough (left in TCP state {@code TIME_WAIT} which is 2 minutes) and also can be
 * slow. One server per test class is a good compromise.
 * <p>
 * The data in the server is always reseet between tests.
 * <p>
 * Using a connection pooling HttpClient (see {@link HttpOp#createPoolingHttpClient()}) is
 * important, both for test performance and for reducing the TCP connection load on the
 * operating system.
 * <p>
 * Usage:
 * </p>
 * <p>
 * In the test suite, put:
 *
 * <pre>
 *  {@literal @BeforeClass} static public void beforeSuiteClass() { FusekiTestServer.ctlBeforeTestSuite(); }
 *  {@literal @AfterClass}  static public void afterSuiteClass()  { FusekiTestServer.ctlAfterTestSuite(); }
 * </pre>
 * <p>
 * In the test class, put:
 *
 * <pre>
 * {@literal @BeforeClass} public static void ctlBeforeClass() { FusekiTestServer.ctlBeforeClass(); }
 * {@literal @AfterClass}  public static void ctlAfterClass()  { FusekiTestServer.ctlAfterClass(); }
 * {@literal @Before}      public void ctlBeforeTest()         { FusekiTestServer.ctlBeforeTest(); }
 * {@literal @After}       public void ctlAfterTest()          { FusekiTestServer.ctlAfterTest(); }
 * </pre>
 * <p>
 * Much of this machinery is unnecessary for just running a server in the background:
 *
 * <pre>
 *   private static FusekiServer server;
 *   private static DatasetGraph serverdsg = DatasetGraphFactory.createTxnMem();
 *
 *   &#64;BeforeClass
 *   public static void beforeClass() {
 *       server = FusekiServer.create()
 *           .setPort(....)
 *           .add("/ds", serverdsg)
 *           .build()
 *           .start();
 *   }
 *
 *   &#64;Before
 *   public void beforeTest() {
 *       // Clear up data in server servers
 *       Txn.executeWrite(serverdsg, (){@literal ->}serverdsg.clear());
 *   }
 *
 *   &#64;AfterClass
 *   public static void afterClass() {
 *       server.stop();
 *   }
 * </pre>
 */
public class SparqlMockServer {
    /* Cut&Paste versions:

    Test suite (TS_*)
    @BeforeClass static public void beforeSuiteClass() { FusekiTestServer.ctlBeforeTestSuite(); }
    @AfterClass  static public void afterSuiteClass()  { FusekiTestServer.ctlAfterTestSuite(); }

    Test class (Test*)
    @BeforeClass public static void ctlBeforeClass() { FusekiTestServer.ctlBeforeClass(); }
    @AfterClass  public static void ctlAfterClass()  { FusekiTestServer.ctlAfterClass(); }
    @Before      public void ctlBeforeTest()         { FusekiTestServer.ctlBeforeTest(); }
    @After       public void ctlAfterTest()          { FusekiTestServer.ctlAfterTest(); }

    */

    // Note: it is important to cleanly close a PoolingHttpClient across server restarts
    // otherwise the pooled connections remain for the old server.

    private static ServerScope serverScope = ServerScope.CLASS;
    private static int currentPort = WebLib.choosePort();
    // Whether to use a transaction on the dataset or to use SPARQL Update.
    private static DatasetGraph dsgTesting;
    private static boolean clearDSGDirectly = true;
    // reference count of start/stop server
    private static AtomicInteger countServer = new AtomicInteger();
    private static FusekiServer server = null;

    // Abstraction that runs a SPARQL server for tests.

    /**
     * Returns currently used port
     *
     * @return the currently used port.
     */
    public static int port() {
        return currentPort;
    }

    /**
     * Function to get root url.
     *
     * @return root url
     */
    public static String urlRoot() {
        return "http://localhost:" + port() + "/";
    }

    /**
     * Function to get database path.
     *
     * @return database path
     */
    public static String datasetPath() {
        return "/mock";
    }

    /**
     * Function to get full database url.
     *
     * @return full database url
     */
    public static String urlDataset() {
        return "http://localhost:" + port() + datasetPath();
    }

    /**
     * Function to get update api.
     *
     * @return update url
     */
    public static String serviceUpdate() {
        return "http://localhost:" + port() + datasetPath() + "/update";
    }

    /**
     * Function to get query api.
     *
     * @return query url
     */
    public static String serviceQuery() {
        return "http://localhost:" + port() + datasetPath() + "/query";
    }

    /**
     * Function to get data api.
     *
     * @return dataGSP url
     */
    public static String serviceGSP() {
        return "http://localhost:" + port() + datasetPath() + "/data";
    }

    /**
     * Setup for the tests by allocating a Fuseki instance to work with
     */
    public static void ctlBeforeTestSuite() {
        if (serverScope == SUITE) {
            setPoolingHttpClient();
            allocServer();
        }
    }

    /**
     * Setup for the tests by allocating a Fuseki instance to work with
     */
    public static void ctlAfterTestSuite() {
        if (serverScope == SUITE) {
            freeServer();
            resetDefaultHttpClient();
        }
    }

    /**
     * Setup for the tests by allocating a Fuseki instance to work with
     */
    public static void ctlBeforeClass() {
        if (serverScope == CLASS) {
            setPoolingHttpClient();
            allocServer();
        }
    }

    /**
     * Clean up after tests by de-allocating the Fuseki instance
     */
    public static void ctlAfterClass() {
        if (serverScope == CLASS) {
            freeServer();
            resetDefaultHttpClient();
        }
    }

    /**
     * Placeholder.
     */
    public static void ctlBeforeTest() {
        if (serverScope == TEST) {
            setPoolingHttpClient();
            allocServer();
        }
    }

    /**
     * Clean up after each test by resetting the Fuseki dataset
     */
    public static void ctlAfterTest() {
        if (serverScope == TEST) {
            freeServer();
            resetDefaultHttpClient();
        } else {
            resetServer();
        }
    }

    /**
     * Setup for the tests by allocating a Fuseki instance to work with
     */
    public static void ctlBeforeEach() {
        setPoolingHttpClient();
        allocServer();
    }

    /**
     * Clean up after tests by de-allocating the Fuseki instance
     */
    public static void ctlAfterEach() {
        freeServer();
        resetDefaultHttpClient();
    }

    /**
     * Set a PoolingHttpClient
     */
    public static void setPoolingHttpClient() {
        setHttpClient(HttpOp.createPoolingHttpClient());
    }

    /**
     * Restore the original setup
     */
    private static void resetDefaultHttpClient() {
        setHttpClient(HttpOp.createDefaultHttpClient());
    }

    /**
     * Set the HttpClient - close the old one if appropriate
     */
    public static void setHttpClient(final HttpClient newHttpClient) {
        final HttpClient hc = HttpOp.getDefaultHttpClient();
        if (hc instanceof CloseableHttpClient) {
            IO.close((CloseableHttpClient) hc);
        }
        HttpOp.setDefaultHttpClient(newHttpClient);
    }

    /*package*/
    static void allocServer() {
        if (countServer.getAndIncrement() == 0) {
            setupServer(true);
        }
    }

    /*package*/
    static void freeServer() {
        if (countServer.decrementAndGet() == 0) {
            teardownServer();
        }
    }

    /*package*/
    static void setupServer(final boolean updateable) {
        dsgTesting = DatasetGraphFactory.createTxnMem();
        server = FusekiServer.create()
                .add(datasetPath(), dsgTesting)
                .port(port())
                .loopback(true)
                .build()
                .start();
    }

    /*package*/
    static void teardownServer() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    /*package*/
    static void resetServer() {
        if (countServer.get() == 0) {
            throw new RuntimeException("No server started!");
        }
        if (clearDSGDirectly) {
            Txn.executeWrite(dsgTesting, () -> dsgTesting.clear());
        } else {
            final Update clearRequest = new UpdateDrop(Target.ALL);
            final UpdateProcessor proc = UpdateExecutionFactory.createRemote(clearRequest, serviceUpdate());
            try {
                proc.execute();
            } catch (Throwable e) {
                e.printStackTrace();
                throw e;
            }
        }
    }

    /*package : for import static */
    enum ServerScope { SUITE, CLASS, TEST }

    // ---- Helper code.

}
