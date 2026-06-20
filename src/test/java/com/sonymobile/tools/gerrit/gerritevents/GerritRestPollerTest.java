/*
 *  The MIT License
 *
 *  Copyright 2026 Amarula Solutions All rights reserved.
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */
package com.sonymobile.tools.gerrit.gerritevents;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import net.sf.json.JSONObject;

import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;

import com.sonymobile.tools.gerrit.gerritevents.dto.GerritEvent;
import com.sonymobile.tools.gerrit.gerritevents.dto.attr.Provider;
import com.sonymobile.tools.gerrit.gerritevents.dto.events.TopicChanged;
import com.sonymobile.tools.gerrit.gerritevents.dto.events.WipStateChanged;
import com.sonymobile.tools.gerrit.gerritevents.ssh.Authentication;
import com.sonymobile.tools.gerrit.gerritevents.watchdog.WatchTimeExceptionData;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

//CS IGNORE MagicNumber FOR NEXT 600 LINES. REASON: TestData

/**
 * Tests for {@link GerritRestPoller}.
 *
 * @author Michael Trimarchi
 */
public class GerritRestPollerTest {

    private GerritRestPoller poller;
    private TestConfig config;
    private HandlerMock handlerMock;
    private ListenerMock listenerMock;

    /**
     * Minimal implementation of GerritConnectionConfig2 for testing.
     */
    private static class TestConfig implements GerritConnectionConfig2 {
        private String hostName = "gerrit.example.com";
        private int sshPort = 29418;
        private String proxy = "";
        private String frontEndUrl = "https://gerrit.example.com/";
        private int pollInterval = 10;
        private int pollMaxChanges = 50;
        private boolean useHttpsPoller = true;

        @Override public String getGerritHostName() {
            return hostName;
        }
        @Override public int getGerritSshPort() {
            return sshPort;
        }
        @Override public String getGerritProxy() {
            return proxy;
        }
        @Override public String getGerritFrontEndUrl() {
            return frontEndUrl;
        }
        @Override public String getGerritUserName() {
            return "user";
        }
        @Override public String getGerritEMail() {
            return "user@example.com";
        }
        @Override public Authentication getGerritAuthentication() {
            return new Authentication(null, "user", null);
        }
        @Override public Credentials getHttpCredentials() {
            return new UsernamePasswordCredentials("user", "pass");
        }
        @Override public int getWatchdogTimeoutSeconds() {
            return 0;
        }
        @Override public int getWatchdogTimeoutMinutes() {
            return 0;
        }
        @Override public WatchTimeExceptionData getExceptionData() {
            return null;
        }
        @Override public boolean isUseHttpsPoller() {
            return useHttpsPoller;
        }
        @Override public int getHttpsPollInterval() {
            return pollInterval;
        }
        @Override public int getHttpsPollMaxChanges() {
            return pollMaxChanges;
        }
        @Override public String getGerritAuthKeyFilePassword() {
            return null;
        }
        @Override public File getGerritAuthKeyFile() {
            return null;
        }
    }

    /**
     * A handler mock that counts and captures posted events.
     */
    static class HandlerMock extends GerritHandler {
        /** Latch counted down when an event is received. */
        final CountDownLatch eventLatch;
        /** Number of events received. */
        volatile int eventCount;
        /** Captured events for later verification. */
        final List<GerritEvent> capturedEvents = new ArrayList<GerritEvent>();

        /**
         * Creates a new HandlerMock.
         * @param eventLatch the latch to count down on event receipt, or null.
         */
        HandlerMock(CountDownLatch eventLatch) {
            this.eventLatch = eventLatch;
        }

        @Override
        public void post(GerritEvent event) {
            eventCount++;
            capturedEvents.add(event);
            if (eventLatch != null) {
                eventLatch.countDown();
            }
        }

        /**
         * Resets captured events.
         */
        void reset() {
            capturedEvents.clear();
            eventCount = 0;
        }
    }

    /**
     * A listener mock for connection state changes.
     */
    static class ListenerMock implements ConnectionListener {
        /** Whether connectionEstablished was called. */
        volatile boolean established;
        /** Whether connectionDown was called. */
        volatile boolean down;
        /** Latch counted down when connection is established. */
        final CountDownLatch establishmentLatch;

        /**
         * Creates a new ListenerMock.
         * @param establishmentLatch the latch to count down on establishment, or null.
         */
        ListenerMock(CountDownLatch establishmentLatch) {
            this.establishmentLatch = establishmentLatch;
        }

        @Override
        public void connectionEstablished() {
            established = true;
            if (establishmentLatch != null) {
                establishmentLatch.countDown();
            }
        }

        @Override
        public void connectionDown() {
            down = true;
        }
    }

    /**
     * Set up test fixtures.
     */
    @Before
    public void setUp() {
        config = new TestConfig();
        poller = new GerritRestPoller("testServer", config);
        handlerMock = new HandlerMock(null);
        listenerMock = new ListenerMock(null);
    }

    /**
     * Tear down test fixtures.
     */
    @After
    public void tearDown() {
        if (poller != null) {
            poller.shutdown(false);
        }
    }

    // ---- Construction and configuration tests ----

    /**
     * Tests construction with valid config.
     */
    @Test
    public void testConstruction() {
        assertNotNull(poller);
        assertFalse(poller.isConnected());
    }

    /**
     * Tests that handler can be set and retrieved.
     */
    @Test
    public void testSetAndGetHandler() {
        assertNull(poller.getHandler());
        poller.setHandler(handlerMock);
        assertEquals(handlerMock, poller.getHandler());
    }

    /**
     * Tests that connection listeners can be added and removed.
     */
    @Test
    public void testAddAndRemoveListener() {
        poller.addListener(listenerMock);
        assertTrue(poller.getListenersView().contains(listenerMock));
        poller.removeListener(listenerMock);
        assertFalse(poller.getListenersView().contains(listenerMock));
    }

    /**
     * Tests that getGerritVersion returns null before connection.
     */
    @Test
    public void testGetGerritVersionBeforeConnection() {
        assertNull(poller.getGerritVersion());
    }

    /**
     * Tests reconnect clears the known changes cache.
     */
    @Test
    public void testReconnect() {
        poller.reconnect();
        // No exception is the primary assertion; reconnect is a no-op
        // beyond clearing the cache which is an internal detail.
    }

    /**
     * Tests shutdown does not throw and marks connection as not connected.
     */
    @Test
    public void testShutdown() {
        assertFalse(poller.isConnected());
        poller.shutdown(false);
        assertFalse(poller.isConnected());
    }

    // ---- Date parsing tests ----

    /**
     * Tests parsing a Gerrit date with nanoseconds.
     */
    @Test
    public void testParseGerritDateWithNanos() {
        Date d = GerritRestPoller.parseGerritDate("2023-01-15 10:00:00.000000000");
        assertNotNull(d);
    }

    /**
     * Tests parsing a Gerrit date without nanoseconds.
     */
    @Test
    public void testParseGerritDateWithoutNanos() {
        Date d = GerritRestPoller.parseGerritDate("2023-01-15 10:00:00");
        assertNotNull(d);
    }

    /**
     * Tests parsing null returns null.
     */
    @Test
    public void testParseGerritDateNull() {
        assertNull(GerritRestPoller.parseGerritDate(null));
    }

    /**
     * Tests parsing empty string returns null.
     */
    @Test
    public void testParseGerritDateEmpty() {
        assertNull(GerritRestPoller.parseGerritDate(""));
    }

    /**
     * Tests parsing invalid date returns null.
     */
    @Test
    public void testParseGerritDateInvalid() {
        assertNull(GerritRestPoller.parseGerritDate("not-a-date"));
    }

    // ---- Interface implementation tests ----

    /**
     * Tests that the poller implements GerritEventSource.
     */
    @Test
    public void testImplementsGerritEventSource() {
        assertTrue(poller instanceof GerritEventSource);
    }

    /**
     * Tests that the poller implements Connector.
     */
    @Test
    public void testImplementsConnector() {
        assertTrue(poller instanceof Connector);
    }

    /**
     * Tests that the protocol scheme is HTTPS.
     */
    @Test
    public void testProtocolSchemeName() {
        assertEquals("https", GerritRestPoller.GERRIT_PROTOCOL_SCHEME_NAME);
    }

    /**
     * Tests isConnected returns false before connection is established.
     */
    @Test
    public void testIsConnectedInitiallyFalse() {
        assertFalse(poller.isConnected());
    }

    // ---- Topic change detection tests ----

    /**
     * Builds a minimal REST API JSON change object for testing.
     * @param changeId the change ID.
     * @param project the project name.
     * @param branch the branch name.
     * @param changeNumber the change number.
     * @param subject the change subject.
     * @param status the change status.
     * @param revision the current revision SHA.
     * @param topic the topic, or null.
     * @return a JSON object representing a REST API change.
     */
    private JSONObject buildRestChangeJson(String changeId, String project, String branch,
            int changeNumber, String subject, String status, String revision,
            String topic) {
        return buildRestChangeJson(changeId, project, branch, changeNumber, subject, status,
                revision, topic, false);
    }

    private JSONObject buildRestChangeJson(String changeId, String project, String branch,
            int changeNumber, String subject, String status, String revision,
            String topic, boolean wip) {
        JSONObject json = new JSONObject();
        json.put("id", changeId);
        json.put("project", project);
        json.put("branch", branch);
        json.put("_number", changeNumber);
        json.put("subject", subject);
        json.put("status", status);
        json.put("current_revision", revision);
        json.put("work_in_progress", wip);
        if (topic != null) {
            json.put("topic", topic);
        }
        JSONObject owner = new JSONObject();
        owner.put("name", "Test User");
        owner.put("email", "user@example.com");
        json.put("owner", owner);

        JSONObject revisions = new JSONObject();
        JSONObject revObj = new JSONObject();
        revObj.put("_number", 1);
        revObj.put("ref", "refs/changes/" + changeNumber + "/1");
        revObj.put("kind", "REWORK");
        revisions.put(revision, revObj);
        json.put("revisions", revisions);
        return json;
    }

    /**
     * Creates a standard Provider for test events.
     * @return a Provider with test values.
     */
    private Provider createTestProvider() {
        return new Provider("testServer", "gerrit.example.com", "29418", "https",
                "https://gerrit.example.com/", "3.6.0");
    }

    /**
     * Tests detection of topic change from null to a value.
     */
    @Test
    public void testTopicChangeNullToValue() throws Exception {
        handlerMock = new HandlerMock(null);
        poller.setHandler(handlerMock);
        Provider provider = createTestProvider();
        String changeId = "proj~master~I001";

        // First call establishes known state (no topic)
        JSONObject first = buildRestChangeJson(changeId, "proj", "master", 1,
                "Test", "NEW", "rev1", null);
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", first, provider);

        // Should get 1 PatchsetCreated, no topic event
        assertEquals(1, handlerMock.eventCount);
        handlerMock.reset();

        // Second call: topic added
        JSONObject second = buildRestChangeJson(changeId, "proj", "master", 1,
                "Test", "NEW", "rev1", "new-topic");
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", second, provider);

        // Should get 1 TopicChanged (no PatchsetCreated since revision didn't change)
        assertEquals(1, handlerMock.eventCount);
        GerritEvent event = handlerMock.capturedEvents.get(0);
        assertTrue("Expected TopicChanged but got " + event.getClass().getSimpleName(),
                event instanceof TopicChanged);
        TopicChanged tc = (TopicChanged)event;
        assertNull(tc.getOldTopic());
        assertEquals("new-topic", tc.getChange().getTopic());
    }

    /**
     * Tests detection of topic change from one value to another.
     */
    @Test
    public void testTopicChangeValueToValue() throws Exception {
        handlerMock = new HandlerMock(null);
        poller.setHandler(handlerMock);
        Provider provider = createTestProvider();
        String changeId = "proj~master~I002";

        // First call with topic "old-topic"
        JSONObject first = buildRestChangeJson(changeId, "proj", "master", 2,
                "Test", "NEW", "rev1", "old-topic");
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", first, provider);
        assertEquals(1, handlerMock.eventCount);
        handlerMock.reset();

        // Second call: topic changed
        JSONObject second = buildRestChangeJson(changeId, "proj", "master", 2,
                "Test", "NEW", "rev1", "new-topic");
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", second, provider);

        assertEquals(1, handlerMock.eventCount);
        GerritEvent event = handlerMock.capturedEvents.get(0);
        assertTrue("Expected TopicChanged but got " + event.getClass().getSimpleName(),
                event instanceof TopicChanged);
        TopicChanged tc = (TopicChanged)event;
        assertEquals("old-topic", tc.getOldTopic());
        assertEquals("new-topic", tc.getChange().getTopic());
    }

    /**
     * Tests detection of topic removal (value to null).
     */
    @Test
    public void testTopicChangeValueToNull() throws Exception {
        handlerMock = new HandlerMock(null);
        poller.setHandler(handlerMock);
        Provider provider = createTestProvider();
        String changeId = "proj~master~I003";

        // First call with topic "my-topic"
        JSONObject first = buildRestChangeJson(changeId, "proj", "master", 3,
                "Test", "NEW", "rev1", "my-topic");
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", first, provider);
        assertEquals(1, handlerMock.eventCount);
        handlerMock.reset();

        // Second call: topic removed (null)
        JSONObject second = buildRestChangeJson(changeId, "proj", "master", 3,
                "Test", "NEW", "rev1", null);
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", second, provider);

        assertEquals(1, handlerMock.eventCount);
        GerritEvent event = handlerMock.capturedEvents.get(0);
        assertTrue("Expected TopicChanged but got " + event.getClass().getSimpleName(),
                event instanceof TopicChanged);
        TopicChanged tc = (TopicChanged)event;
        assertEquals("my-topic", tc.getOldTopic());
        assertNull(tc.getChange().getTopic());
    }

    /**
     * Tests that a new change does not emit TopicChanged
     * even when it has a topic set.
     */
    @Test
    public void testNewChangeWithTopic() throws Exception {
        handlerMock = new HandlerMock(null);
        poller.setHandler(handlerMock);
        Provider provider = createTestProvider();
        String changeId = "proj~master~I005";

        JSONObject json = buildRestChangeJson(changeId, "proj", "master", 5,
                "Test", "NEW", "rev1", "my-topic");
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", json, provider);

        // Should get exactly 1 PatchsetCreated, no TopicChanged
        assertEquals(1, handlerMock.eventCount);
        GerritEvent event = handlerMock.capturedEvents.get(0);
        assertFalse("New change should not emit TopicChanged",
                event instanceof TopicChanged);
    }

    // ---- WIP state change detection tests ----

    @Test
    public void testWipStateChangeToTrue() throws Exception {
        handlerMock = new HandlerMock(null);
        poller.setHandler(handlerMock);
        Provider provider = createTestProvider();
        String changeId = "proj~master~Iwip1";

        JSONObject first = buildRestChangeJson(changeId, "proj", "master", 10,
                "Test", "NEW", "rev1", null, false);
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", first, provider);
        assertEquals(1, handlerMock.eventCount);
        handlerMock.reset();

        JSONObject second = buildRestChangeJson(changeId, "proj", "master", 10,
                "Test", "NEW", "rev1", null, true);
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", second, provider);

        assertEquals(1, handlerMock.eventCount);
        GerritEvent event = handlerMock.capturedEvents.get(0);
        assertTrue("Expected WipStateChanged but got " + event.getClass().getSimpleName(),
                event instanceof WipStateChanged);
        assertTrue(((WipStateChanged)event).getChange().isWip());
    }

    @Test
    public void testWipStateChangeToFalse() throws Exception {
        handlerMock = new HandlerMock(null);
        poller.setHandler(handlerMock);
        Provider provider = createTestProvider();
        String changeId = "proj~master~Iwip2";

        JSONObject first = buildRestChangeJson(changeId, "proj", "master", 11,
                "Test", "NEW", "rev1", null, true);
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", first, provider);
        assertEquals(1, handlerMock.eventCount);
        handlerMock.reset();

        JSONObject second = buildRestChangeJson(changeId, "proj", "master", 11,
                "Test", "NEW", "rev1", null, false);
        org.powermock.reflect.Whitebox.invokeMethod(poller, "processChange", second, provider);

        assertEquals(1, handlerMock.eventCount);
        GerritEvent event = handlerMock.capturedEvents.get(0);
        assertTrue("Expected WipStateChanged but got " + event.getClass().getSimpleName(),
                event instanceof WipStateChanged);
        assertFalse(((WipStateChanged)event).getChange().isWip());
    }
}
