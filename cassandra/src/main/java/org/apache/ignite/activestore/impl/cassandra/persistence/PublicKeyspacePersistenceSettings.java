/*
 * Copyright (c) 2017. EPAM Systems
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

package org.apache.ignite.activestore.impl.cassandra.persistence;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.StringReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceSettings;
import org.apache.ignite.cache.store.cassandra.serializer.JavaSerializer;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Settings for storing data for specified keyspace in Cassandra.
 */
public class PublicKeyspacePersistenceSettings implements Serializable {
    /** Root xml element containing persistence settings specification. */
    private static final String PERSISTENCE_NODE = "persistence";

    /**
     * Default Cassandra keyspace options which should be used to create new keyspace. <ul> <li> <b>SimpleStrategy</b>
     * for replication work well for single data center Cassandra cluster.<br/> If your Cassandra cluster deployed
     * across multiple data centers it's better to use <b>NetworkTopologyStrategy</b>. </li> <li> Three replicas will be
     * created for each data block. </li> <li> Setting DURABLE_WRITES to true specifies that all data should be written
     * to commit log. </li> </ul>
     */
    private static final String DFLT_KEYSPACE_OPTIONS = "replication = {'class' : 'SimpleStrategy', " +
        "'replication_factor' : 3} and durable_writes = true";

    /** Xml attribute specifying Cassandra keyspace to use. */
    private static final String KEYSPACE_ATTR = "keyspace";

    /** Xml attribute specifying ttl (time to leave) for rows inserted in Cassandra. */
    private static final String TTL_ATTR = "ttl";

    /** Xml element specifying Cassandra keyspace options. */
    private static final String KEYSPACE_OPTIONS_NODE = "keyspaceOptions";

    /** Xml element specifying Cassandra table options. */
    private static final String TABLE_OPTIONS_NODE = "tableOptions";

    /** Xml attribute specifying BLOB serializer to use. */
    private static final String SERIALIZER_ATTR = "serializer";

    /**
     * TTL (time to leave) for rows inserted into Cassandra table <a href="https://docs.datastax.com/en/cql/3.1/cql/cql_using/use_expire_c.html">Expiring
     * data</a>.
     */
    private Integer ttl;

    /** Cassandra keyspace (analog of tablespace in relational databases). */
    private String keyspace;

    /**
     * Cassandra table creation options <a href="https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_table_r.html">CREATE
     * TABLE</a>.
     */
    private String tblOptions;

    /**
     * Cassandra keyspace creation options <a href="https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_keyspace_r.html">CREATE
     * KEYSPACE</a>.
     */
    private String keyspaceOptions = DFLT_KEYSPACE_OPTIONS;

    /** Serializer for BLOBs. */
    private Serializer serializer = new JavaSerializer();

    public PublicKeyspacePersistenceSettings() {
    }

    /**
     * Constructs Ignite cache key/value persistence settings.
     *
     * @param settings string containing xml with persistence settings for Ignite cache key/value
     */
    @SuppressWarnings("UnusedDeclaration")
    public PublicKeyspacePersistenceSettings(String settings) {
        init(settings);
    }

    /**
     * Constructs Ignite cache key/value persistence settings.
     *
     * @param settingsFile xml file with persistence settings for Ignite cache key/value
     */
    @SuppressWarnings("UnusedDeclaration")
    public PublicKeyspacePersistenceSettings(File settingsFile) {
        InputStream in;
        try {
            in = new FileInputStream(settingsFile);
        }
        catch (IOException e) {
            throw new IgniteException("Failed to get input stream for Cassandra persistence settings file: " +
                settingsFile.getAbsolutePath(), e);
        }
        try {
            init(loadSettings(in));
        }
        finally {
            U.closeQuiet(in);
        }
    }

    /**
     * Constructs Ignite cache key/value persistence settings.
     *
     * @param settingsRsrc resource containing xml with persistence settings for Ignite cache key/value
     */
    public PublicKeyspacePersistenceSettings(Resource settingsRsrc) {
        InputStream in;
        try {
            in = settingsRsrc.getInputStream();
        }
        catch (IOException e) {
            throw new IgniteException("Failed to get input stream for Cassandra persistence settings resource: " +
                settingsRsrc, e);
        }
        try {
            init(loadSettings(in));
        }
        finally {
            U.closeQuiet(in);
        }
    }

    public PublicKeyspacePersistenceSettings(InputStream in) {
        init(loadSettings(in));
    }

    /**
     * Instantiates Class object for particular class
     *
     * @param clazz class name
     * @return Class object
     */
    private static Class getClassInstance(String clazz) {
        try {
            return Class.forName(clazz);
        }
        catch (ClassNotFoundException ignored) {
        }
        try {
            return Class.forName(clazz, true, Thread.currentThread().getContextClassLoader());
        }
        catch (ClassNotFoundException ignored) {
        }
        try {
            return Class.forName(clazz, true, PersistenceSettings.class.getClassLoader());
        }
        catch (ClassNotFoundException ignored) {
        }
        try {
            return Class.forName(clazz, true, ClassLoader.getSystemClassLoader());
        }
        catch (ClassNotFoundException ignored) {
        }
        throw new IgniteException("Failed to load class '" + clazz + "' using reflection");
    }

    /**
     * Creates new object instance of particular class
     *
     * @param clazz class name
     * @return object
     */
    private static Object newObjectInstance(String clazz) {
        try {
            return getClassInstance(clazz).newInstance();
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to instantiate class '" + clazz + "' using default constructor", e);
        }
    }

    /**
     * Returns ttl to use for while inserting new rows into Cassandra table.
     *
     * @return ttl
     */
    public Integer getTTL() {
        return ttl;
    }

    /** todo */
    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    /**
     * Returns Cassandra keyspace to use.
     *
     * @return keyspace.
     */
    public String getKeyspace() {
        return keyspace;
    }

    /** todo */
    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * Cassandra keyspace creation options <a href="https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_keyspace_r.html">CREATE
     * KEYSPACE</a>.
     *
     * @return keyspace creation options
     */
    public String getKeyspaceOptions() {
        return keyspaceOptions;
    }

    /** todo */
    public void setKeyspaceOptions(String keyspaceOptions) {
        this.keyspaceOptions = keyspaceOptions;
    }

    /**
     * Cassandra table creation options <a href="https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_table_r.html">CREATE
     * TABLE</a>.
     *
     * @return table creation options
     */
    public String getTableOptions() {
        return tblOptions;
    }

    /** todo */
    public void setTableOptions(String tblOptions) {
        this.tblOptions = tblOptions;
    }

    /**
     * Returns serializer to be used for BLOBs.
     *
     * @return serializer.
     */
    public Serializer getSerializer() {
        return serializer;
    }

    /** todo */
    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Loads Ignite cache persistence settings from resource.
     *
     * @param in Input stream.
     * @return String containing xml with Ignite cache persistence settings.
     */
    private String loadSettings(InputStream in) {
        StringBuilder settings = new StringBuilder();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(in));
            String line = reader.readLine();
            while (line != null) {
                if (settings.length() != 0) {
                    settings.append(SystemHelper.LINE_SEPARATOR);
                }
                settings.append(line);
                line = reader.readLine();
            }
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to read input stream for Cassandra persistence settings", e);
        }
        finally {
            U.closeQuiet(reader);
            U.closeQuiet(in);
        }
        return settings.toString();
    }

    /**
     * @param elem Element with data.
     * @param attr Attribute name.
     * @return Numeric value for specified attribute.
     */
    private int extractIntAttribute(Element elem, String attr) {
        String val = elem.getAttribute(attr).trim();
        try {
            return Integer.parseInt(val);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Incorrect value '" + val + "' specified for '" + attr + "' attribute");
        }
    }

    /**
     * Initializes persistence settings from XML string.
     *
     * @param settings XML string containing Ignite cache persistence settings configuration.
     */
    @SuppressWarnings("IfCanBeSwitch")
    private void init(String settings) {
        Document doc;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(new InputSource(new StringReader(settings)));
        }
        catch (Throwable e) {
            throw new IllegalArgumentException("Failed to parse general persistence settings:" +
                SystemHelper.LINE_SEPARATOR + settings, e);
        }
        Element root = doc.getDocumentElement();
        if (!PERSISTENCE_NODE.equals(root.getNodeName())) {
            throw new IllegalArgumentException("Incorrect general persistence settings specified. " +
                "Root XML element should be 'persistence'");
        }
        keyspace = root.hasAttribute(KEYSPACE_ATTR) ? root.getAttribute(KEYSPACE_ATTR).trim() : null;
        if (root.hasAttribute(TTL_ATTR)) {
            ttl = extractIntAttribute(root, TTL_ATTR);
        }
        if (!root.hasChildNodes()) {
            throw new IllegalArgumentException("Incorrect Cassandra general persistence settings specification, " +
                "there are no key and value persistence settings specified");
        }
        NodeList children = root.getChildNodes();
        int cnt = children.getLength();
        for (int i = 0; i < cnt; i++) {
            Node node = children.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            Element el = (Element)node;
            String nodeName = el.getNodeName();
            if (nodeName.equals(TABLE_OPTIONS_NODE)) {
                tblOptions = el.getTextContent();
                tblOptions = tblOptions.replace("\n", " ").replace("\r", "").replace("\t", " ");
            }
            else if (nodeName.equals(KEYSPACE_OPTIONS_NODE)) {
                keyspaceOptions = el.getTextContent();
                keyspaceOptions = keyspaceOptions.replace("\n", " ").replace("\r", "").replace("\t", " ");
            }
            else if (el.hasAttribute(SERIALIZER_ATTR)) {
                Object obj = newObjectInstance(el.getAttribute(SERIALIZER_ATTR).trim());
                if (!(obj instanceof Serializer)) {
                    throw new IllegalArgumentException("Incorrect configuration of Cassandra general persistence " +
                        "settings, serializer class '" + el.getAttribute(SERIALIZER_ATTR) + "' doesn't " +
                        "implement '" + Serializer.class.getName() + "' interface");
                }
                serializer = (Serializer)obj;
            }
        }
    }
}
