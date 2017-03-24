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
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.common.SystemHelper;
import org.apache.ignite.cache.store.cassandra.persistence.KeyPersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceStrategy;
import org.apache.ignite.cache.store.cassandra.persistence.PojoField;
import org.apache.ignite.cache.store.cassandra.persistence.ValuePersistenceSettings;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * Settings to store data for specific tables in Cassandra.
 */
public class PublicKeyValuePersistenceSettings implements Serializable {
    /** Root xml element containing persistence settings specification. */
    private static final String PERSISTENCE_NODE = "persistence";

    /** Xml attribute specifying ttl (time to leave) for rows inserted in Cassandra. */
    private static final String TTL_ATTR = "ttl";

    /** Xml element specifying Cassandra table options. */
    private static final String TABLE_OPTIONS_NODE = "tableOptions";

    /** Xml element specifying Ignite cache key persistence settings. */
    private static final String KEY_PERSISTENCE_NODE = "keyPersistence";

    /** Xml element specifying Ignite cache value persistence settings. */
    private static final String VALUE_PERSISTENCE_NODE = "valuePersistence";

    /**
     * TTL (time to leave) for rows inserted into Cassandra table <a href="https://docs.datastax.com/en/cql/3.1/cql/cql_using/use_expire_c.html">Expiring
     * data</a>.
     */
    private Integer ttl;

    /**
     * Cassandra table creation options <a href="https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_table_r.html">CREATE
     * TABLE</a>.
     */
    private String tblOptions;

    /** Persistence settings for Ignite cache keys. */
    private KeyPersistenceSettings keyPersistenceSettings;

    /** Persistence settings for Ignite cache values. */
    private ValuePersistenceSettings valPersistenceSettings;

    public PublicKeyValuePersistenceSettings() {
    }

    public PublicKeyValuePersistenceSettings(Integer ttl, String tblOptions,
        KeyPersistenceSettings keyPersistenceSettings,
        ValuePersistenceSettings valPersistenceSettings) {
        this.ttl = ttl;
        this.tblOptions = tblOptions;
        this.keyPersistenceSettings = keyPersistenceSettings;
        this.valPersistenceSettings = valPersistenceSettings;
    }

    /**
     * Constructs Ignite cache key/value persistence settings.
     *
     * @param settings string containing xml with persistence settings for Ignite cache key/value
     */
    @SuppressWarnings("UnusedDeclaration")
    public PublicKeyValuePersistenceSettings(String settings) {
        init(settings);
    }

    /**
     * Constructs Ignite cache key/value persistence settings.
     *
     * @param settingsFile xml file with persistence settings for Ignite cache key/value
     */
    @SuppressWarnings("UnusedDeclaration")
    public PublicKeyValuePersistenceSettings(File settingsFile) {
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
    public PublicKeyValuePersistenceSettings(Resource settingsRsrc) {
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

    public PublicKeyValuePersistenceSettings(InputStream in) {
        init(loadSettings(in));
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
     * Cassandra table creation options <a href="https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_table_r.html">CREATE
     * TABLE</a>.
     *
     * @return table creation options
     */
    public String getTableOptions() {
        return tblOptions;
    }

    /** todo */
    public void setTblOptions(String tblOptions) {
        this.tblOptions = tblOptions;
    }

    /**
     * Returns persistence settings for Ignite cache keys.
     *
     * @return keys persistence settings.
     */
    public KeyPersistenceSettings getKeyPersistenceSettings() {
        return keyPersistenceSettings;
    }

    /** todo */
    public void setKeyPersistenceSettings(KeyPersistenceSettings keyPersistenceSettings) {
        this.keyPersistenceSettings = keyPersistenceSettings;
    }

    /**
     * Returns persistence settings for Ignite cache values.
     *
     * @return values persistence settings.
     */
    public ValuePersistenceSettings getValuePersistenceSettings() {
        return valPersistenceSettings;
    }

    /** todo */
    public void setValPersistenceSettings(ValuePersistenceSettings valPersistenceSettings) {
        this.valPersistenceSettings = valPersistenceSettings;
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
                if (settings.length() != 0)
                    settings.append(SystemHelper.LINE_SEPARATOR);

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
            throw new IllegalArgumentException("Failed to parse persistence settings:" +
                SystemHelper.LINE_SEPARATOR + settings, e);
        }
        Element root = doc.getDocumentElement();
        if (!PERSISTENCE_NODE.equals(root.getNodeName())) {
            throw new IllegalArgumentException("Incorrect persistence settings specified. " +
                "Root XML element should be 'persistence'");
        }
        if (root.hasAttribute(TTL_ATTR)) {
            ttl = extractIntAttribute(root, TTL_ATTR);
        }
        if (!root.hasChildNodes()) {
            throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification, " +
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
            else if (nodeName.equals(KEY_PERSISTENCE_NODE)) {
                keyPersistenceSettings = new KeyPersistenceSettings(el);
            }
            else if (nodeName.equals(VALUE_PERSISTENCE_NODE))
                valPersistenceSettings = new ValuePersistenceSettings(el);
        }
        List<PojoField> keyFields;
        if (keyPersistenceSettings != null) {
            keyFields = keyPersistenceSettings.getFields();
            if (PersistenceStrategy.POJO.equals(keyPersistenceSettings.getStrategy()) &&
                (keyFields == null || keyFields.isEmpty())) {
                throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification, " +
                    "there are no key fields found");
            }
        }
        else {
            keyFields = null;
        }
        List<PojoField> valFields;
        if (valPersistenceSettings != null) {
            valFields = valPersistenceSettings.getFields();
            if (PersistenceStrategy.POJO.equals(valPersistenceSettings.getStrategy()) &&
                (valFields == null || valFields.isEmpty())) {
                throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification, " +
                    "there are no value fields found");
            }
        }
        else {
            valFields = null;
        }
        if (keyFields != null && !keyFields.isEmpty() && valFields != null && !valFields.isEmpty()) {
            for (PojoField keyField : keyFields) {
                for (PojoField valField : valFields) {
                    if (keyField.getColumn().equals(valField.getColumn())) {
                        throw new IllegalArgumentException("Incorrect Cassandra persistence settings specification, " +
                            "key column '" + keyField.getColumn() + "' also specified as a value column");
                    }
                }
            }
        }
    }
}
