/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.jmx;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.relation.MBeanServerNotificationFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.jmx.JmxMetadata.toPattern;
import static java.util.Locale.ENGLISH;
import static javax.management.ObjectName.WILDCARD;

public class JmxHistoricalData
{
    private final int maxEntries;
    private final Set<String> tableNames;
    private final Set<String> tables;
    private final Map<String, EvictingQueue<List<Object>>> tableData = new HashMap<>();

    @Inject
    public JmxHistoricalData(JmxConnectorConfig jmxConfig, MBeanServer mbeanServer)
    {
        this(jmxConfig.getMaxEntries(), jmxConfig.getDumpTables(), mbeanServer);
    }

    public JmxHistoricalData(int maxEntries, Set<String> tableNames, MBeanServer mbeanServer)
    {
        this.tableNames = tableNames;
        this.maxEntries = maxEntries;

        tables = tableNames.stream()
                .map(objectNamePattern -> toPattern(objectNamePattern.toLowerCase(ENGLISH)))
                .flatMap(objectNamePattern -> mbeanServer.queryNames(WILDCARD, null).stream()
                        .map(objectName -> objectName.getCanonicalName().toLowerCase(ENGLISH))
                        .filter(name -> name.matches(objectNamePattern)))
                .collect(Collectors.toSet());

        for (String tableName : tables) {
            tableData.put(tableName, EvictingQueue.create(maxEntries));
        }

        MBeanServerNotificationFilter filter = new MBeanServerNotificationFilter();
        filter.enableAllObjectNames();
        try {
            mbeanServer.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME,
                    new AddDynamicTableListener(this.tableNames, this.tableData, this.tables),
                    filter, null);
        }
        catch (InstanceNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> getTables()
    {
        return tables;
    }

    public synchronized void addRow(String tableName, List<Object> row)
    {
        String lowerCaseTableName = tableName.toLowerCase(Locale.ENGLISH);
        if (tableData.containsKey(lowerCaseTableName)) {
            tableData.get(lowerCaseTableName).add(row);
        }
    }

    public synchronized List<List<Object>> getRows(String tableName, List<Integer> selectedColumns)
    {
        String lowerCaseTableName = tableName.toLowerCase(Locale.ENGLISH);
        if (!tableData.containsKey(lowerCaseTableName)) {
            return ImmutableList.of();
        }
        return projectRows(tableData.get(lowerCaseTableName), selectedColumns);
    }

    private List<List<Object>> projectRows(Collection<List<Object>> rows, List<Integer> selectedColumns)
    {
        ImmutableList.Builder<List<Object>> result = ImmutableList.builder();
        for (List<Object> row : rows) {
            List<Object> projectedRow = new ArrayList<>();
            for (Integer selectedColumn : selectedColumns) {
                projectedRow.add(row.get(selectedColumn));
            }
            result.add(projectedRow);
        }
        return result.build();
    }

    public class AddDynamicTableListener
            implements NotificationListener
    {
        private final Set<String> tableNames;
        private final Map<String, EvictingQueue<List<Object>>> tableData;
        private final Set<String> tables;

        private AddDynamicTableListener(
                Set<String> tableNames, Map<String,
                EvictingQueue<List<Object>>> tableData,
                Set<String> tables)
        {
            this.tableNames = tableNames;
            this.tableData = tableData;
            this.tables = tables;
        }

        @Override
        public void handleNotification(Notification notification, Object handback)
        {
            MBeanServerNotification mbeanNotification = (MBeanServerNotification) notification;
            ObjectName objectName = mbeanNotification.getMBeanName();
            List<String> matchedTables = tableNames.stream()
                                                   .filter(tableName -> objectName.getCanonicalName()
                                                                                  .toLowerCase(ENGLISH)
                                                                                  .matches(toPattern(tableName.toLowerCase(ENGLISH))))
                                                   .collect(Collectors.toList());

            if (!matchedTables.isEmpty()
                    && MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(notification.getType())) {
                for (String table : matchedTables) {
                    this.tableData.putIfAbsent(table, EvictingQueue.create(maxEntries));
                    this.tables.add(table);
                }
            }
        }
    }
}
