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
package io.trino.plugin.pulsar.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.Policies;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static org.apache.pulsar.common.util.FieldParser.value;

/**
 * Holds all necessary info about offloading and reading data to/from remote cold storage.
 */
public class OffloadPoliciesImpl
        implements Serializable, OffloadPolicies
{
    public static final List<Field> CONFIGURATION_FIELDS;
    public static final Long DEFAULT_Managed_Ledger_Offload_Threshold_In_Seconds = 1L;
    public static final int DEFAULT_MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024 * 1024;   // 64MB
    public static final int DEFAULT_READ_BUFFER_SIZE_IN_BYTES = 1024 * 1024;      // 1MB
    public static final int DEFAULT_OFFLOAD_MAX_THREADS = 2;
    public static final int DEFAULT_OFFLOAD_MAX_PREFETCH_ROUNDS = 1;
    public static final ImmutableList<String> DRIVER_NAMES = ImmutableList
            .of("S3", "aws-s3", "google-cloud-storage", "filesystem", "azureblob", "aliyun-oss");
    public static final String DEFAULT_OFFLOADER_DIRECTORY = "./offloaders";
    public static final Long DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES = null;
    public static final Long DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS = null;
    public static final String OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE =
            "managedLedgerOffloadAutoTriggerSizeThresholdBytes";
    public static final String DELETION_LAG_NAME_IN_CONF_FILE = "managedLedgerOffloadDeletionLagMs";
    public static final OffloadedReadPriority DEFAULT_OFFLOADED_READ_PRIORITY = OffloadedReadPriority.TIERED_STORAGE_FIRST;
    private static final Logger log = Logger.get(OffloadPoliciesImpl.class);
    private static final long serialVersionUID = 0L;

    static {
        List<Field> temp = new ArrayList<>();
        Class<OffloadPoliciesImpl> clazz = OffloadPoliciesImpl.class;
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Configuration.class)) {
                temp.add(field);
            }
        }
        CONFIGURATION_FIELDS = Collections.unmodifiableList(temp);
    }

    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private final Long managedLedgerOffloadThresholdInSeconds = DEFAULT_Managed_Ledger_Offload_Threshold_In_Seconds;
    // common config
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String offloadersDirectory = DEFAULT_OFFLOADER_DIRECTORY;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String managedLedgerOffloadDriver;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer managedLedgerOffloadMaxThreads = DEFAULT_OFFLOAD_MAX_THREADS;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer managedLedgerOffloadPrefetchRounds = DEFAULT_OFFLOAD_MAX_PREFETCH_ROUNDS;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Long managedLedgerOffloadThresholdInBytes = DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Long managedLedgerOffloadDeletionLagInMillis = DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private OffloadedReadPriority managedLedgerOffloadedReadPriority = DEFAULT_OFFLOADED_READ_PRIORITY;
    // s3 config, set by service configuration or cli
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadRegion;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadBucket;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadServiceEndpoint;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer s3ManagedLedgerOffloadMaxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer s3ManagedLedgerOffloadReadBufferSizeInBytes = DEFAULT_READ_BUFFER_SIZE_IN_BYTES;
    // s3 config, set by service configuration
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadCredentialId;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadCredentialSecret;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadRole;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String s3ManagedLedgerOffloadRoleSessionName = "pulsar-s3-offload";

    // gcs config, set by service configuration or cli
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String gcsManagedLedgerOffloadRegion;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String gcsManagedLedgerOffloadBucket;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer gcsManagedLedgerOffloadMaxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer gcsManagedLedgerOffloadReadBufferSizeInBytes = DEFAULT_READ_BUFFER_SIZE_IN_BYTES;
    // gcs config, set by service configuration
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String gcsManagedLedgerOffloadServiceAccountKeyFile;

    // file system config, set by service configuration
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String fileSystemProfilePath;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String fileSystemURI;

    // --------- new offload configurations ---------
    // they are universal configurations and could be used to `aws-s3`, `google-cloud-storage` or `azureblob`.
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String managedLedgerOffloadBucket;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String managedLedgerOffloadRegion;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private String managedLedgerOffloadServiceEndpoint;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer managedLedgerOffloadMaxBlockSizeInBytes;
    @Configuration
    @JsonProperty(access = JsonProperty.Access.READ_WRITE)
    private Integer managedLedgerOffloadReadBufferSizeInBytes;

    public static OffloadPoliciesImpl create(String driver,
                                             String region,
                                             String bucket,
                                             String endpoint,
                                             String role,
                                             String roleSessionName,
                                             String credentialId,
                                             String credentialSecret,
                                             Integer maxBlockSizeInBytes,
                                             Integer readBufferSizeInBytes,
                                             Long offloadThresholdInBytes,
                                             Long offloadDeletionLagInMillis,
                                             OffloadedReadPriority readPriority)
    {
        OffloadPoliciesImplBuilder builder = builder()
                .managedLedgerOffloadDriver(driver)
                .managedLedgerOffloadThresholdInBytes(offloadThresholdInBytes)
                .managedLedgerOffloadDeletionLagInMillis(offloadDeletionLagInMillis)
                .managedLedgerOffloadBucket(bucket)
                .managedLedgerOffloadRegion(region)
                .managedLedgerOffloadServiceEndpoint(endpoint)
                .managedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes)
                .managedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes)
                .managedLedgerOffloadedReadPriority(readPriority);

        if (driver.equalsIgnoreCase(DRIVER_NAMES.get(0)) || driver.equalsIgnoreCase(DRIVER_NAMES.get(1))) {
            if (role != null) {
                builder.s3ManagedLedgerOffloadRole(role);
            }
            if (roleSessionName != null) {
                builder.s3ManagedLedgerOffloadRoleSessionName(roleSessionName);
            }
            if (credentialId != null) {
                builder.s3ManagedLedgerOffloadCredentialId(credentialId);
            }
            if (credentialSecret != null) {
                builder.s3ManagedLedgerOffloadCredentialSecret(credentialSecret);
            }

            builder.s3ManagedLedgerOffloadRegion(region)
                    .s3ManagedLedgerOffloadBucket(bucket)
                    .s3ManagedLedgerOffloadServiceEndpoint(endpoint)
                    .s3ManagedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes)
                    .s3ManagedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes);
        }
        else if (driver.equalsIgnoreCase(DRIVER_NAMES.get(2))) {
            builder.gcsManagedLedgerOffloadRegion(region)
                    .gcsManagedLedgerOffloadBucket(bucket)
                    .gcsManagedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes)
                    .gcsManagedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes);
        }

        return builder.build();
    }

    public static OffloadPoliciesImpl create(Properties properties)
    {
        OffloadPoliciesImpl data = new OffloadPoliciesImpl();
        Field[] fields = OffloadPoliciesImpl.class.getDeclaredFields();
        Arrays.stream(fields).forEach(f -> {
            if (properties.containsKey(f.getName())) {
                try {
                    f.setAccessible(true);
                    f.set(data, value((String) properties.get(f.getName()), f));
                }
                catch (Exception e) {
                    throw new IllegalArgumentException(format("fail to initialize %s field while setting value %s", f.getName(), properties.get(f.getName())), e);
                }
            }
        });
        data.compatibleWithBrokerConfigFile(properties);
        return data;
    }

    public static OffloadPoliciesImplBuilder builder()
    {
        return new OffloadPoliciesImplBuilder();
    }

    public static String getSupportedDriverNames()
    {
        return StringUtils.join(DRIVER_NAMES, ",");
    }

    private static void setProperty(Properties properties,
                                    String key,
                                    Object value)
    {
        if (value != null) {
            properties.setProperty(key, "" + value);
        }
    }

    /**
     * This method is used to make a compatible with old policies.
     *
     * <p>The filed {@link Policies#offload_threshold} is primitive, so it can't be known whether it had been set.
     * In the old logic, if the field value is -1, it could be thought that the field had not been set.
     *
     * @param nsLevelPolicies namespace level offload policies
     * @param policies namespace policies
     * @return offload policies
     */
    public static OffloadPoliciesImpl oldPoliciesCompatible(OffloadPoliciesImpl nsLevelPolicies,
                                                            Policies policies)
    {
        if (policies == null || (policies.offload_threshold == -1 && policies.offload_deletion_lag_ms == null)) {
            return nsLevelPolicies;
        }
        if (nsLevelPolicies == null) {
            nsLevelPolicies = new OffloadPoliciesImpl();
        }
        if (nsLevelPolicies.getManagedLedgerOffloadThresholdInBytes() == null
                && policies.offload_threshold != -1) {
            nsLevelPolicies.setManagedLedgerOffloadThresholdInBytes(policies.offload_threshold);
        }
        if (nsLevelPolicies.getManagedLedgerOffloadDeletionLagInMillis() == null
                && policies.offload_deletion_lag_ms != null) {
            nsLevelPolicies.setManagedLedgerOffloadDeletionLagInMillis(policies.offload_deletion_lag_ms);
        }
        return nsLevelPolicies;
    }

    /**
     * Merge different level offload policies.
     *
     * <p>policies level priority: topic > namespace > broker
     *
     * @param topicLevelPolicies topic level offload policies
     * @param nsLevelPolicies namespace level offload policies
     * @param brokerProperties broker level offload configuration
     * @return offload policies
     */
    public static OffloadPoliciesImpl mergeConfiguration(OffloadPoliciesImpl topicLevelPolicies,
                                                         OffloadPoliciesImpl nsLevelPolicies,
                                                         Properties brokerProperties)
    {
        try {
            boolean allConfigValuesAreNull = true;
            OffloadPoliciesImpl offloadPolicies = new OffloadPoliciesImpl();
            for (Field field : CONFIGURATION_FIELDS) {
                Object object;
                if (topicLevelPolicies != null && field.get(topicLevelPolicies) != null) {
                    object = field.get(topicLevelPolicies);
                }
                else if (nsLevelPolicies != null && field.get(nsLevelPolicies) != null) {
                    object = field.get(nsLevelPolicies);
                }
                else {
                    object = getCompatibleValue(brokerProperties, field);
                }
                if (object != null) {
                    field.set(offloadPolicies, object);
                    if (allConfigValuesAreNull) {
                        allConfigValuesAreNull = false;
                    }
                }
            }
            if (allConfigValuesAreNull) {
                return null;
            }
            else {
                return offloadPolicies;
            }
        }
        catch (Exception e) {
            log.error("fail to merge configuration. %s", e);
            return null;
        }
    }

    /**
     * Make configurations of the OffloadPolicies compatible with the config file.
     *
     * <p>The names of the fields {@link OffloadPoliciesImpl#managedLedgerOffloadDeletionLagInMillis}
     * and {@link OffloadPoliciesImpl#managedLedgerOffloadThresholdInBytes} are not matched with
     * config file (broker.conf or standalone.conf).
     *
     * @param properties broker configuration properties
     * @param field filed
     * @return field value
     */
    private static Object getCompatibleValue(Properties properties, Field field)
    {
        Object object;
        if (field.getName().equals("managedLedgerOffloadThresholdInBytes")) {
            object = properties.getProperty("managedLedgerOffloadThresholdInBytes",
                    properties.getProperty(OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE));
        }
        else if (field.getName().equals("managedLedgerOffloadDeletionLagInMillis")) {
            object = properties.getProperty("managedLedgerOffloadDeletionLagInMillis",
                    properties.getProperty(DELETION_LAG_NAME_IN_CONF_FILE));
        }
        else {
            object = properties.get(field.getName());
        }
        return value((String) object, field);
    }

    @Override
    public String getOffloadersDirectory()
    {
        return offloadersDirectory;
    }

    public void setOffloadersDirectory(String offloadersDirectory)
    {
        this.offloadersDirectory = offloadersDirectory;
    }

    @Override
    public String getManagedLedgerOffloadDriver()
    {
        return managedLedgerOffloadDriver;
    }

    public void setManagedLedgerOffloadDriver(String managedLedgerOffloadDriver)
    {
        this.managedLedgerOffloadDriver = managedLedgerOffloadDriver;
    }

    @Override
    public Integer getManagedLedgerOffloadMaxThreads()
    {
        return managedLedgerOffloadMaxThreads;
    }

    public void setManagedLedgerOffloadMaxThreads(Integer managedLedgerOffloadMaxThreads)
    {
        this.managedLedgerOffloadMaxThreads = managedLedgerOffloadMaxThreads;
    }

    @Override
    public Integer getManagedLedgerOffloadPrefetchRounds()
    {
        return managedLedgerOffloadPrefetchRounds;
    }

    public void setManagedLedgerOffloadPrefetchRounds(Integer managedLedgerOffloadPrefetchRounds)
    {
        this.managedLedgerOffloadPrefetchRounds = managedLedgerOffloadPrefetchRounds;
    }

    @Override
    public Long getManagedLedgerOffloadThresholdInBytes()
    {
        return managedLedgerOffloadThresholdInBytes;
    }

    public void setManagedLedgerOffloadThresholdInBytes(Long managedLedgerOffloadThresholdInBytes)
    {
        this.managedLedgerOffloadThresholdInBytes = managedLedgerOffloadThresholdInBytes;
    }

    @Override
    public Long getManagedLedgerOffloadDeletionLagInMillis()
    {
        return managedLedgerOffloadDeletionLagInMillis;
    }

    public void setManagedLedgerOffloadDeletionLagInMillis(Long managedLedgerOffloadDeletionLagInMillis)
    {
        this.managedLedgerOffloadDeletionLagInMillis = managedLedgerOffloadDeletionLagInMillis;
    }

    @Override
    public OffloadedReadPriority getManagedLedgerOffloadedReadPriority()
    {
        return managedLedgerOffloadedReadPriority;
    }

    public void setManagedLedgerOffloadedReadPriority(OffloadedReadPriority managedLedgerOffloadedReadPriority)
    {
        this.managedLedgerOffloadedReadPriority = managedLedgerOffloadedReadPriority;
    }

    @Override
    public String getS3ManagedLedgerOffloadRegion()
    {
        return s3ManagedLedgerOffloadRegion;
    }

    public void setS3ManagedLedgerOffloadRegion(String s3ManagedLedgerOffloadRegion)
    {
        this.s3ManagedLedgerOffloadRegion = s3ManagedLedgerOffloadRegion;
    }

    @Override
    public String getS3ManagedLedgerOffloadBucket()
    {
        return s3ManagedLedgerOffloadBucket;
    }

    public void setS3ManagedLedgerOffloadBucket(String s3ManagedLedgerOffloadBucket)
    {
        this.s3ManagedLedgerOffloadBucket = s3ManagedLedgerOffloadBucket;
    }

    @Override
    public String getS3ManagedLedgerOffloadServiceEndpoint()
    {
        return s3ManagedLedgerOffloadServiceEndpoint;
    }

    public void setS3ManagedLedgerOffloadServiceEndpoint(String s3ManagedLedgerOffloadServiceEndpoint)
    {
        this.s3ManagedLedgerOffloadServiceEndpoint = s3ManagedLedgerOffloadServiceEndpoint;
    }

    @Override
    public Integer getS3ManagedLedgerOffloadMaxBlockSizeInBytes()
    {
        return s3ManagedLedgerOffloadMaxBlockSizeInBytes;
    }

    public void setS3ManagedLedgerOffloadMaxBlockSizeInBytes(Integer s3ManagedLedgerOffloadMaxBlockSizeInBytes)
    {
        this.s3ManagedLedgerOffloadMaxBlockSizeInBytes = s3ManagedLedgerOffloadMaxBlockSizeInBytes;
    }

    @Override
    public Integer getS3ManagedLedgerOffloadReadBufferSizeInBytes()
    {
        return s3ManagedLedgerOffloadReadBufferSizeInBytes;
    }

    public void setS3ManagedLedgerOffloadReadBufferSizeInBytes(Integer s3ManagedLedgerOffloadReadBufferSizeInBytes)
    {
        this.s3ManagedLedgerOffloadReadBufferSizeInBytes = s3ManagedLedgerOffloadReadBufferSizeInBytes;
    }

    @Override
    public String getS3ManagedLedgerOffloadCredentialId()
    {
        return s3ManagedLedgerOffloadCredentialId;
    }

    public void setS3ManagedLedgerOffloadCredentialId(String s3ManagedLedgerOffloadCredentialId)
    {
        this.s3ManagedLedgerOffloadCredentialId = s3ManagedLedgerOffloadCredentialId;
    }

    @Override
    public String getS3ManagedLedgerOffloadCredentialSecret()
    {
        return s3ManagedLedgerOffloadCredentialSecret;
    }

    public void setS3ManagedLedgerOffloadCredentialSecret(String s3ManagedLedgerOffloadCredentialSecret)
    {
        this.s3ManagedLedgerOffloadCredentialSecret = s3ManagedLedgerOffloadCredentialSecret;
    }

    @Override
    public String getS3ManagedLedgerOffloadRole()
    {
        return s3ManagedLedgerOffloadRole;
    }

    public void setS3ManagedLedgerOffloadRole(String s3ManagedLedgerOffloadRole)
    {
        this.s3ManagedLedgerOffloadRole = s3ManagedLedgerOffloadRole;
    }

    @Override
    public String getS3ManagedLedgerOffloadRoleSessionName()
    {
        return s3ManagedLedgerOffloadRoleSessionName;
    }

    public void setS3ManagedLedgerOffloadRoleSessionName(String s3ManagedLedgerOffloadRoleSessionName)
    {
        this.s3ManagedLedgerOffloadRoleSessionName = s3ManagedLedgerOffloadRoleSessionName;
    }

    @Override
    public String getGcsManagedLedgerOffloadRegion()
    {
        return gcsManagedLedgerOffloadRegion;
    }

    public void setGcsManagedLedgerOffloadRegion(String gcsManagedLedgerOffloadRegion)
    {
        this.gcsManagedLedgerOffloadRegion = gcsManagedLedgerOffloadRegion;
    }

    @Override
    public String getGcsManagedLedgerOffloadBucket()
    {
        return gcsManagedLedgerOffloadBucket;
    }

    public void setGcsManagedLedgerOffloadBucket(String gcsManagedLedgerOffloadBucket)
    {
        this.gcsManagedLedgerOffloadBucket = gcsManagedLedgerOffloadBucket;
    }

    @Override
    public Integer getGcsManagedLedgerOffloadMaxBlockSizeInBytes()
    {
        return gcsManagedLedgerOffloadMaxBlockSizeInBytes;
    }

    public void setGcsManagedLedgerOffloadMaxBlockSizeInBytes(Integer gcsManagedLedgerOffloadMaxBlockSizeInBytes)
    {
        this.gcsManagedLedgerOffloadMaxBlockSizeInBytes = gcsManagedLedgerOffloadMaxBlockSizeInBytes;
    }

    @Override
    public Integer getGcsManagedLedgerOffloadReadBufferSizeInBytes()
    {
        return gcsManagedLedgerOffloadReadBufferSizeInBytes;
    }

    public void setGcsManagedLedgerOffloadReadBufferSizeInBytes(Integer gcsManagedLedgerOffloadReadBufferSizeInBytes)
    {
        this.gcsManagedLedgerOffloadReadBufferSizeInBytes = gcsManagedLedgerOffloadReadBufferSizeInBytes;
    }

    @Override
    public String getGcsManagedLedgerOffloadServiceAccountKeyFile()
    {
        return gcsManagedLedgerOffloadServiceAccountKeyFile;
    }

    public void setGcsManagedLedgerOffloadServiceAccountKeyFile(String gcsManagedLedgerOffloadServiceAccountKeyFile)
    {
        this.gcsManagedLedgerOffloadServiceAccountKeyFile = gcsManagedLedgerOffloadServiceAccountKeyFile;
    }

    @Override
    public String getFileSystemProfilePath()
    {
        return fileSystemProfilePath;
    }

    public void setFileSystemProfilePath(String fileSystemProfilePath)
    {
        this.fileSystemProfilePath = fileSystemProfilePath;
    }

    @Override
    public String getFileSystemURI()
    {
        return fileSystemURI;
    }

    public void setFileSystemURI(String fileSystemURI)
    {
        this.fileSystemURI = fileSystemURI;
    }

    @Override
    public String getManagedLedgerOffloadBucket()
    {
        return managedLedgerOffloadBucket;
    }

    public void setManagedLedgerOffloadBucket(String managedLedgerOffloadBucket)
    {
        this.managedLedgerOffloadBucket = managedLedgerOffloadBucket;
    }

    @Override
    public String getManagedLedgerOffloadRegion()
    {
        return managedLedgerOffloadRegion;
    }

    public void setManagedLedgerOffloadRegion(String managedLedgerOffloadRegion)
    {
        this.managedLedgerOffloadRegion = managedLedgerOffloadRegion;
    }

    @Override
    public String getManagedLedgerOffloadServiceEndpoint()
    {
        return managedLedgerOffloadServiceEndpoint;
    }

    public void setManagedLedgerOffloadServiceEndpoint(String managedLedgerOffloadServiceEndpoint)
    {
        this.managedLedgerOffloadServiceEndpoint = managedLedgerOffloadServiceEndpoint;
    }

    @Override
    public Integer getManagedLedgerOffloadMaxBlockSizeInBytes()
    {
        return managedLedgerOffloadMaxBlockSizeInBytes;
    }

    public void setManagedLedgerOffloadMaxBlockSizeInBytes(Integer managedLedgerOffloadMaxBlockSizeInBytes)
    {
        this.managedLedgerOffloadMaxBlockSizeInBytes = managedLedgerOffloadMaxBlockSizeInBytes;
    }

    @Override
    public Integer getManagedLedgerOffloadReadBufferSizeInBytes()
    {
        return managedLedgerOffloadReadBufferSizeInBytes;
    }

    public void setManagedLedgerOffloadReadBufferSizeInBytes(Integer managedLedgerOffloadReadBufferSizeInBytes)
    {
        this.managedLedgerOffloadReadBufferSizeInBytes = managedLedgerOffloadReadBufferSizeInBytes;
    }

    public void compatibleWithBrokerConfigFile(Properties properties)
    {
        if (!properties.containsKey("managedLedgerOffloadThresholdInBytes")
                && properties.containsKey(OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE)) {
            setManagedLedgerOffloadThresholdInBytes(
                    Long.parseLong(properties.getProperty(OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE)));
        }

        if (!properties.containsKey("managedLedgerOffloadDeletionLagInMillis")
                && properties.containsKey(DELETION_LAG_NAME_IN_CONF_FILE)) {
            setManagedLedgerOffloadDeletionLagInMillis(
                    Long.parseLong(properties.getProperty(DELETION_LAG_NAME_IN_CONF_FILE)));
        }
    }

    public boolean driverSupported()
    {
        return DRIVER_NAMES.stream().anyMatch(d -> d.equalsIgnoreCase(this.managedLedgerOffloadDriver));
    }

    public boolean isS3Driver()
    {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES.get(0))
                || managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES.get(1));
    }

    public boolean isGcsDriver()
    {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES.get(2));
    }

    public boolean isFileSystemDriver()
    {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES.get(3));
    }

    public boolean bucketValid()
    {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        if (StringUtils.isNotEmpty(managedLedgerOffloadBucket)) {
            return true;
        }
        if (isS3Driver()) {
            return StringUtils.isNotEmpty(s3ManagedLedgerOffloadBucket);
        }
        else if (isGcsDriver()) {
            return StringUtils.isNotEmpty(gcsManagedLedgerOffloadBucket);
        }
        else {
            return isFileSystemDriver();
        }
    }

    public Properties toProperties()
    {
        Properties properties = new Properties();
        setProperty(properties, "managedLedgerOffloadedReadPriority", this.getManagedLedgerOffloadedReadPriority());
        setProperty(properties, "offloadersDirectory", this.getOffloadersDirectory());
        setProperty(properties, "managedLedgerOffloadDriver", this.getManagedLedgerOffloadDriver());
        setProperty(properties, "managedLedgerOffloadMaxThreads",
                this.getManagedLedgerOffloadMaxThreads());
        setProperty(properties, "managedLedgerOffloadPrefetchRounds",
                this.getManagedLedgerOffloadPrefetchRounds());
        setProperty(properties, "managedLedgerOffloadThresholdInBytes",
                this.getManagedLedgerOffloadThresholdInBytes());
        setProperty(properties, "managedLedgerOffloadDeletionLagInMillis",
                this.getManagedLedgerOffloadDeletionLagInMillis());

        if (this.isS3Driver()) {
            setProperty(properties, "s3ManagedLedgerOffloadRegion",
                    this.getS3ManagedLedgerOffloadRegion());
            setProperty(properties, "s3ManagedLedgerOffloadBucket",
                    this.getS3ManagedLedgerOffloadBucket());
            setProperty(properties, "s3ManagedLedgerOffloadServiceEndpoint",
                    this.getS3ManagedLedgerOffloadServiceEndpoint());
            setProperty(properties, "s3ManagedLedgerOffloadMaxBlockSizeInBytes",
                    this.getS3ManagedLedgerOffloadMaxBlockSizeInBytes());
            setProperty(properties, "s3ManagedLedgerOffloadCredentialId",
                    this.getS3ManagedLedgerOffloadCredentialId());
            setProperty(properties, "s3ManagedLedgerOffloadCredentialSecret",
                    this.getS3ManagedLedgerOffloadCredentialSecret());
            setProperty(properties, "s3ManagedLedgerOffloadRole",
                    this.getS3ManagedLedgerOffloadRole());
            setProperty(properties, "s3ManagedLedgerOffloadRoleSessionName",
                    this.getS3ManagedLedgerOffloadRoleSessionName());
            setProperty(properties, "s3ManagedLedgerOffloadReadBufferSizeInBytes",
                    this.getS3ManagedLedgerOffloadReadBufferSizeInBytes());
        }
        else if (this.isGcsDriver()) {
            setProperty(properties, "gcsManagedLedgerOffloadRegion",
                    this.getGcsManagedLedgerOffloadRegion());
            setProperty(properties, "gcsManagedLedgerOffloadBucket",
                    this.getGcsManagedLedgerOffloadBucket());
            setProperty(properties, "gcsManagedLedgerOffloadMaxBlockSizeInBytes",
                    this.getGcsManagedLedgerOffloadMaxBlockSizeInBytes());
            setProperty(properties, "gcsManagedLedgerOffloadReadBufferSizeInBytes",
                    this.getGcsManagedLedgerOffloadReadBufferSizeInBytes());
            setProperty(properties, "gcsManagedLedgerOffloadServiceAccountKeyFile",
                    this.getGcsManagedLedgerOffloadServiceAccountKeyFile());
        }
        else if (this.isFileSystemDriver()) {
            setProperty(properties, "fileSystemProfilePath", this.getFileSystemProfilePath());
            setProperty(properties, "fileSystemURI", this.getFileSystemURI());
        }

        setProperty(properties, "managedLedgerOffloadBucket", this.getManagedLedgerOffloadBucket());
        setProperty(properties, "managedLedgerOffloadRegion", this.getManagedLedgerOffloadRegion());
        setProperty(properties, "managedLedgerOffloadServiceEndpoint",
                this.getManagedLedgerOffloadServiceEndpoint());
        setProperty(properties, "managedLedgerOffloadMaxBlockSizeInBytes",
                this.getManagedLedgerOffloadMaxBlockSizeInBytes());
        setProperty(properties, "managedLedgerOffloadReadBufferSizeInBytes",
                this.getManagedLedgerOffloadReadBufferSizeInBytes());

        return properties;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OffloadPoliciesImpl that)) {
            return false;
        }
        return getOffloadersDirectory().equals(that.getOffloadersDirectory()) &&
                getManagedLedgerOffloadDriver().equals(that.getManagedLedgerOffloadDriver()) &&
                getManagedLedgerOffloadMaxThreads().equals(that.getManagedLedgerOffloadMaxThreads()) &&
                getManagedLedgerOffloadPrefetchRounds().equals(that.getManagedLedgerOffloadPrefetchRounds()) &&
                getManagedLedgerOffloadThresholdInBytes().equals(that.getManagedLedgerOffloadThresholdInBytes()) &&
                getManagedLedgerOffloadDeletionLagInMillis().equals(that.getManagedLedgerOffloadDeletionLagInMillis()) &&
                getManagedLedgerOffloadedReadPriority() == that.getManagedLedgerOffloadedReadPriority() &&
                getS3ManagedLedgerOffloadRegion().equals(that.getS3ManagedLedgerOffloadRegion()) &&
                getS3ManagedLedgerOffloadBucket().equals(that.getS3ManagedLedgerOffloadBucket()) &&
                getS3ManagedLedgerOffloadServiceEndpoint().equals(that.getS3ManagedLedgerOffloadServiceEndpoint()) &&
                getS3ManagedLedgerOffloadMaxBlockSizeInBytes().equals(that.getS3ManagedLedgerOffloadMaxBlockSizeInBytes()) &&
                getS3ManagedLedgerOffloadReadBufferSizeInBytes().equals(that.getS3ManagedLedgerOffloadReadBufferSizeInBytes()) &&
                getS3ManagedLedgerOffloadCredentialId().equals(that.getS3ManagedLedgerOffloadCredentialId()) &&
                getS3ManagedLedgerOffloadCredentialSecret().equals(that.getS3ManagedLedgerOffloadCredentialSecret()) &&
                getS3ManagedLedgerOffloadRole().equals(that.getS3ManagedLedgerOffloadRole()) &&
                getS3ManagedLedgerOffloadRoleSessionName().equals(that.getS3ManagedLedgerOffloadRoleSessionName()) &&
                getGcsManagedLedgerOffloadRegion().equals(that.getGcsManagedLedgerOffloadRegion()) &&
                getGcsManagedLedgerOffloadBucket().equals(that.getGcsManagedLedgerOffloadBucket()) &&
                getGcsManagedLedgerOffloadMaxBlockSizeInBytes().equals(that.getGcsManagedLedgerOffloadMaxBlockSizeInBytes()) &&
                getGcsManagedLedgerOffloadReadBufferSizeInBytes().equals(that.getGcsManagedLedgerOffloadReadBufferSizeInBytes()) &&
                getGcsManagedLedgerOffloadServiceAccountKeyFile().equals(that.getGcsManagedLedgerOffloadServiceAccountKeyFile()) &&
                getFileSystemProfilePath().equals(that.getFileSystemProfilePath()) &&
                getFileSystemURI().equals(that.getFileSystemURI()) &&
                getManagedLedgerOffloadBucket().equals(that.getManagedLedgerOffloadBucket()) &&
                getManagedLedgerOffloadRegion().equals(that.getManagedLedgerOffloadRegion()) &&
                getManagedLedgerOffloadServiceEndpoint().equals(that.getManagedLedgerOffloadServiceEndpoint()) &&
                getManagedLedgerOffloadMaxBlockSizeInBytes().equals(that.getManagedLedgerOffloadMaxBlockSizeInBytes()) &&
                getManagedLedgerOffloadReadBufferSizeInBytes().equals(that.getManagedLedgerOffloadReadBufferSizeInBytes());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getOffloadersDirectory(), getManagedLedgerOffloadDriver(), getManagedLedgerOffloadMaxThreads(), getManagedLedgerOffloadPrefetchRounds(), getManagedLedgerOffloadThresholdInBytes(), getManagedLedgerOffloadDeletionLagInMillis(), getManagedLedgerOffloadedReadPriority(), getS3ManagedLedgerOffloadRegion(), getS3ManagedLedgerOffloadBucket(), getS3ManagedLedgerOffloadServiceEndpoint(), getS3ManagedLedgerOffloadMaxBlockSizeInBytes(), getS3ManagedLedgerOffloadReadBufferSizeInBytes(), getS3ManagedLedgerOffloadCredentialId(), getS3ManagedLedgerOffloadCredentialSecret(), getS3ManagedLedgerOffloadRole(), getS3ManagedLedgerOffloadRoleSessionName(), getGcsManagedLedgerOffloadRegion(), getGcsManagedLedgerOffloadBucket(), getGcsManagedLedgerOffloadMaxBlockSizeInBytes(), getGcsManagedLedgerOffloadReadBufferSizeInBytes(), getGcsManagedLedgerOffloadServiceAccountKeyFile(), getFileSystemProfilePath(), getFileSystemURI(), getManagedLedgerOffloadBucket(), getManagedLedgerOffloadRegion(), getManagedLedgerOffloadServiceEndpoint(), getManagedLedgerOffloadMaxBlockSizeInBytes(), getManagedLedgerOffloadReadBufferSizeInBytes());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("offloadersDirectory", offloadersDirectory)
                .add("managedLedgerOffloadDriver", managedLedgerOffloadDriver)
                .add("managedLedgerOffloadMaxThreads", managedLedgerOffloadMaxThreads)
                .add("managedLedgerOffloadPrefetchRounds", managedLedgerOffloadPrefetchRounds)
                .add("managedLedgerOffloadThresholdInBytes", managedLedgerOffloadThresholdInBytes)
                .add("managedLedgerOffloadDeletionLagInMillis", managedLedgerOffloadDeletionLagInMillis)
                .add("managedLedgerOffloadedReadPriority", managedLedgerOffloadedReadPriority)
                .add("s3ManagedLedgerOffloadRegion", s3ManagedLedgerOffloadRegion)
                .add("s3ManagedLedgerOffloadBucket", s3ManagedLedgerOffloadBucket)
                .add("s3ManagedLedgerOffloadServiceEndpoint", s3ManagedLedgerOffloadServiceEndpoint)
                .add("s3ManagedLedgerOffloadMaxBlockSizeInBytes", s3ManagedLedgerOffloadMaxBlockSizeInBytes)
                .add("s3ManagedLedgerOffloadReadBufferSizeInBytes", s3ManagedLedgerOffloadReadBufferSizeInBytes)
                .add("s3ManagedLedgerOffloadCredentialId", s3ManagedLedgerOffloadCredentialId)
                .add("s3ManagedLedgerOffloadCredentialSecret", s3ManagedLedgerOffloadCredentialSecret)
                .add("s3ManagedLedgerOffloadRole", s3ManagedLedgerOffloadRole)
                .add("s3ManagedLedgerOffloadRoleSessionName", s3ManagedLedgerOffloadRoleSessionName)
                .add("gcsManagedLedgerOffloadRegion", gcsManagedLedgerOffloadRegion)
                .add("gcsManagedLedgerOffloadBucket", gcsManagedLedgerOffloadBucket)
                .add("gcsManagedLedgerOffloadMaxBlockSizeInBytes", gcsManagedLedgerOffloadMaxBlockSizeInBytes)
                .add("gcsManagedLedgerOffloadReadBufferSizeInBytes", gcsManagedLedgerOffloadReadBufferSizeInBytes)
                .add("gcsManagedLedgerOffloadServiceAccountKeyFile", gcsManagedLedgerOffloadServiceAccountKeyFile)
                .add("fileSystemProfilePath", fileSystemProfilePath)
                .add("fileSystemURI", fileSystemURI)
                .add("managedLedgerOffloadBucket", managedLedgerOffloadBucket)
                .add("managedLedgerOffloadRegion", managedLedgerOffloadRegion)
                .add("managedLedgerOffloadServiceEndpoint", managedLedgerOffloadServiceEndpoint)
                .add("managedLedgerOffloadMaxBlockSizeInBytes", managedLedgerOffloadMaxBlockSizeInBytes)
                .add("managedLedgerOffloadReadBufferSizeInBytes", managedLedgerOffloadReadBufferSizeInBytes)
                .toString();
    }

    @Override
    public Long getManagedLedgerOffloadThresholdInSeconds()
    {
        return managedLedgerOffloadThresholdInSeconds;
    }

    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    private @interface Configuration
    {
    }

    public static class OffloadPoliciesImplBuilder
    {
        private final OffloadPoliciesImpl impl = new OffloadPoliciesImpl();

        public OffloadPoliciesImplBuilder offloadersDirectory(String offloadersDirectory)
        {
            impl.offloadersDirectory = offloadersDirectory;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadDriver(String managedLedgerOffloadDriver)
        {
            impl.managedLedgerOffloadDriver = managedLedgerOffloadDriver;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadMaxThreads(Integer managedLedgerOffloadMaxThreads)
        {
            impl.managedLedgerOffloadMaxThreads = managedLedgerOffloadMaxThreads;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadPrefetchRounds(Integer managedLedgerOffloadPrefetchRounds)
        {
            impl.managedLedgerOffloadPrefetchRounds = managedLedgerOffloadPrefetchRounds;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadThresholdInBytes(Long managedLedgerOffloadThresholdInBytes)
        {
            impl.managedLedgerOffloadThresholdInBytes = managedLedgerOffloadThresholdInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadDeletionLagInMillis(Long managedLedgerOffloadDeletionLagInMillis)
        {
            impl.managedLedgerOffloadDeletionLagInMillis = managedLedgerOffloadDeletionLagInMillis;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadedReadPriority(OffloadedReadPriority managedLedgerOffloadedReadPriority)
        {
            impl.managedLedgerOffloadedReadPriority = managedLedgerOffloadedReadPriority;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadRegion(String s3ManagedLedgerOffloadRegion)
        {
            impl.s3ManagedLedgerOffloadRegion = s3ManagedLedgerOffloadRegion;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadBucket(String s3ManagedLedgerOffloadBucket)
        {
            impl.s3ManagedLedgerOffloadBucket = s3ManagedLedgerOffloadBucket;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadServiceEndpoint(String s3ManagedLedgerOffloadServiceEndpoint)
        {
            impl.s3ManagedLedgerOffloadServiceEndpoint = s3ManagedLedgerOffloadServiceEndpoint;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadMaxBlockSizeInBytes(Integer s3ManagedLedgerOffloadMaxBlockSizeInBytes)
        {
            impl.s3ManagedLedgerOffloadMaxBlockSizeInBytes = s3ManagedLedgerOffloadMaxBlockSizeInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadReadBufferSizeInBytes(Integer s3ManagedLedgerOffloadReadBufferSizeInBytes)
        {
            impl.s3ManagedLedgerOffloadReadBufferSizeInBytes = s3ManagedLedgerOffloadReadBufferSizeInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadCredentialId(String s3ManagedLedgerOffloadCredentialId)
        {
            impl.s3ManagedLedgerOffloadCredentialId = s3ManagedLedgerOffloadCredentialId;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadCredentialSecret(String s3ManagedLedgerOffloadCredentialSecret)
        {
            impl.s3ManagedLedgerOffloadCredentialSecret = s3ManagedLedgerOffloadCredentialSecret;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadRole(String s3ManagedLedgerOffloadRole)
        {
            impl.s3ManagedLedgerOffloadRole = s3ManagedLedgerOffloadRole;
            return this;
        }

        public OffloadPoliciesImplBuilder s3ManagedLedgerOffloadRoleSessionName(String s3ManagedLedgerOffloadRoleSessionName)
        {
            impl.s3ManagedLedgerOffloadRoleSessionName = s3ManagedLedgerOffloadRoleSessionName;
            return this;
        }

        public OffloadPoliciesImplBuilder gcsManagedLedgerOffloadRegion(String gcsManagedLedgerOffloadRegion)
        {
            impl.gcsManagedLedgerOffloadRegion = gcsManagedLedgerOffloadRegion;
            return this;
        }

        public OffloadPoliciesImplBuilder gcsManagedLedgerOffloadBucket(String gcsManagedLedgerOffloadBucket)
        {
            impl.gcsManagedLedgerOffloadBucket = gcsManagedLedgerOffloadBucket;
            return this;
        }

        public OffloadPoliciesImplBuilder gcsManagedLedgerOffloadMaxBlockSizeInBytes(Integer gcsManagedLedgerOffloadMaxBlockSizeInBytes)
        {
            impl.gcsManagedLedgerOffloadMaxBlockSizeInBytes = gcsManagedLedgerOffloadMaxBlockSizeInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder gcsManagedLedgerOffloadReadBufferSizeInBytes(Integer gcsManagedLedgerOffloadReadBufferSizeInBytes)
        {
            impl.gcsManagedLedgerOffloadReadBufferSizeInBytes = gcsManagedLedgerOffloadReadBufferSizeInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder gcsManagedLedgerOffloadServiceAccountKeyFile(String gcsManagedLedgerOffloadServiceAccountKeyFile)
        {
            impl.gcsManagedLedgerOffloadServiceAccountKeyFile = gcsManagedLedgerOffloadServiceAccountKeyFile;
            return this;
        }

        public OffloadPoliciesImplBuilder fileSystemProfilePath(String fileSystemProfilePath)
        {
            impl.fileSystemProfilePath = fileSystemProfilePath;
            return this;
        }

        public OffloadPoliciesImplBuilder fileSystemURI(String fileSystemURI)
        {
            impl.fileSystemURI = fileSystemURI;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadBucket(String managedLedgerOffloadBucket)
        {
            impl.managedLedgerOffloadBucket = managedLedgerOffloadBucket;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadRegion(String managedLedgerOffloadRegion)
        {
            impl.managedLedgerOffloadRegion = managedLedgerOffloadRegion;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadServiceEndpoint(String managedLedgerOffloadServiceEndpoint)
        {
            impl.managedLedgerOffloadServiceEndpoint = managedLedgerOffloadServiceEndpoint;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadMaxBlockSizeInBytes(Integer managedLedgerOffloadMaxBlockSizeInBytes)
        {
            impl.managedLedgerOffloadMaxBlockSizeInBytes = managedLedgerOffloadMaxBlockSizeInBytes;
            return this;
        }

        public OffloadPoliciesImplBuilder managedLedgerOffloadReadBufferSizeInBytes(Integer managedLedgerOffloadReadBufferSizeInBytes)
        {
            impl.managedLedgerOffloadReadBufferSizeInBytes = managedLedgerOffloadReadBufferSizeInBytes;
            return this;
        }

        public OffloadPoliciesImpl build()
        {
            return impl;
        }
    }
}
