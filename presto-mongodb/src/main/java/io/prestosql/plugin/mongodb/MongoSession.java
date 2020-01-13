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
package io.prestosql.plugin.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static io.prestosql.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class MongoSession
{
    private static final Logger log = Logger.get(MongoSession.class);
    private static final List<String> SYSTEM_TABLES = Arrays.asList("system.indexes", "system.users", "system.version");

    private static final String TABLE_NAME_KEY = "table";
    private static final String FIELDS_KEY = "fields";
    private static final String FIELDS_NAME_KEY = "name";
    private static final String FIELDS_TYPE_KEY = "type";
    private static final String FIELDS_HIDDEN_KEY = "hidden";

    private static final String OR_OP = "$or";
    private static final String AND_OP = "$and";
    private static final String NOT_OP = "$not";
    private static final String NOR_OP = "$nor";

    private static final String EQ_OP = "$eq";
    private static final String NOT_EQ_OP = "$ne";
    private static final String EXISTS_OP = "$exists";
    private static final String GTE_OP = "$gte";
    private static final String GT_OP = "$gt";
    private static final String LT_OP = "$lt";
    private static final String LTE_OP = "$lte";
    private static final String IN_OP = "$in";
    private static final String NOTIN_OP = "$nin";

    private final TypeManager typeManager;
    private final MongoClient client;

    private final String schemaCollection;
    private final int cursorBatchSize;

    private final LoadingCache<SchemaTableName, MongoTableHandle> tableCache;
    private final String implicitPrefix;

    public MongoSession(TypeManager typeManager, MongoClient client, MongoClientConfig config)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.client = requireNonNull(client, "client is null");
        this.schemaCollection = requireNonNull(config.getSchemaCollection(), "config.getSchemaCollection() is null");
        this.cursorBatchSize = config.getCursorBatchSize();
        this.implicitPrefix = requireNonNull(config.getImplicitRowFieldPrefix(), "config.getImplicitRowFieldPrefix() is null");

        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(1, HOURS)  // TODO: Configure
                .refreshAfterWrite(1, MINUTES)
                .build(CacheLoader.from(this::loadTable));
    }

    public void shutdown()
    {
        client.close();
    }

    public List<String> getAllSchemas()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        for (String name : client.listDatabaseNames()) {
            builder.add(name.toLowerCase(ENGLISH));
        }

        return builder.build();
    }

    public Set<String> getAllTables(String schema)
            throws SchemaNotFoundException
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();

        for (String name : client.getDatabase(getDatabaseName(schema)).listCollectionNames()) {
            if (name.equals(schemaCollection) || SYSTEM_TABLES.contains(name)) {
                continue;
            }

            builder.add(name.toLowerCase(ENGLISH));
        }
        builder.addAll(getTableMetadataNames(schema));

        return builder.build();
    }

    public MongoTableHandle getTable(SchemaTableName table)
            throws TableNotFoundException
    {
        try {
            return tableCache.getUnchecked(table);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    public List<MongoColumnHandle> getColumns(MongoTableHandle table)
    {
        Document tableMeta = getTableMetadata(table);

        ImmutableList.Builder<MongoColumnHandle> columnHandles = ImmutableList.builder();

        for (Document columnMetadata : getColumnMetadata(tableMeta)) {
            MongoColumnHandle columnHandle = buildColumnHandle(columnMetadata);
            columnHandles.add(columnHandle);
        }

        return columnHandles.build();
    }

    public List<MongoIndex> getIndexes(MongoTableHandle table)
            throws TableNotFoundException
    {
        if (isView(table)) {
            return ImmutableList.of();
        }
        return MongoIndex.parse(getCollection(table).listIndexes());
    }

    public void createTable(MongoTableHandle table, List<MongoColumnHandle> columns)
    {
        createTableMetadata(table, columns);
        // collection is created implicitly
    }

    public void dropTable(MongoTableHandle table)
    {
        deleteTableMetadata(table);
        getCollection(table).drop();

        tableCache.invalidate(table.getSchemaTableName());
    }

    private MongoColumnHandle buildColumnHandle(Document columnMeta)
    {
        String name = columnMeta.getString(FIELDS_NAME_KEY);
        String typeString = columnMeta.getString(FIELDS_TYPE_KEY);
        boolean hidden = columnMeta.getBoolean(FIELDS_HIDDEN_KEY, false);

        Type type = typeManager.fromSqlType(typeString);

        return new MongoColumnHandle(name, type, hidden);
    }

    private List<Document> getColumnMetadata(Document doc)
    {
        return (List<Document>) doc.getOrDefault(FIELDS_KEY, ImmutableList.of());
    }

    public MongoCollection<Document> getCollection(MongoTableHandle table)
    {
        return getCollection(table.getDatabaseName(), table.getCollectionName());
    }

    public MongoCollection<Document> getSchemaCollection(String dbName)
    {
        return getCollection(dbName, schemaCollection);
    }

    private MongoCollection<Document> getCollection(String schema, String table)
    {
        return client.getDatabase(schema).getCollection(table);
    }

    public MongoCursor<Document> execute(MongoTableHandle tableHandle, List<MongoColumnHandle> columns)
    {
        Document output = new Document();
        for (MongoColumnHandle column : columns) {
            output.append(column.getName(), 1);
        }
        MongoCollection<Document> collection = getCollection(tableHandle);
        FindIterable<Document> iterable = collection.find(buildQuery(tableHandle.getConstraint())).projection(output);

        if (cursorBatchSize != 0) {
            iterable.batchSize(cursorBatchSize);
        }

        return iterable.iterator();
    }

    @VisibleForTesting
    static Document buildQuery(TupleDomain<ColumnHandle> tupleDomain)
    {
        Document query = new Document();
        if (tupleDomain.getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
                MongoColumnHandle column = (MongoColumnHandle) entry.getKey();
                query.putAll(buildPredicate(column, entry.getValue()));
            }
        }

        return query;
    }

    private static Document buildPredicate(MongoColumnHandle column, Domain domain)
    {
        String name = column.getName();
        Type type = column.getType();
        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            return documentOf(name, isNullPredicate());
        }
        if (domain.getValues().isAll() && !domain.isNullAllowed()) {
            return documentOf(name, isNotNullPredicate());
        }

        List<Object> singleValues = new ArrayList<>();
        List<Document> disjuncts = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                singleValues.add(translateValue(range.getSingleValue(), type));
            }
            else {
                Document rangeConjuncts = new Document();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.put(GT_OP, translateValue(range.getLow().getValue(), type));
                            break;
                        case EXACTLY:
                            rangeConjuncts.put(GTE_OP, translateValue(range.getLow().getValue(), type));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low Marker should never use BELOW bound: " + range);
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High Marker should never use ABOVE bound: " + range);
                        case EXACTLY:
                            rangeConjuncts.put(LTE_OP, translateValue(range.getHigh().getValue(), type));
                            break;
                        case BELOW:
                            rangeConjuncts.put(LT_OP, translateValue(range.getHigh().getValue(), type));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                verify(!rangeConjuncts.isEmpty());
                disjuncts.add(rangeConjuncts);
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(documentOf(EQ_OP, singleValues.get(0)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(documentOf(IN_OP, singleValues));
        }

        if (domain.isNullAllowed()) {
            disjuncts.add(isNullPredicate());
        }

        return orPredicate(disjuncts.stream()
                .map(disjunct -> new Document(name, disjunct))
                .collect(toList()));
    }

    private static Object translateValue(Object source, Type type)
    {
        if (source instanceof Slice) {
            if (type instanceof ObjectIdType) {
                return new ObjectId(((Slice) source).getBytes());
            }
            else {
                return ((Slice) source).toStringUtf8();
            }
        }

        return source;
    }

    private static Document documentOf(String key, Object value)
    {
        return new Document(key, value);
    }

    private static Document orPredicate(List<Document> values)
    {
        checkState(!values.isEmpty());
        if (values.size() == 1) {
            return values.get(0);
        }
        return new Document(OR_OP, values);
    }

    private static Document isNullPredicate()
    {
        return documentOf(EXISTS_OP, true).append(EQ_OP, null);
    }

    private static Document isNotNullPredicate()
    {
        return documentOf(NOT_EQ_OP, null);
    }

    // Internal Schema management
    private Document getTableMetadata(MongoTableHandle table)
            throws TableNotFoundException
    {
        MongoCollection<Document> schema = getSchemaCollection(table.getDatabaseName());

        Document metadata = schema
                .find(documentOf(TABLE_NAME_KEY, table.getCollectionName())).first();

        if (metadata == null) {
            if (!collectionExists(table.getDatabaseName(), table.getCollectionName())) {
                throw new TableNotFoundException(table.getSchemaTableName());
            }
            else {
                metadata = new Document(TABLE_NAME_KEY, table.getCollectionName());
                metadata.append(FIELDS_KEY, guessTableFields(table));

                schema.createIndex(new Document(TABLE_NAME_KEY, 1), new IndexOptions().unique(true));
                schema.insertOne(metadata);
            }
        }

        return metadata;
    }

    public boolean collectionExists(String databaseName, String collectionName)
    {
        for (String name : client.getDatabase(databaseName).listCollectionNames()) {
            if (name.equalsIgnoreCase(collectionName)) {
                return true;
            }
        }
        return false;
    }

    private Set<String> getTableMetadataNames(String schemaName)
            throws TableNotFoundException
    {
        MongoCursor<Document> cursor = client.getDatabase(getDatabaseName(schemaName)).getCollection(schemaCollection)
                .find().projection(new Document(TABLE_NAME_KEY, true)).iterator();

        HashSet<String> names = new HashSet<>();
        while (cursor.hasNext()) {
            names.add((cursor.next()).getString(TABLE_NAME_KEY).toLowerCase(ENGLISH));
        }

        return names;
    }

    private void createTableMetadata(MongoTableHandle table, List<MongoColumnHandle> columns)
            throws TableNotFoundException
    {
        Document metadata = new Document(TABLE_NAME_KEY, table.getCollectionName());

        ArrayList<Document> fields = new ArrayList<>();
        if (!columns.stream().anyMatch(c -> c.getName().equals("_id"))) {
            fields.add(new MongoColumnHandle("_id", OBJECT_ID, true).getDocument());
        }

        fields.addAll(columns.stream()
                .map(MongoColumnHandle::getDocument)
                .collect(toList()));

        metadata.append(FIELDS_KEY, fields);

        MongoCollection<Document> schema = getSchemaCollection(table.getDatabaseName());
        schema.createIndex(new Document(TABLE_NAME_KEY, 1), new IndexOptions().unique(true));
        schema.insertOne(metadata);
    }

    private boolean deleteTableMetadata(MongoTableHandle table)
    {
        DeleteResult result = getSchemaCollection(table.getDatabaseName())
                .deleteOne(documentOf(TABLE_NAME_KEY, table.getCollectionName()));

        return result.getDeletedCount() == 1;
    }

    private List<Document> guessTableFields(MongoTableHandle table)
    {
        Document doc = getCollection(table).find().first();
        if (doc == null) {
            // no records at the collection
            return ImmutableList.of();
        }

        ImmutableList.Builder<Document> builder = ImmutableList.builder();

        for (String key : doc.keySet()) {
            Object value = doc.get(key);
            Optional<TypeSignature> fieldType = guessFieldType(value);
            if (fieldType.isPresent()) {
                Document metadata = new Document();
                metadata.append(FIELDS_NAME_KEY, key);
                metadata.append(FIELDS_TYPE_KEY, fieldType.get().toString());
                metadata.append(FIELDS_HIDDEN_KEY,
                        key.equals("_id") && fieldType.get().equals(OBJECT_ID.getTypeSignature()));

                builder.add(metadata);
            }
            else {
                log.debug("Unable to guess field type from %s : %s", value == null ? "null" : value.getClass().getName(), value);
            }
        }

        return builder.build();
    }

    private Optional<TypeSignature> guessFieldType(Object value)
    {
        if (value == null) {
            return Optional.empty();
        }

        TypeSignature typeSignature = null;
        if (value instanceof String) {
            typeSignature = createUnboundedVarcharType().getTypeSignature();
        }
        else if (value instanceof Integer || value instanceof Long) {
            typeSignature = BIGINT.getTypeSignature();
        }
        else if (value instanceof Boolean) {
            typeSignature = BOOLEAN.getTypeSignature();
        }
        else if (value instanceof Float || value instanceof Double) {
            typeSignature = DOUBLE.getTypeSignature();
        }
        else if (value instanceof Date) {
            typeSignature = TIMESTAMP.getTypeSignature();
        }
        else if (value instanceof ObjectId) {
            typeSignature = OBJECT_ID.getTypeSignature();
        }
        else if (value instanceof List) {
            List<Optional<TypeSignature>> subTypes = ((List<?>) value).stream()
                    .map(this::guessFieldType)
                    .collect(toList());

            if (subTypes.isEmpty() || subTypes.stream().anyMatch(t -> !t.isPresent())) {
                return Optional.empty();
            }

            Set<TypeSignature> signatures = subTypes.stream().map(Optional::get).collect(toSet());
            if (signatures.size() == 1) {
                typeSignature = new TypeSignature(StandardTypes.ARRAY, signatures.stream()
                        .map(TypeSignatureParameter::typeParameter)
                        .collect(Collectors.toList()));
            }
            else {
                // TODO: presto cli doesn't handle empty field name row type yet
                typeSignature = new TypeSignature(StandardTypes.ROW,
                        IntStream.range(0, subTypes.size())
                                .mapToObj(idx -> TypeSignatureParameter.namedTypeParameter(
                                        new NamedTypeSignature(Optional.of(new RowFieldName(format("%s%d", implicitPrefix, idx + 1))), subTypes.get(idx).get())))
                                .collect(toList()));
            }
        }
        else if (value instanceof Document) {
            List<TypeSignatureParameter> parameters = new ArrayList<>();

            for (String key : ((Document) value).keySet()) {
                Optional<TypeSignature> fieldType = guessFieldType(((Document) value).get(key));
                if (!fieldType.isPresent()) {
                    return Optional.empty();
                }

                parameters.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName(key)), fieldType.get())));
            }
            typeSignature = new TypeSignature(StandardTypes.ROW, parameters);
        }

        return Optional.ofNullable(typeSignature);
    }

    private String getDatabaseName(String databaseName)
            throws SchemaNotFoundException
    {
        for (String name : client.listDatabaseNames()) {
            if (name.equalsIgnoreCase(databaseName)) {
                return name;
            }
        }
        return databaseName;
    }

    private String getTableName(String databaseName, String tableName)
            throws SchemaNotFoundException, TableNotFoundException
    {
        for (String name : client.getDatabase(getDatabaseName(databaseName)).listCollectionNames()) {
            if (name.equalsIgnoreCase((tableName))) {
                return name;
            }
        }

        throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
    }

    private MongoTableHandle loadTable(SchemaTableName table)
            throws TableNotFoundException
    {
        String databaseName;
        try {
            databaseName = getDatabaseName(table.getSchemaName());
        }
        catch (SchemaNotFoundException e) {
            throw new TableNotFoundException(table);
        }

        Map<SchemaTableName, MongoTableHandle> tables = new HashMap<>();
        for (String collectionName : client.getDatabase(databaseName).listCollectionNames()) {
            MongoTableHandle tableHandle = new MongoTableHandle(databaseName, collectionName, TupleDomain.all());
            tables.put(tableHandle.getSchemaTableName(), tableHandle);
        }
        tableCache.putAll(tables);

        MongoTableHandle tableHandle = tables.get(table);
        if (tableHandle == null) {
            // From _schema
            tableHandle = new MongoTableHandle(databaseName, table.getTableName(), TupleDomain.all());
            getTableMetadata(tableHandle);
            tableCache.put(tableHandle.getSchemaTableName(), tableHandle);
        }

        return tableHandle;
    }

    private boolean isView(MongoTableHandle table)
    {
        MongoCollection views = client.getDatabase(table.getDatabaseName()).getCollection("system.views");
        Object view = views.find(new Document("_id", table.toString())).first();
        return view != null;
    }
}
