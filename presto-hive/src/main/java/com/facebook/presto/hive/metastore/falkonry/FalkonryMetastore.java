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
package com.facebook.presto.hive.metastore.falkonry;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionStatistics;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.PrincipalType;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.Database.DEFAULT_DATABASE_NAME;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.PrincipalType.ROLE;
import static com.facebook.presto.hive.metastore.PrincipalType.USER;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;

@ThreadSafe
public class FalkonryMetastore
        implements ExtendedHiveMetastore
{
    private static final Logger log = Logger.get(com.facebook.presto.hive.metastore.falkonry.FalkonryMetastore.class);

    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String PRESTO_SCHEMA_FILE_NAME = ".prestoSchema";
    private static final String PRESTO_PERMISSIONS_DIRECTORY_NAME = ".prestoPermissions";

    private final HdfsEnvironment hdfsEnvironment;
    private final Path catalogDirectory;
    private final HdfsContext hdfsContext;
    private final FileSystem metadataFileSystem;

    private final Map<String, List<Column>> columnsMap = new HashMap<>();
    private final Map<String, String> serdes = new HashMap<>();

    private static final String RAWDATA = "RAWDATA";
    private static final String OUTPUTDATA = "OUTPUTDATA";
    private static final String EXPLANATIONDATA = "EXPLANATIONDATA";
    private static final String CONFIDENCEDATA = "CONFIDENCEDATA";
    private static final String BATCHDATA = "BATCHDATA";
    private static final String EPISODEDATA = "EPISODEDATA";

    @Inject
    public FalkonryMetastore(HdfsEnvironment hdfsEnvironment, FalkonryMetastoreConfig config)
    {
        this(hdfsEnvironment, config.getCatalogDirectory(), config.getMetastoreUser());
    }

    public FalkonryMetastore(HdfsEnvironment hdfsEnvironment, String catalogDirectory, String metastoreUser)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.catalogDirectory = new Path(requireNonNull(catalogDirectory, "baseDirectory is null"));
        this.hdfsContext = new HdfsContext(new Identity(metastoreUser, Optional.empty()));
        serdes.put("input", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        serdes.put("output", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        initializeTables();
        try {
            metadataFileSystem = hdfsEnvironment.getFileSystem(hdfsContext, this.catalogDirectory);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private void initializeTables()
    {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("falkonry_source", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("thing", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("time", HiveType.HIVE_LONG, Optional.empty()));
        columns.add(new Column("value", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("batch", HiveType.HIVE_STRING, Optional.empty()));
        columnsMap.put(RAWDATA, columns);

        columns = new ArrayList<>();
        columns.add(new Column("start", HiveType.HIVE_LONG, Optional.empty()));
        columns.add(new Column("end", HiveType.HIVE_LONG, Optional.empty()));
        columns.add(new Column("thing", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("value", HiveType.HIVE_STRING, Optional.empty()));
        columnsMap.put(BATCHDATA, columns);

        columns = new ArrayList<>();
        columns.add(new Column("time", HiveType.HIVE_LONG, Optional.empty()));
        columns.add(new Column("thing", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("value", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("batch", HiveType.HIVE_STRING, Optional.empty()));
        columnsMap.put(OUTPUTDATA, columns);

        columns = new ArrayList<>();
        columns.add(new Column("time", HiveType.HIVE_LONG, Optional.empty()));
        columns.add(new Column("thing", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("signalId", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("value", HiveType.HIVE_FLOAT, Optional.empty()));
        columns.add(new Column("batch", HiveType.HIVE_STRING, Optional.empty()));
        columnsMap.put(CONFIDENCEDATA, columns);

        columns = new ArrayList<>();
        columns.add(new Column("time", HiveType.HIVE_LONG, Optional.empty()));
        columns.add(new Column("thing", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("signalId", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("value", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("score", HiveType.HIVE_FLOAT, Optional.empty()));
        columns.add(new Column("batch", HiveType.HIVE_STRING, Optional.empty()));
        columnsMap.put(EXPLANATIONDATA, columns);

        columns = new ArrayList<>();
        columns.add(new Column("end", HiveType.HIVE_LONG, Optional.empty()));
        columns.add(new Column("time", HiveType.HIVE_LONG, Optional.empty()));
        columns.add(new Column("thing", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("value", HiveType.HIVE_STRING, Optional.empty()));
        columns.add(new Column("batch", HiveType.HIVE_STRING, Optional.empty()));
        columnsMap.put(EPISODEDATA, columns);
    }

    @Override
    public synchronized void createDatabase(Database database)
    {
    }

    @Override
    public synchronized void dropDatabase(String databaseName)
    {
    }

    @Override
    public synchronized void renameDatabase(String databaseName, String newDatabaseName)
    {
    }

    @Override
    public synchronized Optional<Database> getDatabase(String databaseName)
    {
        return Optional.empty();
    }

    @Override
    public synchronized List<String> getAllDatabases()
    {
        List<String> databases = getChildSchemaDirectories(catalogDirectory).stream()
                .map(Path::getName)
                .collect(toList());
        return ImmutableList.copyOf(databases);
    }

    @Override
    public synchronized void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
    }

    @Override
    public synchronized Optional<Table> getTable(String databaseName, String tableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Path location = getTableMetadataDirectory(tableName);
        List<Column> partitionColumns = new ArrayList<>();
        if(!tableName.contains(BATCHDATA)) {
            try {
                log.info("Checking for partitions in " + location.toString());
                FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, location);
                FileStatus[] files = fs.listStatus(location);
                for (int i = 0; i < files.length; i++) {
                    if (files[i].isDirectory() && files[i].getPath().getName().startsWith("signal=")) {
                        partitionColumns.add(new Column("signal", HiveType.HIVE_STRING, Optional.empty()));
                        break;
                    }
                }
            }
            catch (IOException exc) {
                log.warn("Failed while reading partitions in path=" + location);
            }
        }
        List<Column> columns = null;
        if (tableName.contains(RAWDATA)) {
            columns = columnsMap.get(RAWDATA);
        }
        else if (tableName.contains(OUTPUTDATA)) {
            columns = columnsMap.get(OUTPUTDATA);
        }
        else if (tableName.contains(BATCHDATA)) {
            columns = columnsMap.get(BATCHDATA);
        }
        else if (tableName.contains(CONFIDENCEDATA)) {
            columns = columnsMap.get(CONFIDENCEDATA);
        }
        else if (tableName.contains(EXPLANATIONDATA)) {
            columns = columnsMap.get(EXPLANATIONDATA);
        }
        else if (tableName.contains(EPISODEDATA)) {
            columns = columnsMap.get(EPISODEDATA);
        }
        return Optional.of(new Table(
                databaseName,
                tableName,
                "",
                TableType.EXTERNAL_TABLE.name(),
                Storage.builder()
                        .setLocation(location.toString())
                        .setStorageFormat(StorageFormat.fromHiveStorageFormat(HiveStorageFormat.PARQUET))
                        .setBucketProperty(Optional.empty())
                        .setSerdeParameters(serdes)
                        .build(),
                columns,
                partitionColumns,
                Collections.EMPTY_MAP,
                Optional.empty(),
                Optional.empty()));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public synchronized PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        return null;
    }

    @Override
    public synchronized Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames)
    {
        ImmutableMap.Builder<String, PartitionStatistics> statistics = ImmutableMap.builder();
        return statistics.build();
    }

    private Table getRequiredTable(String databaseName, String tableName)
    {
        return getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    private void verifyTableNotExists(String newDatabaseName, String newTableName)
    {
        if (getTable(newDatabaseName, newTableName).isPresent()) {
            throw new TableAlreadyExistsException(new SchemaTableName(newDatabaseName, newTableName));
        }
    }

    @Override
    public synchronized void updateTableStatistics(String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
    }

    @Override
    public synchronized void updatePartitionStatistics(String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
    }

    @Override
    public synchronized Optional<List<String>> getAllTables(String databaseName)
    {
        requireNonNull(databaseName, "databaseName is null");

        Optional<Database> database = getDatabase(databaseName);
        if (!database.isPresent()) {
            return Optional.empty();
        }

        Path databaseMetadataDirectory = getDatabaseMetadataDirectory(databaseName);
        List<String> tables = getChildSchemaDirectories(databaseMetadataDirectory).stream()
                .map(Path::getName)
                .collect(toList());
        return Optional.of(ImmutableList.copyOf(tables));
    }

    @Override
    public synchronized Optional<List<String>> getAllViews(String databaseName)
    {
        return Optional.empty();
    }

    @Override
    public synchronized void dropTable(String databaseName, String tableName, boolean deleteData)
    {
    }

    @Override
    public synchronized void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
    }

    @Override
    public synchronized void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
    }

    public synchronized void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
    }

    @Override
    public synchronized void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
    }

    @Override
    public synchronized void dropColumn(String databaseName, String tableName, String columnName)
    {
    }

    @Override
    public synchronized void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
    }

    @Override
    public synchronized void dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
    }

    @Override
    public synchronized void alterPartition(String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Optional<Table> tableReference = getTable(databaseName, tableName);
        if (!tableReference.isPresent()) {
            return Optional.empty();
        }
        Table table = tableReference.get();

        Path tableMetadataDirectory = getTableMetadataDirectory(table);

        List<ArrayDeque<String>> partitions = listPartitions(tableMetadataDirectory, table.getPartitionColumns());

        List<String> partitionNames = partitions.stream()
                .map(partitionValues -> makePartName(table.getPartitionColumns(), ImmutableList.copyOf(partitionValues)))
                .collect(toList());

        return Optional.of(ImmutableList.copyOf(partitionNames));
    }

    private List<ArrayDeque<String>> listPartitions(Path director, List<Column> partitionColumns)
    {
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of();
        }

        try {
            String directoryPrefix = partitionColumns.get(0).getName() + '=';

            List<ArrayDeque<String>> partitionValues = new ArrayList<>();
            for (FileStatus fileStatus : metadataFileSystem.listStatus(director)) {
                if (!fileStatus.isDirectory()) {
                    continue;
                }
                if (!fileStatus.getPath().getName().startsWith(directoryPrefix)) {
                    continue;
                }

                List<ArrayDeque<String>> childPartitionValues;
                if (partitionColumns.size() == 1) {
                    childPartitionValues = ImmutableList.of(new ArrayDeque<>());
                }
                else {
                    childPartitionValues = listPartitions(fileStatus.getPath(), partitionColumns.subList(1, partitionColumns.size()));
                }

                String value = unescapePathName(fileStatus.getPath().getName().substring(directoryPrefix.length()));
                for (ArrayDeque<String> childPartition : childPartitionValues) {
                    childPartition.addFirst(value);
                    partitionValues.add(childPartition);
                }
            }
            return partitionValues;
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Error listing partition directories", e);
        }
    }

    @Override
    public synchronized Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitionValues, "partitionValues is null");

        Optional<Table> tableReference = getTable(databaseName, tableName);
        if (!tableReference.isPresent()) {
            return Optional.empty();
        }
        Table table = tableReference.get();

        Path partitionDirectory = getPartitionMetadataDirectory(table, partitionValues);
        List<Column> columns = null;
        if (tableName.contains(RAWDATA)) {
            columns = columnsMap.get(RAWDATA);
        }
        else if (tableName.contains(OUTPUTDATA)) {
            columns = columnsMap.get(OUTPUTDATA);
        }
        else if (tableName.contains(BATCHDATA)) {
            columns = columnsMap.get(BATCHDATA);
        }
        else if (tableName.contains(CONFIDENCEDATA)) {
            columns = columnsMap.get(CONFIDENCEDATA);
        }
        else if (tableName.contains(EXPLANATIONDATA)) {
            columns = columnsMap.get(EXPLANATIONDATA);
        }
        else if (tableName.contains(EPISODEDATA)) {
            columns = columnsMap.get(EPISODEDATA);
        }
        return Optional.of(new Partition(
                databaseName,
                tableName,
                partitionValues,
                Storage.builder()
                        .setLocation(partitionDirectory.toString())
                        .setStorageFormat(StorageFormat.fromHiveStorageFormat(HiveStorageFormat.PARQUET))
                        .setBucketProperty(Optional.empty())
                        .setSerdeParameters(serdes)
                        .build(),
                columns,
                Collections.EMPTY_MAP));
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        // todo this should be more efficient by selectively walking the directory tree
        return getPartitionNames(databaseName, tableName).map(partitionNames -> partitionNames.stream()
                .filter(partitionName -> partitionMatches(partitionName, parts))
                .collect(toList()));
    }

    private static boolean partitionMatches(String partitionName, List<String> parts)
    {
        List<String> values = toPartitionValues(partitionName);
        if (values.size() != parts.size()) {
            return false;
        }
        for (int i = 0; i < values.size(); i++) {
            String part = parts.get(i);
            if (!part.isEmpty() && !values.get(i).equals(part)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        ImmutableMap.Builder<String, Optional<Partition>> builder = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            List<String> partitionValues = toPartitionValues(partitionName);
            builder.put(partitionName, getPartition(databaseName, tableName, partitionValues));
        }
        return builder.build();
    }

    @Override
    public synchronized Set<String> getRoles(String user)
    {
        return ImmutableSet.<String>builder()
                .add(PUBLIC_ROLE_NAME)
                .add("admin")  // todo there should be a way to manage the admins list
                .build();
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName)
    {
        Set<HivePrivilegeInfo> privileges = new HashSet<>();
        if (isDatabaseOwner(user, databaseName)) {
            privileges.add(new HivePrivilegeInfo(OWNERSHIP, true));
        }
        return privileges;
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName)
    {
        return null;
    }

    @Override
    public synchronized void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
    {
    }

    @Override
    public synchronized void revokeTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
    {
    }

    private boolean isDatabaseOwner(String user, String databaseName)
    {
        // all users are "owners" of the default database
        if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(databaseName)) {
            return true;
        }

        Optional<Database> databaseMetadata = getDatabase(databaseName);
        if (!databaseMetadata.isPresent()) {
            return false;
        }

        Database database = databaseMetadata.get();

        // a database can be owned by a user or role
        if (database.getOwnerType() == USER && user.equals(database.getOwnerName())) {
            return true;
        }
        if (database.getOwnerType() == ROLE && getRoles(user).contains(database.getOwnerName())) {
            return true;
        }
        return false;
    }

    private Path getDatabaseMetadataDirectory(String databaseName)
    {
        return new Path(catalogDirectory, databaseName);
    }

    private Path getTableMetadataDirectory(Table table)
    {
        return getTableMetadataDirectory(table.getTableName());
    }

    private Path getTableMetadataDirectory(String tableName)
    {
        return new Path(catalogDirectory, tableName);
    }

    private Path getPartitionMetadataDirectory(Table table, List<String> values)
    {
        String partitionName = makePartName(table.getPartitionColumns(), values);
        return getPartitionMetadataDirectory(table, partitionName);
    }

    private Path getPartitionMetadataDirectory(Table table, String partitionName)
    {
        Path tableMetadataDirectory = getTableMetadataDirectory(table);
        return new Path(tableMetadataDirectory, partitionName);
    }

    private Path getPermissionsDirectory(Table table)
    {
        return new Path(getTableMetadataDirectory(table), PRESTO_PERMISSIONS_DIRECTORY_NAME);
    }

    private static Path getPermissionsPath(Path permissionsDirectory, String principalName, PrincipalType principalType)
    {
        return new Path(permissionsDirectory, principalType.name().toLowerCase(Locale.US) + "_" + principalName);
    }

    private List<Path> getChildSchemaDirectories(Path metadataDirectory)
    {
        try {
            if (!metadataFileSystem.isDirectory(metadataDirectory)) {
                return ImmutableList.of();
            }

            ImmutableList.Builder<Path> childSchemaDirectories = ImmutableList.builder();
            for (FileStatus child : metadataFileSystem.listStatus(metadataDirectory)) {
                if (!child.isDirectory()) {
                    continue;
                }
                Path childPath = child.getPath();
                if (childPath.getName().startsWith(".")) {
                    continue;
                }
                if (metadataFileSystem.isFile(new Path(childPath, PRESTO_SCHEMA_FILE_NAME))) {
                    childSchemaDirectories.add(childPath);
                }
            }
            return childSchemaDirectories.build();
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }
}
