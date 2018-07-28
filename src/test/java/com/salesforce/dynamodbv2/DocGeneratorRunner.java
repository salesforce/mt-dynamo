/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2;

import static com.google.common.base.Preconditions.checkArgument;
import static com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccountTest.HOSTED_DYNAMO_ACCOUNT_MAPPER;
import static com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccountTest.LOCAL_DYNAMO_ACCOUNT_MAPPER;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccountTest;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByTable;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbLogger;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbTestRunner;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableCustomDynamicBuilder;
import dnl.utils.text.table.TextTable;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Dumps table contents for allowable permutation of implementation chains.
 *
 * <p>Supported permutations ...
 *
 * <p>account
 * table
 * sharedtable
 * table -> account
 * sharedtable -> account
 * table -> sharedtable
 * sharedtable -> table
 * table -> sharedtable -> account
 * sharedtable -> table -> account
 *
 * <p>MtAmazonDynamoDbByAccount does not support delegating to a mapper and therefore must always be at the end of the
 * chain when it is used.
 *
 * <p>There is also a logger mapper that is used purely to log all requests.  It may be added wherever chaining is
 * supported.  For these tests it is always at the lowest level available.  That is, it is always at the end of the
 * chain unless the account mapper is at the end of the chain in which case it is immediately before the account mapper
 * in the chain.
 *
 * <p>See Javadoc for each test for the chain sequence that each test implements.
 *
 * <p>Note that all tests that involve the account mapper depend on having set up local credentials profiles. See
 * {@link MtAmazonDynamoDbByAccountTest.TestAccountCredentialsMapper} for details.
 *
 * @author msgroi
 */
@Disabled
class DocGeneratorRunner {

    private static final boolean SKIP_ACCOUNT_TEST = false;
    private static final boolean IS_LOCAL_DYNAMO = true;
    private static final String DOCS_DIR = "docs";
    private static final String DOCS_CHAINS_DIR = "docs/chains";
    private static final AmazonDynamoDBClientBuilder AMAZON_DYNAMO_DB_CLIENT_BUILDER = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion(Regions.US_EAST_1);
    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = new MtAmazonDynamoDbContextProviderImpl();
    private static final AmazonDynamoDB LOCAL_AMAZON_DYNAMO_DB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

    /*
     * logger -> account
     */
    @Test
    // Suppresses "'lambda arguments' has incorrect indentation level" warning.
    @SuppressWarnings("checkstyle:Indentation")
    void byAccount() {
        if (SKIP_ACCOUNT_TEST) {
            return;
        }
        AmazonDynamoDB amazonDynamoDb =
                getLoggerBuilder().withAmazonDynamoDb(
                        getAccountBuilder()).build();
        new DocGenerator(
                "byAccount",
                DOCS_DIR + "/byAccount",
                MT_CONTEXT,
                () -> amazonDynamoDb,
                IS_LOCAL_DYNAMO,
                true,
                getAccounts()).runAll();
    }

    /*
     * table -> logger
     */
    // Suppresses "'lambda arguments' has incorrect indentation level" warning.
    @SuppressWarnings("checkstyle:Indentation")
    @Test
    void byTable() {
        AmazonDynamoDB physicalAmazonDynamoDb = getPhysicalAmazonDynamoDb(IS_LOCAL_DYNAMO);
        AmazonDynamoDB amazonDynamoDb =
                getTableBuilder().withAmazonDynamoDb(
                        getLoggerBuilder().withAmazonDynamoDb(physicalAmazonDynamoDb).build()).build();
        new DocGenerator(
                "byTable",
                DOCS_DIR + "/byTable",
                MT_CONTEXT,
                () -> amazonDynamoDb,
                IS_LOCAL_DYNAMO,
                false,
                ImmutableMap.of("na", physicalAmazonDynamoDb)).runAll();
    }

    /*
     * sharedtable -> logger
     */
    // Suppresses "'lambda arguments' has incorrect indentation level" warning.
    @SuppressWarnings("checkstyle:Indentation")
    @Test
    void bySharedTable() {
        AmazonDynamoDB physicalAmazonDynamoDb = getPhysicalAmazonDynamoDb(IS_LOCAL_DYNAMO);
        AmazonDynamoDB amazonDynamoDb =
                getBySharedTableBuilder().withAmazonDynamoDb(
                        getLoggerBuilder().withAmazonDynamoDb(physicalAmazonDynamoDb).build()).build();
        new DocGenerator(
                "bySharedTable",
                DOCS_DIR + "/bySharedTable",
                MT_CONTEXT,
                () -> amazonDynamoDb,
                IS_LOCAL_DYNAMO,
                false,
                ImmutableMap.of("na", physicalAmazonDynamoDb)).runAll();
    }

    /*
     * table -> logger -> account
     */
    // Suppresses "'lambda arguments' has incorrect indentation level" warning.
    @SuppressWarnings("checkstyle:Indentation")
    @Test
    void byTableByAccount() {
        if (SKIP_ACCOUNT_TEST) {
            return;
        }
        AmazonDynamoDB amazonDynamoDb =
                getTableBuilder().withAmazonDynamoDb(
                        getLoggerBuilder().withAmazonDynamoDb(
                                getAccountBuilder()).build()).build();
        new DocGenerator(
                "byTableByAccount",
                DOCS_CHAINS_DIR + "/byTableByAccount",
                MT_CONTEXT,
                () -> amazonDynamoDb,
                IS_LOCAL_DYNAMO,
                false,
                getAccounts()).runAll();
    }

    /*
     * sharedtable -> logger -> account
     */
    // Suppresses "'lambda arguments' has incorrect indentation level" warning.
    @SuppressWarnings("checkstyle:Indentation")
    @Test
    void bySharedTableByAccount() {
        if (SKIP_ACCOUNT_TEST) {
            return;
        }
        AmazonDynamoDB accountAmazonDynamoDb = getAccountBuilder();
        AmazonDynamoDB amazonDynamoDb =
                getBySharedTableBuilder().withAmazonDynamoDb(
                        getLoggerBuilder().withAmazonDynamoDb(
                                accountAmazonDynamoDb).build()).build();
        new DocGenerator(
                "bySharedTableByAccount",
                DOCS_CHAINS_DIR + "/bySharedTableByAccount",
                MT_CONTEXT,
                () -> amazonDynamoDb,
                IS_LOCAL_DYNAMO,
                false,
                getAccounts()).runAll();
    }

    /*
     * table -> sharedtable -> logger
     */
    // Suppresses "'lambda arguments' has incorrect indentation level" warning.
    @SuppressWarnings("checkstyle:Indentation")
    @Test
    void byTableBySharedTable() {
        AmazonDynamoDB physicalAmazonDynamoDb = getPhysicalAmazonDynamoDb(IS_LOCAL_DYNAMO);
        AmazonDynamoDB amazonDynamoDb =
                getTableBuilder().withAmazonDynamoDb(
                        getBySharedTableBuilder().withAmazonDynamoDb(
                                getLoggerBuilder().withAmazonDynamoDb(physicalAmazonDynamoDb).build()).build()).build();
        new DocGenerator(
                "byTableBySharedTable",
                DOCS_CHAINS_DIR + "/byTableBySharedTable",
                MT_CONTEXT,
                () -> amazonDynamoDb,
                IS_LOCAL_DYNAMO,
                false,
                ImmutableMap.of("na", physicalAmazonDynamoDb)).runAll();
    }

    /*
     * sharedtable -> table -> logger
     */
    // Suppresses "'lambda arguments' has incorrect indentation level" warning.
    @SuppressWarnings("checkstyle:Indentation")
    @Test
    void bySharedTableByTable() {
        AmazonDynamoDB physicalAmazonDynamoDb = getPhysicalAmazonDynamoDb(IS_LOCAL_DYNAMO);
        AmazonDynamoDB amazonDynamoDb =
                getBySharedTableBuilder().withAmazonDynamoDb(
                        getTableBuilder().withAmazonDynamoDb(
                                getLoggerBuilder().withAmazonDynamoDb(physicalAmazonDynamoDb).build()).build()).build();
        new DocGenerator(
                "bySharedTableByTable",
                DOCS_CHAINS_DIR + "/bySharedTableByTable",
                MT_CONTEXT,
                () -> amazonDynamoDb,
                IS_LOCAL_DYNAMO,
                false,
                ImmutableMap.of("na", physicalAmazonDynamoDb)).runAll();
    }

    /*
     * table -> sharedtable -> logger -> account
     */
    // Suppresses "'lambda arguments' has incorrect indentation level" warning.
    @SuppressWarnings("checkstyle:Indentation")
    @Test
    void byTableBySharedTableByAccount() {
        if (SKIP_ACCOUNT_TEST) {
            return;
        }
        AmazonDynamoDB accountAmazonDynamoDb = getAccountBuilder();
        AmazonDynamoDB amazonDynamoDb =
                getTableBuilder().withAmazonDynamoDb(
                        getBySharedTableBuilder().withAmazonDynamoDb(
                                getLoggerBuilder().withAmazonDynamoDb(
                                        accountAmazonDynamoDb).build()).build()).build();
        new DocGenerator(
                "byTableBySharedTableByAccount",
                DOCS_CHAINS_DIR + "/byTableBySharedTableByAccount",
                MT_CONTEXT,
                () -> amazonDynamoDb,
                IS_LOCAL_DYNAMO,
                false,
                getAccounts()).runAll();
    }

    /*
     * sharedtable -> table -> logger -> account
     */
    // Suppresses "'lambda arguments' has incorrect indentation level" warning.
    @SuppressWarnings("checkstyle:Indentation")
    @Test
    void bySharedTableByTableByAccount() {
        if (SKIP_ACCOUNT_TEST) {
            return;
        }
        AmazonDynamoDB table =
                getTableBuilder().withAmazonDynamoDb(
                        getLoggerBuilder().withAmazonDynamoDb(
                                getAccountBuilder()).build()).build();
        AmazonDynamoDB amazonDynamoDb =
                getBySharedTableBuilder().withAmazonDynamoDb(table).build();
        new DocGenerator(
                "bySharedTableByTableByAccount",
                DOCS_CHAINS_DIR + "/bySharedTableByTableByAccount",
                MT_CONTEXT,
                () -> amazonDynamoDb,
                IS_LOCAL_DYNAMO,
                false,
                getAccounts()).runAll();
    }

    public class DocGenerator extends MtAmazonDynamoDbTestRunner {

        private final Map<String, List<String>> targetColumnOrderMap = ImmutableMap.<String, List<String>>builder()
                .put("_tablemetadata", ImmutableList.of("table", "data"))
                .put("table1", ImmutableList.of("hashKeyField", "someField"))
                .put("table2", ImmutableList.of("hashKeyField", "someField"))
                .put("mt_sharedtablestatic_s_nolsi", ImmutableList.of("hk", "someField")).build();

        private final String test;
        private final Path outputFile;
        private String tableName1;
        private String tableName2;
        private List<Map<String, String>> ctxTablePairs;
        private final boolean manuallyPrefixTablenames;
        private final Map<String, AmazonDynamoDB> targetAmazonDynamoDbs;

        DocGenerator(String test,
                     String outputFilePath,
                     MtAmazonDynamoDbContextProvider mtContext,
                     Supplier<AmazonDynamoDB> amazonDynamoDbSupplier,
                     boolean isLocalDynamo,
                     boolean prefixTablenames,
                     Map<String, AmazonDynamoDB> targetAmazonDynamoDbs) {
            super(mtContext,
                    amazonDynamoDbSupplier.get(),
                    getPhysicalAmazonDynamoDb(isLocalDynamo),
                    null,
                    isLocalDynamo);
            this.test = test;
            this.outputFile = getOutputFile(outputFilePath);
            this.manuallyPrefixTablenames = prefixTablenames;
            this.targetAmazonDynamoDbs = targetAmazonDynamoDbs;
        }

        void runAll() {
            setup();
            run();
            teardown();
        }

        void setup() {
            tableName1 = buildTableName("table", 1);
            tableName2 = buildTableName("table", 2);
            ctxTablePairs = ImmutableList.of(
                    ImmutableMap.of("ctx1", tableName1),
                    ImmutableMap.of("ctx1", tableName2),
                    ImmutableMap.of("ctx2", tableName1));
            ctxTablePairs.forEach(ctxTablePair -> {
                Entry<String, String> ctxTablePairEntry = ctxTablePair.entrySet().iterator().next();
                recreateTable(ctxTablePairEntry.getKey(), ctxTablePairEntry.getValue());
            });
        }

        void run() {
            // create tables in different contexts
            createTable("ctx1", tableName1);
            createTable("ctx1", tableName2);
            createTable("ctx2", tableName1);

            // insert records into each table
            populateTable("ctx1", tableName1);
            populateTable("ctx1", tableName2);
            populateTable("ctx2", tableName1);

            // dump table contents
            appendToFile("TEST: " + test + "\n\n");
            targetAmazonDynamoDbs.forEach((String key, AmazonDynamoDB value) -> {
                if (targetAmazonDynamoDbs.size() > 1) {
                    appendToFile("account: " + key + "\n\n");
                }
                value.listTables().getTableNames().forEach(tableName -> dumpTablePretty(value, tableName));
            });
        }

        void teardown() {
            deleteTables(ctxTablePairs);
            targetAmazonDynamoDbs.forEach((s, amazonDynamoDb) -> amazonDynamoDb
                    .listTables()
                    .getTableNames()
                    .forEach(tableName -> {
                        if (tableName.startsWith(DocGeneratorRunner.getTablePrefix(true))) {
                            new TestAmazonDynamoDbAdminUtils(amazonDynamoDb)
                                    .deleteTableIfExists(tableName, getPollInterval(), timeoutSeconds);
                        }
                    }));
        }

        void deleteTables(List<Map<String, String>> ctxPairs) {
            ctxPairs.forEach(ctxTablePair -> {
                Entry<String, String> ctxTablePairEntry = ctxTablePair.entrySet().iterator().next();
                deleteTable(ctxTablePairEntry.getKey(), ctxTablePairEntry.getValue());
            });
        }

        void populateTable(String tenantId, String tableName) {
            MT_CONTEXT.setContext(tenantId);
            amazonDynamoDbSupplier.get().putItem(new PutItemRequest()
                    .withTableName(tableName)
                    .withItem(createItem("1")));
            amazonDynamoDbSupplier.get().putItem(new PutItemRequest()
                    .withTableName(tableName)
                    .withItem(createItem("2")));
        }

        void dumpTablePretty(AmazonDynamoDB amazonDynamoDb, String tableName) {
            List<String> columnNames = new ArrayList<>();
            List<Object[]> rows = new ArrayList<>();
            if (tableName.startsWith(DocGeneratorRunner.getTablePrefix(true))) {
                List<Map<String, AttributeValue>> items = amazonDynamoDb.scan(new ScanRequest()
                        .withTableName(tableName))
                        .getItems();
                appendToFile(new String(new char[5]).replace('\0', ' ') + tableName + "\n");
                if (!items.isEmpty()) {
                    items.forEach(item -> {
                        if (columnNames.isEmpty()) {
                            columnNames.addAll(item.keySet());
                        }
                        rows.add(item.values().stream().map(AttributeValue::getS).toArray(Object[]::new));
                    });
                    // sort rows and columns
                    List<String> targetColumns = getTargetColumnOrder(tableName);
                    sortColumns(columnNames, targetColumns, rows);
                    sortRows(rows);
                    // print
                    printToTable(targetColumns, rows);
                }
            }
        }

        private List<String> getTargetColumnOrder(String qualifiedTablename) {
            int dotPos = qualifiedTablename.indexOf(".");
            String unqualifiedTableName = dotPos == -1 ? qualifiedTablename : qualifiedTablename.substring(dotPos + 1);
            List<String> targetColumnOrder = targetColumnOrderMap.get(unqualifiedTableName);
            checkArgument(targetColumnOrder != null && !targetColumnOrder.isEmpty(),
                    "no column ordering found for " + unqualifiedTableName);
            return targetColumnOrder;
        }

        private void sortColumns(List<String> currentColumns, List<String> targetColumns, List<Object[]> rows) {
            // build a list of indices that represent the properly ordered current columns
            List<Integer> indices = targetColumns.stream().map(targetColumn -> {
                for (int i = 0; i < currentColumns.size(); i++) {
                    if (currentColumns.get(i).equals(targetColumn)) {
                        return i;
                    }
                }
                throw new RuntimeException("targetColumn="
                        + targetColumn
                        + " not found in currentColumns="
                        + currentColumns);
            }).collect(Collectors.toList());
            // build a list of rows that contain properly ordered column data
            List<Object[]> rowsWithSortedColumns = rows.stream()
                    .map(row -> indices.stream().map(index -> row[index]).collect(Collectors.toList()).toArray())
                    .collect(Collectors.toList());
            // clear the original row list
            rows.clear();
            // add the properly order column data to the original row list
            rows.addAll(rowsWithSortedColumns);
        }

        private void sortRows(List<Object[]> row) {
            row.sort(Comparator.comparing(row2 -> Joiner.on("").join(row2)));
        }

        void printToTable(List<String> columnNames, List<Object[]> data) {
            String[] columnNamesArr = columnNames.toArray(new String[0]);
            Object[][] dataArr = data.toArray(new Object[0][0]);
            TextTable tt = new TextTable(columnNamesArr, dataArr);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (PrintStream ps = new PrintStream(baos, true, "UTF-8")) {
                tt.printTable(ps, 5);
                appendToFile(new String(baos.toByteArray()));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            appendToFile("\n");
        }

        private Map<String, AttributeValue> createItem(String value) {
            return createItem(hashKeyField, value, "someField", "value-" + value);
        }

        private String buildTableName(String table, int ordinal) {
            return buildTableName(table + ordinal);
        }

        private String buildTableName(String table) {
            return getTablePrefix() + table;
        }

        private Path getOutputFile(String outputFilePath) {
            new File(outputFilePath).getParentFile().mkdirs();
            Path outputFile = Paths.get(outputFilePath);
            try {
                Files.createDirectories(Paths.get(DOCS_DIR));
                Files.deleteIfExists(outputFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return outputFile;
        }

        private void appendToFile(String message) {
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(outputFile, CREATE, APPEND))) {
                out.write(message.getBytes(), 0, message.length());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private String getTablePrefix() {
            return DocGeneratorRunner.getTablePrefix(manuallyPrefixTablenames);
        }

    }

    private AmazonDynamoDB getAccountBuilder() {
        if (IS_LOCAL_DYNAMO) {
            return MtAmazonDynamoDbByAccount.accountMapperBuilder()
                    .withAccountMapper(LOCAL_DYNAMO_ACCOUNT_MAPPER)
                    .withContext(MT_CONTEXT).build();
        } else {
            return MtAmazonDynamoDbByAccount.builder().withAmazonDynamoDbClientBuilder(AMAZON_DYNAMO_DB_CLIENT_BUILDER)
                    .withAccountCredentialsMapper(HOSTED_DYNAMO_ACCOUNT_MAPPER)
                    .withContext(MT_CONTEXT).build();
        }
    }

    private Map<String, AmazonDynamoDB> getAccounts() {
        return (IS_LOCAL_DYNAMO ? LOCAL_DYNAMO_ACCOUNT_MAPPER : HOSTED_DYNAMO_ACCOUNT_MAPPER).get();
    }

    private MtAmazonDynamoDbByTable.MtAmazonDynamoDbBuilder getTableBuilder() {
        return MtAmazonDynamoDbByTable.builder().withTablePrefix(getTablePrefix(true)).withContext(MT_CONTEXT);
    }

    private MtAmazonDynamoDbLogger.MtAmazonDynamoDbBuilder getLoggerBuilder() {
        return MtAmazonDynamoDbLogger.builder()
                .withContext(MT_CONTEXT)
                .withMethodsToLog(ImmutableList.of("createTable",
                        "deleteItem",
                        "deleteTable",
                        "describeTable",
                        "getItem",
                        "putItem",
                        "query",
                        "scan",
                        "updateItem"));
    }

    private SharedTableCustomDynamicBuilder getBySharedTableBuilder() {
        return SharedTableBuilder.builder()
                .withPrecreateTables(false)
                .withContext(MT_CONTEXT)
                .withTruncateOnDeleteTable(true);
    }

    private static String getTablePrefix(boolean prefixTablenames) {
        if (IS_LOCAL_DYNAMO) {
            return "";
        }
        return prefixTablenames ? "oktodelete-" + TestAmazonDynamoDbAdminUtils.getLocalHost() + "." : "";
    }

    private AmazonDynamoDB getPhysicalAmazonDynamoDb(boolean isLocalDynamo) {
        return isLocalDynamo ? LOCAL_AMAZON_DYNAMO_DB : AMAZON_DYNAMO_DB_CLIENT_BUILDER.build();
    }

}