package com.salesforce.dynamodbv2;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MTAmazonDynamoDBContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByAccount;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByIndex;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByTable;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBLogger;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBTestRunner;
import com.salesforce.dynamodbv2.mt.repo.MTDynamoDBTableDescriptionRepo;
import dnl.utils.text.table.TextTable;
import org.junit.jupiter.api.Test;

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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByAccountTest.HOSTED_DYNAMO_ACCOUNT_MAPPER;
import static com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByAccountTest.LOCAL_DYNAMO_ACCOUNT_MAPPER;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/**
 * Dumps table contents for allowable permutation of implementation chains.
 *
 * Supported permutations ...
 *
 * account
 * table
 * index
 * table -> account
 * index -> account
 * table -> index
 * index -> table
 * table -> index -> account
 * index -> table -> account
 *
 * MTAmazonDynamoDBByAccount does not support delegating to a mapper and therefore must always be at the end of the chain when it is used.
 *
 * There is also a logger mapper that is used purely to log all requests.  It may be added wherever chaining is supported.
 * For these tests it is always at the lowest level available.  That is, it is always at the end of the chain unless
 * the account mapper is at the end of the chain in which case it is immediately before the account mapper in the chain.
 *
 * See javadoc for each test for the chain sequence that each test implements.
 *
 * Note that all tests that involve the account mapper depend on having set up local credentials profiles.See TestAccountCredentialsMapper for details.
 *
 * @author msgroi
 */
class DocGeneratorRunner {

    private static final boolean skipAccountTest = false;
    private static final boolean isLocalDynamo = true;
    private static final String docsDir = "docs";
    private static final String docsChainsDir = "docs/chains";
    private static AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1);
    private MTAmazonDynamoDBContextProvider mtContext = new MTAmazonDynamoDBContextProviderImpl();
    private AmazonDynamoDB localAmazonDynamoDB = AmazonDynamoDBLocal.getAmazonDynamoDBLocal();

    /*
     * logger -> account
     */
    @Test
    void byAccount() {
        if (skipAccountTest) return;
        AmazonDynamoDB amazonDynamoDB =
                getLoggerBuilder().withAmazonDynamoDB(
                getAccountBuilder()).build();
        new DocGenerator(
                "byAccount",
                docsDir + "/byAccount",
                mtContext,
                () -> amazonDynamoDB,
                isLocalDynamo,
                true,
                getAccounts()).runAll();
    }

    /*
     * table -> logger
     */
    @Test
    void byTable() {
        AmazonDynamoDB physicalAmazonDynamoDB = getPhysicalAmazonDynamoDB(isLocalDynamo);
        AmazonDynamoDB amazonDynamoDB =
                getTableBuilder().withAmazonDynamoDB(
                getLoggerBuilder().withAmazonDynamoDB(physicalAmazonDynamoDB).build()).build();
        new DocGenerator(
                "byTable",
                docsDir + "/byTable",
                mtContext,
                () -> amazonDynamoDB,
                isLocalDynamo,
                false,
                ImmutableMap.of("na", physicalAmazonDynamoDB)).runAll();
    }

    /*
     * index -> logger
     */
    @Test
    void byIndex() {
        AmazonDynamoDB physicalAmazonDynamoDB = getPhysicalAmazonDynamoDB(isLocalDynamo);
        AmazonDynamoDB amazonDynamoDB =
                getIndexBuilder(physicalAmazonDynamoDB, getPollInterval()).withAmazonDynamoDB(
                getLoggerBuilder().withAmazonDynamoDB(physicalAmazonDynamoDB).build()).build();
        new DocGenerator(
                "byIndex",
                docsDir + "/byIndex",
                mtContext,
                () -> amazonDynamoDB,
                isLocalDynamo,
                false,
                ImmutableMap.of("na", physicalAmazonDynamoDB)).runAll();
    }

    /*
     * table -> logger -> account
     */
    @Test
    void byTableByAccount() {
        if (skipAccountTest) return;
        AmazonDynamoDB amazonDynamoDB =
                getTableBuilder().withAmazonDynamoDB(
                getLoggerBuilder().withAmazonDynamoDB(
                getAccountBuilder()).build()).build();
        new DocGenerator(
                "byTableByAccount",
                docsChainsDir + "/byTableByAccount",
                mtContext,
                () -> amazonDynamoDB,
                isLocalDynamo,
                false,
                getAccounts()).runAll();
    }

    /*
     * index -> logger -> account
     */
    @Test
    void byIndexByAccount() {
        if (skipAccountTest) return;
        AmazonDynamoDB accountAmazonDynamoDB = getAccountBuilder();
        AmazonDynamoDB amazonDynamoDB =
                getIndexBuilder(accountAmazonDynamoDB, 1).withAmazonDynamoDB(
                getLoggerBuilder().withAmazonDynamoDB(
                accountAmazonDynamoDB).build()).build();
        new DocGenerator(
                "byIndexByAccount",
                docsChainsDir + "/byIndexByAccount",
                mtContext,
                () -> amazonDynamoDB,
                isLocalDynamo,
                false,
                getAccounts()).runAll();
    }

    /*
     * table -> index -> logger
     */
    @Test
    void byTableByIndex() {
        AmazonDynamoDB physicalAmazonDynamoDB = getPhysicalAmazonDynamoDB(isLocalDynamo);
        AmazonDynamoDB amazonDynamoDB =
                getTableBuilder().withAmazonDynamoDB(
                getIndexBuilder(physicalAmazonDynamoDB, getPollInterval()).withAmazonDynamoDB(
                getLoggerBuilder().withAmazonDynamoDB(physicalAmazonDynamoDB).build()).build()).build();
        new DocGenerator(
                "byTableByIndex",
                docsChainsDir + "/byTableByIndex",
                mtContext,
                () -> amazonDynamoDB,
                isLocalDynamo,
                false,
                ImmutableMap.of("na", physicalAmazonDynamoDB)).runAll();
    }

    /*
     * index -> table -> logger
     */
    @Test
    void byIndexByTable() {
        AmazonDynamoDB physicalAmazonDynamoDB = getPhysicalAmazonDynamoDB(isLocalDynamo);
        AmazonDynamoDB amazonDynamoDB =
                getIndexBuilder(physicalAmazonDynamoDB, getPollInterval()).withAmazonDynamoDB(
                getTableBuilder().withAmazonDynamoDB(
                getLoggerBuilder().withAmazonDynamoDB(physicalAmazonDynamoDB).build()).build()).build();
        new DocGenerator(
                "byIndexByTable",
                docsChainsDir + "/byIndexByTable",
                mtContext,
                () -> amazonDynamoDB,
                isLocalDynamo,
                false,
                ImmutableMap.of("na", physicalAmazonDynamoDB)).runAll();
    }

    /*
     * table -> index -> logger -> account
     */
    @Test
    void byTableByIndexByAccount() {
        if (skipAccountTest) return;
        AmazonDynamoDB accountAmazonDynamoDB = getAccountBuilder();
        AmazonDynamoDB amazonDynamoDB =
                getTableBuilder().withAmazonDynamoDB(
                getIndexBuilder(accountAmazonDynamoDB, 1).withAmazonDynamoDB(
                getLoggerBuilder().withAmazonDynamoDB(
                accountAmazonDynamoDB).build()).build()).build();
        new DocGenerator(
                "byTableByIndexByAccount",
                docsChainsDir + "/byTableByIndexByAccount",
                mtContext,
                () -> amazonDynamoDB,
                isLocalDynamo,
                false,
                getAccounts()).runAll();
    }

    /*
     * index -> table -> logger -> account
     */
    @Test
    void byIndexByTableByAccount() {
        if (skipAccountTest) return;
        AmazonDynamoDB table =
                getTableBuilder().withAmazonDynamoDB(
                getLoggerBuilder().withAmazonDynamoDB(
                getAccountBuilder()).build()).build();
        AmazonDynamoDB amazonDynamoDB =
                getIndexBuilder(table, 1).withAmazonDynamoDB(table).build();
        new DocGenerator(
                "byIndexByTableByAccount",
                docsChainsDir + "/byIndexByTableByAccount",
                mtContext,
                () -> amazonDynamoDB,
                isLocalDynamo,
                false,
                getAccounts()).runAll();
    }

    public class DocGenerator extends MTAmazonDynamoDBTestRunner {

        private Map<String, List<String>> targetColumnOrder = ImmutableMap.of(
                "_TABLEMETADATA", ImmutableList.of("table", "data"),
                "_DATA_HK_S", ImmutableList.of("hk", "hashKeyField", "someField"),
                "table1", ImmutableList.of("hashKeyField", "someField"),
                "table2", ImmutableList.of("hashKeyField", "someField")
        );

        private String test;
        private Path outputFile;
        private String tableName1;
        private String tableName2;
        private List<Map<String, String>> ctxTablePairs;
        private boolean manuallyPrefixTablenames;
        private Map<String, AmazonDynamoDB> targetAmazonDynamoDBs;

        DocGenerator(String test,
                     String outputFilePath,
                     MTAmazonDynamoDBContextProvider mtContext,
                     Supplier<AmazonDynamoDB> amazonDynamoDBSupplier,
                     boolean isLocalDynamo,
                     boolean prefixTablenames,
                     Map<String, AmazonDynamoDB> targetAmazonDynamoDBs) {
            super(mtContext, amazonDynamoDBSupplier, isLocalDynamo);
            this.test = test;
            this.outputFile = getOutputFile(outputFilePath);
            this.manuallyPrefixTablenames = prefixTablenames;
            this.targetAmazonDynamoDBs = targetAmazonDynamoDBs;
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
                Map.Entry<String, String> ctxTablePairEntry = ctxTablePair.entrySet().iterator().next();
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
            targetAmazonDynamoDBs.forEach((String key, AmazonDynamoDB value) -> {
                if (targetAmazonDynamoDBs.size() > 1) {
                    appendToFile("account: " + key + "\n\n");
                }
                value.listTables().getTableNames().forEach(tableName -> dumpTablePretty(value, tableName));
            });
        }

        void teardown() {
            deleteTables(ctxTablePairs);
            targetAmazonDynamoDBs.forEach((s, amazonDynamoDB) -> amazonDynamoDB.listTables().getTableNames().forEach(tableName -> {
                if (tableName.startsWith(DocGeneratorRunner.getTablePrefix(true))) {
                    new TestAmazonDynamoDBAdminUtils(amazonDynamoDB).deleteTableIfNotExists(tableName, getPollInterval(), timeoutSeconds);
                }
            }));
        }

        void populateTable(String tenantId, String tableName) {
            mtContext.setContext(tenantId);
            amazonDynamoDBSupplier.get().putItem(new PutItemRequest().withTableName(tableName).withItem(createItem("1")));
            amazonDynamoDBSupplier.get().putItem(new PutItemRequest().withTableName(tableName).withItem(createItem("2")));
        }

        void dumpTablePretty(AmazonDynamoDB amazonDynamoDB, String tableName) {
            List<String> columnNames = new ArrayList<>();
            List<Object[]> rows = new ArrayList<>();
            if (tableName.startsWith(DocGeneratorRunner.getTablePrefix(true))) {
                List<Map<String, AttributeValue>> items = amazonDynamoDB.scan(new ScanRequest().withTableName(tableName)).getItems();
                appendToFile(new String(new char[5]).replace('\0', ' ') + tableName + "\n");
                if (!items.isEmpty()) {
                    items.forEach(item -> {
                        if (columnNames.isEmpty()) {
                            columnNames.addAll(item.keySet());
                        }
                        rows.add(item.values().stream().map(AttributeValue::getS).collect(Collectors.toList()).toArray(new Object[0]));
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
            return targetColumnOrder.get(unqualifiedTableName);
        }

        private void sortColumns(List<String> currentColumns, List<String> targetColumns, List<Object[]> rows) {
            // build a list of indices that represent the properly ordered current columns
            List<Integer> indices = targetColumns.stream().map(targetColumn -> {
                for (int i = 0; i < currentColumns.size(); i++) {
                    if (currentColumns.get(i).equals(targetColumn)) {
                        return i;
                    }
                }
                throw new RuntimeException("targetColumn=" + targetColumn + " not found in currentColumns=" + currentColumns);
            }).collect(Collectors.toList());
            // build a list of rows that contain properly ordered column data
            List<Object[]> rowsWithSortedColumns = rows.stream()
                    .map(row -> indices.stream()
                    .map(index -> row[index]).collect(Collectors.toList()).toArray()).collect(Collectors.toList());
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
            return super.createItem(hashKeyField, value, "someField", "value-" + value);
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
                Files.createDirectories(Paths.get(docsDir));
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
        if (isLocalDynamo) {
            return MTAmazonDynamoDBByAccount.accountMapperBuilder()
                    .withAccountMapper(LOCAL_DYNAMO_ACCOUNT_MAPPER)
                    .withContext(mtContext).build();
        } else {
            return MTAmazonDynamoDBByAccount.builder().withAmazonDynamoDBClientBuilder(amazonDynamoDBClientBuilder)
                    .withAccountCredentialsMapper(HOSTED_DYNAMO_ACCOUNT_MAPPER)
                    .withContext(mtContext).build();
        }
    }

    private Map<String, AmazonDynamoDB> getAccounts() {
        return (isLocalDynamo ? LOCAL_DYNAMO_ACCOUNT_MAPPER : HOSTED_DYNAMO_ACCOUNT_MAPPER).get();
    }

    private MTAmazonDynamoDBByTable.MTAmazonDynamoDBBuilder getTableBuilder() {
        return MTAmazonDynamoDBByTable.builder().withTablePrefix(getTablePrefix(true)).withContext(mtContext);
    }

    private MTAmazonDynamoDBLogger.MTAmazonDynamoDBBuilder getLoggerBuilder() {
        return MTAmazonDynamoDBLogger.builder()
                .withContext(mtContext)
                .withMethodsToLog(ImmutableList.of("createTable", "deleteItem", "deleteTable", "describeTable", "getItem",
                        "putItem", "query", "scan", "updateItem"));
    }

    private MTAmazonDynamoDBByIndex.MTAmazonDynamoDBByIndexBuilder getIndexBuilder(AmazonDynamoDB amazonDynamoDBTableRepo, int pollInterval) {
        return MTAmazonDynamoDBByIndex.builder()
                .withContext(mtContext)
                .withPollIntervalSeconds(pollInterval)
                .withTablePrefix(getTablePrefix(true))
                .withTableDescriptionRepo(new MTDynamoDBTableDescriptionRepo(getLoggerBuilder().withAmazonDynamoDB(amazonDynamoDBTableRepo).build(),
                        mtContext,
                        pollInterval,
                        getTablePrefix(true) + "_TABLEMETADATA"))
                .withTruncateOnDeleteTable(true);
    }

    private static String getTablePrefix(boolean prefixTablenames) {
        return isLocalDynamo ? "" : (prefixTablenames ? "oktodelete-" + TestAmazonDynamoDBAdminUtils.getLocalHost() + "." : "");
    }

    private AmazonDynamoDB getPhysicalAmazonDynamoDB(boolean isLocalDynamo) {
        return isLocalDynamo ? localAmazonDynamoDB : amazonDynamoDBClientBuilder.build();
    }

    protected int getPollInterval() {
        return isLocalDynamo ? 0 : 1;
    }

}