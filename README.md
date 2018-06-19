# Multi-tenant AWS Dynamo SDK (mt-dynamodb)

## Description

Multi-tenant AWS Dynamo supports the [AWS Dynamo Java API](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/services/dynamodbv2/document/package-summary.html).
  
You can write your application code against the AmazonDynamoDB interface as you would for any other application.  The implementation will manage storage of data by tenant.  You can tell the implementation how you want the data stored when you create the AmazonDynamoDB using the provided builders.  This library provides 3 multi-tenant builders that allow you to create your AmazonDynamoDB client.  Using these builders you tell the implementation how you want to manage storage of tenant data.  The 3 basic implementations, are `MTAmazonDynamoDBByAccount`, `MTAmazonDynamoDBByTable`, and `SharedTable`.  These implementations allow you to separate your tenants data by AWS account, by table name, or store tenant data colocated in a set of predefined physical tables, respectively.  Builders may also be chained, allowing you to combine storage schemes.  In order for the implementation to manage multi-tenancy on your behalf, you provide it with an `MTAmazonDynamoDBContextProvider`.  The implementation calls back to the context provider whenever it needs to retrieve or store data.  Your implementation returns a string representing a tenant identifier of your choosing.

See details below on each of the 3 tenant storage schemes.  Further details are provided in the Javadoc for each implementation.

Note: Not all `AmazonDynamoDB` methods are currently supported.  See JavaDoc for details.

## Usage

### Multi-tenant Context

`MTAmazonDynamoDBContextProvider`

Each builder requires providing an `MTAmazonDynamoDBContextProvider` implementation.  This context implementation allows your application code to return a unique identifier for a tenant which is used by the implementation to include the tenant identifier in all read and write operations.

### Multi-tenant Builders

Each of the following implementations has a `builder()` method.  See the Javadoc for each class for details on usage.

#### `MTAmazonDynamoDBByAccount`

Allows for dividing tenants into different AWS accounts.  [byAccount](docs/byAccount) shows an example of how data looks in its persisted state.

To use, pass your `AmazonDynamoDBClientBuilder`, `MTAccountCredentialsMapper`, and `MTAmazonDynamoDBContextProvider` to the builder.  At runtime, a String representing the tenant identifier will be passed to your credentials mapper, allowing you to map the context to different AWS credentials implementations.  Note that MTAmazonDynamoDBByAccount does not support delegation and therefore must always be at the end of the chain when it is used.  See details about chaining below.

```
MTAmazonDynamoDBByAccount.builder().withAmazonDynamoDBClientBuilder(AmazonDynamoDBClientBuilder.standard())
                .withAccountCredentialsMapper(new MTAccountCredentialsMapper() {
                    @Override
                    public AWSCredentialsProvider getAWSCredentialsProvider(String tenantIdentifier) {
                        // return a AWSCredentialsProvider based on the tenant identifier
                    }
                })
                .withContext(contextProviderImpl);
```

See JavaDoc for `MTAmazonDynamoDBByAccount` for more details.  See `MTAmazonDynamoDBByAccountTest` for sample code.
 
#### `MTAmazonDynamoDBByTable`

Allows for dividing tenants into their own tables by prefixing table names with the tenant identifier.  [byTable](docs/byTable) shows an example of how data looks in its persisted state.

To use, pass your `AmazonDynamoDB` and `MTAmazonDynamoDBContextProvider` to the builder.  At runtime, the implementation will prefix all table names with the tenant identifier.

```
MTAmazonDynamoDBByTable.builder().withAmazonDynamoDB(AmazonDynamoDBClientBuilder.standard().build())
                .withContext(contextProviderImpl).build()
```

See JavaDoc for `MTAmazonDynamoDBByTable` for more details.  See `MTAmazonDynamoDBByTableTest` for sample code.

#### `SharedTable`

Allows for storing all tenant data in a set of shared tables, dividing tenants by prefixing the table's `HASH` key field with the tenant identifier.  [bySharedTable](docs/bySharedTable) shows an example of how data looks in its persisted state.

To use, pass your `AmazonDynamoDB` and `MTAmazonDynamoDBContextProvider` to the builder.  At runtime, the implementation will prefix the `HASH` key with the tenant identifier.  It will store table definitions in DynamoDB itself in a table called `_TABLEMETADATA`.  Data will be stored in tables starting with the name `mt_sharedtablestatic_`.

```
MTAmazonDynamoDBBySharedTableBuilders.SharedTable.builder()
                .withAmazonDynamoDB(AmazonDynamoDBClientBuilder.standard().build())
                .withContext(mtContext).build()
```

See JavaDoc for `MTAmazonDynamoDBBySharedTableBuilders.SharedTable` for more build-time configuration options and details.  See `MTAmazonDynamoDBBySharedTableTest` for sample code.

#### `SharedTableCustomDynamic` and `SharedTableCustomStatic`

For more flexibility, there are additional builders provided in `MTAmazonDynamoDBBySharedTableBuilders` that allow you to provide custom mappings between "virtual" tables, those accessed by the application code that is using the AWS Dynamo Java API to multi-tenant physical tables.  See Javadoc for `MTAmazonDynamoDBBySharedTableBuilders` for details.

## Table Prefixes

All builders support passing in a table prefix with a `withTablePrefix()` method.  This will provide naming separation between different applications using the `mt-dynamo` library against tables in the same AWS account.  It is recommended that you always provide a table prefix to prevent inadvertent co-mingling of data.  

## Chaining

Builders may also be chained, allowing you to combine storage schemes.  

For example, you may want to split your tenants across 2 AWS accounts.  Within those accounts, you will store your tenant data in tables prefixed by table name.  In that case, you would create an account builder and pass it to a table builder.  The table name would get prefixed with the tenant id and the request would be delegated to the appropriate AWS account based on the tenant id and the AWSCredentialProvider returned by your MTAccountCredentialsMapper.  

Below is a list of supported chaining sequences with a link to an example of how data looks in its persisted state for the configured chaining sequence.

 * [table -> account](docs/chains/byTableByAccount)
 * [sharedtable -> account](docs/chains/bySharedTableByAccount)
 * [table -> sharedtable](docs/chains/byTableBySharedTable)
 * [sharedtable -> table](docs/chains/bySharedTableByTable)
 * [table -> sharedtable -> account](docs/chains/byTableBySharedTableByAccount)
 * [sharedtable -> table -> account](docs/chains/bySharedTableByTableByAccount)

See `DocGeneratorRunner` for examples of how to configure builders for each of the chain sequences.

## Limitations

### Methods

 * All implementations support the following methods `createTable`, `describeTable`, `deleteTable`, `getItem`, `putItem`, `scan`, and `query`.
 * The following methods are NOT supported: `updateTable`, `batchGetItem`, `batchWriteItem`, `createBackup`, `deleteBackup`, `listBackups`, `restoreTableFromBackup`, `createGlobalTable`, `updateGlobalTable`, `describeGlobalTable`, `listGlobalTables`, `describeContinuousBackups`, `describeLimits`, `describeTimeToLive`, `updateTimeToLive`, `listTagsOfResource`, `tagResource`, `untagResource`, `getCachedResponseMetadata`, `waiters`
 * `ScanRequest` and `QueryRequest`'s currently only support EQ conditions.
 * All `SharedTable*` implementations ... 
   * Table Primary Keys: Currently, this implementation supports tables with a primary key containing only a `HASH` field of type `STRING`, or a table containing a `HASH` field and a `RANGE` field both of type `STRING`
   * `GSI`'s / `LSI`'s:  Currently, this implementation supports a single `GSI` with a key schema containing `HASH` key of type `STRING` or a single `LSI` with a key schema containing a `RANGE` key of type `STRING`.  When a query request contains a reference to an index, the `GSI` matching the types of the key schema of the referenced index will be used.  If none is found, then it will look for a matching LSI.
   * Drop Tables: When dropping a table, if you don't explicitly specify `truncateOnDeleteTable=true`, then table data will be left behind even after the table is dropped.  If a table with the same name is later recreated under the same tenant identifier, the data will be restored.  Note that undetermined behavior should be expected in the event that the original table schema is different from the new table schema.
   * Adding/removing `GSI`'s/`LSI`'s:  Adding or removing `GSI`'s or `LSI`'s on a table that contains data will cause queries and scans to yield unexpected results.
   * Projections in all `query` and `scan` requests default to `ProjectionType.ALL`.
   * See implementation-specific limitations in the Javadoc on each builder.
 
## References
1. [The Force.com Multitenant Architecture](https://developer.salesforce.com/page/Multi_Tenant_Architecture)
1. [Multi-Tenant Storage with Amazon DynamoDB](https://aws.amazon.com/blogs/apn/multi-tenant-storage-with-amazon-dynamodb/)
1. [SaaS Storage Strategies](https://d0.awsstatic.com/whitepapers/Multi_Tenant_SaaS_Storage_Strategies.pdf)

## Release Notes

- **v0.9.3** First revision
- **v0.9.6** Added `listStreams()` support
- **v0.9.7** Bug fixes
- **v0.9.8** Replaced `ByIndex` implementation with `SharedTable`

## Backlog

- Additional support for the remainder of the api for all 3 impls.
- Add support for updateTable with table definition invalidation and/or cache timeouts.
- Add support for choosing using `LSI` when there is a `GSI` with matching key schema data types.
