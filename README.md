# Status: Archived

This project is no longer actively maintained.  There's no plan to address issues, provide ongoing support, or updates.  We are leaving the repo here for reference.

# Multitenant AWS Dynamo SDK (mt-dynamodb)

## Description

Multitenant AWS Dynamo supports the [AWS Dynamo Java API](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/services/dynamodbv2/document/package-summary.html).
  
You can write your application code against the Amazon DynamoDB interface as you would for any other application.  The implementation will manage storage of data by tenant.  You can tell the implementation how you want the data stored when you create the AmazonDynamoDB using the provided builders.  This library provides 3 multitenant builders that allow you to create your AmazonDynamoDB client.  Using these builders, you tell the implementation how you want to manage storage of tenant data.  The 3 basic implementations are `MtAmazonDynamoDbByAccount`, `MtAmazonDynamoDbByTable`, and `SharedTable`.  These implementations allow you to separate your tenants' data by AWS account, to separate your tenants' data by table name, or to store tenant data colocated in a set of predefined physical tables, respectively.  Builders may also be chained, allowing you to combine storage schemes.  In order for the implementation to manage multitenancy on your behalf, you provide it with an `MtAmazonDynamoDbContextProvider`.  The implementation calls back to the context provider whenever it needs to retrieve or store data.  Your implementation returns a string representing a tenant identifier of your choosing.

See details below on each of the 3 tenant storage schemes.  Further details are provided in the Javadoc for each implementation.

Note: Not all `AmazonDynamoDB` methods are currently supported.  See Javadoc for details.

## Usage
Importing
```xml
        <dependency>
            <groupId>com.salesforce.dynamodb</groupId>
            <artifactId>mt-dynamodb</artifactId>
            <version>${mt-dynamodb-version}</version>
        </dependency>
```
### Multitenant Context

`MtAmazonDynamoDbContextProvider`

Each builder requires providing an `MtAmazonDynamoDbContextProvider` implementation.  This context implementation allows your application code to return a unique identifier for a tenant which is used by the implementation to include the tenant identifier in all read and write operations.

### Multitenant Builders

Each of the following implementations has a `builder()` method.  See the Javadoc for each class for details on usage.

#### `MtAmazonDynamoDbByAccount`

Allows for dividing tenants into different AWS accounts.  [byAccount](docs/byAccount) shows an example of how data looks in its persisted state.

To use, pass your `AmazonDynamoDBClientBuilder`, `MtAccountCredentialsMapper`, and `MtAmazonDynamoDbContextProvider` to the builder.  At runtime, a String representing the tenant identifier will be passed to your credentials mapper, allowing you to map the context to different AWS credentials implementations.  Note that `MtAmazonDynamoDbByAccount` does not support delegation and therefore must always be at the end of the chain when it is used.  See details about chaining below.

```java
MtAmazonDynamoDbByAccount.builder()
    .withAmazonDynamoDbClientBuilder(AmazonDynamoDBClientBuilder.standard())
    .withAccountCredentialsMapper(new MtAccountCredentialsMapper() {
        @Override
        public AWSCredentialsProvider getAwsCredentialsProvider(String tenantIdentifier) {
            // return an AWSCredentialsProvider based on the tenant identifier
        }
    })
    .withContext(contextProviderImpl)
    .build();
```

See Javadoc for `MtAmazonDynamoDbByAccount` for more details.  See `MtAmazonDynamoDbByAccountTest` for code examples.
 
#### `MtAmazonDynamoDbByTable`

Allows for dividing tenants into their own tables by prefixing table names with the tenant identifier.  [byTable](docs/byTable) shows an example of how data looks in its persisted state.

To use, pass your `AmazonDynamoDB` and `MtAmazonDynamoDbContextProvider` to the builder.  At runtime, the implementation will prefix all table names with the tenant identifier.

```java
MtAmazonDynamoDbByTable.builder()
    .withAmazonDynamoDb(AmazonDynamoDBClientBuilder.standard().build())
    .withContext(contextProviderImpl)
    .build();
```

See Javadoc for `MtAmazonDynamoDbByTable` for more details.  See `MtAmazonDynamoDbByTableTest` for code examples.

#### `SharedTable`

Allows for storing all tenant data in a set of shared tables, dividing tenants by prefixing the table's `HASH` key field with the tenant identifier.  [bySharedTable](docs/bySharedTable) shows an example of how data looks in its persisted state.

To use, pass your `AmazonDynamoDB` and `MtAmazonDynamoDbContextProvider` to the builder.  At runtime, the implementation will prefix the `HASH` key with the tenant identifier.  It will store table definitions in DynamoDB itself in a table called `_TABLE_METADATA`.  Data will be stored in tables starting with the name `mt_shared_table_static_`.

```java
SharedTableBuilder.builder()
    .withAmazonDynamoDb(AmazonDynamoDBClientBuilder.standard().build())
    .withContext(mtContext)
    .build();
```

See Javadoc for `MtAmazonDynamoDbBySharedTableBuilders.SharedTable` for more build-time configuration options and details.  See `MtAmazonDynamoDbBySharedTableTest` for code examples.

## Table Prefixes

All builders support passing in a table prefix with a `withTablePrefix()` method.  This will provide naming separation between different applications using the `mt-dynamo` library against tables in the same AWS account.  It is recommended that you always provide a table prefix to prevent inadvertent commingling of data.

## Chaining

Builders may also be chained, allowing you to combine storage schemes.  

For example, you may want to split your tenants across 2 AWS accounts.  Within those accounts, you will store your tenant data in tables prefixed by table name.  In that case, you would create an account builder and pass it to a table builder.  The table name would get prefixed with the tenant ID and the request would be delegated to the appropriate AWS account based on the tenant ID and the `AWSCredentialProvider` returned by your `MtAccountCredentialsMapper`.

Below is a list of supported chaining sequences, each with a link to an example of how data looks in its persisted state for the configured chaining sequence.

 * [table &rarr; account](docs/chains/byTableByAccount)
 * [shared_table &rarr; account](docs/chains/bySharedTableByAccount)
 * [table &rarr; shared_table](docs/chains/byTableBySharedTable)
 * [shared_table &rarr; table](docs/chains/bySharedTableByTable)
 * [table &rarr; shared_table &rarr; account](docs/chains/byTableBySharedTableByAccount)
 * [shared_table &rarr; table &rarr; account](docs/chains/bySharedTableByTableByAccount)

See `DocGeneratorRunner` for examples of how to configure builders for each of the chain sequences.

## Limitations

### Methods

 * All implementations support the following methods `createTable`, `describeTable`, `deleteTable`, `getItem`, `batchGetItem`, `putItem`, `scan`, and `query`.
 * The following methods are NOT supported: `updateTable`, `batchWriteItem`, `createBackup`, `deleteBackup`, `listBackups`, `restoreTableFromBackup`, `createGlobalTable`, `updateGlobalTable`, `describeGlobalTable`, `listGlobalTables`, `describeContinuousBackups`, `describeLimits`, `describeTimeToLive`, `updateTimeToLive`, `listTagsOfResource`, `tagResource`, `untagResource`, `getCachedResponseMetadata`, `waiters`.
 * For `SharedTable`-specific limitations, see `SharedTableBuilder`.
 
## References
1. [The Force.com Multitenant Architecture](https://developer.salesforce.com/page/Multi_Tenant_Architecture)
1. [Multi-Tenant Storage with Amazon DynamoDB](https://aws.amazon.com/blogs/apn/multi-tenant-storage-with-amazon-dynamodb/)
1. [SaaS Storage Strategies](https://d0.awsstatic.com/whitepapers/Multi_Tenant_SaaS_Storage_Strategies.pdf)

## Release Notes

See [CHANGELOG.md](CHANGELOG.md)
