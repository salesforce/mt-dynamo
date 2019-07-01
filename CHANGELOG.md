# Changelog
All notable changes to this project will be documented in this file.

Multitenant AWS Dynamo supports the [AWS Dynamo Java API](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/services/dynamodbv2/document/package-summary.html).
  
You can write your application code against the Amazon DynamoDB interface as you would for any other application.  The implementation will manage storage of data by tenant.

## 0.9.41 (July 1, 2019)

* Performance improvements for stream caching.
* Parameterized raw ```Cache``` types.

## 0.9.40 (June 18, 2019)

* Exposed the ability to provide implementations of table-description and table-mapping caches to `SharedTableBuilder`s.

## 0.9.39 (May 24, 2019)

* Fixed bug [Batch get that exceeds provisioned throughput or size limit results in `table metadata entry for 'SHARED.mt_shared_table_static_s_no_lsi' does not exist in SHARED._table_metadata` error](https://github.com/salesforce/mt-dynamo/issues/386)

## 0.9.36 (May 23, 2019)

* Add support for mapping to table with purely binary types
* Make metadata-table description configurable
* Add context validation

## 0.9.34 (April 9, 2019)

* Fixed bug [querying secondary indexes may yield unexpected results if two tables in the same tenant have the same index name](https://github.com/salesforce/mt-dynamo/pull/342)
* Fixed bug [removed invalid constraint on the number of secondary indexes](https://github.com/salesforce/mt-dynamo/pull/340)
* Changed source and target compile settings to JDK11

## 0.9.33 (April 5, 2019)

* Fixed bug [getItem() fails with 'The number of conditions on the keys is invalid' in shared table strategy](https://github.com/salesforce/mt-dynamo/issues/333)

## 0.9.32 (March 27, 2019)
* Added support for Billing Mode for default metadata table

## 0.9.31 (March 25, 2019)
* Fixed bug in `CachingAmazonDynamoDbStreams` where an `IllegalArgumentException` was thrown with "Invalid position 
segment in shard iterator string" due to an invalid iterator format

## 0.9.30 (March 14, 2019)

* Added support for Billing Mode (Pay Per Request or default Provisioned)

## 0.9.29 (February 22, 2019)

* Add support for paginating query results

## 0.9.28 (February 6, 2019)

* `SharedTable` support extended for GE, LT, and LE (in addition to EQ and GT) queries on tables with numeric range-key fields (via `KeyConditions` only)

## 0.9.27 (December 10, 2018)

* Fixed bug in `CachingAmazonDynamoDbStreams` where `getRecords` for nonexistent shard was throwing an `UncheckedExecutionException` as opposed to a `ResourceNotFoundException`

## 0.9.26 (December 6, 2018)

* Renamed `MtAmazonDynamoDbContextProviderImpl` to `MtAmazonDynamoDbContextProviderThreadLocalImpl`
* Fixed bug in `SharedTable` where a `'.'` in a table name would cause hash keys to be misread

## 0.9.25 (November 28, 2018)

* Fixed issue by advancing trim-horizon iterator even if no records found
* Disallow *no* context (was `Optional.empty()`)&mdash;base context is now `""`; replace `MtAmazonDynamoDbContextProvider`'s one abstract method, `Optional<String> getContextOpt()`, with `String getContext()`

## 0.9.22 (November 14, 2018)

* Improved logging of table mappings in `SharedTable` implementation

## 0.9.21 (October 29, 2018)

* URL encode `context` and `tenantTableName` in multitenant prefixes

## 0.9.20 (October 19, 2018)

* Fixed `NullPointerException` in `SharedTable` when performing an update request on a GSI hash key attribute
* Fixed `UnsupportedOperationException` exception when performing an update request on a GSI hash key attribute
* Dropped support for `attributeUpdates` methods in `UpdateItemRequest` in `SharedTable`
* Streaming implementation improvements

## 0.9.19 (October 11, 2018)

* `CachingAmazonDynamoDbStreams` improvements

## 0.9.16 (September 26, 2018)

* `SharedTable` support for 'greater than' (GT) queries on tables with numeric range-key fields (via `KeyConditions` only)
* `SharedTable` support for conditional puts
* `SharedTable` fixed `TrimmedDataAccessException` for `TRIM_HORIZON` iterators

## 0.9.15 (September 24, 2018)

* Remove custom `listStreams` method and KCL dependency

## 0.9.14 (September 22, 2018)

* `ByTable` support for streams API*
* `SharedTable` support for latestStreamArn*

## 0.9.13 (September 20, 2018)

* `SharedTable` support for streams API 

## 0.9.11 (September 11, 2018) 

* `SharedTable` support for conditional updates and deletes
* Added `HybridSharedTableBuilder`

## 0.9.10 (August 16, 2018)

* Support for `batchGetItem`

## 0.9.8

* Replaced `ByIndex` implementation with `SharedTable`

## 0.9.7

* Bug fixes

## 0.9.6

* Added `listStreams()` support

## 0.9.3

* First revision
