# Changelog
All notable changes to this project will be documented in this file.

Multitenant AWS Dynamo supports the [AWS Dynamo Java API](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/services/dynamodbv2/document/package-summary.html).
  
You can write your application code against the Amazon DynamoDB interface as you would for any other application.  The implementation will manage storage of data by tenant.

## 0.9.14 (unreleased)

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