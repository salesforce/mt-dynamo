/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.mappers

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.Identity
import com.amazonaws.services.dynamodbv2.model.OperationType
import com.amazonaws.services.dynamodbv2.model.Record
import com.amazonaws.services.dynamodbv2.model.StreamRecord

/**
 * This interface (including all contained interfaces and methods) is
 * experimental. It is subject to breaking changes. Use at your own risk.
 */
interface MtAmazonDynamoDb : AmazonDynamoDB {
    class MtRecord : Record() {

        var context: String? = null
            private set
        var tableName: String? = null
            private set

        fun withContext(context: String): MtRecord {
            this.context = context
            return this
        }

        fun withTableName(tableName: String): MtRecord {
            this.tableName = tableName
            return this
        }

        override fun withAwsRegion(awsRegion: String): MtRecord {
            setAwsRegion(awsRegion)
            return this
        }

        override fun withDynamodb(dynamodb: StreamRecord): MtRecord {
            setDynamodb(dynamodb)
            return this
        }

        override fun withEventID(eventId: String): MtRecord {
            eventID = eventId
            return this
        }

        override fun withEventName(eventName: OperationType): MtRecord {
            setEventName(eventName)
            return this
        }

        override fun withEventName(eventName: String): MtRecord {
            setEventName(eventName)
            return this
        }

        override fun withEventSource(eventSource: String): MtRecord {
            setEventSource(eventSource)
            return this
        }

        override fun withEventVersion(eventVersion: String): MtRecord {
            setEventVersion(eventVersion)
            return this
        }

        override fun withUserIdentity(userIdentity: Identity): MtRecord {
            setUserIdentity(userIdentity)
            return this
        }

        override fun toString(): String {
            return ("MtRecord{" +
                    "context='" + context + '\''.toString() +
                    ", tableName='" + tableName + '\''.toString() +
                    ", recordFields=" + super.toString() +
                    '}'.toString())
        }

        companion object {

            private val serialVersionUID = -6099434068333437314L
        }
    }

    data class TenantTable(val virtualTableName: String, val tenantName: String)
}
