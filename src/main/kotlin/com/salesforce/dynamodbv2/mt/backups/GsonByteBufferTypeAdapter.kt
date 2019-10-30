/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.backups

import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonParseException
import com.google.gson.JsonPrimitive
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer
import java.lang.reflect.Type
import java.nio.ByteBuffer
import java.util.Base64

/**
 * This class was (shamelessly) plagiarized from
 * <pre>https://github.com/oktolab/gson-utils/blob/master/src/main/java/br/com/oktolab/gson/adapter/
 * GsonByteBufferTypeAdapter.java</pre>. It gives GSON the ability to serialize and deserialize byte buffers, which
 * are used extensively by the object service to compact primary keys into binary format. In order to support taking
 * backups of said dynamo data, this class is used by the GSON utility to write and read said backups.
 */
class GsonByteBufferTypeAdapter : JsonDeserializer<ByteBuffer>, JsonSerializer<ByteBuffer> {

    override fun serialize(src: ByteBuffer, typeOfSrc: Type, context: JsonSerializationContext): JsonElement {
        return JsonPrimitive(Base64.getEncoder().encodeToString(src.array()))
    }

    @Throws(JsonParseException::class)
    override fun deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): ByteBuffer {
        val bytes = Base64.getDecoder().decode(json.asString)
        return ByteBuffer.wrap(bytes)
    }
}