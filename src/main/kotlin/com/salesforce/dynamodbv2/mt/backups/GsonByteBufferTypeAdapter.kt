/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.backups

import com.google.gson.*
import java.lang.reflect.Type
import java.nio.ByteBuffer
import java.util.*

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