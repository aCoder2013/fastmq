package com.song.fastmq.storage.common.utils

import org.junit.Assert.*
import org.junit.Test

/**
 * @author song
 */
class JsonUtilsTest {

    @Test
    @Throws(Exception::class)
    fun get() {
        assertNotNull(JsonUtils.get())
    }

    @Test
    @Throws(Exception::class)
    fun toJson() {
        val obj = "Hello World"
        assertEquals("\"Hello World\"", JsonUtils.toJson(obj))
    }

    @Test
    @Throws(Exception::class)
    fun fromJson() {
        assertEquals("Hello World", JsonUtils.fromJson("\"Hello World\"", String::class.java))
    }

    @Test
    @Throws(Exception::class)
    fun fromInvalidJson() {
        try {
            JsonUtils.fromJson("Hello World", String::class.java)
            fail("Should throw a exception")
        } catch (e: JsonUtils.JsonException) {
        }
    }

    @Test
    fun fromJsonQuietly() {
        assertEquals(null, JsonUtils.fromJsonQuietly("Hello World", String::class.java))
    }
}