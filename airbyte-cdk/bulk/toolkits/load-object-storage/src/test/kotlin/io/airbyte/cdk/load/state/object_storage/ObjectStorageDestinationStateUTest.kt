/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.state.object_storage

import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.Overwrite
import io.airbyte.cdk.load.file.object_storage.ObjectStorageClient
import io.airbyte.cdk.load.file.object_storage.ObjectStoragePathFactory
import io.airbyte.cdk.load.file.object_storage.PathMatcher
import io.airbyte.cdk.load.file.object_storage.RemoteObject
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.impl.annotations.MockK
import java.util.concurrent.ConcurrentLinkedQueue
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ObjectStorageDestinationStateUTest {
    data class MockObj(override val key: String, override val storageConfig: Unit = Unit) :
        RemoteObject<Unit>

    @MockK lateinit var stream: DestinationStream
    @MockK lateinit var client: ObjectStorageClient<*>
    @MockK lateinit var pathFactory: ObjectStoragePathFactory

    @BeforeEach
    fun setup() {
        every { stream.descriptor } returns DestinationStream.Descriptor("test", "stream")
        every { pathFactory.getPathMatcher(any(), any()) } answers
            {
                val suffix = secondArg<String>()
                PathMatcher(Regex("([a-z]+)$suffix"), mapOf("suffix" to 2))
            }
        every { pathFactory.getLongestStreamConstantPrefix(any(), any()) } returns ""
    }

    private suspend fun validateState() {
        val mockObjects =
            ConcurrentLinkedQueue(
                listOf(
                    MockObj("dog"),
                    MockObj("dog-1"),
                    MockObj("dog-3"),
                    MockObj("cat"),
                    MockObj("turtle-100")
                )
            )
        coEvery { client.list(any()) } answers
            {
                val prefix = firstArg<String>()
                mockObjects.asFlow().filter { it.key.startsWith(prefix) }
            }

        val persister = ObjectStorageFallbackPersister(client, pathFactory)
        val state = persister.load(stream)

        assertEquals("dog-4", state.ensureUnique("dog"))
        assertEquals("dog-5", state.ensureUnique("dog"))
        assertEquals("cat-1", state.ensureUnique("cat"))
        assertEquals("turtle-101", state.ensureUnique("turtle"))
        assertEquals("turtle-102", state.ensureUnique("turtle"))
        assertEquals("spider", state.ensureUnique("spider"))
    }

    @Test
    fun `test that fallback persister skips metadata and infers the unique key during append`() =
        runTest {
            // Skip metadata search
            every { stream.importType } returns Append
            every { stream.generationId } returns 1L
            every { stream.minimumGenerationId } returns 0L

            validateState()
            coVerify(exactly = 0) { client.getMetadata(any()) }
        }

    @Test
    fun `test that fallback persister searches metadata and infers the unique key during truncate`() =
        runTest {
            every { stream.importType } returns Overwrite
            every { stream.generationId } returns 1L
            every { stream.minimumGenerationId } returns 1L

            coEvery { client.getMetadata(any()) } returns emptyMap()

            validateState()
            coVerify { client.getMetadata(any()) }
        }
}
