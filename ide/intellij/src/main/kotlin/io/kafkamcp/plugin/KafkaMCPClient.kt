package io.kafkamcp.plugin

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.util.concurrent.TimeUnit

/**
 * Client for the KafkaMCP IDE integration API.
 * Connects to the KafkaMCP server's /api/ide/v1/ endpoints.
 */
class KafkaMCPClient(private val baseUrl: String = "http://localhost:3100") {

    private val client = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .build()

    private val gson = Gson()
    private val jsonType = "application/json".toMediaType()

    data class TopicNode(
        val name: String,
        val partitions: Int,
        val replicationFactor: Int,
        val messageCount: Long,
        val isInternal: Boolean,
        val schemaSubject: String?
    )

    data class MessagePreview(
        val topic: String,
        val partition: Int,
        val offset: Long,
        val key: String?,
        val value: String,
        val timestamp: String
    )

    data class CompletionItem(
        val label: String,
        val kind: String,
        val detail: String,
        val insertText: String
    )

    /**
     * Connect to KafkaMCP and establish an IDE session.
     */
    fun connect(ide: String, version: String): Map<String, Any>? {
        val body = gson.toJson(mapOf("ide" to ide, "version" to version))
        val request = Request.Builder()
            .url("$baseUrl/api/ide/v1/connect")
            .post(body.toRequestBody(jsonType))
            .build()

        return executeRequest(request)
    }

    /**
     * Browse available Kafka topics.
     */
    fun browseTopics(cluster: String? = null, includeInternal: Boolean = false): List<TopicNode> {
        val url = buildString {
            append("$baseUrl/api/ide/v1/topics?internal=$includeInternal")
            if (cluster != null) append("&cluster=$cluster")
        }

        val request = Request.Builder().url(url).get().build()
        val response = executeRequest(request) ?: return emptyList()

        @Suppress("UNCHECKED_CAST")
        val topics = response["topics"] as? List<Map<String, Any>> ?: return emptyList()
        return topics.map { t ->
            TopicNode(
                name = t["name"] as? String ?: "",
                partitions = (t["partitions"] as? Double)?.toInt() ?: 0,
                replicationFactor = (t["replication_factor"] as? Double)?.toInt() ?: 0,
                messageCount = (t["message_count"] as? Double)?.toLong() ?: 0,
                isInternal = t["is_internal"] as? Boolean ?: false,
                schemaSubject = t["schema_subject"] as? String
            )
        }
    }

    /**
     * Preview messages from a topic.
     */
    fun previewMessages(topic: String, cluster: String? = null): List<MessagePreview> {
        val url = buildString {
            append("$baseUrl/api/ide/v1/messages?topic=$topic")
            if (cluster != null) append("&cluster=$cluster")
        }

        val request = Request.Builder().url(url).get().build()
        val response = executeRequest(request) ?: return emptyList()

        @Suppress("UNCHECKED_CAST")
        val messages = response["messages"] as? List<Map<String, Any>> ?: return emptyList()
        return messages.map { m ->
            MessagePreview(
                topic = m["topic"] as? String ?: topic,
                partition = (m["partition"] as? Double)?.toInt() ?: 0,
                offset = (m["offset"] as? Double)?.toLong() ?: 0,
                key = m["key"] as? String,
                value = m["value"] as? String ?: "",
                timestamp = m["timestamp"] as? String ?: ""
            )
        }
    }

    /**
     * Get autocompletion suggestions.
     */
    fun getCompletions(prefix: String, kind: String? = null): List<CompletionItem> {
        val url = buildString {
            append("$baseUrl/api/ide/v1/completions?prefix=$prefix")
            if (kind != null) append("&kind=$kind")
        }

        val request = Request.Builder().url(url).get().build()
        val response = executeRequest(request) ?: return emptyList()

        @Suppress("UNCHECKED_CAST")
        val items = response["completions"] as? List<Map<String, Any>> ?: return emptyList()
        return items.map { c ->
            CompletionItem(
                label = c["label"] as? String ?: "",
                kind = c["kind"] as? String ?: "",
                detail = c["detail"] as? String ?: "",
                insertText = c["insert_text"] as? String ?: ""
            )
        }
    }

    /**
     * Check server health.
     */
    fun healthCheck(): Boolean {
        return try {
            val request = Request.Builder()
                .url("$baseUrl/api/ide/v1/health")
                .get()
                .build()
            val response = executeRequest(request)
            response?.get("status") == "healthy"
        } catch (e: Exception) {
            false
        }
    }

    private fun executeRequest(request: Request): Map<String, Any>? {
        return try {
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) return null
                val body = response.body?.string() ?: return null
                val type = object : TypeToken<Map<String, Any>>() {}.type
                gson.fromJson(body, type)
            }
        } catch (e: Exception) {
            null
        }
    }
}
