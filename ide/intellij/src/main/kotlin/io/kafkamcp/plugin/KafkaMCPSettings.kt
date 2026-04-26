package io.kafkamcp.plugin

import com.intellij.openapi.components.PersistentStateComponent
import com.intellij.openapi.components.Service
import com.intellij.openapi.components.State
import com.intellij.openapi.components.Storage

/**
 * Persistent settings for the KafkaMCP plugin.
 */
@Service
@State(name = "KafkaMCPSettings", storages = [Storage("kafkamcp.xml")])
class KafkaMCPSettings : PersistentStateComponent<KafkaMCPSettings.State> {

    data class State(
        var serverUrl: String = "http://localhost:3100",
        var apiKey: String = "",
        var autoConnect: Boolean = true,
        var showInternalTopics: Boolean = false,
        var maxPreviewMessages: Int = 10,
        var refreshIntervalSeconds: Int = 30
    )

    private var myState = State()

    override fun getState(): State = myState

    override fun loadState(state: State) {
        myState = state
    }

    var serverUrl: String
        get() = myState.serverUrl
        set(value) { myState.serverUrl = value }

    var apiKey: String
        get() = myState.apiKey
        set(value) { myState.apiKey = value }

    var autoConnect: Boolean
        get() = myState.autoConnect
        set(value) { myState.autoConnect = value }
}
