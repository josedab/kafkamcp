import * as vscode from 'vscode';

/**
 * KafkaMCP VS Code Extension
 * 
 * Provides tree views for topics, schemas, and consumer groups,
 * along with commands for consuming/producing messages, running
 * workflows, and natural language queries.
 */

let mcpServerUrl: string;
let agentId: string;

export function activate(context: vscode.ExtensionContext) {
    const config = vscode.workspace.getConfiguration('kafkamcp');
    mcpServerUrl = config.get('serverUrl', 'http://localhost:8080');
    agentId = config.get('agentId', 'vscode-extension');

    // Register tree data providers
    const topicProvider = new TopicTreeProvider();
    const schemaProvider = new SchemaTreeProvider();
    const groupProvider = new ConsumerGroupTreeProvider();
    const workflowProvider = new WorkflowTreeProvider();

    vscode.window.registerTreeDataProvider('kafkamcp-topics', topicProvider);
    vscode.window.registerTreeDataProvider('kafkamcp-schemas', schemaProvider);
    vscode.window.registerTreeDataProvider('kafkamcp-groups', groupProvider);
    vscode.window.registerTreeDataProvider('kafkamcp-workflows', workflowProvider);

    // Register commands
    context.subscriptions.push(
        vscode.commands.registerCommand('kafkamcp.connect', () => connect()),
        vscode.commands.registerCommand('kafkamcp.refreshTopics', () => topicProvider.refresh()),
        vscode.commands.registerCommand('kafkamcp.consumeMessages', (topic: string) => consumeMessages(topic)),
        vscode.commands.registerCommand('kafkamcp.produceMessage', (topic: string) => produceMessage(topic)),
        vscode.commands.registerCommand('kafkamcp.describeTopic', (topic: string) => describeTopic(topic)),
        vscode.commands.registerCommand('kafkamcp.searchMessages', () => searchMessages()),
        vscode.commands.registerCommand('kafkamcp.viewSchema', (subject: string) => viewSchema(subject)),
        vscode.commands.registerCommand('kafkamcp.runWorkflow', () => runWorkflow()),
        vscode.commands.registerCommand('kafkamcp.analyzeCost', (topic: string) => analyzeCost(topic)),
        vscode.commands.registerCommand('kafkamcp.nlQuery', () => nlQuery()),
    );

    vscode.window.showInformationMessage('KafkaMCP extension activated');
}

export function deactivate() {}

// --- MCP Client ---

async function mcpCall(tool: string, args: Record<string, unknown>): Promise<unknown> {
    // In production, this would use the MCP protocol over SSE/HTTP
    // For now, this is a scaffold showing the expected interface
    const response = await fetch(`${mcpServerUrl}/mcp/v1/tools/${tool}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-Agent-ID': agentId,
        },
        body: JSON.stringify({ arguments: args }),
    });
    return response.json();
}

async function mcpResource(uri: string): Promise<unknown> {
    const response = await fetch(`${mcpServerUrl}/mcp/v1/resources/${encodeURIComponent(uri)}`, {
        headers: { 'X-Agent-ID': agentId },
    });
    return response.json();
}

// --- Commands ---

async function connect() {
    const url = await vscode.window.showInputBox({
        prompt: 'KafkaMCP Server URL',
        value: mcpServerUrl,
    });
    if (url) {
        mcpServerUrl = url;
        vscode.window.showInformationMessage(`Connected to ${url}`);
    }
}

async function consumeMessages(topic: string) {
    if (!topic) {
        topic = await vscode.window.showInputBox({ prompt: 'Topic name' }) || '';
    }
    if (!topic) return;

    try {
        const result = await mcpCall('kafka_consume', { topic, offset: 'latest', limit: 10 });
        const doc = await vscode.workspace.openTextDocument({
            content: JSON.stringify(result, null, 2),
            language: 'json',
        });
        await vscode.window.showTextDocument(doc);
    } catch (err) {
        vscode.window.showErrorMessage(`Failed to consume: ${err}`);
    }
}

async function produceMessage(topic: string) {
    if (!topic) {
        topic = await vscode.window.showInputBox({ prompt: 'Topic name' }) || '';
    }
    if (!topic) return;

    const value = await vscode.window.showInputBox({ prompt: 'Message value (JSON)' });
    if (!value) return;

    try {
        const result = await mcpCall('kafka_produce', {
            topic,
            messages: [{ value: JSON.parse(value) }],
        });
        vscode.window.showInformationMessage(`Message produced: ${JSON.stringify(result)}`);
    } catch (err) {
        vscode.window.showErrorMessage(`Failed to produce: ${err}`);
    }
}

async function describeTopic(topic: string) {
    if (!topic) {
        topic = await vscode.window.showInputBox({ prompt: 'Topic name' }) || '';
    }
    if (!topic) return;

    try {
        const result = await mcpCall('kafka_describe_topic', { topic });
        const doc = await vscode.workspace.openTextDocument({
            content: JSON.stringify(result, null, 2),
            language: 'json',
        });
        await vscode.window.showTextDocument(doc);
    } catch (err) {
        vscode.window.showErrorMessage(`Failed to describe: ${err}`);
    }
}

async function searchMessages() {
    const topic = await vscode.window.showInputBox({ prompt: 'Topic name' });
    if (!topic) return;
    const query = await vscode.window.showInputBox({ prompt: 'Search term (value contains)' });
    if (!query) return;

    try {
        const result = await mcpCall('kafka_search', { topic, value_contains: query, limit: 20 });
        const doc = await vscode.workspace.openTextDocument({
            content: JSON.stringify(result, null, 2),
            language: 'json',
        });
        await vscode.window.showTextDocument(doc);
    } catch (err) {
        vscode.window.showErrorMessage(`Search failed: ${err}`);
    }
}

async function viewSchema(subject: string) {
    if (!subject) {
        subject = await vscode.window.showInputBox({ prompt: 'Schema subject' }) || '';
    }
    if (!subject) return;

    try {
        const result = await mcpCall('kafka_get_schema', { subject });
        const doc = await vscode.workspace.openTextDocument({
            content: JSON.stringify(result, null, 2),
            language: 'json',
        });
        await vscode.window.showTextDocument(doc);
    } catch (err) {
        vscode.window.showErrorMessage(`Failed to get schema: ${err}`);
    }
}

async function runWorkflow() {
    try {
        const result = await mcpCall('kafka_workflows', { action: 'list' }) as any;
        const templates = result?.templates || [];
        const items = templates.map((t: any) => ({
            label: t.name,
            description: t.description,
            detail: `Category: ${t.category} | Steps: ${t.step_count}`,
        }));

        const selected = await vscode.window.showQuickPick(items, { placeHolder: 'Select workflow template' });
        if (!selected) return;

        vscode.window.showInformationMessage(`Starting workflow: ${selected.label}`);
    } catch (err) {
        vscode.window.showErrorMessage(`Failed: ${err}`);
    }
}

async function analyzeCost(topic: string) {
    if (!topic) {
        topic = await vscode.window.showInputBox({ prompt: 'Topic name' }) || '';
    }
    if (!topic) return;

    try {
        const result = await mcpCall('kafka_cost_analyze', { topic });
        const doc = await vscode.workspace.openTextDocument({
            content: JSON.stringify(result, null, 2),
            language: 'json',
        });
        await vscode.window.showTextDocument(doc);
    } catch (err) {
        vscode.window.showErrorMessage(`Cost analysis failed: ${err}`);
    }
}

async function nlQuery() {
    const query = await vscode.window.showInputBox({
        prompt: 'Ask in plain English (e.g., "show me all failed orders in the last hour")',
    });
    if (!query) return;

    try {
        const result = await mcpCall('kafka_nl_query', { query });
        const doc = await vscode.workspace.openTextDocument({
            content: JSON.stringify(result, null, 2),
            language: 'json',
        });
        await vscode.window.showTextDocument(doc);
    } catch (err) {
        vscode.window.showErrorMessage(`Query failed: ${err}`);
    }
}

// --- Tree Providers ---

class TopicTreeProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
    private _onDidChangeTreeData = new vscode.EventEmitter<void>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: vscode.TreeItem): vscode.TreeItem {
        return element;
    }

    async getChildren(): Promise<vscode.TreeItem[]> {
        try {
            const data = await mcpResource('kafka://topics') as any;
            const topics = data?.topics || [];
            return topics.map((t: any) => {
                const item = new vscode.TreeItem(t.name, vscode.TreeItemCollapsibleState.None);
                item.description = `${t.partitions}P | ~${t.message_count_estimate} msgs`;
                item.contextValue = 'topic';
                item.tooltip = `Partitions: ${t.partitions}\nReplication: ${t.replication_factor}`;
                return item;
            });
        } catch {
            return [new vscode.TreeItem('Connect to KafkaMCP server first')];
        }
    }
}

class SchemaTreeProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
    getTreeItem(element: vscode.TreeItem): vscode.TreeItem { return element; }
    async getChildren(): Promise<vscode.TreeItem[]> {
        try {
            const data = await mcpResource('kafka://schemas') as any;
            const subjects = data?.subjects || [];
            return subjects.map((s: any) => {
                const item = new vscode.TreeItem(s.subject || s.name, vscode.TreeItemCollapsibleState.None);
                item.contextValue = 'schema';
                return item;
            });
        } catch {
            return [new vscode.TreeItem('Schema Registry not configured')];
        }
    }
}

class ConsumerGroupTreeProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
    getTreeItem(element: vscode.TreeItem): vscode.TreeItem { return element; }
    async getChildren(): Promise<vscode.TreeItem[]> {
        try {
            const data = await mcpResource('kafka://groups') as any;
            const groups = data?.groups || [];
            return groups.map((g: any) => {
                const item = new vscode.TreeItem(g.group_id, vscode.TreeItemCollapsibleState.None);
                item.description = `${g.state} | Lag: ${g.total_lag}`;
                return item;
            });
        } catch {
            return [new vscode.TreeItem('Connect to KafkaMCP server first')];
        }
    }
}

class WorkflowTreeProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
    getTreeItem(element: vscode.TreeItem): vscode.TreeItem { return element; }
    async getChildren(): Promise<vscode.TreeItem[]> {
        try {
            const data = await mcpResource('kafka://workflows') as any;
            const templates = data?.templates || [];
            return templates.map((t: any) => {
                const item = new vscode.TreeItem(t.name, vscode.TreeItemCollapsibleState.None);
                item.description = t.category;
                item.tooltip = t.description;
                return item;
            });
        } catch {
            return [new vscode.TreeItem('Workflows available after connection')];
        }
    }
}
