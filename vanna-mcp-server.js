
const { Server } = require('@modelcontextprotocol/sdk/server/index.js');
const { StdioServerTransport } = require('@modelcontextprotocol/sdk/server/stdio.js');
const { CallToolRequestSchema, ListToolsRequestSchema } = require('@modelcontextprotocol/sdk/types.js');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

class VannaMCPServer {
  constructor() {
    this.server = new Server(
      { name: 'vanna-mcp-server', version: '2.0.0' },
      { capabilities: { tools: {} } }
    );

    // State management
    this.pythonProcess = null;
    this.isVannaReady = false;
    this.initializationAttempts = 0;
    this.maxAttempts = 3;
    this.requestId = 1;
    this.pendingRequests = new Map();
    this.errorCounts = { total: 0, validation: 0, database: 0, python: 0, timeout: 0 };

    // Configuration
    this.logFile = path.join(__dirname, 'vanna-mcp-server.log');
    
    this.log('=== Optimized Vanna MCP Server Starting ===');
    this.setupHandlers();
    this.initializeVanna();
  }

  log(message) {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}\n`;
    try {
      fs.appendFileSync(this.logFile, logMessage);
      console.error(message);
    } catch (e) {
      console.error(message);
    }
  }

  logError(type, message, context = {}) {
    this.errorCounts.total++;
    this.errorCounts[type] = (this.errorCounts[type] || 0) + 1;
    this.log(`ERROR [${type}]: ${message} | Context: ${JSON.stringify(context)}`);
  }

  validateInput(input, type = 'general') {
    if (!input) return { isValid: false, error: 'ERROR: Missing input' };
    
    if (type === 'question') {
      if (typeof input !== 'string') return { isValid: false, error: 'ERROR: Question must be text' };
      
      const trimmed = input.trim();
      if (trimmed.length < 5) return { isValid: false, error: 'ERROR: Question too short (min 5 chars)' };
      if (trimmed.length > 500) return { isValid: false, error: 'ERROR: Question too long (max 500 chars)' };
      
      // Check for dangerous patterns
      const dangerous = /drop\s+table|delete\s+from|truncate|alter\s+table|create\s+table|insert\s+into|update\s+set/i;
      if (dangerous.test(trimmed)) {
        return { isValid: false, error: 'ERROR: Unsafe operations not allowed. Only SELECT queries permitted.' };
      }
    }
    
    return { isValid: true };
  }

  generateUserFriendlyError(error, context = {}) {
    const errorMessage = error.message || error.toString();
    
    const patterns = [
      { pattern: /timeout/i, response: 'Request timeout. Try simpler question or wait.' },
      { pattern: /connection|network/i, response: 'Connection issue. Please try again.' },
      { pattern: /syntax error|invalid sql/i, response: 'Query error. Please rephrase clearly.' },
      { pattern: /permission|access denied/i, response: 'Access denied to requested data.' },
      { pattern: /not found|doesn\'t exist/i, response: 'Requested data not found.' },
      { pattern: /python process/i, response: 'System restarting. Try again shortly.' }
    ];

    for (const { pattern, response } of patterns) {
      if (pattern.test(errorMessage)) return response;
    }

    return `Error: ${errorMessage}\n\nTry:\n• Rephrase question\n• Use 'get help'\n• Check system with 'test vanna status'`;
  }

  async initializeVanna() {
    this.log('Starting Vanna initialization...');
    await this.ensureFiles();
    this.startPython();
  }

  async ensureFiles() {
    const files = [
      { source: 'C:\\Users\\Nikhil-HL\\mcp-test\\minimal_mcp_server.py', target: path.join(__dirname, 'minimal_mcp_server.py') },
      { source: 'C:\\Users\\Nikhil-HL\\mcp-test\\.env', target: path.join(__dirname, '.env') }
    ];

    for (const file of files) {
      if (!fs.existsSync(file.target) && fs.existsSync(file.source)) {
        try {
          fs.copyFileSync(file.source, file.target);
          this.log(`Copied ${path.basename(file.target)}`);
        } catch (error) {
          this.logError('python', `Copy failed: ${path.basename(file.target)}`, { error: error.message });
        }
      }
    }
  }

  startPython() {
    if (this.initializationAttempts >= this.maxAttempts) {
      this.logError('python', 'Max attempts reached', { attempts: this.initializationAttempts });
      return;
    }

    this.initializationAttempts++;
    const pythonPath = 'C:\\Users\\Nikhil-HL\\AppData\\Local\\Programs\\Python\\Python313\\python.exe';
    const scriptPath = path.join(__dirname, 'minimal_mcp_server.py');

    if (!fs.existsSync(scriptPath) || !fs.existsSync(pythonPath)) {
      this.logError('python', 'Python or script not found');
      return;
    }

    this.pythonProcess = spawn(pythonPath, [scriptPath], {
      cwd: __dirname,
      env: { ...process.env, PYTHONPATH: __dirname, VANNA_TELEMETRY: 'false' },
      stdio: ['pipe', 'pipe', 'pipe']
    });

    const initTimer = setTimeout(() => {
      this.logError('timeout', 'Python init timeout');
      this.killPython();
      setTimeout(() => this.startPython(), 5000);
    }, 60000);

    this.pythonProcess.stderr.on('data', (data) => {
      const output = data.toString();
      this.log(`Python: ${output.trim()}`);

      if (output.includes('MCP_SERVER_READY') || output.includes('ready')) {
        clearTimeout(initTimer);
        this.isVannaReady = true;
        this.initializationAttempts = 0;
        this.log('SUCCESS: Vanna ready');
      }
    });

    let buffer = '';
    this.pythonProcess.stdout.on('data', (data) => {
      buffer += data.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed && trimmed.startsWith('{')) {
          try {
            const response = JSON.parse(trimmed);
            if (response.id && this.pendingRequests.has(response.id)) {
              const { resolve, reject } = this.pendingRequests.get(response.id);
              this.pendingRequests.delete(response.id);
              response.error ? reject(new Error(response.error.message)) : resolve(response);
            }
          } catch (e) {
            this.logError('python', 'JSON parse error', { line: trimmed.substring(0, 100) });
          }
        }
      }
    });

    this.pythonProcess.on('error', (error) => {
      clearTimeout(initTimer);
      this.logError('python', 'Process error', { error: error.message });
      this.isVannaReady = false;
      setTimeout(() => this.startPython(), 5000);
    });

    this.pythonProcess.on('exit', (code, signal) => {
      clearTimeout(initTimer);
      this.logError('python', 'Process exited', { code, signal });
      this.isVannaReady = false;
      
      for (const [, { reject }] of this.pendingRequests) {
        reject(new Error('Python process exited'));
      }
      this.pendingRequests.clear();

      if (signal !== 'SIGTERM' && code !== 0 && this.initializationAttempts < this.maxAttempts) {
        setTimeout(() => this.startPython(), 5000);
      }
    });
  }

  killPython() {
    if (this.pythonProcess) {
      try {
        this.pythonProcess.kill('SIGTERM');
        this.pythonProcess = null;
        this.isVannaReady = false;
      } catch (error) {
        this.logError('python', 'Kill error', { error: error.message });
      }
    }
  }

  async sendToVanna(request) {
    if (!this.pythonProcess || !this.isVannaReady) {
      throw new Error('System initializing. Please wait and try again.');
    }

    return new Promise((resolve, reject) => {
      const requestStr = JSON.stringify(request) + '\n';
      this.pendingRequests.set(request.id, { resolve, reject });

      const timeout = setTimeout(() => {
        if (this.pendingRequests.has(request.id)) {
          this.pendingRequests.delete(request.id);
          this.logError('timeout', 'Request timeout', { requestId: request.id });
          reject(new Error('Request timeout. Try simpler question.'));
        }
      }, 30000);

      try {
        this.pythonProcess.stdin.write(requestStr);
      } catch (error) {
        clearTimeout(timeout);
        this.pendingRequests.delete(request.id);
        this.logError('python', 'Write failed', { error: error.message });
        reject(new Error('Communication error. Please try again.'));
      }
    });
  }

  setupHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: [
        {
          name: 'query_totara_db',
          description: 'Query Totara LMS database with validation',
          inputSchema: {
            type: 'object',
            properties: { question: { type: 'string', description: 'Specific question about Totara data' } },
            required: ['question']
          }
        },
        {
          name: 'test_vanna_status',
          description: 'Test system status',
          inputSchema: { type: 'object', properties: {} }
        },
        {
          name: 'get_help',
          description: 'Get usage help',
          inputSchema: { type: 'object', properties: {} }
        },
        {
          name: 'list_active_users',
          description: 'List active users',
          inputSchema: {
            type: 'object',
            properties: { limit: { type: 'integer', default: 10, minimum: 1, maximum: 50 } }
          }
        },
        {
          name: 'get_error_stats',
          description: 'Get error statistics',
          inputSchema: { type: 'object', properties: {} }
        },
        {
          name: 'test_vanna_server',
          description: 'Test server status',
          inputSchema: { type: 'object', properties: {} }
        }
      ]
    }));

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;
      this.log(`Tool: ${name}`);

      try {
        switch (name) {
          case 'query_totara_db': return await this.queryDatabase(args);
          case 'test_vanna_status': return await this.testStatus();
          case 'get_help': return await this.getHelp();
          case 'list_active_users': return await this.listUsers(args);
          case 'get_error_stats': return await this.getErrorStats();
          case 'test_vanna_server': return await this.testServer();
          default: throw new Error(`Unknown tool: ${name}`);
        }
      } catch (error) {
        this.logError('validation', `Tool ${name} failed`, { error: error.message, args });
        return { content: [{ type: 'text', text: this.generateUserFriendlyError(error, { tool: name }) }] };
      }
    });

    // Error handling
    ['SIGINT', 'SIGTERM'].forEach(signal => {
      process.on(signal, () => {
        this.log(`Received ${signal}, shutting down...`);
        this.killPython();
        process.exit(0);
      });
    });

    process.on('uncaughtException', (error) => {
      this.logError('python', 'Uncaught exception', { error: error.message });
    });

    process.on('unhandledRejection', (reason) => {
      this.logError('python', 'Unhandled rejection', { reason });
    });
  }

  async testServer() {
    const uptime = process.uptime();
    const uptimeFormatted = `${Math.floor(uptime / 3600)}h ${Math.floor((uptime % 3600) / 60)}m`;
    
    return {
      content: [{
        type: 'text',
        text: `Server Status:
• Server: Running
• Vanna: ${this.isVannaReady ? 'Ready' : 'Initializing'}
• Python: ${this.pythonProcess ? 'Active' : 'Inactive'}
• Uptime: ${uptimeFormatted}
• Errors: ${this.errorCounts.total}
• Attempts: ${this.initializationAttempts}/${this.maxAttempts}

Tools: query_totara_db, test_vanna_status, get_help, list_active_users, get_error_stats

Enhanced: Smart validation, user-friendly errors, detailed logging`
      }]
    };
  }

  async getErrorStats() {
    const uptime = `${Math.floor(process.uptime() / 3600)}h ${Math.floor((process.uptime() % 3600) / 60)}m`;
    
    return {
      content: [{
        type: 'text',
        text: `Error Statistics:
• Total: ${this.errorCounts.total}
• Validation: ${this.errorCounts.validation || 0}
• Database: ${this.errorCounts.database || 0}
• Python: ${this.errorCounts.python || 0}
• Timeout: ${this.errorCounts.timeout || 0}

System:
• Uptime: ${uptime}
• Vanna: ${this.isVannaReady ? 'Ready' : 'Initializing'}
• Pending: ${this.pendingRequests.size}

Health: ${this.errorCounts.total < 5 ? 'Excellent' : this.errorCounts.total < 15 ? 'Good' : 'Needs Attention'}`
      }]
    };
  }

  async getHelp() {
    return {
      content: [{
        type: 'text',
        text: `Totara LMS Query Help

GOOD Questions:
• "Show me all active users"
• "List courses with enrollment counts"
• "Find users enrolled in Computer Science"
• "Show completion statistics"

User-Specific:
• "Show courses for user ID 2"
• "List progress for humanadmin"
• "Show my enrolled courses"

Analytics:
• "How many completed courses this month?"
• "Which courses have highest enrollment?"

AVOID:
• Vague: "help", "users", "courses"
• Unsafe: "delete", "drop", "alter"
• Non-DB: "What's the weather?"

Tips: Be specific, use action words, mention users/courses/dates
Commands: list_active_users, test_vanna_status`
      }]
    };
  }

  async testStatus() {
    try {
      if (!this.isVannaReady) {
        return {
          content: [{
            type: 'text',
            text: `System Initializing
Attempt: ${this.initializationAttempts}/${this.maxAttempts}
Python: ${this.pythonProcess ? 'Starting' : 'Not started'}

Please wait and try again.`
          }]
        };
      }

      const request = {
        jsonrpc: "2.0",
        id: this.requestId++,
        method: "tools/call",
        params: { name: "test_vanna_status", arguments: {} }
      };

      const response = await this.sendToVanna(request);
      return response.result || { content: [{ type: 'text', text: 'Status check completed' }] };

    } catch (error) {
      this.logError('database', 'Status test failed', { error: error.message });
      return {
        content: [{
          type: 'text',
          text: `Status Check Failed: ${this.generateUserFriendlyError(error)}

Try: test_vanna_server for basic diagnostics`
        }]
      };
    }
  }

  async listUsers(args = {}) {
    try {
      const limit = Math.max(1, Math.min(50, args.limit || 10));
      
      const request = {
        jsonrpc: "2.0",
        id: this.requestId++,
        method: "tools/call",
        params: { name: "list_active_users", arguments: { limit } }
      };

      const response = await this.sendToVanna(request);
      return response.result || { content: [{ type: 'text', text: `User list completed (${limit} requested)` }] };

    } catch (error) {
      this.logError('database', 'List users failed', { error: error.message, limit: args.limit });
      return {
        content: [{
          type: 'text',
          text: `Cannot List Users: ${this.generateUserFriendlyError(error)}

Try: Smaller limit, check system status, wait if busy`
        }]
      };
    }
  }

  async queryDatabase(args) {
    try {
      const validation = this.validateInput(args.question, 'question');
      if (!validation.isValid) {
        this.logError('validation', 'Invalid input', { question: args.question });
        return {
          content: [{
            type: 'text',
            text: `${validation.error}

Examples:
• "Show me all active users"
• "List courses with enrollments"

Use 'get help' for more examples`
          }]
        };
      }

      if (!this.isVannaReady) {
        this.logError('validation', 'Query while not ready', { question: args.question });
        return {
          content: [{
            type: 'text',
            text: `System Starting: "${args.question}"

Please wait 30-60 seconds for initialization.
Try 'test_vanna_server' to check status.`
          }]
        };
      }

      const request = {
        jsonrpc: "2.0",
        id: this.requestId++,
        method: "tools/call",
        params: { name: "query_totara_db", arguments: { question: args.question } }
      };

      const response = await this.sendToVanna(request);
      return response.result || { content: [{ type: 'text', text: `Query completed: "${args.question}"` }] };

    } catch (error) {
      this.logError('database', 'Query failed', { question: args.question, error: error.message });
      return {
        content: [{
          type: 'text',
          text: `Query Failed: "${args.question}"

${this.generateUserFriendlyError(error)}

Suggestions:
• Rephrase more simply
• Use 'get help' for examples
• Check 'test_vanna_status'`
        }]
      };
    }
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    this.log('SUCCESS: Optimized Vanna MCP Server started');
  }
}

// Start server
const server = new VannaMCPServer();
server.run().catch((error) => {
  console.error('Failed to start server:', error);
  process.exit(1);
});