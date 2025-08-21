#!/usr/bin/env python3

import os
import json
import asyncio
import sys
import re
from dotenv import load_dotenv
import signal
import warnings

# Suppress warnings
warnings.filterwarnings("ignore")
os.environ['ANONYMIZED_TELEMETRY'] = 'false'

from vanna.openai.openai_chat import OpenAI_Chat
from vanna.chromadb.chromadb_vector import ChromaDB_VectorStore

def signal_handler(signum, frame):
    print(f"\n[Python] Received signal {signum}, shutting down gracefully...", file=sys.stderr)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class VannaMCPServer:
    def __init__(self):
        print("[Python] Starting Optimized Vanna MCP Server...", file=sys.stderr)
        load_dotenv()
        
        # Configuration
        self.default_user_id = 2
        self.config = {
            'min_length': 5,
            'max_length': 500,
            'blocked_keywords': ['drop', 'delete', 'truncate', 'alter', 'create', 'insert', 'update'],
            'vague_patterns': [
                r'^(hi|hello|hey|what|how|can you|please)$',
                r'^(help|info|information|status|test|check)$',
                r'^(users?|courses?|enrollments?|data|students?|learners?)$',
                r'^(show|list|find|get|count)$'
            ],
            'db_keywords': [
                'user', 'course', 'enrollment', 'student', 'learner', 'totara', 'lms',
                'show', 'list', 'find', 'get', 'count', 'how many', 'what', 'who',
                'progress', 'completion', 'certificate', 'grade', 'activity'
            ],
            'user_patterns': [
                r'user\s*id\s*(\d+)',
                r'userid\s*(\d+)',
                r'for\s*user\s*(\d+)',
                r'user\s*(\d+)'
            ],
            'username_patterns': [
                r'for\s*user\s*([a-zA-Z0-9@.\-_]+)',
                r'user\s*([a-zA-Z0-9@.\-_]+)',
                r'for\s*([a-zA-Z0-9@.\-_]+@[a-zA-Z0-9.\-_]+)',
                r'username\s*([a-zA-Z0-9@.\-_]+)'
            ],
            'personal_keywords': [
                "my", "i am", "i'm", "me", "my courses", "my enrollments",
                "my progress", "my learning", "what am i", "what courses am i"
            ]
        }
        
        self.vanna = None
        self.init_vanna()
    
    def validate_question(self, question):
        """Comprehensive question validation with all checks in one method"""
        if not question or not isinstance(question, str):
            return False, "ERROR: Invalid Input - Please provide a valid question as text."
        
        question = question.strip()
        question_lower = question.lower()
        
        # Length check
        if len(question) < self.config['min_length']:
            return False, f"ERROR: Question Too Short - Minimum {self.config['min_length']} chars. Example: 'Show me all active users'"
        if len(question) > self.config['max_length']:
            return False, f"ERROR: Question Too Long - Maximum {self.config['max_length']} characters."
        
        # Security check
        if any(kw in question_lower for kw in self.config['blocked_keywords']):
            return False, "ERROR: Unsafe Operation - Only SELECT queries allowed."
        
        # Vague question check
        if any(re.match(pattern, question_lower) for pattern in self.config['vague_patterns']):
            return False, f'ERROR: Too Vague - "{question}" needs more context.\n\nExamples:\n• "users" -> "Show me all active users"\n• "courses" -> "List all courses"\n• "help" -> "Show enrollment help"'
        
        # Context check
        has_db_context = any(kw in question_lower for kw in self.config['db_keywords'])
        if len(question_lower.split()) < 3 and has_db_context:
            return False, f'ERROR: Too Vague - "{question}" needs more context.\n\nTry: "Show me all active users" or "List courses with enrollment counts"'
        
        if not has_db_context:
            return False, 'ERROR: Unclear Request - Question must relate to Totara LMS.\n\nExamples:\n• "Show me all active users"\n• "List courses with enrollments"'
        
        return True, None
    
    def extract_user_context(self, question):
        """Extract user context using pattern matching"""
        question_lower = question.lower()
        
        # Check user ID patterns
        for pattern in self.config['user_patterns']:
            match = re.search(pattern, question_lower)
            if match:
                return {"type": "user_id", "value": int(match.group(1))}
        
        # Check username patterns
        for pattern in self.config['username_patterns']:
            match = re.search(pattern, question_lower)
            if match:
                username = match.group(1)
                if '@' in username or username in ['humanadmin', 'admin']:
                    return {"type": "username", "value": username}
        
        # Check personal queries
        if any(kw in question_lower for kw in self.config['personal_keywords']):
            return {"type": "personal", "value": self.default_user_id}
        
        return None
    
    def init_vanna(self):
        """Initialize Vanna with minimal setup"""
        try:
            class MyVanna(ChromaDB_VectorStore, OpenAI_Chat):
                def __init__(self, config=None):
                    ChromaDB_VectorStore.__init__(self, config=config)
                    OpenAI_Chat.__init__(self, config=config)
            
            from openai import AzureOpenAI
            
            self.vanna = MyVanna({
                'model': os.getenv('AZURE_OPENAI_DEPLOYMENT', 'gpt-4o-mini'),
                'path': './chromadb_fresh'
            })
            
            self.vanna.client = AzureOpenAI(
                api_key=os.getenv('AZURE_OPENAI_KEY'),
                azure_endpoint=os.getenv('AZURE_OPENAI_ENDPOINT'),
                api_version=os.getenv('AZURE_OPENAI_VERSION', '2024-12-01-preview')
            )
            
            print("[Python] SUCCESS: Vanna initialized", file=sys.stderr)
            self.connect_database()
            self.load_training_data()
            
        except Exception as e:
            print(f"[Python] ERROR: Init failed: {e}", file=sys.stderr)
            sys.exit(1)
    
    def connect_database(self):
        """Connect to database with error handling"""
        try:
            self.vanna.connect_to_mysql(
                host=os.getenv('DB_HOST'),
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                port=int(os.getenv('DB_PORT', 3306))
            )
            print("[Python] SUCCESS: Database connected", file=sys.stderr)
        except Exception as e:
            print(f"[Python] ERROR: Database failed: {e}", file=sys.stderr)
            sys.exit(1)
    
    def load_training_data(self):
        """Streamlined training data loading"""
        try:
            from azure.search.documents import SearchClient
            from azure.core.credentials import AzureKeyCredential
            
            endpoint = os.getenv('AZURE_SEARCH_ENDPOINT')
            api_key = os.getenv('AZURE_SEARCH_ADMIN_KEY')
            
            if not endpoint or not api_key:
                print("[Python] WARNING: Azure Search not configured", file=sys.stderr)
                return
            
            search_client = SearchClient(
                endpoint=endpoint,
                index_name="vanna-totara-enhanced",
                credential=AzureKeyCredential(api_key)
            )
            
            results = search_client.search(
                search_text="*",
                top=2000,
                select=["content", "content_type", "question"]
            )
            
            count = 0
            for result in results:
                try:
                    content = result.get('content', '').strip()
                    content_type = result.get('content_type', '').lower()
                    question = result.get('question', '').strip()
                    
                    if not content:
                        continue
                    
                    if 'ttl_' in content and 'CREATE TABLE' in content:
                        self.vanna.train(ddl=content)
                    elif 'SELECT' in content.upper() and 'ttl_' in content and question:
                        self.vanna.train(question=question, sql=content)
                    elif content_type in ['documentation', 'doc']:
                        self.vanna.train(documentation=content)
                    else:
                        continue
                        
                    count += 1
                except:
                    continue
            
            # Add critical training
            self.vanna.train(
                question="list users enrolled in courses",
                sql="""SELECT u.id, CONCAT(u.firstname, ' ', u.lastname) as name, u.email, c.fullname as course_name
                FROM ttl_user_enrolments e
                JOIN ttl_enrol en ON e.enrolid = en.id
                JOIN ttl_course c ON en.courseid = c.id
                JOIN ttl_user u ON e.userid = u.id
                WHERE e.status = 0 AND u.deleted = 0"""
            )
            
            print(f"[Python] SUCCESS: Training loaded ({count} items)", file=sys.stderr)
            
        except Exception as e:
            print(f"[Python] WARNING: Training failed: {e}", file=sys.stderr)
    
    def format_response(self, content_text):
        """Standard response formatting"""
        return {"content": [{"type": "text", "text": content_text}]}
    
    def format_results(self, question, sql, results, user_context=None):
        """Format query results"""
        if results is not None and len(results) > 0:
            results_dict = results.to_dict('records') if hasattr(results, 'to_dict') else results
            
            parts = [f"Question: {question}"]
            if user_context:
                parts.append(f"User Context: {user_context}")
            parts.extend([
                f"Generated SQL:\n```sql\n{sql}\n```",
                f"Results: ({len(results_dict)} rows)\n```json\n{json.dumps(results_dict, indent=2, default=str)}\n```",
                "*Generated using Vanna AI*"
            ])
            return "\n".join(parts)
        else:
            return f"""Question: {question}
Generated SQL:
```sql
{sql}
```
Results: No data found.

Suggestions:
• Try broader search criteria
• Check if specified users/courses exist
• Use 'list_active_users' to see available data"""
    
    def generate_help_text(self):
        """Generate help text"""
        return """HELP: Totara LMS Query Guide

GOOD Questions:
• "Show me all active users"
• "List courses with enrollment counts"
• "Find users enrolled in Computer Science course"
• "Show course completion statistics"

User-Specific:
• "Show courses for user ID 2"
• "List progress for user humanadmin"
• "Show my enrolled courses"

AVOID:
• Too vague: "help", "users", "courses"
• Unsafe: "delete", "drop", "alter"
• Non-database: "What's the weather?"

Tips:
• Be specific about what data you want
• Use action words: "show", "list", "find", "count"
• Mention users, courses, or time periods

Commands: list_active_users, test_vanna_status"""
    
    async def handle_mcp_request(self, request):
        """Unified request handler"""
        try:
            if request['method'] == 'tools/list':
                return {
                    "tools": [
                        {
                            "name": "query_totara_db",
                            "description": "Query Totara LMS database",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "question": {"type": "string"}
                                },
                                "required": ["question"]
                            }
                        },
                        {
                            "name": "test_vanna_status",
                            "description": "Test system status",
                            "inputSchema": {
                                "type": "object",
                                "properties": {}
                            }
                        },
                        {
                            "name": "list_active_users",
                            "description": "List active users",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "limit": {
                                        "type": "integer",
                                        "default": 10,
                                        "minimum": 1,
                                        "maximum": 50
                                    }
                                }
                            }
                        },
                        {
                            "name": "get_help",
                            "description": "Get usage help",
                            "inputSchema": {
                                "type": "object",
                                "properties": {}
                            }
                        }
                    ]
                }
            
            elif request['method'] == 'tools/call':
                tool_name = request['params']['name']
                args = request['params'].get('arguments', {})
                
                if tool_name == 'query_totara_db':
                    return await self.query_with_vanna(args['question'])
                elif tool_name == 'test_vanna_status':
                    return await self.test_status()
                elif tool_name == 'list_active_users':
                    return await self.list_users(args.get('limit', 10))
                elif tool_name == 'get_help':
                    return self.format_response(self.generate_help_text())
            
            return {"error": "Unknown request method"}
            
        except Exception as e:
            return {"error": f"Request failed: {str(e)}"}
    
    async def query_with_vanna(self, question):
        """Process query with validation and execution"""
        try:
            # Validate
            is_valid, error_msg = self.validate_question(question)
            if not is_valid:
                return self.format_response(f"{error_msg}\n\nExamples:\n• 'Show me all active users'\n• 'List courses with enrollments'")
            
            # Extract context and generate SQL
            user_context = self.extract_user_context(question)
            enhanced_question = f"{question} (targeting {user_context['type']} {user_context['value']})" if user_context else question
            
            try:
                sql = self.vanna.generate_sql(enhanced_question)
                if not sql or 'SELECT' not in sql.upper():
                    return self.format_response(f"ERROR: Could not generate query for: {question}\n\nTry rephrasing more clearly.")
                
                results = self.vanna.run_sql(sql)
                return self.format_response(self.format_results(question, sql, results, user_context))
                
            except Exception as e:
                return self.format_response(f"ERROR: Query failed: {str(e)}\n\nTry: Simpler question, check system status")
                
        except Exception as e:
            return self.format_response(f"ERROR: Unexpected error: {str(e)}")
    
    async def test_status(self):
        """Test system status"""
        try:
            user_count = self.vanna.run_sql("SELECT COUNT(*) as count FROM ttl_user WHERE deleted = 0").iloc[0]['count']
            training_count = len(self.vanna.get_training_data() or [])
            
            status = f"""SUCCESS: Vanna MCP Server Status
Vanna: Active with Azure OpenAI
Database: Connected ({user_count} users)
Training: {training_count} items loaded
Error Handling: Enhanced validation enabled
System: Ready for queries"""
            
            return self.format_response(status)
        except Exception as e:
            return self.format_response(f"ERROR: Status check failed: {str(e)}")
    
    async def list_users(self, limit=10):
        """List active users"""
        try:
            limit = max(1, min(50, limit))
            sql = f"""SELECT id, username, firstname, lastname, email
            FROM ttl_user 
            WHERE deleted = 0 AND suspended = 0 AND id > 1
            ORDER BY timecreated DESC 
            LIMIT {limit}"""
            
            results = self.vanna.run_sql(sql)
            
            if results is not None and len(results) > 0:
                users = results.to_dict('records')
                user_list = [f"ID {u['id']}: {u['firstname']} {u['lastname']} ({u['username']})" for u in users]
                
                text = f"""Active Users (Top {len(users)}):

{chr(10).join(user_list)}

Usage Examples:
• 'show courses for user ID 2'
• 'my enrolled courses' (defaults to ID {self.default_user_id})"""
                
                return self.format_response(text)
            else:
                return self.format_response("No active users found.")
                
        except Exception as e:
            return self.format_response(f"ERROR: Failed to list users: {str(e)}")
    
    async def run(self):
        """Main server loop"""
        print("[Python] INFO: Optimized server ready", file=sys.stderr)
        print("MCP_SERVER_READY", file=sys.stderr, flush=True)
        
        try:
            while True:
                line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
                if not line:
                    break
                
                try:
                    request = json.loads(line.strip())
                    response = await self.handle_mcp_request(request)
                    
                    json_response = {
                        "jsonrpc": "2.0",
                        "id": request.get("id"),
                        "result": response
                    }
                    
                    print(json.dumps(json_response), flush=True)
                    
                except json.JSONDecodeError:
                    print("[Python] Warning: Invalid JSON", file=sys.stderr)
                except Exception as e:
                    error_response = {
                        "jsonrpc": "2.0",
                        "id": request.get("id") if 'request' in locals() else None,
                        "error": {
                            "code": -32603,
                            "message": str(e)
                        }
                    }
                    print(json.dumps(error_response), flush=True)
                    
        except KeyboardInterrupt:
            print("\n[Python] Graceful shutdown...", file=sys.stderr)
        except Exception as e:
            print(f"[Python] Server error: {e}", file=sys.stderr)

async def main():
    """Main entry point"""
    try:
        server = VannaMCPServer()
        await server.run()
    except Exception as e:
        print(f"[Python] ERROR: Failed to start: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())