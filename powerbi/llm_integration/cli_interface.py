"""
Command Line Interface for Power BI LLM Q&A

Provides command-line interface for LLM Q&A service
"""

import argparse
import json
from typing import Dict, List
from src.mock_llm import MockLLMService

class CLIInterface:
    """Command line interface"""
    
    def __init__(self):
        self.llm_processor = MockLLMService()
        self.query_history = []
    
    def run_interactive_mode(self):
        """Run interactive mode"""
        print(" ICU Data Q&A - Interactive Mode")
        print("Type 'help' for commands, 'quit' to exit")
        print("-" * 50)
        
        while True:
            try:
                query = input("\n Enter your query: ").strip()
                
                if query.lower() in ['quit', 'exit', 'q']:
                    print(" Goodbye!")
                    break
                elif query.lower() == 'help':
                    self.show_help()
                elif query.lower() == 'history':
                    self.show_history()
                elif query.lower() == 'suggestions':
                    self.show_suggestions()
                elif query:
                    self.process_query(query)
                else:
                    print(" Please enter a query or 'help' for commands")
                    
            except KeyboardInterrupt:
                print("\n Goodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")
    
    def process_query(self, query: str):
        """Process query"""
        print(f"\n🔍 Processing: {query}")
        
        try:
            result = self.llm_processor.get_mock_result(query)
            
            # Record query history
            self.query_history.append({
                'query': query,
                'success': result['success'],
                'intent': result.get('intent', 'unknown')
            })
            
            if result['success']:
                print(f" Intent: {result['intent']}")
                print(f" DAX Query: {result['dax_query']}")
                
                if result['result']:
                    print("\n Results:")
                    for item in result['result']:
                        print(f"  {item}")
                
                if result['explanation']:
                    print(f"\n Explanation: {result['explanation']}")
                
                print(f" Execution Time: {result.get('execution_time', 'N/A')}")
            else:
                print(f"Error: {result['error']}")
                
        except Exception as e:
            print(f"Processing Error: {e}")
    
    def show_help(self):
        """Show help information"""
        help_text = """
 Available Commands:
  help        - Show this help message
  history     - Show query history
  suggestions - Show query suggestions
  quit/exit/q - Exit the program

 Example Queries:
  - Show me the average MELD score
  - How many high-risk patients do we have?
  - What is the MELD score trend?
  - Find patients with MELD score above 25
        """
        print(help_text)
    
    def show_history(self):
        """Show query history"""
        if not self.query_history:
            print("No queries in history")
            return
        
        print("\n Query History:")
        for i, record in enumerate(self.query_history[-5:], 1):  # Show last 5 records
            status = "" if record['success'] else "❌"
            print(f"  {i}. {status} {record['query']} ({record['intent']})")
    
    def show_suggestions(self):
        """Show query suggestions"""
        suggestions = [
            "Show me the average MELD score",
            "How many high-risk patients do we have?",
            "What is the MELD score trend?",
            "Find patients with MELD score above 25",
            "Show risk distribution by category",
            "Compare this month to last month"
        ]
        
        print("\n Query Suggestions:")
        for i, suggestion in enumerate(suggestions, 1):
            print(f"  {i}. {suggestion}")
    
    def run_single_query(self, query: str):
        """Run single query"""
        print(f" ICU Data Q&A - Single Query Mode")
        print(f"Query: {query}")
        print("-" * 50)
        
        self.process_query(query)
    
    def run_batch_queries(self, queries: List[str]):
        """Run batch queries"""
        print(f"ICU Data Q&A - Batch Mode")
        print(f"Processing {len(queries)} queries")
        print("-" * 50)
        
        for i, query in enumerate(queries, 1):
            print(f"\n[{i}/{len(queries)}] Processing: {query}")
            self.process_query(query)
            print("-" * 30)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="ICU Data Q&A CLI")
    parser.add_argument("--query", "-q", help="Single query to process")
    parser.add_argument("--file", "-f", help="File containing queries (one per line)")
    parser.add_argument("--interactive", "-i", action="store_true", help="Run in interactive mode")
    
    args = parser.parse_args()
    
    cli = CLIInterface()
    
    if args.query:
        cli.run_single_query(args.query)
    elif args.file:
        try:
            with open(args.file, 'r') as f:
                queries = [line.strip() for line in f if line.strip()]
            cli.run_batch_queries(queries)
        except FileNotFoundError:
            print(f" File not found: {args.file}")
    else:
        cli.run_interactive_mode()

if __name__ == "__main__":
    main()
