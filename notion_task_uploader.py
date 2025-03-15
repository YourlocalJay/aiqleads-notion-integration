#!/usr/bin/env python3
"""
AIQLeads Notion Task Uploader

This script combines multiple CSV task lists and uploads them to a Notion database.
It handles duplicate task detection and updates existing tasks when appropriate.

Usage:
    python notion_task_uploader.py <csv_files> --mode [create|update|both]

Example:
    python notion_task_uploader.py tasks1.csv tasks2.csv --mode both
"""

import argparse
import asyncio
import csv
import httpx
import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional, Set

# Notion API configuration
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
NOTION_API_BASE_URL = "https://api.notion.com/v1"

# Mapping for standardized column names across different CSV formats
COLUMN_MAPPING = {
    # Original CSV column name -> Standardized column name
    "Week/Milestone": "Sprint",
    "Task Name": "Category",
    "Description": "Task Description",
    "Assigned LLM Tools": "Tools/APIs Required",
    "Files Created": "API/Tokens Required",
    "Dependencies": "Dependencies",
    "Enhancements": "Notes/Comments",
    "Acceptance Criteria": "Notes/Comments",
    "Status": "Status",
    "Priority": "Priority",
    "Due Date": "Due Date",
    "Estimated Effort": "Estimated Effort"
}

class NotionTaskUploader:
    def __init__(self, api_key: str, database_id: str):
        """Initialize the Notion Task Uploader with API credentials"""
        self.api_key = api_key
        self.database_id = database_id
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
        self.existing_tasks = {}  # category -> notion_page_id mapping
    
    async def initialize(self):
        """Fetch existing tasks to avoid duplicates"""
        try:
            await self._fetch_existing_tasks()
            print(f"Found {len(self.existing_tasks)} existing tasks in Notion database")
        except Exception as e:
            print(f"Error initializing Notion uploader: {str(e)}")
            raise
    
    async def _fetch_existing_tasks(self):
        """Query Notion database to get existing tasks"""
        async with httpx.AsyncClient() as client:
            url = f"{NOTION_API_BASE_URL}/databases/{self.database_id}/query"
            has_more = True
            start_cursor = None
            
            while has_more:
                payload = {}
                if start_cursor:
                    payload["start_cursor"] = start_cursor
                
                response = await client.post(
                    url,
                    headers=self.headers,
                    json=payload
                )
                response.raise_for_status()
                data = response.json()
                
                # Process results
                for page in data.get("results", []):
                    properties = page.get("properties", {})
                    category_property = properties.get("Category", {})
                    title = category_property.get("title", [])
                    
                    if title and title[0].get("text", {}).get("content"):
                        task_name = title[0]["text"]["content"]
                        self.existing_tasks[task_name] = page["id"]
                
                # Check if there are more results
                has_more = data.get("has_more", False)
                if has_more:
                    start_cursor = data.get("next_cursor")
    
    async def create_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new task in Notion"""
        # Convert task data to Notion properties format
        properties = self._convert_to_notion_properties(task_data)
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{NOTION_API_BASE_URL}/pages",
                headers=self.headers,
                json={
                    "parent": {"database_id": self.database_id},
                    "properties": properties
                }
            )
            
            response.raise_for_status()
            return response.json()
    
    async def update_task(self, page_id: str, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing task in Notion"""
        # Convert task data to Notion properties format
        properties = self._convert_to_notion_properties(task_data)
        
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                f"{NOTION_API_BASE_URL}/pages/{page_id}",
                headers=self.headers,
                json={"properties": properties}
            )
            
            response.raise_for_status()
            return response.json()
    
    def _convert_to_notion_properties(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert standardized task data to Notion properties format"""
        properties = {}
        
        # Handle Category/Task Name (Title field in Notion)
        category = task_data.get("Category", "")
        properties["Category"] = {
            "title": [{"text": {"content": category}}]
        }
        
        # Handle Sprint/Week/Milestone
        sprint = task_data.get("Sprint", "")
        if sprint:
            properties["Sprint"] = {
                "rich_text": [{"text": {"content": sprint}}]
            }
        
        # Handle Task Description
        description = task_data.get("Task Description", "")
        if description:
            properties["Task Description"] = {
                "rich_text": [{"text": {"content": description}}]
            }
        
        # Handle Tools/APIs Required
        tools = task_data.get("Tools/APIs Required", "")
        if tools:
            properties["Tools/APIs Required"] = {
                "rich_text": [{"text": {"content": tools}}]
            }
        
        # Handle API/Tokens Required
        api_tokens = task_data.get("API/Tokens Required", "")
        if api_tokens:
            properties["API/Tokens Required"] = {
                "rich_text": [{"text": {"content": api_tokens}}]
            }
        
        # Handle Status as a select field
        status = task_data.get("Status", "Not Started")
        if status:
            properties["Status"] = {
                "select": {"name": status}
            }
        
        # Handle Priority as a select field
        priority = task_data.get("Priority", "Normal")
        if priority:
            properties["Priority"] = {
                "select": {"name": priority}
            }
        
        # Handle Due Date as a date field
        due_date = task_data.get("Due Date", "")
        if due_date:
            # Make sure date is in ISO format (YYYY-MM-DD)
            try:
                if "-" not in due_date:
                    # Try to parse MM/DD/YYYY format
                    dt = datetime.strptime(due_date, "%m/%d/%Y")
                    due_date = dt.strftime("%Y-%m-%d")
                properties["Due Date"] = {
                    "date": {"start": due_date}
                }
            except ValueError:
                # If date parsing fails, don't add the property
                pass
        
        # Handle Estimated Effort as a select field
        effort = task_data.get("Estimated Effort", "")
        if effort:
            properties["Estimated Effort"] = {
                "select": {"name": effort}
            }
        
        # Handle Dependencies
        dependencies = task_data.get("Dependencies", "")
        if dependencies:
            properties["Dependencies"] = {
                "rich_text": [{"text": {"content": dependencies}}]
            }
        
        # Handle Notes/Comments
        notes = task_data.get("Notes/Comments", "")
        if notes:
            properties["Notes/Comments"] = {
                "rich_text": [{"text": {"content": notes}}]
            }
        
        return properties

async def process_csv_files(files: List[str], mode: str, uploader: NotionTaskUploader) -> Dict[str, Any]:
    """Process multiple CSV files and upload tasks to Notion"""
    all_tasks = []
    unique_categories = set()
    
    # Read and standardize all tasks from CSV files
    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Standardize column names
                    task = {}
                    for old_key, value in row.items():
                        if old_key in COLUMN_MAPPING:
                            new_key = COLUMN_MAPPING[old_key]
                            # Append to existing value if it's Notes/Comments
                            if new_key == "Notes/Comments" and new_key in task and value:
                                task[new_key] += f" | {value}"
                            else:
                                task[new_key] = value
                        else:
                            # Keep original key if not in mapping
                            task[old_key] = value
                    
                    # Make sure we have a Category (Task Name)
                    if "Category" not in task and "Task Name" in task:
                        task["Category"] = task["Task Name"]
                    
                    # Skip empty tasks
                    if not task.get("Category"):
                        continue
                    
                    # Track unique categories to detect duplicates
                    category = task.get("Category")
                    if category in unique_categories:
                        print(f"Warning: Duplicate task found: {category}")
                    unique_categories.add(category)
                    
                    all_tasks.append(task)
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
    
    # Upload to Notion based on mode
    results = {
        "total": len(all_tasks),
        "created": 0,
        "updated": 0,
        "errors": 0
    }
    
    for task in all_tasks:
        category = task.get("Category")
        try:
            if category in uploader.existing_tasks:
                # Task exists - update if needed
                if mode in ["update", "both"]:
                    await uploader.update_task(uploader.existing_tasks[category], task)
                    results["updated"] += 1
                    print(f"Updated task: {category}")
            else:
                # New task - create
                if mode in ["create", "both"]:
                    await uploader.create_task(task)
                    results["created"] += 1
                    print(f"Created task: {category}")
        except Exception as e:
            print(f"Error processing task {category}: {str(e)}")
            results["errors"] += 1
    
    return results

async def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Upload tasks from CSV to Notion')
    parser.add_argument('csv_files', nargs='+', help='CSV files to process')
    parser.add_argument('--mode', choices=['create', 'update', 'both'], default='both',
                      help='Mode: create new tasks, update existing, or both')
    parser.add_argument('--api-key', help='Notion API key (or set NOTION_API_KEY env var)')
    parser.add_argument('--database-id', help='Notion database ID (or set NOTION_DATABASE_ID env var)')
    
    args = parser.parse_args()
    
    # Get API key and database ID
    api_key = args.api_key or NOTION_API_KEY
    database_id = args.database_id or NOTION_DATABASE_ID
    
    if not api_key:
        print("Error: Notion API key is required. Please provide it as an argument or set the NOTION_API_KEY environment variable.")
        return 1
    
    if not database_id:
        print("Error: Notion database ID is required. Please provide it as an argument or set the NOTION_DATABASE_ID environment variable.")
        return 1
    
    try:
        # Initialize uploader
        uploader = NotionTaskUploader(api_key, database_id)
        await uploader.initialize()
        
        # Process CSV files
        results = await process_csv_files(args.csv_files, args.mode, uploader)
        
        # Print summary
        print("\nUpload Summary:")
        print(f"Total tasks processed: {results['total']}")
        print(f"Tasks created: {results['created']}")
        print(f"Tasks updated: {results['updated']}")
        print(f"Errors: {results['errors']}")
        
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))