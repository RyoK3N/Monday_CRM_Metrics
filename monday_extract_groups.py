# monday_extract_groups.py

import os
import requests
import sys
import csv
from tqdm import tqdm

def fetch_groups(board_id, api_key):
    """
    Fetches groups from a specified Monday.com board.

    Args:
        board_id (str): The ID of the board.
        api_key (str): Your Monday.com API key.

    Returns:
        list: A list of groups with their IDs and titles.
    """
    query = """
    query ($boardId: [ID!]!) {
      boards(ids: $boardId) {
        groups {
          id
          title
        }
      }
    }
    """

    variables = {
        "boardId": [str(board_id)]  
    }

    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }

    response = requests.post(
        "https://api.monday.com/v2",
        json={"query": query, "variables": variables},
        headers=headers
    )

    if response.status_code != 200:
        raise Exception(f"Query failed with status code {response.status_code}: {response.text}")

    data = response.json()

    if 'errors' in data:
        error_messages = "\n".join([error['message'] for error in data['errors']])
        raise Exception(f"GraphQL Errors:\n{error_messages}")

    boards = data.get('data', {}).get('boards', [])
    if not boards:
        raise Exception(f"No boards found with ID {board_id}.")

    board = boards[0]
    groups = board.get('groups', [])

    if not groups:
        raise Exception(f"No groups found in board {board_id}.")

    return groups

def fetch_items(board_id, group_id, api_key, limit=10):
    """
    Fetches items from a specific group within a Monday.com board.

    Args:
        board_id (str): The ID of the board.
        group_id (str): The ID of the group.
        api_key (str): Your Monday.com API key.
        limit (int): Number of items to fetch.

    Returns:
        list: A list of items with their details.
    """
    query = """
    query ($boardId: [ID!]!, $groupId: [String!]!, $limit: Int!) {
      boards(ids: $boardId) {
        groups(ids: $groupId) {
          id
          title
          items_page(limit: $limit) {
            items {
              id
              name
              column_values {
                id
                text
              }
            }
          }
        }
      }
    }
    """

    variables = {
        "boardId": [str(board_id)],    
        "groupId": [str(group_id)],  
        "limit": limit
    }

    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }

    response = requests.post(
        "https://api.monday.com/v2",
        json={"query": query, "variables": variables},
        headers=headers
    )

    if response.status_code != 200:
        raise Exception(f"Query failed with status code {response.status_code}: {response.text}")

    data = response.json()

    if 'errors' in data:
        error_messages = "\n".join([error['message'] for error in data['errors']])
        raise Exception(f"GraphQL Errors:\n{error_messages}")

    boards = data.get('data', {}).get('boards', [])
    if not boards:
        raise Exception(f"No boards found with ID {board_id}.")

    board = boards[0]
    groups = board.get('groups', [])
    if not groups:
        raise Exception(f"No groups found with ID '{group_id}' in board {board_id}.")

    group = groups[0]
    items_page = group.get('items_page', {})
    items = items_page.get('items', [])

    return items

def export_items_to_csv(items, filename):
    """
    Exports fetched items to a CSV file.

    Args:
        items (list): List of items to export.
        filename (str): The name of the CSV file.
    """
    if not items:
        return

    headers = ['Item ID', 'Item Name']
    column_ids = []
    for column in items[0]['column_values']:
        headers.append(column['id'])
        column_ids.append(column['id'])

    with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()

        for item in items:
            row = {
                'Item ID': item['id'],
                'Item Name': item['name']
            }
            for column in item['column_values']:
                row[column['id']] = column.get('text', '')
            writer.writerow(row)

def fetch_items_recursive(board_id, group_id, api_key, limit=500):
    """
    Recursively fetches all items from a specific group within a Monday.com board using cursor-based pagination.
    
    Args:
        board_id (str): The ID of the board.
        group_id (str): The ID of the group.
        api_key (str): Your Monday.com API key.
        limit (int, optional): Number of items to fetch per request. Defaults to 500.
    
    Returns:
        list: A complete list of all items in the group.
    """
    all_items = []
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }

    initial_query = """
    query ($boardId: [ID!]!, $groupId: [String!]!, $limit: Int!) {
      boards(ids: $boardId) {
        groups(ids: $groupId) {
          id
          title
          items_page(limit: $limit) {
            cursor
            items {
              id
              name
              column_values {
                id
                text
              }
            }
          }
        }
      }
    }
    """

    variables = {
        "boardId": [str(board_id)],
        "groupId": [str(group_id)],
        "limit": limit
    }

    response = requests.post(
        "https://api.monday.com/v2",
        json={"query": initial_query, "variables": variables},
        headers=headers
    )

    if response.status_code != 200:
        raise Exception(f"Initial query failed with status code {response.status_code}: {response.text}")

    data = response.json()

    if 'errors' in data:
        error_messages = "\n".join([error['message'] for error in data['errors']])
        raise Exception(f"GraphQL Errors in initial query:\n{error_messages}")

    try:
        group = data['data']['boards'][0]['groups'][0]
        items_page = group.get('items_page', {})
        items = items_page.get('items', [])
        all_items.extend(items)
        cursor = items_page.get('cursor')
    except (IndexError, KeyError) as e:
        raise Exception(f"Error parsing initial response: {e}")

    while cursor:
        next_query = """
        query ($limit: Int!, $cursor: String!) {
          next_items_page(limit: $limit, cursor: $cursor) {
            cursor
            items {
              id
              name
              column_values {
                id
                text
              }
            }
          }
        }
        """

        next_variables = {
            "limit": limit,
            "cursor": cursor
        }

        response = requests.post(
            "https://api.monday.com/v2",
            json={"query": next_query, "variables": next_variables},
            headers=headers
        )

        if response.status_code != 200:
            raise Exception(f"Next items query failed with status code {response.status_code}: {response.text}")

        data = response.json()

        if 'errors' in data:
            error_messages = "\n".join([error['message'] for error in data['errors']])
            raise Exception(f"GraphQL Errors in next_items_page query:\n{error_messages}")

        try:
            next_page = data['data']['next_items_page']
            items = next_page.get('items', [])
            all_items.extend(items)
            cursor = next_page.get('cursor')
        except (KeyError, TypeError) as e:
            raise Exception(f"Error parsing next_items_page response: {e}")

    return all_items

def fetch_and_export_all_groups(board_id, group_list, name_list, api_key, limit=500):
    """
    Fetches items from all specified groups and exports them to corresponding CSV files.

    Args:
        board_id (str): The ID of the board.
        group_list (list): List of group IDs to fetch.
        name_list (list): List of filenames for each group.
        api_key (str): Your Monday.com API key.
        limit (int, optional): Number of items to fetch per request. Defaults to 500.
    """
    groups = fetch_groups(board_id, api_key)
    group_dict = {group['id']: group for group in groups}

    for group_id, filename in tqdm(zip(group_list, name_list), total=len(group_list), desc="Fetching Groups"):
        if group_id not in group_dict:
            # Optionally, handle missing groups as needed
            continue

        items = fetch_items_recursive(board_id, group_id, api_key, limit)
        export_items_to_csv(items, filename)
