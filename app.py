import sys
import requests
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Sales Dashboard", layout="wide")

def fetch_groups(board_id, api_key):
    """
    Fetches groups from a specified Monday.com board.
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
        st.error(f"Query failed with status code {response.status_code}")
        st.stop()

    data = response.json()

    if 'errors' in data:
        st.error("GraphQL Errors:")
        for error in data['errors']:
            st.error(error['message'])
        st.stop()

    boards = data.get('data', {}).get('boards', [])
    if not boards:
        st.error(f"No boards found with ID {board_id}.")
        st.stop()

    board = boards[0]
    groups = board.get('groups', [])

    if not groups:
        st.error(f"No groups found in board {board_id}.")
        st.stop()

    return groups

def fetch_items_recursive(board_id, group_id, api_key, limit=500):
    """
    Recursively fetches all items from a specific group within a Monday.com board using cursor-based pagination.
    """
    all_items = []
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }

    # Define the first query with the cursor method
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

    # Perform the first request to get the first set of items
    response = requests.post(
        "https://api.monday.com/v2",
        json={"query": initial_query, "variables": variables},
        headers=headers
    )

    if response.status_code != 200:
        st.error(f"Initial query failed with status code {response.status_code}")
        st.stop()

    data = response.json()

    if 'errors' in data:
        st.error("GraphQL Errors in initial query:")
        for error in data['errors']:
            st.error(error['message'])
        st.stop()

    # Extract items and cursor from the first response
    try:
        group = data['data']['boards'][0]['groups'][0]
        items_page = group.get('items_page', {})
        items = items_page.get('items', [])
        all_items.extend(items)
        cursor = items_page.get('cursor')
    except (IndexError, KeyError) as e:
        st.error(f"Error parsing initial response: {e}")
        st.stop()

    # Loop to fetch next pages using next_items_page method
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
            st.error(f"Next items query failed with status code {response.status_code}")
            st.stop()

        data = response.json()

        if 'errors' in data:
            st.error("GraphQL Errors in next_items_page query:")
            for error in data['errors']:
                st.error(error['message'])
            st.stop()

        # Extract items and cursor from the next_items_page response
        try:
            next_page = data['data']['next_items_page']
            items = next_page.get('items', [])
            all_items.extend(items)
            cursor = next_page.get('cursor')
        except (KeyError, TypeError) as e:
            st.error(f"Error parsing next_items_page response: {e}")
            st.stop()

    return all_items

def items_to_dataframe(items):
    """
    Converts a list of items to a pandas DataFrame.
    """
    if not items:
        st.warning("No items to convert.")
        return pd.DataFrame()
    
    data = []
    column_ids = [column['id'] for column in items[0]['column_values']]
    headers = ['Item ID', 'Item Name'] + column_ids

    for item in items:
        row = {
            'Item ID': item['id'],
            'Item Name': item['name']
        }
        for column in item['column_values']:
            row[column['id']] = column.get('text', '')
        data.append(row)
    
    df = pd.DataFrame(data, columns=headers)
    return df

@st.cache_data(ttl=3600)
def fetch_data():
    """
    Fetches data from Monday.com and returns a dictionary of DataFrames.
    """
    # Data fetching and storage
    BOARD_ID = "6942829967"        # Board ID 
    group_list = ["topics", "new_group34578__1", "new_group27351__1",
                  "new_group54376__1", "new_group64021__1", "new_group65903__1", "new_group62617__1"]    
    name_list = ["scheduled", "unqualified", "won", "cancelled", "noshow", "proposal", "lost"]
 
    LIMIT = 500                    

    # Fetch API key from st.secrets
    try:
        api_key = st.secrets["MONDAY_API_KEY"]
    except KeyError:
        st.error("Error: MONDAY_API_KEY is not set in secrets.toml.")
        st.stop()

    # Fetch all groups and find target group by ID
    groups = fetch_groups(BOARD_ID, api_key)
    dataframes = {}
    total_groups = len(group_list)
    progress_bar = st.progress(0)
    for i, item in enumerate(group_list):
        target_group = None
        for group in groups:
            if group['id'] == item:
                target_group = group
                break
        if not target_group:
            st.error(f"Group with ID '{item}' not found in board {BOARD_ID}.")
            st.stop()
        st.write(f"Fetching items from Group: {target_group['title']} (ID: {target_group['id']})")
        # Fetch items from the target group
        items = fetch_items_recursive(BOARD_ID, target_group['id'], api_key, LIMIT)
        df_items = items_to_dataframe(items)
        dataframes[name_list[i]] = df_items
        progress_bar.progress(int((i+1) / total_groups * 100))

    # Rename columns in each dataframe
    columns_with_titles = {
        'name': 'Name',
        'auto_number__1': 'Auto number',
        'person': 'Owner',
        'last_updated__1': 'Last updated',
        'link__1': 'Linkedin',
        'phone__1': 'Phone',
        'email__1': 'Email',
        'text7__1': 'Company',
        'date4': 'Sales Call Date',
        'status9__1': 'Follow Up Tracker',
        'notes__1': 'Notes',
        'interested_in__1': 'Interested In',
        'status4__1': 'Plan Type',
        'numbers__1': 'Deal Value',
        'status6__1': 'Email Template #1',
        'dup__of_email_template__1': 'Email Template #2',
        'status__1': 'Deal Status',
        'status2__1': 'Send Panda Doc?',
        'utm_source__1': 'UTM Source',
        'date__1': 'Deal Status Date',
        'utm_campaign__1': 'UTM Campaign',
        'utm_medium__1': 'UTM Medium',
        'utm_content__1': 'UTM Content',
        'link3__1': 'UTM LINK',
        'lead_source8__1': 'Lead Source',
        'color__1': 'Channel FOR FUNNEL METRICS',
        'subitems__1': 'Subitems',
        'date5__1': 'Date Created'
    }
    for key in dataframes.keys():
        df = dataframes[key]
        df.rename(columns=columns_with_titles, inplace=True)
        dataframes[key] = df

    return dataframes

def process_data(dataframes, st_date, end_date, date_filter_column):
    """
    Processes the dataframes to calculate metrics and returns a DataFrame.
    Now allows filtering based on 'Date Created' or 'Sales Call Date'.
    """
    op_cancelled = dataframes['cancelled']
    op_lost = dataframes['lost']
    op_noshow = dataframes['noshow']
    op_proposal = dataframes['proposal']
    op_scheduled = dataframes['scheduled']
    op_unqualified = dataframes['unqualified']
    op_won = dataframes['won']
    list_all = [op_cancelled, op_lost, op_noshow, op_proposal, op_scheduled, op_unqualified, op_won]
    all_deal = pd.concat(list_all, ignore_index=True)

    # Convert date columns to datetime
    date_columns = ['Sales Call Date', 'Date Created']
    for df in list_all + [all_deal]:
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

    # Initialize DataFrame
    df = pd.DataFrame()
    df['Owner'] = pd.Series(all_deal['Owner'].dropna()).unique()

    # Filtering based on date
    st_date = pd.to_datetime(st_date)
    end_date = pd.to_datetime(end_date)

    def filter_df(df_subset):
        if date_filter_column in df_subset.columns:
            filtered_df = df_subset[
                (df_subset[date_filter_column] >= st_date) &
                (df_subset[date_filter_column] <= end_date)
            ]
            return filtered_df
        else:
            return pd.DataFrame(columns=df_subset.columns)

    # Calculate New Calls Booked per owner
    new_calls_booked = filter_df(all_deal).groupby('Owner').size()
    df['New Calls Booked'] = df['Owner'].map(new_calls_booked).fillna(0).astype(int)

    # Sales Call Taken
    df_subset = pd.concat([op_unqualified, op_proposal, op_won, op_lost], ignore_index=True)
    sales_calls_taken = filter_df(df_subset).groupby('Owner').size()
    df['Sales Call Taken'] = df['Owner'].map(sales_calls_taken).fillna(0).astype(int)

    # Show Rate 
    show_rate = sales_calls_taken / df.set_index('Owner')['New Calls Booked']
    show_rate = show_rate.replace([np.inf, -np.inf], 0).fillna(0)
    df['Show Rate'] = df['Owner'].map(show_rate).fillna(0)

    # Unqualified Rate  
    df_subset = op_unqualified.copy()
    uq = filter_df(df_subset).groupby('Owner').size()
    uq_rate = uq / df.set_index('Owner')['New Calls Booked']
    uq_rate = uq_rate.replace([np.inf, -np.inf], 0).fillna(0)
    df['Unqualified Rate'] = df['Owner'].map(uq_rate).fillna(0)

    # Cancellation Rate  
    df_subset = op_cancelled.copy()
    canc = filter_df(df_subset).groupby('Owner').size()
    canc_rate = canc / df.set_index('Owner')['New Calls Booked']
    canc_rate = canc_rate.replace([np.inf, -np.inf], 0).fillna(0)
    df['Cancellation Rate'] = df['Owner'].map(canc_rate).fillna(0)

    # Proposal Rate  
    df_subset = op_proposal.copy()
    prop = filter_df(df_subset).groupby('Owner').size()
    prop_rate = prop / df.set_index('Owner')['New Calls Booked']
    prop_rate = prop_rate.replace([np.inf, -np.inf], 0).fillna(0)
    df['Proposal Rate'] = df['Owner'].map(prop_rate).fillna(0)

    # Close Rate  
    df_subset = op_won.copy()
    close = filter_df(df_subset).groupby('Owner').size()
    close_rate = close / df.set_index('Owner')['New Calls Booked']
    close_rate = close_rate.replace([np.inf, -np.inf], 0).fillna(0)
    df['Close Rate'] = df['Owner'].map(close_rate).fillna(0)

    # Close Rate (Show)
    close_rate_show = close / df.set_index('Owner')['Sales Call Taken']
    close_rate_show = close_rate_show.replace([np.inf, -np.inf], 0).fillna(0)
    df['Close Rate(Show)'] = df['Owner'].map(close_rate_show).fillna(0)

    # Close Rate (MQL)
    df_subset2 = op_proposal.copy()
    prop_show_mql = filter_df(df_subset2).groupby('Owner').size()
    close_show_rate_mql = close / prop_show_mql
    close_show_rate_mql = close_show_rate_mql.replace([np.inf, -np.inf], 0).fillna(0)
    df['Close Rate(MQL)'] = df['Owner'].map(close_show_rate_mql).fillna(0)

    # Closed Revenue 
    df_subset = op_won.copy()
    close_rev = filter_df(df_subset)
    close_rev = close_rev.copy()  # Make a copy to avoid SettingWithCopyWarning
    close_rev['Deal Value'] = pd.to_numeric(close_rev['Deal Value'], errors='coerce').fillna(0)
    owner_sum = close_rev.groupby('Owner')['Deal Value'].sum()
    df['Closed Revenue'] = df['Owner'].map(owner_sum).fillna(0)

    # Revenue per Call 
    rev_per_call = owner_sum / df.set_index('Owner')['New Calls Booked']
    rev_per_call = rev_per_call.replace([np.inf, -np.inf], 0).fillna(0)
    df['Revenue Per Call'] = df['Owner'].map(rev_per_call).fillna(0)

    # Revenue per Showed Up 
    rev_per_showed_up = owner_sum / df.set_index('Owner')['Sales Call Taken']
    rev_per_showed_up = rev_per_showed_up.replace([np.inf, -np.inf], 0).fillna(0)
    df['Revenue Per Showed Up'] = df['Owner'].map(rev_per_showed_up).fillna(0)

    # Revenue Per Proposal
    rev_per_proposal = owner_sum / prop
    rev_per_proposal = rev_per_proposal.replace([np.inf, -np.inf], 0).fillna(0)
    df['Revenue Per Proposal'] = df['Owner'].map(rev_per_proposal).fillna(0)

    # Pipeline Revenue 
    df_subset = op_proposal.copy()
    pipeline_rev = filter_df(df_subset)
    pipeline_rev = pipeline_rev.copy()
    pipeline_rev['Deal Value'] = pd.to_numeric(pipeline_rev['Deal Value'], errors='coerce').fillna(0)
    owner_sum_prop = pipeline_rev.groupby('Owner')['Deal Value'].sum()
    df['Pipeline Revenue'] = df['Owner'].map(owner_sum_prop).fillna(0)

    return df

def main():
    st.title("Sales Dashboard")
    st.markdown("This dashboard automatically fetches data from Monday.com, processes it, and provides visual insights.")

    # Fetch data
    with st.spinner("Fetching data..."):
        dataframes = fetch_data()
    st.success("Data fetched successfully.")

    # Date range input
    st.sidebar.subheader("Filter Options")

    # Date Filter Column Selection
    date_filter_column = st.sidebar.selectbox(
        "Select Date Filter Column",
        options=['Date Created', 'Sales Call Date'],
        index=0
    )

    st_date = st.sidebar.date_input("Start Date", value=pd.to_datetime('2024-10-01'))
    end_date = st.sidebar.date_input("End Date", value=pd.to_datetime('2024-10-25'))

    # Process data and store in session_state
    if ('df' not in st.session_state or 
        st.session_state.st_date != st_date or 
        st.session_state.end_date != end_date or 
        st.session_state.date_filter_column != date_filter_column):
        with st.spinner("Processing data..."):
            st.session_state.df = process_data(dataframes, st_date, end_date, date_filter_column)
            st.session_state.st_date = st_date
            st.session_state.end_date = end_date
            st.session_state.date_filter_column = date_filter_column
        st.success("Data processed successfully.")
    df = st.session_state.df

    # Display metrics
    st.subheader("Metrics Table")
    st.dataframe(df.style.format({
        'Show Rate': "{:.2%}",
        'Unqualified Rate': "{:.2%}",
        'Cancellation Rate': "{:.2%}",
        'Proposal Rate': "{:.2%}",
        'Close Rate': "{:.2%}",
        'Close Rate(Show)': "{:.2%}",
        'Close Rate(MQL)': "{:.2%}",
        'Closed Revenue': "${:,.2f}",
        'Revenue Per Call': "${:,.2f}",
        'Revenue Per Showed Up': "${:,.2f}",
        'Revenue Per Proposal': "${:,.2f}",
        'Pipeline Revenue': "${:,.2f}",
    }), use_container_width=True)

    # Select owner to view metrics
    st.subheader("Owner Specific Metrics")
    owner_list = df['Owner'].unique()
    selected_owner = st.selectbox("Select Owner", owner_list)
    owner_metrics = df[df['Owner'] == selected_owner].reset_index(drop=True)

    # Ensure that all values are strings for display
    owner_metrics_display = owner_metrics.T.reset_index()
    owner_metrics_display.columns = ['Metric', 'Value']
    owner_metrics_display['Value'] = owner_metrics_display['Value'].astype(str)

    st.write(f"Metrics for **{selected_owner}**:")
    st.table(owner_metrics_display)

    # Visualization
    st.subheader("Visualizations")

    # Interactive Pie Chart for Closed Revenue Distribution
    st.markdown("### Closed Revenue Distribution")
    pie_data = df[['Owner', 'Closed Revenue']]
    fig_pie = px.pie(pie_data, names='Owner', values='Closed Revenue', hole=0.4)
    st.plotly_chart(fig_pie, use_container_width=True)

    # Bar Chart for Revenue Per Call
    st.markdown("### Revenue Per Call per Owner")
    bar_data = df[['Owner', 'Revenue Per Call']]
    fig_bar = px.bar(bar_data, x='Owner', y='Revenue Per Call', text='Revenue Per Call')
    fig_bar.update_layout(xaxis_title='Owner', yaxis_title='Revenue Per Call', showlegend=False)
    fig_bar.update_traces(texttemplate='$%{text:.2f}', textposition='outside')
    st.plotly_chart(fig_bar, use_container_width=True)

    # Additional Bar Charts
    st.markdown("### Closed Revenue per Owner")
    chart_data = df[['Owner', 'Closed Revenue']]
    fig_closed_rev = px.bar(chart_data, x='Owner', y='Closed Revenue', text='Closed Revenue')
    fig_closed_rev.update_layout(xaxis_title='Owner', yaxis_title='Closed Revenue', showlegend=False)
    fig_closed_rev.update_traces(texttemplate='$%{text:.2f}', textposition='outside')
    st.plotly_chart(fig_closed_rev, use_container_width=True)

    st.markdown("### Show Rate per Owner")
    chart_data = df[['Owner', 'Show Rate']]
    fig_show_rate = px.bar(chart_data, x='Owner', y='Show Rate', text='Show Rate')
    fig_show_rate.update_layout(xaxis_title='Owner', yaxis_title='Show Rate', showlegend=False)
    fig_show_rate.update_yaxes(tickformat=".0%")
    fig_show_rate.update_traces(texttemplate='%{text:.2%}', textposition='outside')
    st.plotly_chart(fig_show_rate, use_container_width=True)

if __name__ == "__main__":
    main()
