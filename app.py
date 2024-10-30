import os
import sys
import requests
import pandas as pd
import numpy as np
# from dotenv import load_dotenv
import streamlit as st
import plotly.express as px
from monday_extract_groups import fetch_items_recursive, fetch_groups

# Load environment variables
# load_dotenv()

# Configure Streamlit page
st.set_page_config(page_title="Sales Dashboard", layout="wide")

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
    BOARD_ID = "6942829967"  # Replace with your actual Board ID
    group_list = [
        "topics",
        "new_group34578__1",
        "new_group27351__1",
        "new_group54376__1",
        "new_group64021__1",
        "new_group65903__1",
        "new_group62617__1"
    ]    
    name_list = [
        "scheduled",
        "unqualified",
        "won",
        "cancelled",
        "noshow",
        "proposal",
        "lost"
    ]
    LIMIT = 500  # Items limit per group

    # Fetch API key from secrets
    try:
        api_key = st.secrets["MONDAY_API_KEY"]
    except KeyError:
        st.error("Error: MONDAY_API_KEY is not set in secrets.toml.")
        st.stop()

    # Fetch all groups from the board
    groups = fetch_groups(BOARD_ID, api_key)
    dataframes = {}

    total_groups = len(group_list)
    progress_bar = st.progress(0.0)
    progress_step = 1.0 / total_groups  # Corrected to ensure values are between 0.0 and 1.0

    for i, (group_id, group_name) in enumerate(zip(group_list, name_list)):
        # Find the target group
        target_group = next((group for group in groups if group['id'] == group_id), None)
        if not target_group:
            st.error(f"Group with ID '{group_id}' not found in board {BOARD_ID}.")
            st.stop()
        
        st.write(f"Fetching items from Group: **{target_group['title']}** (ID: {target_group['id']})")
        
        # Fetch items from the target group
        items = fetch_items_recursive(BOARD_ID, target_group['id'], api_key, LIMIT)
        df_items = items_to_dataframe(items)
        dataframes[group_name] = df_items
        
        # Update progress bar
        progress_bar.progress((i + 1) * progress_step)

    # Define column renaming mapping
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

    # Rename columns in each dataframe
    for key in dataframes.keys():
        df = dataframes[key]
        df.rename(columns=columns_with_titles, inplace=True)
        dataframes[key] = df

    return dataframes

def process_data(dataframes, st_date, end_date, column):
    # Extract individual DataFrames
    op_cancelled = dataframes['cancelled']
    op_lost = dataframes['lost']
    op_noshow = dataframes['noshow']
    op_proposal = dataframes['proposal']
    op_scheduled = dataframes['scheduled']
    op_unqualified = dataframes['unqualified']
    op_won = dataframes['won']
    
    # Combine all DataFrames
    list_all = [op_cancelled, op_lost, op_noshow, op_proposal, op_scheduled, op_unqualified, op_won]
    all_deal = pd.concat(list_all, ignore_index=True)
    
    def filter_date(df, dtcolumn):
        """
        Filters the dataframe based on the provided date range.
        """
        df = df.copy()
        df[dtcolumn] = df[dtcolumn].apply(
            lambda x: pd.to_datetime(x, errors='coerce').date() if pd.notna(x) else pd.NaT
        )

        start_date = pd.to_datetime(st_date).date()
        end_date_ = pd.to_datetime(end_date).date()

        # Filter rows where the date is within the specified range
        return df[(df[dtcolumn] >= start_date) & (df[dtcolumn] <= end_date_)]

    # Initialize a new DataFrame inside the function
    df = pd.DataFrame()

    # Initial Column 
    df['Owner'] = pd.Series(all_deal['Owner'].dropna()).unique()

    # Formulas 
    subset = all_deal
    st.write("All deals subset shape:", subset.shape)
    
    # Calculate New Calls Booked per owner
    new_calls_booked = filter_date(subset, column).groupby('Owner').size()

    # New Calls Booked = Sum of all opportunities in all deal stages filtered by date created
    df['New Calls Booked'] = df['Owner'].map(new_calls_booked).fillna(0).astype(int)
    total_ncb = df['New Calls Booked'].sum()  # Total New Calls Booked

    # Sales Call Taken
    df_subset = pd.concat([op_unqualified, op_proposal, op_won, op_lost], ignore_index=True)
    sales_calls_taken = filter_date(df_subset, column).groupby('Owner').size()
    df['Sales Call Taken'] = df['Owner'].map(sales_calls_taken).fillna(0).astype(int)
    total_sct = df['Sales Call Taken'].sum()

    # Show Rate 
    show_rate = sales_calls_taken / df.set_index('Owner')['New Calls Booked']
    df['Show Rate %'] = df['Owner'].map(show_rate).fillna(0) * 100
    total_show = (total_sct / total_ncb) * 100 if total_ncb != 0 else 0

    # Unqualified Rate  
    df_subset = op_unqualified.copy()
    uq = filter_date(df_subset, column).groupby('Owner').size()
    uq_rate = uq / df.set_index('Owner')['New Calls Booked']
    df['Unqualified Rate %'] = df['Owner'].map(uq_rate).fillna(0) * 100
    total_uq_rate = (filter_date(df_subset, column).shape[0] / total_ncb) * 100 if total_ncb != 0 else 0

    # Cancellation Rate  
    df_subset = op_cancelled.copy()
    canc = filter_date(df_subset, column).groupby('Owner').size()
    canc_rate = canc / df.set_index('Owner')['New Calls Booked']
    canc_rate = canc_rate.replace([np.inf, -np.inf], 0).fillna(0)
    df['Cancellation Rate %'] = df['Owner'].map(canc_rate).fillna(0) * 100
    total_canc_rate = (filter_date(df_subset, column).shape[0] / total_ncb) * 100 if total_ncb != 0 else 0

    # Proposal Rate  
    df_subset = op_proposal.copy()
    prop = filter_date(df_subset, column).groupby('Owner').size()
    prop_rate = prop / df.set_index('Owner')['New Calls Booked']
    prop_rate = prop_rate.replace([np.inf, -np.inf], 0).fillna(0)
    df['Proposal Rate %'] = df['Owner'].map(prop_rate).fillna(0) * 100
    total_prop_rate = ((filter_date(op_proposal.copy(), column).shape[0] + filter_date(op_won.copy(), column).shape[0]) / total_ncb) * 100 if total_ncb != 0 else 0

    # Close Rate  
    df_subset = op_won.copy()
    close = filter_date(df_subset, column).groupby('Owner').size()
    close_rate = close / df.set_index('Owner')['New Calls Booked']
    close_rate = close_rate.replace([np.inf, -np.inf], 0).fillna(0)
    df['Close Rate %'] = df['Owner'].map(close_rate).fillna(0) * 100
    total_close = filter_date(df_subset, column).shape[0]
    total_close_rate = (total_close / total_ncb) * 100 if total_close != 0 else 0

    # Close Rate (Show)
    close_rate_show = close / df.set_index('Owner')['Sales Call Taken']
    close_rate_show = close_rate_show.replace([np.inf, -np.inf], 0).fillna(0)
    df['Close Rate(Show) %'] = df['Owner'].map(close_rate_show).fillna(0) * 100
    total_close_rate_show = (total_close/total_sct ) * 100 if total_close != 0 else 0

    # Close Rate (MQL)
    df_subset2 = op_proposal.copy()
    prop_show_mql = filter_date(df_subset2, column).groupby('Owner').size()
    close_show_rate_mql = close / prop_show_mql
    close_show_rate_mql = close_show_rate_mql.replace([np.inf, -np.inf], 0).fillna(0)
    df['Close Rate(MQL) %'] = df['Owner'].map(close_show_rate_mql).fillna(0) * 100
    total_proposal_mql = filter_date(df_subset2, column).shape[0] + filter_date(op_won.copy(), column).shape[0]
    total_close_rate_mql = (total_close / total_proposal_mql ) * 100 if total_close != 0 else 0

    # Closed Revenue 
    df_subset = op_won.copy()
    close_rev = filter_date(df_subset, column)
    close_rev = close_rev.copy()  # Make a copy to avoid SettingWithCopyWarning
    close_rev['Deal Value'] = pd.to_numeric(close_rev['Deal Value'], errors='coerce').fillna(0)
    owner_sum = close_rev.groupby('Owner')['Deal Value'].sum()
    df['Closed Revenue $'] = df['Owner'].map(owner_sum).fillna(0)
    total_cr = df['Closed Revenue $'].sum()

    # Revenue per Call 
    rev_per_call = owner_sum / df.set_index('Owner')['New Calls Booked']
    rev_per_call = rev_per_call.replace([np.inf, -np.inf], 0).fillna(0)
    df['Revenue Per Call $'] = df['Owner'].map(rev_per_call).fillna(0)
    total_rev_per_call = df['Revenue Per Call $'].sum()

    # Revenue per Showed Up 
    rev_per_showed_up = owner_sum / df.set_index('Owner')['Sales Call Taken']
    rev_per_showed_up = rev_per_showed_up.replace([np.inf, -np.inf], 0).fillna(0)
    df['Revenue Per Showed Up $'] = df['Owner'].map(rev_per_showed_up).fillna(0)
    total_rev_per_showedup = df['Revenue Per Showed Up $'].sum()

    # Revenue Per Proposal
    rev_per_proposal = owner_sum / prop
    rev_per_proposal = rev_per_proposal.replace([np.inf, -np.inf], 0).fillna(0)
    df['Revenue Per Proposal $'] = df['Owner'].map(rev_per_proposal).fillna(0) * 1  # Ensure it's numeric
    total_rev_per_proposal = df['Revenue Per Proposal $'].sum()

    # Pipeline Revenue 
    df_subset = op_proposal.copy()
    pipeline_rev = filter_date(df_subset, column)
    pipeline_rev = pipeline_rev.copy()
    pipeline_rev['Deal Value'] = pd.to_numeric(pipeline_rev['Deal Value'], errors='coerce').fillna(0)
    owner_sum_prop = pipeline_rev.groupby('Owner')['Deal Value'].sum()
    df['Pipeline Revenue $'] = df['Owner'].map(owner_sum_prop).fillna(0)
    total_pipeline_rev = df['Pipeline Revenue $'].sum()

    # Define the totals array
    totals = [
        total_ncb,
        total_sct,                # Sales Call Taken
        total_prop_rate,          # Proposal Rate %
        total_show,               # Show Rate %
        total_uq_rate,            # Unqualified Rate %
        total_canc_rate,          # Cancellation Rate %
        total_close_rate,         # Close Rate %
        total_close_rate_show,    # Close Rate(Show) %
        total_close_rate_mql,     # Close Rate(MQL) %
        total_cr,                 # Closed Revenue $
        total_rev_per_call,       # Revenue Per Call $
        total_rev_per_showedup,   # Revenue Per Showed Up $
        total_rev_per_proposal,   # Revenue Per Proposal $
        total_pipeline_rev        # Pipeline Revenue $
    ]

    # List of columns corresponding to totals array
    columns_for_totals = [
        'New Calls Booked',
        'Sales Call Taken',
        'Proposal Rate %',
        'Show Rate %',
        'Unqualified Rate %',
        'Cancellation Rate %',
        'Close Rate %',
        'Close Rate(Show) %',
        'Close Rate(MQL) %',
        'Closed Revenue $',
        'Revenue Per Call $',
        'Revenue Per Showed Up $',
        'Revenue Per Proposal $',
        'Pipeline Revenue $'
    ]

    # Create a dictionary for the total row
    total_dict = {col: val for col, val in zip(columns_for_totals, totals)}
    total_dict['Owner'] = 'Total'

    # Create a DataFrame for the total row
    total_df = pd.DataFrame([total_dict])

    # Append the total row to the original DataFrame
    df = pd.concat([df, total_df], ignore_index=True)

    return df

def main():
    st.title("Sales Dashboard : Monday.com")
    st.markdown("""
    This dashboard automatically fetches data from Monday.com, processes it, and provides visual insights into your sales performance.
    """)

    # Initialize session state for 'dataframes' if not already present
    if 'dataframes' not in st.session_state:
        st.session_state['dataframes'] = None

    # Fetch Data Button
    if st.sidebar.button('Fetch Data'):
        # Fetch data with a spinner
        with st.spinner("Fetching data from Monday.com..."):
            try:
                st.session_state['dataframes'] = fetch_data()
                st.success("Data fetched successfully!")
            except Exception as e:
                st.error(f"An error occurred while fetching data: {e}")
    
    # Sidebar for filter options
    st.sidebar.header("Filter Options")

    # Date Filter Column Selection
    date_filter_column = st.sidebar.selectbox(
        "Select Date Filter Column",
        options=['Date Created', 'Sales Call Date'],
        index=0
    )

    # Date Range Inputs
    st_date = st.sidebar.date_input("Start Date", value=pd.to_datetime('2024-10-01'))
    en_date = st.sidebar.date_input("End Date", value=pd.to_datetime('2024-10-25'))

    # Validate date inputs
    if st_date > en_date:
        st.sidebar.error("Error: Start Date must be before End Date.")
        st.stop()

    # Process Data Button
    if st.sidebar.button('Process Data'):
        if st.session_state['dataframes'] is None:
            st.error("Please fetch data first by clicking the 'Fetch Data' button.")
            st.stop()
        with st.spinner("Processing data..."):
            try:
                processed_df = process_data(
                    st.session_state['dataframes'],
                    st_date,
                    en_date,
                    date_filter_column
                )
                st.success("Data processed successfully!")
            except KeyError as e:
                st.error(f"Data processing error: Missing key {e}")
                st.stop()
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")
                st.stop()

        # Display the processed dataframe
        st.subheader("Processed Data")
        st.dataframe(processed_df)

        # Visualizations 1
        st.subheader("Key Metrics")
        col1, col2 = st.columns(2)

        with col1:
            fig1 = px.bar(
                processed_df, 
                x='Owner', 
                y='Closed Revenue $', 
                title="Closed Revenue by Owner",
                labels={'Closed Revenue $': 'Closed Revenue ($)'},
                color='Closed Revenue $',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            fig3 = px.bar(
                processed_df, 
                x='Owner', 
                y='Close Rate %', 
                title="Close Rate by Owner",
                labels={'Close Rate %': 'Close Rate (%)'},
                color='Close Rate %',
                color_continuous_scale='Purples'
            )
            st.plotly_chart(fig3, use_container_width=True)

        # Visualizations 2
        col1, col2 = st.columns(2)

        with col1:
            fig1 = px.bar(
                processed_df, 
                x='Owner', 
                y='New Calls Booked', 
                title="New Calls by Owner",
                labels={'New Calls Booked': 'New Sales Calls'},
                color='New Calls Booked',
                color_continuous_scale='ice'
            )
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            fig3 = px.bar(
                processed_df, 
                x='Owner', 
                y='Sales Call Taken', 
                title="Sales Call Taken by Owner",
                labels={'Sales Call Taken': 'Number of Sales Calls Taken'},
                color='Sales Call Taken',
                color_continuous_scale='inferno'
            )
            st.plotly_chart(fig3, use_container_width=True)

        # Visualizations 3
        col1, col2 = st.columns(2)

        with col1:
            fig1 = px.pie(
                processed_df, 
                names='Owner', 
                values='Closed Revenue $', 
                title="Closed Revenue Distribution by Owner",
                labels={'Closed Revenue $': 'Closed Revenue ($)'}
            )
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            fig2 = px.pie(
                processed_df, 
                names='Owner', 
                values='Revenue Per Proposal $', 
                title="Revenue Earned Per Proposal by Owner",
                labels={'Revenue Per Proposal $': 'Revenue Per Proposal ($)'}
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Additional visualizations can be added here
        st.markdown("---")
        st.subheader("Correlations in the Metrics")
        correlation = processed_df.select_dtypes(include=[np.number]).corr()
        fig_corr = px.imshow(
            correlation, 
            text_auto=True, 
            aspect="auto",
            title="Correlation Matrix"
        )
        st.plotly_chart(fig_corr, use_container_width=True)

if __name__ == "__main__":
    main()
