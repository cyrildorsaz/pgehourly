import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from utils import fetch_energy_data, process_pricing_data

# Page configuration
st.set_page_config(
    page_title="Energy Pricing Dashboard",
    page_icon="⚡",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .stProgress > div > div > div > div {
        background-color: #1f77b4;
    }
    .big-font {
        font-size:24px !important;
    }
    </style>
""", unsafe_allow_html=True)

# Title
st.title("⚡ Energy Pricing Dashboard")

# Add location and circuit information
st.markdown("""
<div style='background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem; margin-bottom: 1rem;'>
    <p style='margin: 0; font-size: 1.1rem;'>
        📍 <strong>Location:</strong> San Francisco<br>
        🔌 <strong>Circuit ID:</strong> 012640401
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# Date range selector
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input(
        "Start Date",
        datetime.now().date()
    )
with col2:
    end_date = st.date_input(
        "End Date",
        datetime.now().date(),
       # min_value=start_date  # Only keep this constraint to ensure end_date isn't before start_date I removed it because it was causing an error. 
    )

def fetch_and_display_data():
    try:
        with st.spinner('Fetching data...'):
            # Convert dates to required format (YYYYMMDD)
            start_str = start_date.strftime('%Y%m%d')
            end_str = end_date.strftime('%Y%m%d')

            # Fetch data
            data = fetch_energy_data(start_str, end_str)

            if data:
                # Process the data
                df = process_pricing_data(data)

                # Display summary statistics
                st.subheader("Summary Statistics")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Average Price", f"${df['price'].mean():.2f}/kWh")
                with col2:
                    st.metric("Max Price", f"${df['price'].max():.2f}/kWh")
                with col3:
                    st.metric("Min Price", f"${df['price'].min():.2f}/kWh")

                # Plot the data
                st.subheader("Price Trends")
                fig = px.line(
                    df,
                    x='datetime',
                    y='price',
                    title='Energy Pricing Over Time',
                    labels={'datetime': 'Date & Time', 'price': 'Price ($/kWh)'}
                )
                fig.update_layout(
                    xaxis_title="Date & Time",
                    yaxis_title="Price ($/kWh)",
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)

                # Display raw data
                st.subheader("Raw Data")

                # Create a style function for the dataframe
                def highlight_prices(df):
                    # Get indices of 4 lowest and 4 highest prices
                    lowest_indices = df['price'].nsmallest(4).index
                    highest_indices = df['price'].nlargest(4).index

                    # Create empty style DataFrame
                    styles = pd.DataFrame('', index=df.index, columns=df.columns)

                    # Apply background colors
                    styles.loc[lowest_indices, 'price'] = 'background-color: #90EE90'  # light green
                    styles.loc[highest_indices, 'price'] = 'background-color: #FFA500'  # orange

                    return styles

                # Apply styling and display
                styled_df = df.style.apply(highlight_prices, axis=None)
                st.dataframe(
                    styled_df,
                    hide_index=True,
                    use_container_width=True,
                    height=(len(df) + 1) * 35 + 3  # Dynamically set height based on number of rows
                )

                # Download button
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download Data as CSV",
                    data=csv,
                    file_name=f"energy_pricing_{start_str}_to_{end_str}.csv",
                    mime="text/csv"
                )

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")

# Fetch data automatically on page load
fetch_and_display_data()

# Manual refresh button
if st.button("Refresh Data"):
    fetch_and_display_data()

# Footer
st.markdown("---")
st.markdown("Data provided by GridX Energy Pricing API")
