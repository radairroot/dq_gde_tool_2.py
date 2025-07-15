# 2025 July 11:  Created version 3 for output text file formatting , but it wasnt needed.
# Version 4 has a 1 second sleep timer before invoking the Blacklisting_API.exe command (not sure this is helpful)
# This is the latest working version with the up to date BL functions

#                                                C:\Users\RobAdair\Documents\python\blocklist_tool
     

import streamlit as st
import pandas as pd
import os
import subprocess # Import subprocess module to run external commands
import csv # Import csv module for quoting constants
from dotenv import load_dotenv # Import load_dotenv
from sqlalchemy import create_engine, text # Import create_engine and text from SQLAlchemy

# --- Streamlit Page Configuration (MUST BE THE FIRST STREAMLIT COMMAND) ---
st.set_page_config(page_title="DQ-General Data Exclusion Tool",page_icon='🤖', layout="centered")    #layout was originally "centered"

# Load environment variables from .env file
load_dotenv()


# --- Constants for File Handling ---
OUTPUT_FOLDER = "dq_output_files"
COMPOSITE_BLOCKLIST_FILE = "blocklist.text" # The final merged file for Blacklisting_API.exe

# Ensure the output folder exists
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)
    st.info(f"Created output folder: '{OUTPUT_FOLDER}'")

# --- Helper Function to Establish Database Connection Engine ---
@st.cache_resource # Cache the database connection engine
def get_rsr_conn():
    """
    Establishes and returns a SQLAlchemy engine for connecting to the RSR database.
    The database connection string is loaded from the 'RSR_CONN' environment variable.
    """
    host_str = os.getenv("RSR_CONN")
    if not host_str:
        st.error("RSR_CONN environment variable not set. Please configure your .env file.")
        st.stop() # Stop the app if the critical environment variable is missing
    try:
        engine = create_engine(host_str)
        # Test connection
        with engine.connect() as connection:
            connection.execute(text("SELECT 1")) # Use text() for literal SQL
        st.success("Database connection engine created successfully!")
        return engine
    except Exception as e:
        st.error(f"Error creating database engine or connecting: {e}")
        st.warning(f"Please ensure your RSR_CONN string in the .env file is correct.")
        st.stop() # Stop the app if connection fails
        return None # This return won't be reached due to st.stop()

# --- Function to Execute SQL Queries using SQLAlchemy Engine ---
def execute_sql(engine, sql_query, fetch_results=True):
    """Executes an SQL query using SQLAlchemy engine and optionally fetches results."""
    if not engine:
        st.error("No database engine available.")
        return None

    try:
        st.info(f"Executing SQL: `{sql_query}`")
        if fetch_results:
            # Use pandas read_sql to execute query and get DataFrame directly
            df = pd.read_sql(sql_query, engine)
            return df
        else:
            # For non-SELECT statements, execute directly with engine and commit
            with engine.connect() as connection:
                connection.execute(text(sql_query)) # Use text() for literal SQL
                connection.commit()
            return True
    except Exception as e:
        st.error(f"Error executing SQL query: {e}")
        st.warning("Ensure the SQL function exists, the CSID is valid, and the query syntax is correct.")
        return None

# --- Function to Save DataFrame to Text File ---
def save_dataframe_to_text_file(df, filename_prefix, csid, col_name):
    """Saves a DataFrame to a text file within the OUTPUT_FOLDER."""
    if df.empty:
        st.info(f"No data to save for {filename_prefix} - {col_name}.")
        return None

    file_path = os.path.join(OUTPUT_FOLDER, f"{filename_prefix}_{csid}_{col_name}.txt")
    try:
        # Assuming the Blacklisting_API.exe expects comma-separated values without header
        # Similar to how bl_tool.py outputs its data
        df.to_csv(file_path, sep=',', index=False, header=False, quoting=csv.QUOTE_NONE)
        st.success(f"Results for `{col_name}` saved to: `{file_path}`")
        return file_path
    except Exception as e:
        st.error(f"Error saving results to file '{file_path}': {e}")
        return None

# --- Function to Merge All Generated Text Files ---
def merge_output_files():
    """Merges all .txt files in OUTPUT_FOLDER into a single composite file."""
    composite_file_path = COMPOSITE_BLOCKLIST_FILE
    merged_content = []
    generated_files_count = 0

    st.subheader("Merging Output Files...")
    for filename in os.listdir(OUTPUT_FOLDER):
        if filename.endswith(".txt"):
            file_path = os.path.join(OUTPUT_FOLDER, filename)
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                    if content.strip(): # Only add content if it's not empty
                        merged_content.append(content.strip())
                        generated_files_count += 1
                st.info(f"Included '{filename}' in merge.")
            except Exception as e:
                st.warning(f"Could not read file '{filename}' during merge: {e}")

    if merged_content:
        try:
            with open(composite_file_path, 'w') as f:
                f.write("\n".join(merged_content))
            st.success(f"Successfully merged {generated_files_count} files into '{composite_file_path}'.")
            return composite_file_path
        except Exception as e:
            st.error(f"Error writing composite file '{composite_file_path}': {e}")
            return None
    else:
        st.warning("No individual output files found to merge.")
        return None


# --- Streamlit App Layout ---
def main():
    st.title("DQ-General Data Exclusion Tool")
    st.markdown("---")

    # Get database connection engine
    engine = get_rsr_conn()
    if not engine: # get_rsr_conn already calls st.stop() on error, but this is a safeguard
        st.stop()

    st.subheader("1. Enter CSID")
    # Use session state to persist CSID input across reruns
    if 'csid_input' not in st.session_state:
        st.session_state.csid_input = ""
    csid = st.text_input("Please enter the CSID:", key="csid_input_widget", value=st.session_state.csid_input).strip()
    st.session_state.csid_input = csid # Update session state with current input

    st.markdown("---")

    # Initialize session state for check_results_df if not present
    if 'check_results' not in st.session_state:
        st.session_state.check_results = pd.DataFrame() # Initialize as empty DataFrame

    # Store a list of generated file paths in session state
    if 'generated_file_paths' not in st.session_state:
        st.session_state.generated_file_paths = []

    if st.button("Run dq.fn_auto_check", use_container_width=True):
        if not csid:
            st.error("Please enter a CSID to run the check.")
        else:
            # Clear previous generated file paths and check results when running a new check
            st.session_state.generated_file_paths = []
            st.session_state.check_results = pd.DataFrame() # Clear previous results

            st.subheader(f"Running `dq.fn_auto_check` for CSID: `{csid}`")
            sql_check_function = f"SELECT * FROM dq.fn_auto_check('{csid}');"
            check_results_df = execute_sql(engine, sql_check_function, fetch_results=True)

            if check_results_df is not None and not check_results_df.empty:
                st.session_state.check_results = check_results_df # Store in session state
                st.success("`dq.fn_auto_check` executed successfully!")
            else:
                st.warning("`dq.fn_auto_check` returned no results or an error occurred.")


    # --- Section 2: Review & Run Additional Fixes (if needed) ---
    # This section will only appear if check_results_df is available in session state
    if not st.session_state.check_results.empty:
        st.subheader("`dq.fn_auto_check` Results:")
        st.dataframe(st.session_state.check_results, use_container_width=True)

        st.markdown("---")
        st.subheader("2. Review & Run Additional Fixes (if needed)")

        # Define the mapping from 'sequence' column value to its SQL fix function
        fix_function_map = {
            "10_auto": "SELECT * FROM dq.fn_dq_gde_vzw_6('{csid}') WHERE excluded IS FALSE AND collection_type_id IN (1,10)",
            "11_auto": "SELECT * FROM analytic.fn_cua_area_allevents('{csid}')",
            "12_auto": "SELECT * FROM dq.fn_dq_gde_natl_twn('{csid}') WHERE collection_type_id IN (10,11) AND excluded IS FALSE",
            "13_auto": "SELECT * FROM dq.fn_dq_gde_rb_m2m_fail('{csid}') WHERE excluded IS FALSE ORDER BY start_time",
            "14_auto": "SELECT * FROM dq.fn_dq_gde_2call_ind('{csid}') WHERE excluded IS FALSE AND collection_type_id IN (1,5)",
            "19_auto": "SELECT * FROM analytic.fn_dq_blacklisting_call_incomplete_15({csid})", # Added fix for 19_auto
            "21_auto": "SELECT * from dq.fn_dq_m2moosl3({csid})",
            "25_auto": "SELECT * from analytic.fn_dq_blacklisting_export_m2m({csid})",
            "26_auto": "SELECT * FROM analytic.fn_dq_blacklisting_export_uneven_test_no_dish({csid})",
            "27_auto": "SELECT * from analytic.fn_dq_device_offline_no_bus_rules_02({csid})",
            "28_auto": "SELECT * FROM analytic.fn_dq_call_setup_60({csid})",
            "29_auto": "SELECT * FROM dq.fn_dq_gde_airplane_m({csid})"
            # placehoder for simultaneous call failure

        }

        # Iterate through each row of the check_results_df from session state
        for index, row in st.session_state.check_results.iterrows():
            sequence = str(row.get("sequence", "")).strip().lower()
            outcome = str(row.get("outcome", "")).strip().lower() # Ensure outcome is lowercase for comparison
            category = str(row.get("category", "")).strip() # Get category for display if available

            # Check if the sequence is one we have a fix for
            if sequence in fix_function_map:
                # Condition: outcome is exactly "incomplete" (lowercase)
                if outcome == "incomplete":
                    st.info(f"Status for `{sequence}` (Category: `{category}`, Outcome: `{row['outcome']}`) is: **{row['outcome']}**")
                    # Format the SQL query with the CSID
                    fix_sql_template = fix_function_map[sequence]
                    fix_function_call = fix_sql_template.format(csid=csid)

                    if st.button(f"Run Fix for {sequence}", key=f"run_{sequence}_fix", use_container_width=True):
                        st.subheader(f"Running fix for `{sequence}`")
                        fix_results_df = execute_sql(engine, fix_function_call, fetch_results=True)
                        if fix_results_df is not None:
                            if not fix_results_df.empty:
                                st.success(f"Fix function for `{sequence}` executed successfully and returned data!")
                                st.dataframe(fix_results_df, use_container_width=True) # Display full DataFrame
                                
                                # Save results to a file
                                saved_file = save_dataframe_to_text_file(fix_results_df, "dq_fix_results", csid, sequence)
                                if saved_file:
                                    st.session_state.generated_file_paths.append(saved_file)
                            else:
                                st.info(f"Fix function for `{sequence}` executed, but returned no data.")
                        else:
                            st.error(f"Failed to execute fix function for `{sequence}`.")
                else:
                    st.success(f"Status for `{sequence}` (Category: `{category}`, Outcome: `{row['outcome']}`) is: **{row['outcome']}** (No action needed)")
            else:
                st.warning(f"Sequence `{sequence}` found in `dq.fn_auto_check` results, but no corresponding fix function is defined in the app configuration.")
    else:
        st.info("Run `dq.fn_auto_check` above to populate the data quality results.")
    
    st.markdown("---")
    st.subheader("3. Merge All Output Files")
    if st.button("Merge All Generated Files", use_container_width=True):
        merged_file = merge_output_files()
        if merged_file:
            st.success(f"Composite file created at: `{merged_file}`")
            st.info(f"Content of '{COMPOSITE_BLOCKLIST_FILE}':")
            try:
                with open(merged_file, 'r') as f:
                    st.code(f.read(), language='text')
            except Exception as e:
                st.error(f"Could not read composite file content: {e}")
        else:
            st.warning("No files were merged or an error occurred during merging.")


    st.markdown("---")
    st.subheader("4. Execute Blocklisting API (aim carefully and consider safety equipment)")
    st.warning("Clicking this button will attempt to run `Blacklisting_API.exe` on the server where this Streamlit app is running.")

    if st.button("Run Blacklisting Command", use_container_width=True):
        if not os.path.exists(COMPOSITE_BLOCKLIST_FILE):
            st.error(f"Error: The composite file '{COMPOSITE_BLOCKLIST_FILE}' was not found. Please click 'Merge All Generated Files' first to create it.")
        else:
            command_to_run = [
                'Blacklisting_API.exe',
                COMPOSITE_BLOCKLIST_FILE,
                'assign'
            ]

            st.info(f"Attempting to run command: `{ ' '.join(command_to_run) }`")
            
            try:
                result = subprocess.run(
                    command_to_run,
                    capture_output=True,
                    text=True,
                    check=True
                )

                st.success("Blacklisting API command executed successfully!")
                if result.stdout:
                    st.write("--- Command Output (STDOUT) ---")
                    st.code(result.stdout)
                if result.stderr:
                    st.warning("--- Command Errors (STDERR) ---")
                    st.code(result.stderr)

            except FileNotFoundError:
                st.error(f"Error: The executable '{command_to_run[0]}' was not found.")
                st.info("Please ensure `Blacklisting_API.exe` is in the same directory as this Streamlit app, or its full path is specified correctly, or it's added to your system's PATH.")
            except subprocess.CalledProcessError as e:
                st.error(f"Blacklisting API command failed with exit code {e.returncode}.")
                st.write("--- Command Output (STDOUT) ---")
                st.code(e.stdout)
                st.write("--- Command Errors (STDERR) ---")
                st.code(e.stderr)
            except Exception as e:
                st.error(f"An unexpected error occurred while trying to run the command: {e}")

    st.markdown("---")
    st.info("Remember to configure your .env file with the RSR_CONN environment variable (e.g., RSR_CONN=postgresql://user:password@host:port/database)!")
    st.markdown("Developed by your AI Assistant.")


if __name__ == "__main__":
    main()








