# dq_gde_tool_2.py
General Data Exclusion Tool
Created July 15 2025 

First pass at tool to simplify the data exclusion (blocklisting) process

Requirements:
1. python Modules
  - streamlit
  - pandas
  - os
  - subprocess
  - csv
  - dotenv
  - sqlalchemy

 2. The RM VPN connection must be established

 3. the blacklisting_API.exe file must be in the same folder as the python script running the tool

Notes:
2 folders will be created when running the script.  These folders are blocklist_history and dq_output_files
They can be used as reference to attach to individual DQ JIRA tickets for later investigations.
 
