# csv_to_redshift.py
This script allows Redshift users to quickly upload a .CSV file to a new table in Redshift. 

Requires that users have credentials for a Redshift instance, and permissions to `DROP`/`CREATE` tables in a `public` schema.

### Background:
At Simple, Redshift users often needed to quickly upload datasets to Redshift in order to carry out analyses. While users could upload files using import tools in some GUI query editors (e.g., Navicat, DataGrip), these tools often suffered from slow performance, particularly when attempting to upload large datasets. 

This script aims to provide users with a quick and easy way to get a file into Redshift. It attempts to sanitize inputs and text fields, but is not necessarily suited for all use cases. 

### Usage:
```bash
$> python csv_to_redshift.py {PATH_TO_FILE} {REDSHIFT_TABLE_NAME}
```

Example:
```bash
$> python csv_to_redshift.py ../path/to/file.csv  example_redshift_table_name
```