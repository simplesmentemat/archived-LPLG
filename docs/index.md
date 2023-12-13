# Welcome to LPLG

Welcome to the official documentation for LPLG, an advanced tool created to extract comprehensive player and team data from the LPL using the LOLQQ database. This guide is designed to help you understand the library's essential functions and maximize its capabilities.

## Getting Started

Before using the commands, make sure to complete the necessary setup. This involves installing LPLG and configuring your environment to connect to the LOLQQ database.

## Commands Overview

LPLG offers a variety of commands for different purposes:

- `get_schedule_data()`: Fetches the current season's scheduling information.
- `get_stage_data()`: Retrieves details on the various stages of the season.
- `get_match_data()`: Provides match IDs along with corresponding match details.
- `get_team_details()`: Generates a CSV file containing detailed team profiles.
- `get_player_details()`: Produces a CSV file with comprehensive player statistics.
- `write_csv()`: Writes the retrieved data to a CSV file.
- `write_parquet()`: Saves the data in a Parquet file format for efficient storage and retrieval.

Each command is designed to return specific data sets; for detailed usage, refer to the function definitions in the `module/` directory.

## Project Structure

An understanding of the project structure will help you navigate and utilize the library more effectively:

```plaintext
config/
    config.py       # Configuration settings and credentials
docs/
    ...             # Documentation files and usage examples
module/
    parse.py        # Parser functions for processing data queries
schemas/
    schemas.py      # Data schemas defining the structure of retrieved data sets
```

For further information, please refer to the documentation within each directory, which offers in-depth explanations of the modules and their configurations.

## Example Usage

Here's an example to demonstrate the usage of the newly added functions:
```python
from LPLGather import parse

# without write_csv or write_parquet
df_example = parse.get_match_data(190, 1)
print(df_example)

# Using write_csv() to save data as a CSV file
df_example_csv = parse.get_match_data(190, 1).write_csv()
print(df_example_csv)

# Using write_parquet() to save data as a Parquet file
df_example_parquet = parse.get_match_data(190, 1).write_parquet()
print(df_example_parquet)
```