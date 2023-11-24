# Welcome to LPLG

Welcome to the official documentation for LPLG, a robust tool designed for retrieving comprehensive player and team data from the LPL, leveraging LOLQQ as the underlying database. This guide aims to facilitate your understanding of the library’s core functionalities and assist you in making the most of its features.

## Getting Started

Before diving into the commands, ensure you have the necessary setup completed. This involves installing LPLG and setting up your environment to connect to LOLQQ database. Follow the setup instructions provided in the `config/` directory.

## Commands Overview

Here is a list of the primary commands available in LPLG and their intended use cases:

- `get_schedule_data()`: Fetches the current season's scheduling information.
- `get_stage_data()`: Retrieves details on the various stages of the season.
- `get_match_data()`: Provides match IDs along with corresponding match details.
- `get_team_details()`: Generates a CSV file containing detailed team profiles.
- `get_player_details()`: Produces a CSV file with comprehensive player statistics.

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
