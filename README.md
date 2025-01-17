# Personal Portfolio Management Framework

Overview
A framework for personal portfolio management, including classification, storage, and analysis of various financial products including equities, fixed income, derivatives, and more.
Key Features

Comprehensive instrument classification following ANSI standards
Support for multiple asset classes:

- Equities (Common Stock, Preferred Stock, ADRs)

- Fixed Income (Government Bonds, Corporate Bonds)

- Derivatives (Futures, Options, Swaps)

- Money Market Instruments

- Investment Funds

- Commodities


SQL database integration with SQLAlchemy ORM Excel data import/export capabilities Day count calculations for different 
financial conventions.


Prerequisites:

- Python 3.8 or higher
- SQL Server with ODBC Driver 17
- Microsoft Excel (for Excel integration features)

Installation:

Clone the repository

* bash Copygit clone git@github.com:YOUR-USERNAME/YOUR-REPOSITORY.git *

Create a virtual environment:

bashCopypython -m venv venv
# On Windows
venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate

Install dependencies

bashCopypip install -r requirements.txt
Project Structure
Copy└── pylib/
    └── library/
        ├── config/          # Configuration and enumerations
        ├── instrument/      # Core instrument classes
        ├── sql/            # Database models and queries
        ├── excel/          # Excel data handlers
        └── utils/          # Utility functions
Database Setup
The system uses SQL Server as its database. Ensure you have:

SQL Server installed and running
ODBC Driver 17 for SQL Server installed
Appropriate database permissions

Usage Example
pythonCopyfrom pylib.library.sql.declarative_base import DeclarativeBase
from pylib.library.sql.querier_instrument import QuerierInstrument

# Initialize database connection
db = DeclarativeBase()
session = db.make_session()

# Query instruments
querier = QuerierInstrument()
instruments = querier.get_all_instruments(session)
Configuration
Database connection parameters can be configured in pylib/library/sql/database.py.
Contributing
If you'd like to contribute, please:

Fork the repository
Create a feature branch
Submit a Pull Request

License
[Add your chosen license here]
Contact
[Add your contact information here]