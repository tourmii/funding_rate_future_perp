# Funding Rate Future Perp

A Python-based funding rate aggregator that fetches and monitors perpetual futures funding rates from major cryptocurrency exchanges, both centralized (CEX) and decentralized (DEX).

## Overview

This tool continuously collects funding rate data from multiple exchanges and stores it in MongoDB for analysis and monitoring. It's useful for traders and analysts who want to track funding rates across different platforms to identify arbitrage opportunities or market sentiment.

## Supported Exchanges

### Centralized Exchanges (CEX)
- **Binance**
- **Bybit**
- **OKX**
- **Bitget**
- **Coinbase**

### Decentralized Exchanges (DEX)
- **Apex**
- **Vertex**
- **Merkle**

## Features

- Real-time funding rate collection from 8+ exchanges
- Configurable polling intervals
- Built-in scheduler support
- MongoDB integration for data persistence
- Modular architecture with separate fetchers for each exchange
- CLI interface for easy configuration

## Installation

1. Clone the repository:
```bash
git clone https://github.com/tourmii/funding_rate_future_perp.git
cd funding_rate_future_perp
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the funding rate fetcher with the following command:

```bash
python run.py get-funding-rates \
  --mongodb-uri "mongodb://localhost:27017" \
  --database "funding_rates" \
  --collection "rates" \
  --private-key "YOUR_VERTEX_PRIVATE_KEY" \
  --interval 3600 \
  --scheduler "^false@30"
```

### Command-line Options

| Option | Short | Required | Default | Description |
|--------|-------|----------|---------|-------------|
| `--mongodb-uri` | `-m` | Yes | - | MongoDB connection URI |
| `--database` | `-d` | Yes | - | MongoDB database name |
| `--collection` | `-c` | Yes | - | MongoDB collection name |
| `--private-key` | `-p` | Yes | - | Private key for Vertex DEX |
| `--interval` | - | No | 3600 | Interval in seconds between data fetches |
| `--scheduler` | - | No | ^false@30 | Scheduler configuration format |

## Project Structure

```
funding_rate_future_perp/
├── cex/                    # Centralized exchange fetchers
│   ├── binance.py
│   ├── bitget.py
│   ├── bybit.py
│   ├── coinbase.py
│   └── okx.py
├── dex/                    # Decentralized exchange fetchers
│   ├── apex.py
│   ├── merkle.py
│   └── vertex.py
├── cli/                    # Command-line interface
├── get_funding_rates.py    # Main fetching logic
├── run.py                  # Entry point
└── requirements.txt        # Python dependencies
```

## Requirements

- Python 3.7+
- MongoDB instance (local or remote)
- Private key for Vertex DEX access

## Data Storage

Funding rate data is stored in MongoDB with timestamps and exchange information, allowing for:
- Historical analysis
- Cross-exchange comparison
- Arbitrage opportunity detection
- Market sentiment tracking

## Example Data

The repository includes sample data:
- `ethperp_funding_2024.csv` - Historical ETH perpetual funding rates from 2024

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the MIT License.

## Disclaimer

This tool is for informational purposes only. Always do your own research before making trading decisions.
