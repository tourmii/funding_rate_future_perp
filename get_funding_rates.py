from dex.apex import ApexFundingRateFetcher
from dex.merkle import MerkleFundingRateFetcher
from dex.vertex import VertexFundingRateFetcher
from cex.binance import BinanceFundingRateFetcher
from cex.bitget import BitgetFundingRateFetcher
from cex.okx import OKXFundingRateFetcher
from cex.bybit import BybitFundingRateFetcher
from cex.coinbase import CoinbaseFundingRateFetcher
import click
import time
from cli_scheduler.scheduler_job import scheduler_format

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--mongodb-uri', '-m', required=True, help='MongoDB URI')
@click.option('--database', '-d', required=True, help='MongoDB database name')
@click.option('--collection', '-c', required=True, help='MongoDB collection name')
@click.option('--private-key', '-p', required=True, help='Private key for Vertex')
@click.option('--interval', default=3600, show_default=True, type=int, help='Interval in seconds to fetch data')
@click.option('--scheduler', default='^false@30', show_default=True, type=str, help=f'Scheduler with format {scheduler_format}')


def get_funding_rates(mongodb_uri, database, collection, scheduler, private_key, interval):
    apex_fetcher = ApexFundingRateFetcher(mongodb_uri, database, collection, scheduler)
    vertex_fetcher = VertexFundingRateFetcher(private_key, mongodb_uri, database, collection, scheduler)
    merkle_fetcher = MerkleFundingRateFetcher(mongodb_uri, database, collection, scheduler)
    binance_fetcher = BinanceFundingRateFetcher(mongodb_uri, database, collection, scheduler)
    bitget_fetcher = BitgetFundingRateFetcher(mongodb_uri, database, collection, scheduler)
    okx_fetcher = OKXFundingRateFetcher(mongodb_uri, database, collection, scheduler)
    bybit_fetcher = BybitFundingRateFetcher(mongodb_uri, database, collection, scheduler)
    coinbase_fetcher = CoinbaseFundingRateFetcher(mongodb_uri, database, collection, scheduler)
    while True:
        apex_fetcher.fetch_data()
        vertex_fetcher.fetch_data_vertex()
        merkle_fetcher.fetch_data()
        binance_fetcher.fetch_data()
        bitget_fetcher.fetch_data()
        okx_fetcher.fetch_data()
        bybit_fetcher.fetch_data()
        coinbase_fetcher.fetch_data()
        print("Sleeping for {} seconds...".format(interval))
        time.sleep(interval)