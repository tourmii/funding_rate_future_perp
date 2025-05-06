from dex.apex import ApexFundingRateFetcher
from dex.aster import AsterFundingRateFetcher
from dex.drift import DriftFundingRateScraper
from dex.gmx import GMXFundingRateFetcher
from dex.vertex import VertexFundingRateFetcher
import click
import time
from cli_scheduler.scheduler_job import scheduler_format

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--mongodb-uri', '-m', required=True, help='MongoDB URI')
@click.option('--database', '-d', required=True, help='MongoDB database name')
@click.option('--collection', '-c', required=True, help='MongoDB collection name')
@click.option('--private-key', '-p', required=True, help='Private key for Vertex')
@click.option('--interval', default=1800, show_default=True, type=int, help='Interval in seconds to fetch data')
@click.option('--scheduler', default='^false@30', show_default=True, type=str, help=f'Scheduler with format {scheduler_format}')


def get_funding_rates(mongodb_uri, database, collection, scheduler, private_key, interval):
    gmx_fetcher = GMXFundingRateFetcher(mongodb_uri, database, collection, scheduler)
    drift_fetcher = DriftFundingRateScraper(mongodb_uri, database, collection, scheduler)
    apex_fetcher = ApexFundingRateFetcher(mongodb_uri, database, collection, scheduler)
    vertex_fetcher = VertexFundingRateFetcher(private_key, mongodb_uri, database, collection, scheduler)
    aster_fetcher = AsterFundingRateFetcher(mongodb_uri, database, collection, scheduler)
    while True:
        gmx_fetcher.fetch_data()
        drift_fetcher.fetch_data()
        apex_fetcher.fetch_data()
        vertex_fetcher.fetch_data_vertex()
        aster_fetcher.fetch_data_aster()
        time.sleep(interval)