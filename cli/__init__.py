import click 

from get_funding_rates import get_funding_rates

@click.group()
@click.version_option(version="1.0.0")
@click.pass_context

def cli(ctx):
    """Funding Rate CLI Tool"""
    pass

cli.add_command(get_funding_rates, "get_funding_rates")