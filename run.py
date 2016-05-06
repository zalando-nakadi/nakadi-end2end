#!/usr/bin/env python

import end2end.main
import click


@click.command()
@click.option('--config', help='Configuration file name')
@click.option('--port', help='Port to listen on')
def start_server(config, port):
    return end2end.main.start(config, port)


if __name__ == '__main__':
    start_server()
