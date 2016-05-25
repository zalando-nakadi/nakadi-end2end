#!/usr/bin/env python2

import end2end.main
import click


@click.command()
@click.option('--config', help='Configuration file name')
@click.option('--port', help='Port to listen on')
@click.option('--token', help='Token to use. By default berry token is used')
def start_server(config, port, token):
    return end2end.main.start(config, port, token)


if __name__ == '__main__':
    start_server()
