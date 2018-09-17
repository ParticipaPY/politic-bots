from src.bot_detector.bot_detector import BotDetector
from src.utils.db_manager import DBManager
from src.analyzer.network_analysis import NetworkAnalyzer

import ast
import click


# Taken from
# https://stackoverflow.com/questions/47631914/how-to-pass-several-list-of-arguments-to-click-option
class PythonLiteralOption(click.Option):
    def type_cast_value(self, ctx, value):
        try:
            return ast.literal_eval(value)
        except:
            raise click.BadParameter(value)


@click.command()
@click.option('--users', cls=PythonLiteralOption, help='List of user names to examine', default=[])
def run_bot_detector(users):
    conf = '../config.json'
    # create database of user if it doesn't exist
    users_db = DBManager('users')
    if users_db.num_records_collection() == 0:
        na = NetworkAnalyzer()
        na.create_users_db()
    bot_detector = BotDetector(conf)
    bot_detector.compute_bot_probability(users)


if __name__ == "__main__":
    run_bot_detector()
