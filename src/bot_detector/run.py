import ast
import click
import pathlib
import os
import sys

# Add the src directory to the sys.path
sys.path.append(str(pathlib.Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).parents[0]))


from src.bot_detector.bot_detector import BotDetector
from src.utils.db_manager import DBManager
from src.analyzer.network_analysis import NetworkAnalyzer
from src.utils.utils import get_user

# Taken from
# https://stackoverflow.com/questions/47631914/how-to-pass-several-list-of-arguments-to-click-option
class ToList(click.Option):
    def type_cast_value(self, ctx, value):
        try:
            return ast.literal_eval(value)
        except:
            raise click.BadParameter(value)


@click.command()
@click.option('--users', cls=ToList, help='List of user names to examine', default=[])
def run_bot_detector(users):
    bot_detector = BotDetector()
    # create database of user if it doesn't exist
    users_db = DBManager('users')
    if users_db.num_records_collection() == 0:
        na = NetworkAnalyzer()
        na.create_users_db()
    bot_detector.compute_bot_probability(users)
    #bot_detector.compute_fake_promoter_heuristic(users)


if __name__ == "__main__":
    cd_name = os.path.basename(os.getcwd())
    if cd_name != 'src':
       click.UsageError('Illegal use: this script must run from the src directory')
    run_bot_detector()
