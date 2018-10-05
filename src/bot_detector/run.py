from src.bot_detector.bot_detector import BotDetector
from src.utils.db_manager import DBManager
from src.analyzer.network_analysis import NetworkAnalyzer

import ast
import click


# Taken from
# https://stackoverflow.com/questions/47631914/how-to-pass-several-list-of-arguments-to-click-option
class ToList(click.Option):
    def type_cast_value(self, ctx, value):
        try:
            return ast.literal_eval(value)
        except:
            raise click.BadParameter(value)


#@click.command()
#@click.option('--users', cls=ToList, help='List of user names to examine', default=[])
def run_bot_detector(users):
    # create database of user if it doesn't exist
    users_db = DBManager('users')
    if users_db.num_records_collection() == 0:
        na = NetworkAnalyzer()
        na.create_users_db()
    bot_detector = BotDetector()
    bot_detector.compute_bot_probability(users)


if __name__ == "__main__":
    run_bot_detector([])

    #cd_name = os.path.basename(os.getcwd())
    #if cd_name != 'bot_detector':
    #    click.UsageError('Illegal use: this script must run from the bot_detector directory')
    #run_bot_detector()
