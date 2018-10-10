
import click
import os
import pathlib
import sys

# Add the src directory to the sys.path
sys.path.append(str(pathlib.Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))).parents[0]))


from src.bot_detector.bot_detector import BotDetector


@click.command()
@click.option('--csv_file', help='Name of the csv file where to save the bot analysis', default='user_bots.csv')
def save_analysis_file(csv_file):
    bot_detector = BotDetector()
    bot_detector.to_csv(csv_file, include_verified_accounts=False)


if __name__ == "__main__":
    cd_name = os.path.basename(os.getcwd())
    if cd_name != 'src':
       click.UsageError('Illegal use: this script must run from the src directory')
    save_analysis_file()
