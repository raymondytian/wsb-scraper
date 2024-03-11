import praw
import pandas as pd
import numpy as np
from collections import defaultdict
from pathlib import Path
from datetime import datetime
import configparser
import sys
import logging

# GLOBALS
base_path = Path(__file__).parent


class Tee:
    def write(self, *args, **kwargs):
        self.out1.write(*args, **kwargs)
        self.out2.write(*args, **kwargs)

    def flush(self):
        self.out1.flush()
        self.out2.flush()

    def __init__(self, out1, out2):
        self.out1 = out1
        self.out2 = out2


def setup_logging():
    start_time = datetime.now()
    logging.basicConfig(filename=str(base_path) + "/.logs/{:%Y-%m-%d_%H-%M-%S}.log".format(
        start_time), level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    log_file = str(base_path) + "/.logs/{:%Y-%m-%d_%H-%M-%S}.log".format(
        datetime.now())
    log_file_path = Path(log_file)

    sys.stdout = Tee(sys.stdout, open(log_file, 'a+'))
    sys.stderr = Tee(sys.stderr, open(log_file, 'a+'))

    print("Logging to: " + log_file)


def get_equities():
    df = pd.read_csv(str(base_path) + "/data/constituents.csv")
    df.Symbol = df.Symbol.str.lower()
    df.Security = df.Security.str.lower()
    name_to_ticker = dict(zip(df.Security, df.Symbol))
    return name_to_ticker


def auth_reddit():
    reddit_name = str(base_path) + "/.config/reddit_name"
    reddit_name_path = Path(reddit_name)

    with open(reddit_name_path, 'r') as f:
        reddit_name = f.read().strip()

    config = configparser.ConfigParser()
    config.read(str(base_path) + "/.config/praw.ini")

    client_id = config[reddit_name]['client_id']
    client_secret = config[reddit_name]['client_secret']
    password = config[reddit_name]['password']
    username = config[reddit_name]['username']
    user_agent = config[reddit_name]['user_agent']

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        password=password,
        user_agent=user_agent,
        username=username,
    )

    if reddit.user.me() != username:
        print("Error: Could not authenticate with Reddit")
        exit(1)

    reddit.read_only = True

    return reddit


def get_submission(reddit):
    subreddit = reddit.subreddit("wallstreetbets")
    hot = subreddit.hot(limit=10)
    for submission in hot:
        if submission.stickied and "what are your moves tomorrow" in submission.title.lower():
            return submission


def get_submission_comments(submission):
    submission.comments.replace_more(limit=None)
    comments = np.array(
        [comment.body.lower().strip() for comment in submission.comments.list()])
    return comments


def match_equities_comments(name_to_ticker, comments):
    ticker_mentions = defaultdict(int)

    for comment in comments:
        words = comment.split()
        for ticker in ({name_to_ticker[word]
                        for word in set(words) & set(name_to_ticker.keys())} | set(words) & set(name_to_ticker.values())):
            ticker_mentions[ticker] += 1

    return pd.DataFrame(ticker_mentions.items(), columns=["Ticker", "Mentions"])


def export_equity_mentions(equity_mentions):
    equity_mentions.to_csv(str(base_path) + "/results/equity_mentions_{:%Y-%m-%d_%H-%M-%S}.csv".format(
        datetime.now()), index=False)


def main():
    setup_logging()
    equities = get_equities()
    logging.info("Equities: " + str(equities))
    reddit = auth_reddit()
    logging.info("Authenticated with Reddit.")
    submission = get_submission(reddit)
    logging.info("Found daily submission: " +
                 submission.id + " " + submission.title)
    comments = get_submission_comments(submission)
    logging.info(
        "Fetched comments from submission. Number of parent-level comments: " + str(len(comments)))
    equity_mentions = match_equities_comments(equities, comments)
    logging.info("Matched equities in comments.")
    export_equity_mentions(equity_mentions)
    logging.info("Exported equity mentions to CSV.")


if __name__ == "__main__":
    main()
