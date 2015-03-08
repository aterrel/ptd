import glob
import gzip
import json

import twitter

from creds import *

STREAM_PREFIX = "/data/twitter/sample"
MAX_UPDATES = 1e6

def rotate_stream_file():
    files = glob.glob(STREAM_PREFIX+"*")
    count = len(files)
    return gzip.open(STREAM_PREFIX + "." +  str(count) + ".json.gz", 'a')

def main():
    auth=twitter.OAuth(ACCESS_TOKEN, ACCESS_TOKEN_SECRET, 
                       CONSUMER_KEY, CONSUMER_SECRET)
    twitter_stream = twitter.TwitterStream(auth=auth)
    iterator = twitter_stream.statuses.sample()

    while True:
        count = 0
        with rotate_stream_file() as f:
            for tweet in iterator:
                json.dump(tweet, f)
                f.write("\n")
                count += 1
                if count > MAX_UPDATES:
                    break
    

if __name__ == "__main__":
    main()
