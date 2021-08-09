import argparse
from functions import *
from pyspark.sql import SparkSession
######################################### Main script starts here ########################################################


# Parse script arguments
parser = argparse.ArgumentParser()

parser.add_argument("-m","--metrics", dest="metrics", type= str, required=True, nargs="+"
                    ,help="list of metrics to compute, available metrics are:\n"
                          "pageviews, pageviews_with_consent, consents_asked, consents_given, consents_given_with_consent, avg_pageviews_per_user"

                    )

parser.add_argument("-i","--input", dest="path", type= str, required=True
                    ,help="input path. The path can be either a single json file or a directory storing json files. Wildcards (*) usage is possible")

args = parser.parse_args()



# Init spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("didomiDataChallenge") \
    .getOrCreate()


# Read input data
df = read_input(spark, args.path)

# Compute metrics
if "pageviews" in args.metrics:
    pageviews = pageviews(df)
    pageviews.show()

if "pageviews_with_consent" in args.metrics:
    pageviews_with_consent = pageviews_with_consent(df)
    pageviews_with_consent.show()

if "consents_asked" in args.metrics:
    consents_asked = consents_asked(df)
    consents_asked.show()

if "consents_given" in args.metrics:
    consents_given = consents_given(df)
    consents_given.show()
    consents_given.write.json(args.output + "consents_given")

if "consents_given_with_consent" in args.metrics:
    consents_given_with_consent = consents_given_with_consent(df)
    consents_given_with_consent.show()

if "avg_pageviews_per_user" in args.metrics:
    avg_pageviews_per_user = avg_pageview_per_user(df)
    avg_pageviews_per_user.show()