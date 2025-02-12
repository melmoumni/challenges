# Data engineering challenge

This challenge is used by Didomi for evaluating candidates for data engineering positions.

This challenge is a chance for engineers at Didomi to see how you code and organize a project to implement a specification.

## Deliverables

The expected deliverable is a fully functional project that includes the following:

- Code of the app
- Tests of the app
- Documentation for launching a development environment and running the app

## Technical stack

The application should use the following stack:

- [Spark](https://spark.apache.org/)
- Python or Scala

Except for these requirements, feel free to use whichever libraries, frameworks or tools you deem necessary.

## Expectations

Your code will be reviewed by multiple engineers at Didomi and will serve as the base for a discussion in interviews.  
We want to see how you approach working on a complete project and strongly recommend that you work on this challenge alone. We will particularly focus on your attention to details and expect the code to be professionally structured, commented, documented, and tested.

If anything is unclear, feel free to ask any question that might help you understand the specifications or requirements better.

## Delivery

Your application can be sent to us as a GitHub repository (in which case you are welcome to fork this repository) or as a compressed archive containing all the deliverables.

## The challenge

In some specific cases, companies need to collect consent from consumers before using their data. For instance, app users might need to explicitly consent to share their geolocation before a company can use it for advertising.

As users interact with the Didomi platform, we collect different types of events like:

- "Page view" when a user visits a webpage
- "Consent asked" when a user is asked for consent (ie a consent notice is displayed)
- "Consent given" when a user gives consent (ie has clicked on a Agree or Disagree in the notice)

The goal of this challenge is to build a very simple Spark app that processes events and summarizes various metrics as time-series data.

[Download the example data for the challenge.](./input-example.zip)

## Input

### Format

Events are stored as JSON Lines files with the following format:

```js
{
    "timestamp": "2020-01-21T15:19:34Z",
    "id": "94cabac0-088c-43d3-976a-88756d21132a",
    "type": "pageview",
    "domain": "www.website.com",
    "user": {
        "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
        "country": "US",
        "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
    }
}
```

| Property       | Values                                       | Description                          |
| -------------- | -------------------------------------------- | ------------------------------------ |
| `timestamp`    | ISO 8601 date                                | Date of the event                    |
| `id`           | UUID                                         | Unique event ID                      |
| `type`         | `pageview`, `consent.given`, `consent.asked` | Event type                           |
| `domain`       | Domain name                                  | Domain where the event was collected |
| `user.id`      | UUID                                         | Unique user ID                       |
| `user.token`   | JSON-String                                  | Contains status of purposes/vendors  |
| `user.country` | ISO 3166-1 alpha-2 country code              | Country of the user                  |

### Consent status

We consider an event as positive consent when at least one purpose is enabled.

### Partitioning

The data is partitioned by date/hour with Hive partition structure.

## Output

The Spark job is expected to compute the following metrics:

| Metric                        | Description                                                                      |
| ----------------------------- | -------------------------------------------------------------------------------- |
| `pageviews`                   | Number of events of type `pageview`                                              |
| `pageviews_with_consent`      | Number of events of type `pageview` with consent (at least one enabled purpose)  |
| `consents_asked`              | Number of events of type `consent.asked`                                         |
| `consents_given`              | Number of events of type `consent.given`                                         |
| `consents_given_with_consent` | Number of events of type `consent.given` with consent                            |
| `avg_pageviews_per_user`      | Average number of events of type `pageviews` per user                            |

The metrics should be grouped by the following dimensions:

- Date and hour (YYYY-MM-DD-HH)
- Domain
- User country

## Processing

On top of computing the metrics listed above, the following operations must be run:

- Deduplication of events based on event ID

## Howto run the app?
The app was tested using spark 3.1.2 (pyspark)
Tests were implemented using [pytest](https://docs.pytest.org/en/6.2.x/) and [pyspark-test](https://github.com/debugger24/pyspark-test) were used
### Environment setup
Install pyspark
```bash
pip install pyspark (installs spark 3.1.2)
```
Install pytest
```bash
pip install pytest pyspark-test
```
### Run the app
The app can be executed using spark-submit.
The app was tested locally only (--master local[*])
```bash
usage: spark-submit --py-files functions.py main.py [-h] -m METRICS [METRICS ...] -i PATHS [PATHS ...]

mandatory arguments:
-m METRICS [METRICS ...], --metrics METRICS [METRICS ...]
                 list of metrics to compute, available metrics are:
                 pageviews, pageviews_with_consent, consents_asked,
                 consents_given, consents_given_with_consent,
                 avg_pageviews_per_user
-i PATHS [PATHS ...], --input PATHS [PATHS ...]
                 input paths. The path can be either a single json file
                 or a path pattern of a directory storing multiple json files (/my/path/*.json). Wildcards (*) usage
                 is possible                                     
optional arguments:
-h, --help            show this help message and exit

```
Examples:
```bash
# computes pageviews metri only
spark-submit --py-files functions.py main.py -m pageviews -i /my/input/path/datehour=2021-01-23-10/*.json /my/input/path/datehour=2021-01-23-11/*.json
# computes pageviews and avg_pageviews_per_user
spark-submit --py-files functions.py main.py -m pageviews avg_pageviews_per_user -i /my/input/path/datehour=2021-01-23-10/*.json /my/input/path/datehour=2021-01-23-11/*.json
```

### Run the tests
```bash
pytest --no-header -v
``` 