import pytest
from pyspark_test import assert_pyspark_df_equal
from pyspark.sql import SparkSession
from functions import pageviews, consents_asked, consents_given, pageviews_with_consent, consents_given_with_consent, avg_pageview_per_user


@pytest.fixture(scope="session")
def spark_session(request):
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("didomiDataChallengeTests") \
        .getOrCreate()

    request.addfinalizer(lambda: spark_session.sparkContext.stop())

    return spark_session


def test_pageviews(spark_session):
    data = [{
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-088c-43d3-976a-88756d21132a",
        "type": "pageview",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "US",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    },
    {
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-083c-43d3-976a-88756d21132a",
        "type": "consent.given",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "US",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    }
    ]
    expected_data = [{"dateHour": "2021-01-23-11", "domain": "www.website.com", "country": "US", "count": 1}]
    expected_result = spark_session.createDataFrame(expected_data)
    df = spark_session.createDataFrame(data)

    result = pageviews(df)
    assert_pyspark_df_equal(result, expected_result)


def test_consents_asked(spark_session):
    data = [{
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-088c-43d3-976a-88756d21132a",
        "type": "consent.asked",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "FR",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    },
    {
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-288c-43d3-976a-88756d21132a",
        "type": "pageview",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "FR",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    }
    ]
    expected_data = [{"dateHour": "2021-01-23-11", "domain": "www.website.com", "country": "FR", "count": 1}]
    expected_result = spark_session.createDataFrame(expected_data)
    df = spark_session.createDataFrame(data)

    result = consents_asked(df)
    assert_pyspark_df_equal(result, expected_result)

def test_consents_given(spark_session):
    data = [{
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-088c-43d3-976a-88756d21132a",
        "type": "consent.given",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    },
    {
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-088f-43d3-976a-88756d21132a",
        "type": "consent.asked",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    }
    ]
    expected_data = [{"dateHour": "2021-01-23-11", "domain": "www.website.com", "country": "ES", "count": 1}]
    expected_result = spark_session.createDataFrame(expected_data)
    df = spark_session.createDataFrame(data)

    result = consents_given(df)
    assert_pyspark_df_equal(result, expected_result)

def test_pageviews_with_consent(spark_session):
    data = [{
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-088c-43d3-976a-88756d21132a",
        "type": "pageview",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    },
    {
        "datetime": "2021-01-23 11:05:07",
        "id": "97cabac0-088c-43d3-976a-88756d21132a",
        "type": "pageview",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": [],
                "disabled": []
            }
        }
    },
    {
        "datetime": "2021-01-23 11:05:07",
        "id": "34cabac0-088c-43d3-976a-88756d21132a",
        "type": "consent.given",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    }
    ]
    expected_data = [{"dateHour": "2021-01-23-11", "domain": "www.website.com", "country": "ES", "count": 1}]
    expected_result = spark_session.createDataFrame(expected_data)
    df = spark_session.createDataFrame(data)

    result = pageviews_with_consent(df)
    assert_pyspark_df_equal(result, expected_result)

def test_consents_given_with_consent(spark_session):
    data = [{
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-088c-43d3-976a-88756d21132a",
        "type": "consent.given",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    },
    {
        "datetime": "2021-01-23 11:05:07",
        "id": "94cbbac0-088c-43d3-976a-88756d21132a",
        "type": "consent.given",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": [],
                "disabled": []
            }
        }
    },
    {
        "datetime": "2021-01-23 11:05:07",
        "id": "94cbbzc0-088c-43d3-976a-88756d21132a",
        "type": "pageview",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    }
    ]
    expected_data = [{"dateHour": "2021-01-23-11", "domain": "www.website.com", "country": "ES", "count": 1}]
    expected_result = spark_session.createDataFrame(expected_data)
    df = spark_session.createDataFrame(data)

    result = consents_given_with_consent(df)
    assert_pyspark_df_equal(result, expected_result)


def test_avg_pageview_per_user(spark_session):
    data = [{
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-088c-43d3-976a-88756d21132a",
        "type": "pageview",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    },
    {
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-088c-43d3-936a-88756d21132a",
        "type": "pageview",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    },
    {
        "datetime": "2021-01-23 11:05:07",
        "id": "94cabac0-088c-43d1-976a-88756d21132a",
        "type": "pageview",
        "domain": "www.website.com",
        "user": {
            "id": "09fcb803-2771-4096-bcfd-0fb1afef684a",
            "country": "ES",
            "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
        },
        "parsedToken": {
            "vendors": {
                "enabled": ["vendor"],
                "disabled": []
            },
            "purposes": {
                "enabled": ["analytics"],
                "disabled": []
            }
        }
    }

    ]
    expected_data = [{"dateHour": "2021-01-23-11", "domain": "www.website.com", "country": "ES", "avg(count)": 1.5}]
    expected_result = spark_session.createDataFrame(expected_data)
    df = spark_session.createDataFrame(data)

    result = avg_pageview_per_user(df)
    assert_pyspark_df_equal(result, expected_result)