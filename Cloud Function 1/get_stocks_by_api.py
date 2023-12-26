from polygon import RESTClient
import json
from typing import cast
from urllib3 import HTTPResponse
import datetime

client = RESTClient(api_key="<API_KEY>", trace=False)

maang_companies = ["META", "AAPL", "AMZN", "GOOGL", "NFLX"]

stock_data = {}
def get_stocks_data():
    """
    Fetches stock data for multiple companies for the previous day.

    Returns:
        HTTPResponse: The aggregated stock data for the previous day.
    """
    today = datetime.date.today()
    one_day_prior = today - datetime.timedelta(days=1)
    one_day_prior_str = one_day_prior.strftime("%Y-%m-%d")

    for stock in maang_companies:
        aggs = cast(
            HTTPResponse,
            client.get_aggs(
                stock,
                1,
                "day",
                one_day_prior_str,
                one_day_prior_str,
                raw=True
            ),
        )

        data = json.loads(aggs.data)
        stock_data[stock] = data
    json_data = json.dumps(stock_data, indent=2)
    return json_data
