

import datetime

from streaming.publisher.firms_publisher import FirmsPublisher
from utils.constant import MAP_KEY


def publish_firms_data(
    date_value: str,
) -> None:

    satellite = "VIIRS_SNPP_NRT"
    areas = "95,-11,141,6"
    interval = 0.1
    day_range = 1

    publisher = FirmsPublisher(
        map_key=MAP_KEY,
        satellite=satellite,
        day_range=day_range,
        area=areas
    )

    publisher.replay(
        interval=interval,
        date=datetime.strptime(date_value, "%Y-%m-%d").date(),  # noqa: DTZ007
        dry_run=False,
    )
