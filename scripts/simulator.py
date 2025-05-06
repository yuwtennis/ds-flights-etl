"""Simulator"""

import argparse
import asyncio
import logging
import sys
import time
from argparse import Namespace
from datetime import datetime, UTC
from typing import Generator

from google.cloud import pubsub_v1, bigquery
from google.cloud.pubsub_v1.publisher.futures import Future

from dsflightsetl.flight import EventType, VALID_DATETIME_FMT
from dsflightsetl.message import Message, TopicResource


def as_datetime(date_str: str):
    """

    :param date_str:
    :return:
    """
    return datetime.strptime(date_str, f"{VALID_DATETIME_FMT} %Z").replace(tzinfo=UTC)


def parse_args() -> Namespace:
    """

    :return:
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--project_id", type=str)
    parser.add_argument("--dataset_id", type=str, default="dsongcp")
    parser.add_argument("--table_id", type=str, default="flights_simevents")
    parser.add_argument("--start_time", type=str)
    parser.add_argument("--end_time", type=str)

    return parser.parse_args()


def compute_sleep_secs(
    notify_time: datetime,
    sim_start_time: datetime,
    prog_start_time: datetime,
    speed_factor: int,
) -> int:
    """

    :param notify_time:
    :param sim_start_time:
    :param prog_start_time:
    :param speed_factor:
    :return:
    """
    prog_curr_time = datetime.now(UTC)

    time_elapsed = (prog_curr_time - prog_start_time).seconds
    sim_elapsed_time = (notify_time - sim_start_time).seconds / speed_factor

    # Calculate the relative time
    return int(sim_elapsed_time - time_elapsed)


def extract(
    client: bigquery.Client,
    start_time: str,
    end_time: str,
    dataset_id: str,
    table_id: str,
) -> Generator[Message, None, None]:
    """

    :param client:
    :param start_time:
    :param end_time:
    :param dataset_id:
    :param table_id:
    :return:
    """
    sql = f"""
    SELECT
        EVENT_TYPE AS event_type,
        EVENT_TIME AS notify_time,
        EVENT_DATA AS event_data
    FROM
        {dataset_id}.{table_id}
    WHERE
        EVENT_TIME >= @start_time
        AND EVENT_TIME < @end_time
    ORDER BY
        EVENT_TIME ASC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
        ]
    )

    for row in client.query(sql, job_config=job_config):
        yield Message(**row)


def notify(  # pylint: disable=too-many-arguments
    publisher: pubsub_v1.PublisherClient,
    events: Generator[Message, None, None],
    topics: list[TopicResource],
    sim_start_time: datetime,
    prog_start_time: datetime,
    speed_factor: int,
) -> None:
    """

    :param publisher:
    :param events:
    :param topics:
    :param sim_start_time:
    :param prog_start_time:
    :param speed_factor:
    :return:
    """
    tonotify = {}
    for topic in topics:
        tonotify[str(topic)] = []

    for event in events:
        if (
            compute_sleep_secs(
                event.notify_time, sim_start_time, prog_start_time, speed_factor
            )
            > 1
        ):
            asyncio.run(publish(publisher, topics, tonotify))

            tonotify = {str(topic): [] for topic in topics}

            # Recalculate
            to_sleep_secs = compute_sleep_secs(
                event.notify_time, sim_start_time, prog_start_time, speed_factor
            )
            if to_sleep_secs > 0:
                logging.info("Sleeping %s seconds", to_sleep_secs)
                time.sleep(to_sleep_secs)
        topic_resource: TopicResource = next(
            filter(
                lambda tr: tr.event_type
                == event.event_type,  # pylint: disable=cell-var-from-loop
                topics,
            )
        )
        tonotify[str(topic_resource)].append(event.event_data)

    publish(publisher, topics, tonotify)


async def publish(
    publisher: pubsub_v1.PublisherClient,
    topics: list[TopicResource],
    all_events: dict[str, list[Message]],
) -> None:
    """

    :param publisher:
    :param topics:
    :param all_events:
    :return:
    """
    for topic in topics:
        logging.info("Publishing %s %s events", len(all_events), str(topic))
        for event in all_events[str(topic)]:
            future: Future = publisher.publish(str(topic), event.encode())
            logging.info("Published message ID is %s", future.result())


def main(argv: sys.argv):  # pylint: disable=unused-argument
    """main"""
    logging.basicConfig(level=logging.INFO)
    logging.info("Start running simulator...")
    args = parse_args()

    bq_client = bigquery.Client()
    publisher = pubsub_v1.PublisherClient()

    prog_start_time = datetime.now(UTC)

    rows = extract(
        bq_client, args.start_time, args.end_time, args.dataset_id, args.table_id
    )
    notify(
        publisher,
        rows,
        [
            TopicResource(project_id=args.project_id, event_type=evt.value)
            for evt in [EventType.DEPARTED, EventType.ARRIVED, EventType.WHEELSOFF]
        ],
        as_datetime(args.start_time),
        prog_start_time,
        60,
    )


if __name__ == "__main__":
    main(sys.argv)
