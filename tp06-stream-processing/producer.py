"""Publish synthetic campus events to the TP05 Kafka broker."""

import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone

from kafka import KafkaProducer


SAMPLE = [
    ("S101", "check-in", "Computer Science", 2),
    ("S204", "check-in", "Mathematics", 8),
    ("S101", "open-course", "Computer Science", 15),
    ("S305", "check-in", "Physics", 22),
    ("S411", "check-in", "Computer Science", 35),
    ("S204", "check-in", "Mathematics", 48),
    ("S512", "check-in", "Physics", 12),
]

PROFILES = {
    "sample": SAMPLE,
    "resume": [("S601", "check-in", "Physics", 44)],
    "advance": [
        ("S701", "check-in", "Computer Science", 110),
        ("S702", "check-in", "Mathematics", 110),
        ("S703", "check-in", "Physics", 110),
    ],
    "delayed": [("S801", "check-in", "Physics", 12)],
    "mixed": SAMPLE + [("S305", "library-entry", "Physics", 25)],
}
START_IDS = {"resume": 8, "advance": 9, "delayed": 12}


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--topic", default="campus-events")
    parser.add_argument("--profile", choices=PROFILES, default="sample")
    parser.add_argument("--count", type=int, help="Repeat the sample pattern up to this many events.")
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--date", type=date.fromisoformat, help="Synthetic campus date (YYYY-MM-DD).")
    args = parser.parse_args()
    if args.count is not None and (args.count < 1 or args.profile not in ("sample", "mixed")):
        parser.error("--count must be positive and applies to sample and mixed profiles")
    if args.interval < 0:
        parser.error("--interval must be nonnegative")
    return args


def main():
    args = options()
    campus_clock = timezone(timedelta(hours=1))
    campus_date = args.date or datetime.now(campus_clock).date()
    base = datetime(campus_date.year, campus_date.month, campus_date.day, 7, 14, 50, tzinfo=timezone.utc)
    pattern = PROFILES[args.profile]
    count = args.count if args.count is not None else len(pattern)
    started = time.perf_counter()
    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        acks="all",
    )
    try:
        for index in range(count):
            student, kind, department, seconds = pattern[index % len(pattern)]
            event_time = base + timedelta(seconds=seconds + 60 * (index // len(pattern)))
            event = {
                "eventId": START_IDS.get(args.profile, 1) + index,
                "studentId": student,
                "eventType": kind,
                "department": department,
                "eventTime": event_time.isoformat().replace("+00:00", "Z"),
            }
            partition = index % 3
            record = producer.send(args.topic, value=event, partition=partition).get(timeout=15)
            print(f"Sent p{record.partition}@{record.offset}: {json.dumps(event)}", flush=True)
            if index + 1 < count:
                time.sleep(args.interval)
    finally:
        producer.close()
    elapsed = time.perf_counter() - started
    print(f"Published {count} events in {elapsed:.2f}s ({count / elapsed:.2f} events/s).")


if __name__ == "__main__":
    main()
