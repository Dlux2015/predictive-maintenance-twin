"""
Local Kinesis mock for development and demo — no AWS account required.

Implements the subset of the boto3 Kinesis client interface used by this
project, storing records in an in-memory deque. Drop-in replacement:

    # Production
    client = boto3.client("kinesis", region_name="us-east-1")

    # Development / CI
    from infra.kinesis_stub import KinesisStub
    client = KinesisStub()

Records are available for inspection via get_records() and can be dumped
to a JSONL file for offline analysis.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

_MAX_RECORDS = 1_000
_FAKE_SHARD_ID = "shardId-000000000000"


class KinesisStub:
    """
    In-memory Kinesis Data Stream mock.

    Mimics the boto3 Kinesis client interface for the operations used by
    the predictive maintenance producer and tests.
    """

    def __init__(self, max_records: int = _MAX_RECORDS) -> None:
        """
        Initialise the stub.

        Parameters
        ----------
        max_records:
            Maximum number of records to buffer before dropping oldest.
        """
        self._records: deque[dict] = deque(maxlen=max_records)
        self._iterators: dict[str, int] = {}  # iterator_id → cursor position
        self._sequence: int = 0
        logger.info("KinesisStub initialised (max_records=%d)", max_records)

    # ------------------------------------------------------------------
    # Producer interface
    # ------------------------------------------------------------------

    def put_record(
        self,
        StreamName: str,
        Data: bytes,
        PartitionKey: str,
        **kwargs: Any,
    ) -> dict:
        """
        Buffer a single record and return a fake response.

        Parameters
        ----------
        StreamName:
            Kinesis stream name (ignored — all records share one buffer).
        Data:
            Raw bytes payload.
        PartitionKey:
            Partition routing key (stored for inspection).

        Returns
        -------
        Fake Kinesis put_record response dict.
        """
        self._sequence += 1
        seq_number = f"{self._sequence:021d}"
        entry = {
            "SequenceNumber": seq_number,
            "ShardId": _FAKE_SHARD_ID,
            "PartitionKey": PartitionKey,
            "Data": Data.decode("utf-8", errors="replace"),
            "ApproximateArrivalTimestamp": datetime.now(timezone.utc).isoformat(),
            "StreamName": StreamName,
        }
        self._records.append(entry)
        logger.debug("KinesisStub.put_record: seq=%s partition=%s", seq_number, PartitionKey)
        return {"ShardId": _FAKE_SHARD_ID, "SequenceNumber": seq_number}

    def put_records(
        self,
        StreamName: str,
        Records: list[dict],
        **kwargs: Any,
    ) -> dict:
        """
        Buffer multiple records and return a fake batch response.

        Parameters
        ----------
        StreamName:
            Kinesis stream name.
        Records:
            List of dicts with 'Data' and 'PartitionKey' keys.

        Returns
        -------
        Fake Kinesis put_records response dict.
        """
        results = []
        for rec in Records:
            resp = self.put_record(
                StreamName=StreamName,
                Data=rec["Data"],
                PartitionKey=rec["PartitionKey"],
            )
            results.append({"SequenceNumber": resp["SequenceNumber"], "ShardId": _FAKE_SHARD_ID})

        logger.debug("KinesisStub.put_records: buffered %d records", len(Records))
        return {
            "FailedRecordCount": 0,
            "Records": results,
        }

    # ------------------------------------------------------------------
    # Consumer interface
    # ------------------------------------------------------------------

    def get_shard_iterator(
        self,
        StreamName: str,
        ShardId: str,
        ShardIteratorType: str,
        **kwargs: Any,
    ) -> dict:
        """
        Return a fake shard iterator pointing to the current buffer position.

        Parameters
        ----------
        StreamName, ShardId, ShardIteratorType:
            Standard Kinesis parameters (ShardIteratorType is honoured for
            TRIM_HORIZON and LATEST only; others default to LATEST).

        Returns
        -------
        Dict with 'ShardIterator' key.
        """
        iterator_id = str(uuid.uuid4())
        if ShardIteratorType == "TRIM_HORIZON":
            self._iterators[iterator_id] = 0
        else:
            self._iterators[iterator_id] = len(self._records)
        logger.debug("KinesisStub.get_shard_iterator: id=%s type=%s", iterator_id, ShardIteratorType)
        return {"ShardIterator": iterator_id}

    def get_records(
        self,
        ShardIterator: str,
        Limit: int = 100,
        **kwargs: Any,
    ) -> dict:
        """
        Return buffered records from the given iterator position.

        Parameters
        ----------
        ShardIterator:
            Iterator ID returned by get_shard_iterator.
        Limit:
            Maximum number of records to return.

        Returns
        -------
        Fake Kinesis get_records response dict.
        """
        cursor = self._iterators.get(ShardIterator, len(self._records))
        all_records = list(self._records)
        batch = all_records[cursor: cursor + Limit]

        kinesis_records = [
            {
                "SequenceNumber": r["SequenceNumber"],
                "ApproximateArrivalTimestamp": r["ApproximateArrivalTimestamp"],
                "Data": r["Data"].encode("utf-8"),
                "PartitionKey": r["PartitionKey"],
            }
            for r in batch
        ]

        new_cursor = cursor + len(batch)
        new_iterator_id = str(uuid.uuid4())
        self._iterators[new_iterator_id] = new_cursor
        # Clean up old iterator
        self._iterators.pop(ShardIterator, None)

        logger.debug("KinesisStub.get_records: returned %d records", len(kinesis_records))
        return {
            "Records": kinesis_records,
            "NextShardIterator": new_iterator_id,
            "MillisBehindLatest": max(0, (len(self._records) - new_cursor) * 100),
        }

    # ------------------------------------------------------------------
    # Inspection / utility
    # ------------------------------------------------------------------

    def buffer_size(self) -> int:
        """Return the number of records currently in the buffer."""
        return len(self._records)

    def dump_to_file(self, path: str) -> None:
        """
        Write all buffered records to a JSONL file for offline inspection.

        Parameters
        ----------
        path:
            Destination file path (will be overwritten if it exists).
        """
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            for record in self._records:
                fh.write(json.dumps(record) + "\n")
        logger.info("KinesisStub: dumped %d records to %s", len(self._records), path)

    def clear(self) -> None:
        """Clear all buffered records and reset the sequence counter."""
        self._records.clear()
        self._iterators.clear()
        self._sequence = 0
        logger.info("KinesisStub: buffer cleared")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    stub = KinesisStub()

    # Quick smoke test
    stub.put_record(StreamName="pmt-sensor-stream", Data=b'{"device_id":"device-001"}', PartitionKey="device-001")
    stub.put_records(
        StreamName="pmt-sensor-stream",
        Records=[
            {"Data": b'{"device_id":"device-002"}', "PartitionKey": "device-002"},
            {"Data": b'{"device_id":"device-003"}', "PartitionKey": "device-003"},
        ],
    )
    it = stub.get_shard_iterator("pmt-sensor-stream", _FAKE_SHARD_ID, "TRIM_HORIZON")
    records = stub.get_records(it["ShardIterator"], Limit=10)
    print(f"Buffered {stub.buffer_size()} records, got {len(records['Records'])} back")
    stub.dump_to_file("/tmp/kinesis_stub_demo.jsonl")
