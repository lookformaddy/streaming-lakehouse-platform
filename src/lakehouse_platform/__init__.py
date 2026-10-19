"""Streaming Lakehouse Platform.

Kafka-style ingestion -> data quality gate -> stream processing with
event-time windows -> lakehouse tables with a transaction log, operated
against latency and data-quality SLIs.
"""

from .events import Event
from .generator import SyntheticOrderGenerator
from .metrics import LatencyTracker, SLIEvaluator
from .pipeline import PipelineReport, StreamingPipeline
from .processing import TumblingWindowProcessor, WindowResult
from .quality import DataQualityEngine, QualityReport
from .table import LakehouseTable
from .transport import FileReplaySource, FileSink, InProcessBus

__version__ = "0.1.0"

__all__ = [
    "Event",
    "SyntheticOrderGenerator",
    "LatencyTracker",
    "SLIEvaluator",
    "PipelineReport",
    "StreamingPipeline",
    "TumblingWindowProcessor",
    "WindowResult",
    "DataQualityEngine",
    "QualityReport",
    "LakehouseTable",
    "FileReplaySource",
    "FileSink",
    "InProcessBus",
]
