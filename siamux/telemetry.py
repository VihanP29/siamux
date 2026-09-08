import json
import logging
import os
import sys

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from siamux.config import get_settings

_configured_pid = None
logger = logging.getLogger("siamux")


def configure(service: str):
    global _configured_pid
    if _configured_pid == os.getpid():
        return
    _configured_pid = os.getpid()
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        logger.addHandler(logging.StreamHandler(sys.__stderr__))
    logger.propagate = False
    provider = TracerProvider(resource=Resource.create({"service.name": service}))
    endpoint = get_settings().otlp_endpoint
    if endpoint:
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)


def event(name: str, **fields):
    ctx = trace.get_current_span().get_span_context()
    logger.info(json.dumps({"event": name, "trace_id": format(ctx.trace_id, "032x"), **fields}))
