import json

from opentelemetry import trace
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from siamux.db import Run, session_factory
from siamux.worker import process_run


def test_trace_continues_from_api_to_worker_and_provider(submit):
    exporter = InMemorySpanExporter()
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(exporter))
    run_id = submit(input="private prompt").json()["id"]
    with session_factory()() as session:
        carrier = session.get(Run, run_id).trace_context
    assert "traceparent" in carrier
    assert process_run(run_id)
    spans = exporter.get_finished_spans()
    api_span = next(s for s in spans if s.name == "HTTP POST")
    worker_span = next(s for s in spans if s.name == "run.execute")
    provider_span = next(s for s in spans if s.name == "provider.generate")
    assert api_span.context.trace_id == worker_span.context.trace_id
    assert worker_span.parent.span_id == api_span.context.span_id
    assert provider_span.parent.span_id == worker_span.context.span_id
    assert "private prompt" not in json.dumps([dict(s.attributes) for s in spans])
