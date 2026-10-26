"""Minimal Server-Sent Events (SSE) parser.

SSE is a plain-text protocol over HTTP:
    event: message
    id: [{"topic":"...","partition":0,"offset":123}]
    data: {"wiki":"azwiki", ...}
    <blank line = end of event>

Written by hand (instead of a library) so the parsing rules are explicit and
unit-testable.
"""
from dataclasses import dataclass
from typing import Iterable, Iterator


@dataclass
class SSEEvent:
    event: str = "message"
    id: str | None = None
    data: str = ""


def parse_sse(lines: Iterable[str]) -> Iterator[SSEEvent]:
    event, data_lines, event_id = "message", [], None
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line == "":                       # dispatch
            if data_lines:
                yield SSEEvent(event=event, id=event_id, data="\n".join(data_lines))
            event, data_lines, event_id = "message", [], None
            continue
        if line.startswith(":"):             # comment / heartbeat
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "event":
            event = value
        elif field == "data":
            data_lines.append(value)
        elif field == "id":
            event_id = value
    if data_lines:                           # stream closed without trailing blank line
        yield SSEEvent(event=event, id=event_id, data="\n".join(data_lines))
