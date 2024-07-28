from __future__ import annotations

import base64
import json
import pathlib
from collections.abc import Callable, Generator, Iterable


class UnknownEncodingError(ValueError):
    pass


def iterextract(
    lines: Iterable[bytes],
    extract_filter: Callable[[pathlib.PurePosixPath], bool] | None = None,
) -> Generator[tuple[pathlib.PurePosixPath, bytes]]:
    url = None
    content = []
    should_extract = False
    for line in lines:
        if content:
            content.append(line)
            if b"},\n" in line:
                should_extract = False

                data = b"{" + b"".join(content).rstrip(b",\n") + b"}"
                content = []

                assert url
                yield url, decode_content(json.loads(data)["content"])

        elif b'"url":' in line:
            data = b"{" + line.rstrip(b",\n ") + b"}"
            url = pathlib.PurePosixPath(json.loads(data)["url"])
            should_extract = extract_filter(url) if extract_filter else True

        elif should_extract and b'"content":' in line:
            content.append(line)


def decode_content(content: dict[str, str]) -> bytes:
    if "text" not in content:
        raise ValueError("No content available")

    encoding = content.get("encoding")
    if not encoding:
        return content["text"].encode()

    if encoding == "base64":
        return base64.b64decode(content["text"])

    message = f"Unknown encoding: {encoding}"
    raise UnknownEncodingError(message)
