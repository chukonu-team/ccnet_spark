import numpy as np
from pathlib import Path
import pandas as pd
from typing import Iterable,Iterator, List, Optional, Set, Union,Sequence
import gzip
from typing import (
    Iterable,
    Iterator,
    List,
    Optional,
    TextIO,
    Union,
)
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(process)d:%(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
)
from urllib.parse import urlparse

FileDescriptor = Union[Path, List[Path], str]
ReadableFileLike = Union[Iterable[str], FileDescriptor, None]

def _close_when_exhausted(file: TextIO) -> Iterable[str]:
    with file:
        yield from file
def open_read(filename: ReadableFileLike) -> Iterable[str]:
    """
    `open_read` will decompress gzip files, given they have ".gz" suffix.
    """
    assert isinstance(filename, Path)
    assert filename.suffix == ".gz"
    
    logging.getLogger(__name__).info(f"Opening {filename} with mode 'rt'")

    file: TextIO = gzip.open(filename, "rt")  # type: ignore

    return _close_when_exhausted(file)

def parse_doc(headers: List[str], doc: List[str]) -> Optional[dict]:
    """Headers format is:
    WARC/1.0
    WARC-Type: conversion
    WARC-Target-URI: [url]
    WARC-Date: [crawldate: 2019-02-15T19:15:59Z]
    WARC-Record-ID: <urn:uuid:8865156e-d5f1-4734-9c68-4b46eaf2bb7e>
    WARC-Refers-To: <urn:uuid:340152e2-65cf-4143-b522-8ce4e2d069d7>
    WARC-Block-Digest: sha1:S3DTWCONT2L6ORTGCY2KXEZ37LNBB7V2
    Content-Type: text/plain
    Content-Length: 7743
    """
    if not headers or not doc:
        return None

    try:
        warc_type = headers[1].split()[1]
        if warc_type != "conversion":
            return None
        url = headers[2].split()[1]
        date = headers[3].split()[1]
        digest = headers[6].split()[1]
        length = int(headers[8].split()[1])
    except Exception as e:
        logging.warning("Can't parse header:", e, headers, doc)
        return None

    # Docs are separated by two empty lines.
    last = None
    if not doc[-1] and not doc[-2]:
        last = -2
    title, doc = doc[0], doc[1:last]

    return {
        "url": url,
        "date_download": date,
        "digest": digest,
        "length": length,
        "nlines": len(doc),
        "source_domain": urlparse(url).netloc,
        "title": title,
        "raw_content": "\n".join(doc),
    }


def group_by_docs(warc_lines: Iterable[str]) -> Iterable[dict]:
    doc: List[str] = []
    headers, read_headers = [], True
    for warc in warc_lines:
        warc = warc.strip()
        if read_headers:
            headers.append(warc)
            read_headers = warc != ""
            continue

        if warc == "WARC/1.0":
            # We reached the beginning of the new doc.
            parsed = parse_doc(headers, doc)
            if parsed is not None:
                yield parsed
            headers, doc, read_headers = [warc], [], True
            continue

        doc.append(warc)

    # Return the last document
    if doc:
        parsed = parse_doc(headers, doc)
        if parsed is not None:
            yield parsed


# def parse_warc_file(lines: Iterable[str], min_len: int = 1) -> Iterator[dict]:
#     n_doc = 0
#     n_ok = 0
#     for doc in group_by_docs(lines):
#         n_doc += 1
#         if not doc or len(doc["raw_content"]) < min_len:
#             continue
#         n_ok += 1
#         yield doc
#     if n_doc > 0:
#         logging.info(f"Kept {n_ok:_d} documents over {n_doc:_d} ({n_ok / n_doc:.1%}).")
#     else:
#         logging.info(f"Found no documents")
def parse_warc_file(lines: Iterable[str], min_len: int = 1) -> Iterator[pd.DataFrame]:
    docs_data = []  # 用于存储解析后的文档数据
    for doc in group_by_docs(lines):
        if doc and len(doc["raw_content"]) >= min_len:
            docs_data.append(doc)

    if docs_data:
        df = pd.DataFrame(docs_data)
        logging.info(f"Created DataFrame with {len(df)} documents")
        return df
    else:
        logging.info("No documents to create DataFrame")
        return pd.DataFrame()
