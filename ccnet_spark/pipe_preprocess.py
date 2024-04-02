from pathlib import Path
import pandas as pd
from typing import Iterable, Iterator, List, Optional, Union, TextIO
import gzip
from urllib.parse import urlparse
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(process)d:%(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
)

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


def getSegmentPath(segment: int, cache_folder: str, date: str = "2019-09"):
    cache_file_prefix = "CC-MAIN-20190215183319-20190215205319-"
    cache_file_sufix = ".warc.wet.gz"
    segment_str = str(segment).zfill(5)  # Pad with leading zeros
    return Path(
        cache_folder
        + "/"
        + date
        + "/"
        + cache_file_prefix
        + segment_str
        + cache_file_sufix
    )


def load_segment2pdf(
    segment: int,
    cache_folder: str,
    date: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 30,
):
    samplePath = "_sampleRate_" + str(int(sampleRate if isSample * 100 else 100))
    output_path = (
        cache_folder
        + "/cache_pdf_parquet/"
        + date
        + "/"
        + str(segment)
        + "_pdf_"
        + samplePath
        + ".parquet"
    )  # 设置输出路径
    if not os.path.exists(output_path):
        os.makedirs("/".join(output_path.split("/")[:-1]), exist_ok=True)
        segmentPath = getSegmentPath(
            segment=segment, cache_folder=cache_folder, date=date
        )
        segmentFile = open_read(segmentPath)
        pandas_df = parse_warc_file(segmentFile, min_len=min_len)
        if isSample:
            sampleCount = int(sampleRate * len(pandas_df))
            pandas_df = pandas_df.sample(n=sampleCount, random_state=1)
        pandas_df.to_parquet(output_path)  # 保存为 parquet 文件
    else:
        pandas_df = pd.read_parquet(output_path)
    logging.info(
        f"load segment {segment}, {len(pandas_df)} docs, with sampleRate:{sampleRate*100 if isSample else 100 }%,min_len:{min_len},with date:{date}"
    )
    return pandas_df


def load_segment2sdf(
    spark,
    segment: int,
    cache_folder: str,
    date: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 30,
):
    samplePath = "_sampleRate_" + str(int(sampleRate if isSample * 100 else 100))
    output_path = (
        cache_folder
        + "/cache_sdf_parquet/"
        + date
        + "/"
        + str(segment)
        + "_sdf_"
        + samplePath
        + ".parquet"
    )  # 设置输出路径
    if not os.path.exists(output_path):
        os.makedirs("/".join(output_path.split("/")[:-1]), exist_ok=True)
        pandas_df = load_segment2pdf(
            segment=segment,
            cache_folder=cache_folder,
            date=date,
            isSample=isSample,
            sampleRate=sampleRate,
            min_len=min_len,
        )
        spark_df = spark.createDataFrame(pandas_df)
        spark_df.write.parquet(f"file:///{output_path}")
    else:
        spark_df = spark.read.parquet(f"file:///{output_path}")
    logging.info(
        f"load segment {segment}, with sampleRate:{sampleRate*100 if isSample else 100 }%,min_len:{min_len},with date:{date}"
    )
    return spark_df


def load_segments(
    spark,
    segments: List[int],
    cache_folder: str,
    date: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
):
    merged_sdf = None
    for seg in segments:
        sdf = load_segment2sdf(
            spark,
            segment=seg,
            cache_folder=cache_folder,
            date=date,
            isSample=isSample,
            sampleRate=sampleRate,
            min_len=min_len,
        )
        if merged_sdf:
            merged_sdf = merged_sdf.unionAll(sdf)  # Merge DataFrames
        else:
            merged_sdf = sdf
    return merged_sdf
