from pyspark.sql import SparkSession
import os
import requests
from io import BytesIO
import gzip
import re
from tqdm import tqdm
import time
from typing import Iterable, Iterator, List, Optional, Union, TextIO
import logging
from pathlib import Path
from urllib.parse import urlparse
from functools import reduce
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(process)d:%(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from urllib.parse import urlparse
from pathlib import Path
import logging
import gzip

FileDescriptor = Union[Path, List[Path], str]
ReadableFileLike = Union[Iterable[str], FileDescriptor, None]

# 定义下载函数
def download_segment(segment, dump, cache_dir, root_dir):
    download_dir = os.path.join(cache_dir, 'commoncrawl_data', dump)
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    paths_path_url = os.path.join(root_dir, "crawl-data", "CC-MAIN-" + dump, "wet.paths.gz")
    paths_path = os.path.join(download_dir, 'wet.paths.gz')

    if not os.path.exists(paths_path):
        response = requests.get(paths_path_url, stream=True)
        if response.status_code == 200:
            with open(paths_path, 'wb') as file:
                total_size = int(response.headers.get('content-length', 0))
                with tqdm(total=total_size, unit='B', unit_scale=True,
                          desc=f'Downloading {paths_path_url}', ascii=True) as pbar:
                    for data in response.iter_content(chunk_size=1024):
                        file.write(data)
                        pbar.update(len(data))
            print(f"Downloaded {paths_path_url}")
        else:
            print(f"Failed to download {paths_path_url}. Status code: {response.status_code}")

    with gzip.open(paths_path, 'rb') as gzip_file:
        bytes_data = BytesIO(gzip_file.read())
        for index, line in enumerate(bytes_data):
            segment_path = line.decode('utf-8').strip()
            pattern = r"-(\d+)\.warc"
            match = re.search(pattern, segment_path)
            if match:
                segment_number = int(match.group(1))
                if segment_number == segment:
                    download_url = os.path.join(root_dir, segment_path)
                    file_name = download_url.split("/")[-1]
                    file_path_saved = os.path.join(cache_dir, 'commoncrawl_data', dump, file_name)
                    if not os.path.exists(file_path_saved):
                        response = requests.get(download_url, stream=True)
                        if response.status_code == 200:
                            with open(file_path_saved, 'wb') as file:
                                total_size = int(response.headers.get('content-length', 0))
                                with tqdm(total=total_size, unit='B', unit_scale=True,
                                          desc=f'Downloading {file_name}', ascii=True) as pbar:
                                    for data in response.iter_content(chunk_size=1024):
                                        file.write(data)
                                        pbar.update(len(data))
                            print(f"Downloaded {file_path_saved}")
                    return file_path_saved
    return None

# 停止 SparkSession
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
def parse_warc_file(lines: Iterable[str],segmentName:str, min_len: int = 1):
    docs_data = []  # 用于存储解析后的文档数据
    for doc in group_by_docs(lines):
        if doc and len(doc["raw_content"]) >= min_len:
            doc["cc_segment"] = segmentName
            docs_data.append(doc)
    return docs_data
def download_and_parse(segment,segment_name, dump, cache_dir, root_dir):
    file_path = download_segment(segment, dump, cache_dir, root_dir)
    return parse_warc_file(open_read(Path(file_path)), segment_name)

def get_paths_path(dump:str,cache_dir:str):
    download_dir = os.path.join(cache_dir, 'commoncrawl_data', dump)
    # 确保下载目录存在
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # 指定Common Crawl WET.paths.gz文件的URL
    paths_path_url =  os.path.join(ROOT_DIR,"crawl-data","CC-MAIN-"+dump,"wet.paths.gz")
    paths_path = os.path.join(download_dir, 'wet.paths.gz')
    return paths_path
if __name__ == "__main__":
    import time
    t1=time.time()
    # 定义要下载的段列表
    segments = [i for i in range(100)]
    dump = "2019-18"  # 替换为你的 dump 名称
    cache_dir = "/metadata0/wxl_data/cached_data/"  # 替换为你的缓存目录
    root_dir = "https://data.commoncrawl.org"  # 替换为你的根目录
    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("Parallel File Download") \
        .config("spark.driver.memory", "50g") \
        .config("spark.driver.maxResultSize", "50g") \
        .getOrCreate()
    res = spark.sparkContext.parallelize(segments).flatMap(lambda segment: download_and_parse(segment,segment, dump, cache_dir, root_dir))
    df = spark.createDataFrame(res)
    c = df.count()
    t2 = time.time()
    print("=====================================================================")
    print(f"timeconsume: download_and_parse: {t2 - t1}s, doc count: {c}; segments count:{len(segments)}")
    print("=====================================================================")
