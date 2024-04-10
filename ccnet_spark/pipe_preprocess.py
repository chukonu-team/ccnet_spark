from pathlib import Path
import pandas as pd
from typing import Iterable, Iterator, List, Optional, Union, TextIO
import gzip
from urllib.parse import urlparse
import os
import logging
import requests
from io import BytesIO
from tqdm import tqdm

from .util import convert_to_absolute_path
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(process)d:%(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
)
ROOT_DIR = "https://data.commoncrawl.org"
FileDescriptor = Union[Path, List[Path], str]
ReadableFileLike = Union[Iterable[str], FileDescriptor, None]


def get_paths_path(dump:str,cache_dir:str):
    download_dir = os.path.join(cache_dir, 'commoncrawl_data', dump)
    # 确保下载目录存在
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # 指定Common Crawl WET.paths.gz文件的URL
    paths_path_url =  os.path.join(ROOT_DIR,"crawl-data","CC-MAIN-"+dump,"wet.paths.gz")
    paths_path = os.path.join(download_dir, 'wet.paths.gz')
    if not os.path.exists(paths_path):
        # 下载WET.paths.gz文件
        response = requests.get(paths_path_url, stream=True)
        if response.status_code == 200:
            # 保存.gz文件到本地
            with open(paths_path, 'wb') as file:
                total_size = int(response.headers.get('content-length', 0))
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=f'Downloading {paths_path_url}', ascii=True) as pbar:
                    for data in response.iter_content(chunk_size=1024):
                        file.write(data)
                        pbar.update(len(data))
            print(f"Downloaded {paths_path_url}")
        else:
            print(f"Failed to download {paths_path_url}. Status code: {response.status_code}")
    return paths_path
def get_segment_count(dump:str,cache_dir:str):
    paths_path = get_paths_path(dump=dump,cache_dir=cache_dir)
    with gzip.open(paths_path, 'rb') as gzip_file:
        bytes_data = BytesIO(gzip_file.read())
        return len(bytes_data)
def get_segment_path(dump:str,cache_dir:str,segment:int):   
    paths_path = get_paths_path(dump=dump,cache_dir=cache_dir)
    with gzip.open(paths_path, 'rb') as gzip_file:
        bytes_data = BytesIO(gzip_file.read())
        # 逐行读取路径
        for index,line in enumerate(bytes_data):
            # 解码每一行（路径），这里假设文件是以UTF-8编码的
            segment_path = line.decode('utf-8').strip()
            # print(index,path)  # 打印路径，或者在这里进行其他处理
            if(index==segment):
                download_url=os.path.join(ROOT_DIR,segment_path)
                file_name=download_url.split("/")[-1]
                file_path_saved =os.path.join(cache_dir, 'commoncrawl_data', dump,file_name)
                if not os.path.exists(file_path_saved):
                    # 下载WET.paths.gz文件
                    response = requests.get(download_url, stream=True)
                    if response.status_code == 200:
                        with open(file_path_saved, 'wb') as file:
                            total_size = int(response.headers.get('content-length', 0))
                            with tqdm(total=total_size, unit='B', unit_scale=True, desc=f'Downloading {file_name}', ascii=True) as pbar:
                                for data in response.iter_content(chunk_size=1024):
                                    file.write(data)
                                    pbar.update(len(data))
                        print(f"Downloaded {file_path_saved}")
                return file_path_saved
    return None

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

def parse_warc_file(lines: Iterable[str],segmentName:str, min_len: int = 1) -> Iterator[pd.DataFrame]:
    docs_data = []  # 用于存储解析后的文档数据
    for doc in group_by_docs(lines):
        if doc and len(doc["raw_content"]) >= min_len:
            doc["cc_segment"] = segmentName
            docs_data.append(doc)

    if docs_data:
        df = pd.DataFrame(docs_data)
        logging.info(f"Created DataFrame with {len(df)} documents")
        return df
    else:
        logging.info("No documents to create DataFrame")
        return pd.DataFrame()


def load_segment2pdf(
    segment: int,
    cache_dir: str,
    dump: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
):
    cache_pdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) +"_segment_"+ str(segment)+"_min_len_"+str(min_len)+".parquet"
    cache_pdf_path = os.path.join(cache_dir,"pdf_parquet",dump,cache_pdf_name)


     # 设置输出路径
    if not os.path.exists(cache_pdf_path):
        os.makedirs("/".join(cache_pdf_path.split("/")[:-1]), exist_ok=True)
        segmentPath = get_segment_path(
            dump=dump,
            cache_dir=cache_dir,
            segment=segment
        )
        segmentName=segmentPath.split("/")[-1].split(".")[0]
        segmentPath = Path(segmentPath)
        segmentFile = open_read(segmentPath)
        pandas_df = parse_warc_file(segmentFile,segmentName, min_len=min_len)
        if isSample:
            sampleCount = int(sampleRate * len(pandas_df))
            pandas_df = pandas_df.sample(n=sampleCount, random_state=1)
        pandas_df.to_parquet(cache_pdf_path)  # 保存为 parquet 文件
    else:
        pandas_df = pd.read_parquet(cache_pdf_path)
    logging.info(
        f"load segment {segment}, {len(pandas_df)} docs, with sampleRate:{sampleRate*100 if isSample else 100 }%,min_len:{min_len},with dump:{dump}"
    )
    return pandas_df

def load_segment2sdf(
    spark,
    segment: int,
    cache_dir: str,
    dump: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
):
    cache_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) +"_segment_"+ str(segment)+"_min_len_"+str(min_len)+".parquet"
    cache_sdf_path = os.path.join(cache_dir,"sdf_parquet",dump,cache_sdf_name)
    cache_sdf_path = convert_to_absolute_path(cache_sdf_path)
    if not os.path.exists(cache_sdf_path):
        os.makedirs("/".join(cache_sdf_path.split("/")[:-1]), exist_ok=True)
        pandas_df = load_segment2pdf(
            segment=segment,
            cache_dir=cache_dir,
            dump=dump,
            isSample=isSample,
            sampleRate=sampleRate,
            min_len=min_len,
        )
        spark_df = spark.createDataFrame(pandas_df)
        spark_df.write.parquet(f"file:///{cache_sdf_path}")
    else:
        spark_df = spark.read.parquet(f"file:///{cache_sdf_path}")
    logging.info(
        f"load segment {segment}, with sampleRate:{sampleRate*100 if isSample else 100 }%,min_len:{min_len},with date:{dump}"
    )
    return spark_df

def load_segments(
    spark,
    segments: List[int],
    cache_dir: str,
    dump: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
):
    merged_sdf = None
    for seg in segments:
        sdf = load_segment2sdf(
            spark,
            segment=seg,
            cache_dir=cache_dir,
            dump=dump,
            isSample=isSample,
            sampleRate=sampleRate,
            min_len=min_len,
        )
        if merged_sdf:
            merged_sdf = merged_sdf.unionAll(sdf)  # Merge DataFrames
        else:
            merged_sdf = sdf
    return merged_sdf
