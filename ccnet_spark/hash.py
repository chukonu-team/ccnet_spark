import warnings
from typing import Iterable, Iterator, Sequence, Sized, Tuple, Type,Optional
import numpy as np
import hashlib

from .text_normalizer import normalize_for_dedup
HASH_TYPE: Type[np.uint64] = np.uint64

GETPY_WARNING = False


class AbstractDedupHashSet(Sized, Iterable[np.uint64]):
    """A dict-like that returns `True` for keys that have been added more than once.

    The API is batched and expect np.array as input. This batching grants better
    perf when using the C++ implementation.
    """

    dtype: Type[np.uint64] = HASH_TYPE

    def __repr__(self):
        implementation = type(self).__name__
        return f"[{implementation}, len: {len(self)}"

    def __len__(self) -> int:
        ...

    def __contains__(self, values: Sequence[np.uint64]) -> np.ndarray:
        ...

    def __getitem__(self, values) -> np.ndarray:
        ...

    def __setitem__(self, keys, values) -> None:
        ...

    def items(self) -> Iterable[Tuple[np.uint64, np.uint8]]:
        ...

    def keys(self) -> Iterable[np.uint64]:
        ...

    def __iter__(self) -> Iterator[np.uint64]:
        return iter(self.keys())

    def add(self, h, contains=None):
        """Add the given keys. First time a key is added the value is set to 0,
        then it's set to one."""
        if not isinstance(h, np.ndarray):
            h = np.array(h, dtype=HASH_TYPE)
        if contains is None:
            contains = self.__contains__(h)

        self.__setitem__(h, contains)
        return contains

    def merge(self, keys, values):
        contains = self.__contains__(keys)
        self.__setitem__(keys, contains | values)

    def dump(self, filename):
        return self.dump_np(filename)

    def load(self, filename):
        return self.load_np(filename)

    def dump_np(self, filename):
        kv_type = np.dtype([("k", HASH_TYPE), ("v", np.uint8)])
        items = np.fromiter(self.items(), dtype=kv_type, count=len(self))
        with open(filename, "wb") as f:
            np.save(f, items)

    def load_np(self, filename):
        items = np.load(str(filename))
        keys = items["k"].copy()
        values = items["v"].copy()
        self.merge(keys, values)

    def dump_np2(self, filename):
        keys = np.fromiter(
            (k for (k, v) in self.items()), dtype=HASH_TYPE, count=len(self)
        )
        with open(filename, "wb") as f:
            np.save(f, keys)

        values = np.fromiter(
            (v for (k, v) in self.items()), dtype=np.uint8, count=len(self)
        )
        with open(str(filename) + ".val", "wb") as f:
            np.save(f, values)

    def load_np2(self, filename):
        keys = np.load(filename)
        values = np.load(str(filename) + ".val")
        self.merge(keys, values)

class NaiveHashSet(dict, AbstractDedupHashSet):
    """Pure python implementation of AbstractDedupHashSet.

    This implementation is quite fast, since Python dict are heavily optimized.
    """

    def __init__(self, iterable=None):
        super().__init__()
        global GETPY_WARNING
        if GETPY_WARNING:
            warnings.warn(
                "Module 'getpy' not found. Deduplication will take more RAM."
                " Try `pip install cc_net[getpy]"
            )
        GETPY_WARNING = False

    def __contains__(self, values):
        """Returns `True` if the object has been added at list once."""
        contains_point = super().__contains__
        return np.fromiter(
            map(contains_point, values), count=len(values), dtype=np.uint8
        )

    def __getitem__(self, values):
        """Returns `True` if the object has been added at list twice."""
        get_point = super().get
        return np.fromiter(
            map(lambda x: get_point(x, False), values),
            count=len(values),
            dtype=np.uint8,
        )

    def __setitem__(self, keys, values):
        assert len(keys) == len(values)
        for k, v in zip(keys, values):
            dict.__setitem__(self, k, v)

HASH_TYPE: Type[np.uint64] = np.uint64
HASH_SIZE = HASH_TYPE(0).nbytes
# def compute_hashes(content) -> Optional[np.ndarray]:
#     if not content:
#         return None
#     lines = content.split("\n")
#     # save hashes as bytes but reinterpret them as uint64.
#     hashes = np.fromiter(
#         (
#             hashlib.sha1(bytes(normalize_for_dedup(l), encoding="utf-8")).digest()[
#                 :HASH_SIZE
#             ]
#             for l in lines
#         ),
#         dtype=np.dtype((bytes, HASH_SIZE)),
#         count=len(lines),
#     )
#     return np.ndarray(dtype=HASH_TYPE, buffer=hashes.data, shape=hashes.shape)
def compute_hashes(content) -> Optional[np.ndarray]:
    if not content:
        return None
    lines = content.split("\n")  # 按换行分割内容
    hashes = []
    for l in lines:
        # 这里假设 normalize_for_dedup 函数已经定义好，用于处理 l
        normalized_line = normalize_for_dedup(l)
        # 计算 SHA-1 哈希值，并截取前 HASH_SIZE 个字节
        line_hash = hashlib.sha1(bytes(normalized_line, encoding="utf-8")).digest()[:HASH_SIZE]
        hashes.append(line_hash)
    # 将列表转换为 NumPy 数组
    return np.array(hashes, dtype=np.uint64)
