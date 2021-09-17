from pathlib import Path
from typing import Iterator, Tuple, ItemsView, ValuesView, KeysView, Iterable, Set, Optional, Callable, Any, Union, \
    Mapping

from rocksdb import DB
from yarl import URL

from .rocksdb import Store as RocksDbStore, _DBValueTuple
from .. import EventT
from ..types import TP, CollectionT, AppT
from ..types.stores import KT, VT


class RocksDbCache(RocksDbStore):

    def __init__(self, url: Union[str, URL], app: AppT, table: CollectionT, *, key_index_size: int = None,
                 options: Mapping[str, Any] = None, **kwargs: Any) -> None:
        super().__init__(url, app, table, key_index_size=key_index_size, options=options, **kwargs)

    def __getitem__(self, key: KT) -> VT:
        return super().__getitem__(key)

    def __setitem__(self, key: KT, value: VT) -> None:
        super().__setitem__(key, value)

    def __delitem__(self, key: KT) -> None:
        super().__delitem__(key)

    def __iter__(self) -> Iterator[KT]:
        return super().__iter__()

    def __len__(self) -> int:
        return super().__len__()

    def __contains__(self, key: object) -> bool:
        return super().__contains__(key)

    def keys(self) -> KeysView:
        return super().keys()

    def _keys_decoded(self) -> Iterator[KT]:
        return super()._keys_decoded()

    def values(self) -> ValuesView:
        return super().values()

    def _values_decoded(self) -> Iterator[VT]:
        return super()._values_decoded()

    def items(self) -> ItemsView:
        return super().items()

    def _items_decoded(self) -> Iterator[Tuple[KT, VT]]:
        return super()._items_decoded()

    def clear(self) -> None:
        super().clear()

    def persisted_offset(self, tp: TP) -> Optional[int]:
        return super().persisted_offset(tp)

    def set_persisted_offset(self, tp: TP, offset: int) -> None:
        super().set_persisted_offset(tp, offset)

    async def need_active_standby_for(self, tp: TP) -> bool:
        return await super().need_active_standby_for(tp)

    def apply_changelog_batch(self, batch: Iterable[EventT], to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        super().apply_changelog_batch(batch, to_key, to_value)

    def _set(self, key: bytes, value: Optional[bytes]) -> None:
        super()._set(key, value)

    def _db_for_partition(self, partition: int) -> DB:
        return super()._db_for_partition(partition)

    def _open_for_partition(self, partition: int) -> DB:
        return super()._open_for_partition(partition)

    def _get(self, key: bytes) -> Optional[bytes]:
        return super()._get(key)

    def _get_bucket_for_key(self, key: bytes) -> Optional[_DBValueTuple]:
        return super()._get_bucket_for_key(key)

    def _del(self, key: bytes) -> None:
        super()._del(key)

    async def on_rebalance(self, assigned: Set[TP], revoked: Set[TP], newly_assigned: Set[TP],
                           generation_id: int = 0) -> None:
        return await super().on_rebalance(assigned, revoked, newly_assigned, generation_id)

    async def stop(self) -> None:
        return await super().stop()

    def revoke_partitions(self, table: CollectionT, tps: Set[TP]) -> None:
        super().revoke_partitions(table, tps)

    async def assign_partitions(self, table: CollectionT, tps: Set[TP], generation_id: int = 0) -> None:
        return await super().assign_partitions(table, tps, generation_id)

    def _recover_db(self, tp: TP, is_standby: bool) -> None:
        super()._recover_db(tp, is_standby)

    async def _try_open_db_for_partition(self, partition: int, max_retries: int = 30, retry_delay: float = 1.0,
                                         generation_id: int = 0) -> DB:
        return await super()._try_open_db_for_partition(partition, max_retries, retry_delay, generation_id)

    def _contains(self, key: bytes) -> bool:
        return super()._contains(key)

    def _dbs_for_key(self, key: bytes) -> Iterable[DB]:
        return super()._dbs_for_key(key)

    def _dbs_for_actives(self) -> Iterator[DB]:
        return super()._dbs_for_actives()

    def _size(self) -> int:
        return super()._size()

    def _visible_keys(self, db: DB) -> Iterator[bytes]:
        return super()._visible_keys(db)

    def _visible_items(self, db: DB) -> Iterator[Tuple[bytes, bytes]]:
        return super()._visible_items(db)

    def _visible_values(self, db: DB) -> Iterator[bytes]:
        return super()._visible_values(db)

    def _size1(self, db: DB) -> int:
        return super()._size1(db)

    def _iterkeys(self) -> Iterator[bytes]:
        return super()._iterkeys()

    def _itervalues(self) -> Iterator[bytes]:
        return super()._itervalues()

    def _iteritems(self) -> Iterator[Tuple[bytes, bytes]]:
        return super()._iteritems()

    def _clear(self) -> None:
        pass

    def reset_state(self) -> None:
        super().reset_state()

    def partition_path(self, partition: int) -> Path:
        return super().partition_path(partition)

    def backup_path(self, partition: int) -> Path:
        return super().backup_path(partition)

    def _path_with_suffix(self, path: Path, *, suffix: str = ".db") -> Path:
        return super()._path_with_suffix(path, suffix=suffix)

    def __hash__(self) -> int:
        return super().__hash__()

    async def on_recovery_completed(self, active_tps: Set[TP], standby_tps: Set[TP]) -> None:
        return await super().on_recovery_completed(active_tps, standby_tps)

    def _encode_key(self, key: KT) -> bytes:
        return super()._encode_key(key)

    def _encode_value(self, value: VT) -> Optional[bytes]:
        return super()._encode_value(value)

    def _decode_key(self, key: Optional[bytes]) -> KT:
        return super()._decode_key(key)

    def _decode_value(self, value: Optional[bytes]) -> VT:
        return super()._decode_value(value)

    def _repr_info(self) -> str:
        return super()._repr_info()

    @classmethod
    def fromkeys(cls, iterable: Iterable[KT], value: VT = None) -> 'FastUserDict':
        return super().fromkeys(iterable, value)

    def copy(self) -> dict:
        return super().copy()

    def update(self, *args: Any, **kwargs: Any) -> None:
        super().update(*args, **kwargs)

    def pop(self, key: KT) -> VT:
        return super().pop(key)

    def popitem(self) -> Tuple[KT, VT]:
        return super().popitem()

    def setdefault(self, key: KT, default: VT = ...) -> VT:
        return super().setdefault(key, default)

    def get(self, key: KT) -> Optional[VT_co]:
        return super().get(key)
