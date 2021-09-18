from collections import defaultdict
from typing import (
    Iterator,
    Tuple,
    ItemsView,
    ValuesView,
    KeysView,
    Iterable,
    Set,
    Optional,
    Callable,
    Any,
    Union,
    Mapping,
    Dict,
)

from yarl import URL

from .rocksdb import Store as RocksDbStore
from .. import EventT, current_event
from ..types import TP, CollectionT, AppT
from ..types.stores import KT, VT


def _current_partition() -> int:
    event = current_event()
    assert event is not None
    return event.message.partition


class CachingRocksDbStore(RocksDbStore):
    """A write-through cache for rocksdb to reduce db (disk) access"""

    def __init__(
        self,
        url: Union[str, URL],
        app: AppT,
        table: CollectionT,
        *,
        key_index_size: int = None,
        options: Mapping[str, Any] = None,
        **kwargs: Any
    ) -> None:
        self._cache: Dict[int, Dict[KT, VT]] = defaultdict(dict)  # cache per partition
        super().__init__(
            url, app, table, key_index_size=key_index_size, options=options, **kwargs
        )

    def __getitem__(self, key: KT) -> VT:
        partition = _current_partition()
        return self._cache[partition][key]

    def __setitem__(self, key: KT, value: VT) -> None:
        super().__setitem__(key, value)
        partition = _current_partition()
        self._cache[partition][key] = value

    def __delitem__(self, key: KT) -> None:
        super().__delitem__(key)
        partition = _current_partition()
        del self._cache[partition][key]

    def __iter__(self) -> Iterator[KT]:
        partition = _current_partition()
        for key in self._cache[partition]:
            yield key

    def __len__(self) -> int:
        partition = _current_partition()
        return len(self._cache[partition])

    def __contains__(self, key: object) -> bool:
        partition = _current_partition()
        return key in self._cache[partition]

    def keys(self) -> KeysView:
        partition = _current_partition()
        return self._cache[partition].keys()

    def values(self) -> ValuesView:
        partition = _current_partition()
        return self._cache[partition].values()

    def items(self) -> ItemsView:
        partition = _current_partition()
        return self._cache[partition].items()

    def clear(self) -> None:
        super().clear()
        self._cache.clear()

    def apply_changelog_batch(
        self,
        batch: Iterable[EventT],
        to_key: Callable[[Any], Any],
        to_value: Callable[[Any], Any],
    ) -> None:
        super().apply_changelog_batch(batch, to_key, to_value)

        for event in batch:
            msg = event.message
            partition = msg.partition
            decoded_key = self._decode_key(msg.key)

            if msg.value is None:
                self._cache[partition].pop(decoded_key, None)
            else:
                decoded_val = self._decode_value(msg.value)
                self._cache[partition][decoded_key] = decoded_val

    def revoke_partitions(self, table: CollectionT, tps: Set[TP]) -> None:
        super().revoke_partitions(table, tps)
        for tp in tps:
            self._cache.pop(tp.partition, None)

    async def on_rebalance(
        self,
        assigned: Set[TP],
        revoked: Set[TP],
        newly_assigned: Set[TP],
        generation_id: int = 0,
    ) -> None:
        await super().on_rebalance(
            assigned, revoked, newly_assigned, generation_id
        )

        # populate cache from recovered dbs
        active_tps = self.app.assignor.assigned_actives()
        standby_tps = self.app.assignor.assigned_standbys()
        assigned_tps = active_tps.union(standby_tps)

        for tp in assigned_tps:
            partition = tp.partition
            db = self._db_for_partition(partition)
            for key, val in self._visible_items(db):
                decoded_key = self._decode_key(key)
                decoded_val = self._decode_value(val)
                self._cache[partition][decoded_key] = decoded_val

    def reset_state(self) -> None:
        super().reset_state()
        self._cache.clear()

    def copy(self) -> dict:
        partition = _current_partition()
        return self._cache[partition].copy()

    def update(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("[update] not supported")

    def get(self, key: KT) -> Optional[VT]:
        partition = _current_partition()
        return self._cache[partition].get(key)

    def pop(self, key: KT, *default: Optional[VT]) -> VT:
        super().pop(key, *default)
        partition = _current_partition()
        return self._cache[partition].pop(key, *default)

    def popitem(self) -> Tuple[KT, VT]:
        partition = _current_partition()
        self._cache[partition].popitem()
        return super().popitem()

    def setdefault(self, key: KT, default: VT = ...) -> VT:
        raise NotImplementedError("[setdefault] not supported")
