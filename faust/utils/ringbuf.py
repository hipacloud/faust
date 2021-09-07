import time
import typing


class SequenceOverwritten(Exception):
    pass


class SequenceNotFound(Exception):
    pass


class Empty(Exception):
    pass


class RingBuffer:
    def __init__(
        self,
        size: int,
        factory: typing.Callable[[], typing.Any],
    ):
        if not size % 2 == 0:
            raise AttributeError("size must be a factor of 2 for efficient arithmetic.")

        self.size: int = size
        self.factory = factory

        self._cursor_position = 0  # position of next write
        self._ring: typing.List[typing.Any] = [self.factory() for _ in range(size)]

    def put(self, value) -> int:
        cursor_position = self._cursor_position
        ring_index = cursor_position % self.size

        self._cursor_position += 1
        self._ring[ring_index] = value
        return cursor_position

    def get(self, idx: int) -> typing.Tuple[int, typing.Any]:
        cursor_position = self._cursor_position
        if idx >= cursor_position:
            raise SequenceNotFound()

        if idx < cursor_position - self.size:
            raise SequenceOverwritten()

        return (idx, self._ring[idx % self.size].get())

    def get_latest(self) -> typing.Tuple[int, typing.Any]:
        cursor_position = self._cursor_position
        if cursor_position <= 0:
            raise Empty()

        idx = cursor_position - 1

        return self.get(idx)

    def clear(self) -> None:
        self._ring = [self.factory() for _ in range(self.size)]
        self._cursor_position = 0


if __name__ == '__main__':
    buffer = RingBuffer(10, lambda: 0)
    print(buffer._ring)

    for i in range(12):
        buffer.put(i)

    start = time.time()
    for i in range(1_000_000):
        sum(buffer._ring) / buffer.size
    elapsed = time.time() - start
    print(elapsed)

