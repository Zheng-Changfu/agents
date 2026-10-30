"""Run against a real LiveKit server using LIVEKIT_URL/API_KEY/API_SECRET.

Point PYTHONPATH at the SDK checkout being reviewed. Only generated PCM is sent.
No provider, GPU, business application, or mock is involved.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import struct
import time
import uuid
from pathlib import Path

from livekit import api, rtc
from livekit.agents.voice.avatar import AudioSegmentEnd, DataStreamAudioReceiver

RATE = 24_000
SAMPLES = 240
SENDER = "sender"
RECEIVER = "receiver"


def report(case: str, **details: object) -> None:
    print(json.dumps({"at": int(time.time() * 1000), "case": case, **details}), flush=True)


def pcm(value: int) -> bytes:
    return struct.pack("<h", value) * SAMPLES


async def audio_frame(receiver: DataStreamAudioReceiver, value: int) -> None:
    frame = await asyncio.wait_for(anext(receiver), 10)
    if not isinstance(frame, rtc.AudioFrame) or bytes(frame.data) != pcm(value):
        raise AssertionError("未收到预期的真实 PCM 帧")


async def segment_end(receiver: DataStreamAudioReceiver, *, interrupted: bool) -> None:
    event = await asyncio.wait_for(anext(receiver), 10)
    if not isinstance(event, AudioSegmentEnd) or event.interrupted != interrupted:
        raise AssertionError("真实段结束标记的 interrupted 值不符")


async def stream(sender: rtc.Room, value: int) -> rtc.ByteStreamWriter:
    writer = await sender.local_participant.stream_bytes(
        name="generated-pcm",
        topic="lk.audio_stream",
        destination_identities=[RECEIVER],
        attributes={"sample_rate": str(RATE), "num_channels": "1"},
    )
    await writer.write(pcm(value))
    return writer


async def normal_segment(sender: rtc.Room, receiver: DataStreamAudioReceiver, value: int) -> None:
    writer = await stream(sender, value)
    await writer.aclose()
    await audio_frame(receiver, value)
    await segment_end(receiver, interrupted=False)


async def clear(sender: rtc.Room) -> None:
    result = await sender.local_participant.perform_rpc(
        destination_identity=RECEIVER, method="lk.clear_buffer", payload=""
    )
    if result != "ok":
        raise AssertionError("真实 clear_buffer RPC 未确认")


async def main() -> None:
    url = os.environ.get("LIVEKIT_URL", "ws://127.0.0.1:7880")
    key = os.environ["LIVEKIT_API_KEY"]
    secret = os.environ["LIVEKIT_API_SECRET"]
    expected_source = os.environ.get("PROBE_SDK_ROOT")
    if expected_source:
        actual = Path(inspect.getfile(DataStreamAudioReceiver)).resolve()
        if not actual.is_relative_to(Path(expected_source).resolve()):
            raise AssertionError("未加载指定的隔离 SDK 源码")
    report("source", isolated=bool(expected_source), generated_pcm=True)

    prefix = "receiver-probe-" + uuid.uuid4().hex[:10]
    names = [prefix + "-a", prefix + "-b"]
    peers: list[rtc.Room] = []
    receivers: list[DataStreamAudioReceiver] = []
    writers: list[rtc.ByteStreamWriter] = []
    senders: list[rtc.Room] = []
    destinations: list[rtc.Room] = []

    async with api.LiveKitAPI(url, key, secret) as client:
        try:
            for name in names:
                await client.room.create_room(api.CreateRoomRequest(name=name, empty_timeout=60))
                for identity, target in ((RECEIVER, destinations), (SENDER, senders)):
                    peer = rtc.Room()
                    peers.append(peer)
                    token = (
                        api.AccessToken(key, secret)
                        .with_identity(identity)
                        .with_grants(api.VideoGrants(room_join=True, room=name))
                        .to_jwt()
                    )
                    await peer.connect(url, token, options=rtc.RoomOptions(auto_subscribe=False))
                    target.append(peer)
                receiver = DataStreamAudioReceiver(
                    destinations[-1], sender_identity=SENDER, frame_size_ms=10
                )
                receivers.append(receiver)
                await receiver.start()
            report("connected", rooms=2, peers=4, equal_sender_identities=True)

            duplicate = DataStreamAudioReceiver(
                destinations[0], sender_identity=SENDER, frame_size_ms=10
            )
            receivers.append(duplicate)
            try:
                await duplicate.start()
            except ValueError:
                pass
            else:
                raise AssertionError("同一 Room 的第二个 receiver 本应注册失败")
            await duplicate.aclose()
            await normal_segment(senders[0], receivers[0], 1111)
            report("duplicate_receiver_close", passed=True, original_pcm_received=True)

            for target, other in ((0, 1), (1, 0)):
                target_writer = await stream(senders[target], 2000 + target)
                other_writer = await stream(senders[other], 3000 + other)
                writers.extend((target_writer, other_writer))
                await audio_frame(receivers[target], 2000 + target)
                await audio_frame(receivers[other], 3000 + other)
                await clear(senders[target])
                await segment_end(receivers[target], interrupted=True)
                await other_writer.write(pcm(4000 + other))
                await audio_frame(receivers[other], 4000 + other)
                await other_writer.aclose()
                writers.remove(other_writer)
                await segment_end(receivers[other], interrupted=False)
                await target_writer.aclose()
                writers.remove(target_writer)
                report(
                    "cross_room_clear",
                    target="a" if target == 0 else "b",
                    interrupted=True,
                    other_room_continued=True,
                    passed=True,
                )

            await receivers[0].aclose()
            replacement = DataStreamAudioReceiver(
                destinations[0], sender_identity=SENDER, frame_size_ms=10
            )
            receivers.append(replacement)
            await replacement.start()
            await normal_segment(senders[0], replacement, 5000)
            writer = await stream(senders[0], 6000)
            writers.append(writer)
            await audio_frame(replacement, 6000)
            await clear(senders[0])
            await segment_end(replacement, interrupted=True)
            await writer.aclose()
            writers.remove(writer)
            report("recreate_same_room", passed=True, normal_end=True, clear_end=True)
        finally:
            cleanup_results = await asyncio.gather(
                *(writer.aclose() for writer in writers), return_exceptions=True
            )
            cleanup_results += await asyncio.gather(
                *(receiver.aclose() for receiver in reversed(receivers)), return_exceptions=True
            )
            cleanup_results += await asyncio.gather(
                *(peer.disconnect() for peer in reversed(peers)), return_exceptions=True
            )
            rooms = await client.room.list_rooms(api.ListRoomsRequest(names=names))
            for room in rooms.rooms:
                await client.room.delete_room(api.DeleteRoomRequest(room=room.name))
            remaining = len((await client.room.list_rooms(api.ListRoomsRequest(names=names))).rooms)
            report("cleanup", remaining_rooms=remaining, disconnected_peers=len(peers))
            if remaining:
                raise AssertionError("临时 LiveKit 房间未清理")
            if any(isinstance(result, BaseException) for result in cleanup_results):
                raise RuntimeError("临时 RTC 资源关闭时发生异常")
    report("complete", passed=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
