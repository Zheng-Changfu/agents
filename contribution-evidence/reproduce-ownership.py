"""Minimal real-RTC reproduction; credentials are read only from environment."""

import argparse
import asyncio
import inspect
import json
import logging
import os
import uuid
from pathlib import Path

from livekit import api, rtc
from livekit.agents.voice.avatar import DataStreamAudioReceiver


def report(case, **details):
    print(json.dumps({"case": case, **details}), flush=True)


async def main(expect_regression):
    url = os.environ.get("LIVEKIT_URL", "ws://127.0.0.1:7880")
    key, secret = os.environ["LIVEKIT_API_KEY"], os.environ["LIVEKIT_API_SECRET"]
    source = Path(os.environ["PROBE_SDK_ROOT"]).resolve()
    if not Path(inspect.getfile(DataStreamAudioReceiver)).resolve().is_relative_to(source):
        raise AssertionError("The isolated SDK source was not loaded")
    names = ["receiver-repro-" + uuid.uuid4().hex[:10] for _ in range(2)]
    peers, receivers, senders, destinations = [], [], [], []
    observed = [0, 0]
    async with api.LiveKitAPI(url, key, secret) as client:
        try:
            for index, name in enumerate(names):
                await client.room.create_room(api.CreateRoomRequest(name=name, empty_timeout=60))
                for identity, group in (("receiver", destinations), ("sender", senders)):
                    peer = rtc.Room()
                    peers.append(peer)
                    token = (
                        api.AccessToken(key, secret)
                        .with_identity(identity)
                        .with_grants(api.VideoGrants(room_join=True, room=name))
                        .to_jwt()
                    )
                    await peer.connect(url, token, options=rtc.RoomOptions(auto_subscribe=False))
                    group.append(peer)
                receiver = DataStreamAudioReceiver(destinations[-1], sender_identity="sender")
                receivers.append(receiver)

                def cleared(index=index):
                    observed[index] += 1

                receiver.on("clear_buffer", cleared)
                await receiver.start()
            await senders[0].local_participant.perform_rpc(
                destination_identity="receiver", method="lk.clear_buffer", payload=""
            )
            isolated = observed == [1, 0]
            report("cross_room_clear", expected=[1, 0], observed=observed, passed=isolated)

            await receivers[0].aclose()
            replacement = DataStreamAudioReceiver(destinations[0], sender_identity="sender")
            receivers.append(replacement)
            try:
                await replacement.start()
            except ValueError:
                recreated = False
            else:
                recreated = True
            report("close_then_recreate", passed=recreated)
            if expect_regression:
                if isolated or recreated:
                    raise AssertionError("Both baseline ownership regressions were expected")
                report("baseline", regression_reproduced=True)
            elif not isolated or not recreated:
                raise AssertionError("Receiver ownership checks failed")
            else:
                report("patched", passed=True)
        finally:
            cleanup = await asyncio.gather(
                *(receiver.aclose() for receiver in reversed(receivers)), return_exceptions=True
            )
            cleanup += await asyncio.gather(
                *(peer.disconnect() for peer in reversed(peers)), return_exceptions=True
            )
            rooms = await client.room.list_rooms(api.ListRoomsRequest(names=names))
            for room in rooms.rooms:
                await client.room.delete_room(api.DeleteRoomRequest(room=room.name))
            remaining = len((await client.room.list_rooms(api.ListRoomsRequest(names=names))).rooms)
            report("cleanup", remaining_rooms=remaining, disconnected_peers=len(peers))
            if remaining or any(isinstance(result, BaseException) for result in cleanup):
                raise AssertionError("Temporary RTC resources did not close cleanly")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--expect-regression", action="store_true")
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main(arguments.expect_regression))
