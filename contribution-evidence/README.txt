DataStreamAudioReceiver: isolated real-RTC reproduction and proposal

Upstream repository: https://github.com/livekit/agents
Exact base: 11c8ad8d74c272cf046e65d83cfe7be46abaec64
Proposal: proposal.diff, two files, 24 insertions and 40 deletions.

What was observed

The same ownership reproduction was run against unmodified upstream source and
against that source with proposal.diff applied. Both used a real local LiveKit
server, two separate rooms, four RTC clients, and the same sender identity in
both rooms. There were no mocked connections, handlers, frames, or RPC calls.

Unmodified source:
- A clear-buffer RPC addressed to room A invoked the receiver callback in B.
  Expected callback counts [1, 0]; observed [0, 1].
- Closing the receiver did not allow another receiver to register on that Room.

Patched source:
- The callback counts were [1, 0].
- Closing and recreating the receiver on the same Room succeeded.

The additional receiver-acceptance.py probe transmitted generated 24 kHz mono
PCM through real byte streams. It verified duplicate-start failure followed by
safe close, clear isolation in both directions, interrupted versus normal end
markers, and PCM/RPC operation after close and recreation. The result is in
patched-acceptance.jsonl. Every run explicitly disconnected its four clients,
deleted its temporary rooms, and queried zero remaining temporary rooms.

The interrupted field means only that clear_buffer discarded the reader. It does
not diagnose missing trailers, network truncation, or every interrupted playback.
The final edit after these runs clarified that docstring only; runtime code was
unchanged and no additional network run was performed for that documentation edit.

Reproduction

1. Use a dedicated Python 3.12 virtual environment. Clone the upstream repository
   and select the exact commit:

   git clone https://github.com/livekit/agents.git sdk
   git -C sdk checkout --detach 11c8ad8d74c272cf046e65d83cfe7be46abaec64
   python -m pip install -e ./sdk/livekit-agents

2. Set LIVEKIT_URL, LIVEKIT_API_KEY, and LIVEKIT_API_SECRET in that process's
   environment for a real server on which temporary rooms may be created/deleted.
   Credentials must not be included in published logs.

3. Set PYTHONPATH and PROBE_SDK_ROOT to the absolute sdk/livekit-agents directory.
   Each probe checks that DataStreamAudioReceiver was imported from this directory.

4. Run the ownership reproduction against unmodified source:

   python reproduce-ownership.py --expect-regression

   Its two passed values should be false and regression_reproduced should be true.
   Exit code zero means the expected baseline regressions were reproduced; it
   does not mean the unmodified SDK passed the ownership checks.

5. Apply the proposal and start fresh Python processes:

   git -C sdk apply /absolute/path/to/proposal.diff
   python reproduce-ownership.py
   python receiver-acceptance.py

   Ownership checks and PCM acceptance should now pass. Each probe uses unique
   temporary room names and removes only the rooms it created.

Scope and diagnostics

This validates SDK receiver routing, registration ownership, and the clear_buffer
end-marker fact. It does not validate a GPU model, an avatar provider, an application
call flow, all AvatarRunner consumers, or long-term native-handle/memory retention.
Room count zero proves temporary-room cleanup, not absence of every SDK leak.

The two short RPC-only ownership runs both logged the same native publisher
negotiation error during teardown. The longer generated-PCM acceptance did not.
These messages are retained in runtime-diagnostics.txt, without local filesystem
paths. This proposal does not claim to fix that independent teardown diagnostic.
The local environment also emitted a PyJWT key-length configuration warning.

No unit tests, remote branch, issue, or pull request were created by these runs.
No production source or installed dependency was modified. Source/patch checks
and Ruff using the upstream configuration passed. Results are observations from
the stated environment; the files contain no API credentials or user media.
