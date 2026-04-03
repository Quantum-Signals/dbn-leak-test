#!/usr/bin/env python3
"""
Minimal memory leak reproducer, no callback or application code.
"""

import asyncio
import importlib.metadata
import os
import resource
import time

import databento as db
import databento_dbn
from databento.common.enums import SlowReaderBehavior

print(f"databento {db.__version__}, databento-dbn {importlib.metadata.version('databento-dbn')}")

#GATEWAY = "172.17.0.1"
#PORT = 13000
DATASET = "GLBX.MDP3"
SYMBOLS = ["ESM6", "NQM6", "RTYM6", "YMM6"]

def rss_mib() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

async def main() -> None:
    client = db.Live(
        key=os.environ.get("DATABENTO_API_KEY", "test"),
        #gateway=GATEWAY,
        #port=PORT,
        ts_out=True,
        slow_reader_behavior=SlowReaderBehavior.SKIP,
        heartbeat_interval_s=10,
    )
    client.subscribe(dataset=DATASET, symbols=SYMBOLS, schema="mbp-10", stype_in="raw_symbol")
    client.start()

    start = time.monotonic()
    print(f"{'elapsed':>8}  {'rss_mib':>9}")
    print(f"{'-'*8}  {'-'*9}")

    try:
        while True:
            await asyncio.sleep(10.0)
            elapsed = time.monotonic() - start
            print(f"{elapsed:>7.0f}s  {rss_mib():>8.1f}M", flush=True)
    except KeyboardInterrupt:
        pass

    print(f"\nFinal: {time.monotonic()-start:.0f}s, RSS={rss_mib():.1f}M")
    client.stop()


if __name__ == "__main__":
    asyncio.run(main())