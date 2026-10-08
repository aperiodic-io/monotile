"""Small DBN files (Databento's binary encoding) of synthetic US equities data, and Databento's own
CSV of each (pretty prices and times, symbols mapped), to hold brrrrr's DBN reader to."""
import datetime as dt, io, os
import databento_dbn as d
import zstandard

out = "/out"
os.makedirs(out, exist_ok=True)
DAY = dt.date(2024, 1, 2)
T0 = 1704205800_000_000_000  # 2024-01-02 14:30:00 UTC, ns
AAPL, MSFT = 101, 202
from types import SimpleNamespace as N
mappings = [
    N(raw_symbol=s, intervals=[N(start_date=DAY, end_date=DAY + dt.timedelta(days=1), symbol=str(i))])
    for s, i in [("AAPL", AAPL), ("MSFT", MSFT)]
]
PX = 1_000_000_000
A = {"T": d.Action.TRADE, "A": d.Action.ADD, "C": d.Action.CANCEL, "R": d.Action.CLEAR}
S = {"B": d.Side.BID, "A": d.Side.ASK, "N": d.Side.NONE}

def meta(schema, version=3):
    return d.Metadata(
        dataset="XNAS.ITCH", start=T0, end=T0 + 3600 * 10**9, stype_in=d.SType.RAW_SYMBOL,
        stype_out=d.SType.INSTRUMENT_ID, schema=schema, symbols=["AAPL", "MSFT"], partial=[], not_found=[],
        mappings=mappings, version=version)

def write(name, schema, records, version=3):
    body = meta(schema, version).encode() + b"".join(bytes(r) for r in records)
    open(f"{out}/{name}.dbn", "wb").write(body)
    open(f"{out}/{name}.dbn.zst", "wb").write(zstandard.ZstdCompressor().compress(body))
    t = io.BytesIO()
    tc = d.Transcoder(t, d.Encoding.CSV, d.Compression.NONE, pretty_px=True, pretty_ts=True, map_symbols=True)
    tc.write(body)
    tc.flush()
    open(f"{out}/{name}.csv", "wb").write(t.getvalue())

ms = 1_000_000
trades = [
    d.TradeMsg(2, AAPL, T0 + 0 * ms + 123, 185_640_000_000, 100, A["T"], S["B"], 0, T0 + 0 * ms + 456, sequence=1),
    d.TradeMsg(2, MSFT, T0 + 5 * ms, 370_010_000_000, 50, A["T"], S["A"], 0, T0 + 5 * ms + 999, sequence=2),
    d.TradeMsg(2, AAPL, T0 + 61_000 * ms, 185_700_000_000, 200, A["T"], S["A"], 0, T0 + 61_000 * ms + 1, sequence=3),
    d.TradeMsg(2, AAPL, T0 + 62_500 * ms, 185_650_500_000, 25, A["T"], S["N"], 0, T0 + 62_500 * ms + 1, sequence=4),
    d.TradeMsg(2, MSFT, T0 + 125_000 * ms, 370_120_000_000, 10, A["T"], S["B"], 0, T0 + 125_000 * ms + 7, sequence=5),
]
write("xnas-itch-20240102.trades", d.Schema.TRADES, trades)
write("xnas-itch-20240102.trades.v1", d.Schema.TRADES, trades, version=1)

def quote(iid, t, bid, ask, bsz, asz, seq):
    return d.MBP1Msg(2, iid, t, bid, bsz, A["A"], S["B"], 0, t + 50, sequence=seq,
                     levels=d.BidAskPair(bid_px=bid, ask_px=ask, bid_sz=bsz, ask_sz=asz, bid_ct=1, ask_ct=2))
quotes = [
    quote(AAPL, T0 - 10 * ms, 185_630_000_000, 185_650_000_000, 300, 200, 11),
    quote(MSFT, T0 - 1 * ms, 370_000_000_000, 370_020_000_000, 100, 100, 12),
    quote(AAPL, T0 + 60_000 * ms, 185_680_000_000, 185_710_000_000, 100, 400, 13),
    quote(MSFT, T0 + 120_000 * ms, 370_100_000_000, 370_130_000_000, 100, 100, 14),
]
write("xnas-itch-20240102.mbp-1", d.Schema.MBP_1, quotes)

bars = [
    d.OHLCVMsg(d.RType.OHLCV_1M, 2, AAPL, T0, 185_640_000_000, 185_640_000_000, 185_640_000_000, 185_640_000_000, 100),
    d.OHLCVMsg(d.RType.OHLCV_1M, 2, MSFT, T0, 370_010_000_000, 370_010_000_000, 370_010_000_000, 370_010_000_000, 50),
    d.OHLCVMsg(d.RType.OHLCV_1M, 2, AAPL, T0 + 60 * 10**9, 185_700_000_000, 185_700_000_000, 185_650_500_000, 185_650_500_000, 225),
]
write("xnas-itch-20240102.ohlcv-1m", d.Schema.OHLCV_1M, bars)

orders = [
    d.MBOMsg(2, AAPL, T0 + 1, 9001, 185_630_000_000, 100, A["A"], S["B"], T0 + 2, flags=130, channel_id=0, sequence=21),
    d.MBOMsg(2, AAPL, T0 + 3, 9001, 185_630_000_000, 100, A["C"], S["B"], T0 + 4, channel_id=0, sequence=22),
    d.MBOMsg(2, MSFT, T0 + 5, 9002, 0x7FFF_FFFF_FFFF_FFFF, 0, A["R"], S["N"], T0 + 6, channel_id=0, sequence=23),
]
write("xnas-itch-20240102.mbo", d.Schema.MBO, orders)
for f in sorted(os.listdir(out)):
    print(f, os.path.getsize(f"{out}/{f}"))
