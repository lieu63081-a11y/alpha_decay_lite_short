"""
process_b_alpha_engine.py -- सिर्फ़ Process B (Alpha Engine) का हिस्सा
================================================================================

यह फ़ाइल alpha_decay_lite.py के PROCESS B (Alpha Engine) हिस्से की एक
focused reference copy है, ताकि सिर्फ़ signal processing / calculators /
score computation वाले हिस्से को अलग से पढ़ा जा सके, पूरी 2200+ lines
में scroll किए बिना।

** यह फ़ाइल standalone runnable नहीं है ** -- यह सिर्फ़ पढ़ने के लिए है।
   Actually चलाना है तो alpha_decay_lite.py या alpha_decay_lite_hindi.py
   use करें, क्योंकि Process B अकेला कुछ नहीं कर सकता -- उसे Process A
   से ticks चाहिए और Process C को scores भेजने हैं।

--------------------------------------------------------------------------
इसमें क्या है (जो Process B के काम करने के लिए ज़रूरी है):
--------------------------------------------------------------------------

  1. clip_value                          -- value को [min, max] range में clamp करे
  2. class RollingWindowSum              -- O(1) amortized rolling sum, out-of-order-safe
  3. start_heartbeat_thread              -- Process B की "मैं ज़िंदा हूँ" heartbeat
  4. class SymbolState                   -- हर symbol की rolling state (dataclass)

  --- 9 CALCULATORS (हर एक amortized O(1) per tick) ---
  5. calculate_vwap_distance             -- #1: price VWAP से कितनी दूर
  6. calculate_relative_volume           -- #2: RVOL (आज vs औसत)
  7. calculate_aggressive_buy_pressure   -- #3: net खरीद/बिक्री दबाव
  8. calculate_absorption                -- #4: भारी volume + flat price
  9. calculate_book_imbalance            -- #5: bid/ask depth असंतुलन
 10. calculate_spread_ratio              -- #6: spread filter
 11. calculate_gap_fade_unrestricted     -- gap-fade का core formula
 12. calculate_gap_fade                  -- #7: वही, पर सिर्फ़ 9:15-9:30 में
 13. calculate_session_weight            -- #8: दिन के हिस्से के हिसाब से weight
      (Calculator #9 = EMA smoother, alpha_engine के अंदर inline है)

  --- सब कुछ जोड़कर final score ---
 14. compute_feature_score               -- सभी 9 को जोड़कर composite score
 15. classify_trade_direction            -- Lee-Ready + tick-rule (buy/sell?)
 16. update_rolling_windows              -- हर tick पर सारी windows update

  --- मुख्य entry point ---
 17. alpha_engine                        -- Process B का main loop
     (ZMQ से ticks पढ़ो -> calculators चलाओ -> EMA smooth करो -> ZMQ पर score भेजो)

--------------------------------------------------------------------------
जो इस फ़ाइल में नहीं है (मुख्य alpha_decay_lite.py देखें):
--------------------------------------------------------------------------

  - Process A (data_factory)             -- Angel WebSocket से ticks लाना
  - Process C (broadcaster_loop)         -- अलर्ट भेजना, Telegram bot
  - Launcher / spawn / shutdown          -- तीनों processes को manage करना
  - determine_signal_modes               -- कौन सा signal mode qualify करे
  - Cooldowns, kill switches, आदि (Process C का हिस्सा)

================================================================================
"""
import os
import time
import math
import json
import signal
import threading
from collections import deque
from dataclasses import dataclass, field

import zmq
import redis
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# CONFIGURATION CONSTANTS (सिर्फ़ Process B जो use करता है)
# पूरी list के लिए alpha_decay_lite.py देखें
# ============================================================
ZMQ_TICKS_ADDRESS   = "ipc:///tmp/alpha_ticks.ipc"    # Process A -> Process B (ticks भेजने के लिए)
ZMQ_SCORES_ADDRESS  = "ipc:///tmp/alpha_scores.ipc"   # Process B -> Process C (scores भेजने के लिए)
REDIS_URL           = os.getenv("REDIS_URL", "redis://localhost:6379/0")

SPREAD_RATIO_MAX             = 0.0015   # 0.15% -- इससे ऊपर होने पर score को 0 कर दिया जाता है
EMA_TIME_CONSTANT_SECONDS    = 1.0      # score smoother का tau (calculator #9)

ZMQ_HIGH_WATER_MARK          = 10000    # ZMQ socket पर max queued messages, इसके बाद drop होंगे
LATENCY_WARNING_MS           = 100      # tick से score तक latency इससे ज़्यादा हो तो warning log करें

RVOL_WARMUP_SECONDS          = 300      # RVOL तभी meaningful है जब 5+ मिनट का history हो
RESET_DROP_FRACTION_THRESHOLD = 0.5     # cumulative volume में >50% की गिरावट को broker-side
                                        # counter reset (rebase) माना जाएगा, न कि stale/out-of-order
                                        # packet (reject) -- update_rolling_windows देखें
PEAK_SCORE_HALF_LIFE_SECONDS  = 60.0    # "peak score" amplitude कितनी तेज़ी से decay करे

PROCESS_HEARTBEAT_TTL_SECONDS      = 3   # process-alive heartbeat का TTL (kill switch #2)
DATA_FLOW_HEARTBEAT_TTL_SECONDS    = 5   # data-is-flowing heartbeat का TTL (kill switch #1)
HEARTBEAT_WRITE_INTERVAL_SECONDS   = 1



# ============================================================
# HELPER FUNCTIONS
# ============================================================

def clip_value(value, minimum, maximum):
    """value को [minimum, maximum] range में clamp करता है (inclusive)।"""
    return max(minimum, min(maximum, value))


# ============================================================
# AMORTIZED-O(1) ROLLING WINDOW HELPER
# ============================================================

class RollingWindowSum:
    """(timestamp, value) pairs का running sum एक trailing time window पर
    रखता है। add() और running total दोनों amortized O(1) हैं: हर add()
    call एक नई entry append करती है और window से बाहर हो चुकी entries
    को evict करती है, इसलिए कभी भी पूरा re-scan नहीं करना पड़ता।"""
    __slots__ = ("window_seconds", "entries", "running_sum")

    def __init__(self, window_seconds):
        self.window_seconds = window_seconds
        self.entries = deque()      # हर entry (timestamp, value) tuple है
        self.running_sum = 0.0

    def add(self, timestamp, value):
        """नया (timestamp, value) sample add करता है और window_seconds से
        पुरानी entries को evict करता है। ज़रूरी: यह हर tick पर call करना
        चाहिए, चाहे value=0.0 ही क्यों न हो, ताकि eviction हमेशा चलता रहे
        और shांत समय में पुराना data पीछे न रह जाए।

        अगर sample का timestamp entries के back (सबसे नई entry) से पुराना
        है, तो उसे reject करें (append न करें)। इस class के हर consumer
        को यह भरोसा है कि entries strictly non-decreasing chronological
        order में रहेंगी: ऊपर की eviction सिर्फ़ deque के FRONT को inspect
        करती है, और span_seconds() भी entries[-1][0] - entries[0][0]
        मानकर compute करता है कि back हमेशा नया है।

        अगर एक stale या out-of-order sample (network jitter, WebSocket
        reconnect से replayed buffered packet, duplicate tick) फिर भी
        back पर append कर दिया जाए, तो दोनों assumptions एक साथ टूट
        जाती हैं:
        - span_seconds() NEGATIVE हो सकता है (simulation से verified:
          एक out-of-order packet जो पहले से processed stream के मुकाबले
          ~30s "past" में आ रहा हो, negative span देता है, जो तुरंत
          calculate_relative_volume के warmup check को misfire करता है
          और RVOL को neutral 1.0 पर push कर देता है -- यह सिर्फ़ उसी tick
          के लिए होता है)।
        - इससे भी ज़्यादा गंभीर, और लंबे समय तक चलने वाला असर: sample
          की VALUE खुद running_sum में जुड़ जाती है और पूरे window_seconds
          तक बिना evict हुए बैठी रहती है, क्योंकि front-only eviction उस
          entry तक कभी पहुँच ही नहीं सकती जो newer entries के पीछे दबी
          हो (arrival order में बाद में आई, timestamp order में पहले)।
          Simulation से verified: एक anomalous volume value जो out-of-order
          inject किया गया, 20-minute window के running_sum के अंदर पूरे
          ~20 मिनट तक बना रहा, चुपचाप उसे inflate करता रहा।

        यहाँ sample को reject करने से हर window का invariant सुरक्षित रहता
        है जो इस class पर बने हैं (volume_sum_last_60_seconds,
        volume_sum_last_20_minutes, aggressive_signed_volume_30s,
        aggressive_absolute_volume_30s) -- ठीक उसी तरह जैसे
        last_traded_price_history_30s (जो एक सादा deque है, इस class पर
        नहीं बना) के लिए out-of-order guard अलग से पहले ही जोड़ा गया है।"""
        if self.entries and timestamp < self.entries[-1][0]:
            return
        self.entries.append((timestamp, value))
        self.running_sum += value
        eviction_cutoff = timestamp - self.window_seconds
        while self.entries and self.entries[0][0] < eviction_cutoff:
            self.running_sum -= self.entries.popleft()[1]

    def span_seconds(self):
        """वर्तमान में window की सबसे पुरानी और सबसे नई entry के बीच का time
        span। 'अभी तक पर्याप्त history नहीं है' (warmup) cases पकड़ने के
        लिए इस्तेमाल होता है।"""
        if len(self.entries) < 2:
            return 0.0
        return self.entries[-1][0] - self.entries[0][0]


# ============================================================
# HEARTBEAT HELPER (Process B जो use करता है)
# ============================================================

def start_heartbeat_thread(heartbeat_name, redis_client, additional_check=None):
    """एक background daemon thread शुरू करता है जो हर
    HEARTBEAT_WRITE_INTERVAL_SECONDS पर Redis key alpha:hb:{heartbeat_name}
    को refresh करता है, PROCESS_HEARTBEAT_TTL_SECONDS के TTL के साथ। अगर
    additional_check दिया गया हो, तो वो एक zero-argument callable होना
    चाहिए जो (extra_heartbeat_name, is_currently_alive) return करे; जब
    is_currently_alive True हो, तो alpha:hb:{extra_heartbeat_name} भी
    refresh होगा, DATA_FLOW_HEARTBEAT_TTL_SECONDS के TTL के साथ।"""

    def heartbeat_loop():
        while True:
            try:
                redis_client.setex(f"alpha:hb:{heartbeat_name}",
                                    PROCESS_HEARTBEAT_TTL_SECONDS, "1")
                if additional_check:
                    extra_name, is_alive = additional_check()
                    if is_alive:
                        redis_client.setex(f"alpha:hb:{extra_name}",
                                            DATA_FLOW_HEARTBEAT_TTL_SECONDS, "1")
            except Exception:
                pass
            time.sleep(HEARTBEAT_WRITE_INTERVAL_SECONDS)

    threading.Thread(target=heartbeat_loop, daemon=True,
                      name=f"heartbeat-{heartbeat_name}").start()


# ============================================================
# PER-SYMBOL STATE (हर stock की rolling state)
# ============================================================

class SymbolState:
    """हर symbol की per-symbol rolling state, Alpha Engine maintain करता
    है। जितने भी symbols अभी track हो रहे हैं, हर एक के लिए इसकी एक
    instance होती है।"""
    volume_sum_last_60_seconds: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(60))
    volume_sum_last_20_minutes: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(1200))
    # 30s window पर trade-classified (Lee-Ready) net और gross volume,
    # aggressive-buy calculator और absorption calculator दोनों share करते हैं।
    aggressive_signed_volume_30s: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(30))
    aggressive_absolute_volume_30s: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(30))
    last_traded_price_history_30s: deque = field(default_factory=deque)   # (timestamp, price) tuples
    last_cumulative_volume: int = -1          # -1 = uninitialized (पहला tick baseline अभी set नहीं हुआ)
    last_traded_price: float = 0.0            # tick-rule fallback classifier इसका इस्तेमाल करता है
    smoothed_ema_score: float = 0.0
    last_tick_timestamp: float = 0.0
    peak_score_amplitude: float = 0.0


# ============================================================
# 9 CALCULATORS (हर एक amortized O(1) per tick)
# ============================================================

def calculate_vwap_distance(_symbol_state, tick_data):
    """Calculator #1: last-traded-price की daily VWAP से percentage दूरी।
    चूंकि daily VWAP cumulative है, इसकी 60-second slope दोपहर तक लगभग
    zero हो जाती है -- इसलिए VWAP से दूरी ही meaningful mean-reversion
    signal है, slope नहीं।"""
    if not tick_data["daily_vwap"] or not tick_data["last_traded_price"]:
        return 0.0
    percent_distance = ((tick_data["last_traded_price"] - tick_data["daily_vwap"])
                         / tick_data["daily_vwap"] * 100)
    return clip_value(percent_distance * 5.0, -2.0, 2.0)

def calculate_relative_volume(symbol_state, _tick_data):
    """Calculator #2: RVOL = last-60-seconds volume भाग PRIOR baseline
    minutes का average per-minute volume (सबसे recent 60 seconds को
    baseline से जान-बूझकर बाहर रखा गया है ताकि self-inclusion bias न
    हो)। शुरुआती 5 minutes में (warmup में) neutral 1.0 return करता है,
    ताकि near-empty baseline से false spikes न बनें।

    ज़रूरी: baseline divisor वो ACTUAL elapsed baseline minutes है जो
    अभी तक बीते हैं (max 19, क्योंकि window में सबसे ज़्यादा 20 minutes
    होते हैं और उनमें से 1 excluded most-recent-60s है), NOT hardcoded
    19.0। अगर हम एक fixed 19.0 से divide करते हैं जबकि window सिर्फ़
    5-6 मिनट से accumulate हो रही है, तो baseline volume लगभग 3-4x
    कम आँका जाएगा, जो RVOL को उसी factor से inflate कर देगा और MOMENTUM
    mode के rvol >= 1.5 threshold को बिल्कुल normal volume पर भी
    spurious satisfy कर सकता है। Simulation से verified: 5 minutes के
    steady (non-spiking) volume पर, fixed-19.0 version ने rvol=4.75
    report किया जबकि सही value 1.00 है; elapsed-minutes version पूरे
    समय सही 1.00 report करता है।"""
    window = symbol_state.volume_sum_last_20_minutes
    if window.span_seconds() < RVOL_WARMUP_SECONDS:
        return 1.0
    baseline_volume_sum = max(
        window.running_sum - symbol_state.volume_sum_last_60_seconds.running_sum, 0.0)
    elapsed_baseline_minutes = max(1.0, (window.span_seconds() - 60) / 60)
    baseline_minutes = min(19.0, elapsed_baseline_minutes)
    baseline_volume_per_minute = baseline_volume_sum / baseline_minutes
    return symbol_state.volume_sum_last_60_seconds.running_sum / max(baseline_volume_per_minute, 1.0)

def calculate_aggressive_buy_pressure(symbol_state, _tick_data):
    """Calculator #3: rolling 30-second NET aggressive खरीद/बिक्री pressure,
    Lee-Ready (with tick-rule fallback) trade classification से बना।
    Single tick classification के बजाय rolling window इस्तेमाल करने से
    noise काफ़ी कम हो जाता है।"""
    gross_classified_volume = symbol_state.aggressive_absolute_volume_30s.running_sum
    if gross_classified_volume < 100:
        return 0.0
    net_classified_volume = symbol_state.aggressive_signed_volume_30s.running_sum
    return clip_value(net_classified_volume / gross_classified_volume * 2.0, -2.0, 2.0)

def calculate_absorption(symbol_state, tick_data):
    """Calculator #4: trade-classified, SYMMETRIC absorption signal।
       +1.5 : भारी net SELL volume जबकि price flat है -> sellers absorb
              हो रहे हैं (BULLISH signal -- नीचे hidden buyer है)
       -1.5 : भारी net BUY volume जबकि price flat है -> buyers absorb
              हो रहे हैं (BEARISH trap -- ऊपर hidden seller है)
        0.0 : कोई condition पूरी नहीं हुई"""
    price_history = symbol_state.last_traded_price_history_30s
    if len(price_history) < 5 or not tick_data["last_traded_price"]:
        return 0.0
    lowest_price_in_window = min(price for _, price in price_history)
    highest_price_in_window = max(price for _, price in price_history)
    price_range_ratio = ((highest_price_in_window - lowest_price_in_window)
                          / tick_data["last_traded_price"])
    if price_range_ratio > 0.001:
        return 0.0   # price इतना flat नहीं है कि इसे absorption माना जाए

    if symbol_state.aggressive_signed_volume_30s.span_seconds() < 10:
        return 0.0   # अभी तक पर्याप्त classified-volume history नहीं है

    gross_classified_volume = symbol_state.aggressive_absolute_volume_30s.running_sum
    if gross_classified_volume < 100:
        return 0.0

    net_to_gross_ratio = (symbol_state.aggressive_signed_volume_30s.running_sum
                          / gross_classified_volume)
    if net_to_gross_ratio < -0.4:
        return 1.5    # भारी net selling, absorbed -> bullish
    if net_to_gross_ratio > 0.4:
        return -1.5   # भारी net buying, absorbed -> bearish trap
    return 0.0

def calculate_book_imbalance(_symbol_state, tick_data):
    """Calculator #5: near-price order-book depth imbalance, TOP-5 bid
    levels और TOP-5 ask levels के quantities का sum use करके compute
    होता है। Score [-2.0, +2.0] range में: positive मतलब near-price
    buy pressure sell pressure से ज़्यादा है, negative मतलब उल्टा।

    ज़रूरी (क्यों top-5, न कि total_buy_quantity / total_sell_quantity):
    Angel के total_buy_quantity / total_sell_quantity WHOLE-BOOK
    aggregates हैं जो किसी भी price level पर open हर limit order को
    count करते हैं, दूर पड़े retail "wish list" orders भी शामिल हैं
    (जैसे LTP ₹1100 है और कोई ₹900 पर buy order डाले हुए है)। वो
    orders लगभग कभी trade नहीं होंगे, लेकिन total में जुड़ते हैं --
    तो एक perfectly balanced near-price book पर भी एक lopsided
    garbage tail ratio को +/-0.9 तक push कर सकता है बिना किसी असली
    directional signal के। Simulation से verified: near-price top-5
    balanced 4000 vs 4000, plus 4,50,000 far-off retail buys LTP से
    ~20% नीचे, whole-book imbalance +0.90 (score +1.80, strong
    bullish) देता था vs सही 0.00 (no bias) top-5 formula से। Top-5
    sums वो actual near-price liquidity measure करते हैं जो short-
    term price moves drive करती है, न कि retail "sasta chahiye" वाला
    tail unreachable prices पर।

    NOTE: यह calculator top5_buy_quantity / top5_sell_quantity fields
    को tick_data से पढ़ता है। ये fields Process A (data_factory का
    on_tick_received callback) में populate होते हैं, जो
    best_5_buy_data / best_5_sell_data के 5 levels को iterate करके
    उनकी quantities का sum निकालता है। यह file (process_b_alpha_
    engine.py) standalone runnable नहीं है -- असल में चलाने पर
    Process A से ये fields मिलते हैं।"""
    top5_buy_quantity  = tick_data["top5_buy_quantity"]
    top5_sell_quantity = tick_data["top5_sell_quantity"]
    combined_quantity = top5_buy_quantity + top5_sell_quantity
    if not combined_quantity:
        return 0.0
    imbalance_ratio = (top5_buy_quantity - top5_sell_quantity) / combined_quantity
    return clip_value(imbalance_ratio * 2, -2.0, 2.0)

def calculate_spread_ratio(_symbol_state, tick_data):
    """Calculator #6: last-traded-price के hisाब से bid-ask spread का
    fraction। यह spread FILTER का input है (SPREAD_RATIO_MAX देखें), खुद
    से कोई directional score contribution नहीं है।"""
    if (tick_data["last_traded_price"] and tick_data["best_bid_price"]
            and tick_data["best_ask_price"]):
        return ((tick_data["best_ask_price"] - tick_data["best_bid_price"])
                / tick_data["last_traded_price"])
    return 1.0

def calculate_gap_fade_unrestricted(tick_data):
    """Gap-fade का core formula, बिना किसी time-of-day gating के। दिन के
    open और पिछले close के बीच का negated percentage gap return करता है,
    तो positive result का मतलब 'gap को fade करो long जाकर' और negative
    result का मतलब 'fade करो short जाकर'। इसके दो अलग-अलग उपयोग हैं,
    दो अलग-अलग ज़रूरतों के साथ:
      1. नीचे calculate_gap_fade() इसे entry-only 9:15:00-9:29:59 IST time
         gate के साथ wrap करता है, नए GAP_FADE entries तय करने के लिए।
      2. broadcaster_loop का exit check किसी पहले से TRACKED GAP_FADE
         position के लिए score payload से सीधे gap_fade_raw_score_
         unrestricted पढ़ता है (alpha_engine का publish step देखें), न कि
         time-gated value, specifically ताकि exit decision clock से न
         चले। इस split से पहले, entry AND exit दोनों same time-gated
         calculate_gap_fade() output इस्तेमाल करते थे, जो 9:15-9:30 window
         end होते ही unconditionally zero हो जाता है -- यानी हर खुली
         GAP_FADE position ठीक 9:30:00 पर हर रोज़ force-exit हो जाती थी
         चाहे underlying gap actually closed हुआ हो या नहीं (simulation से
         verified: genuine 2.5% gap पर entry हो, price बिल्कुल unchanged
         रहे, फिर भी जैसे ही clock 9:30:00 पर पहुँचा, should_exit=True
         हो गया)।"""
    if not tick_data["previous_close_price"] or not tick_data["open_price"]:
        # previous_close_price यहाँ check होता है क्योंकि यह इस formula
        # का divisor है; open_price भी check होता है क्योंकि अगर feed ने
        # अभी उसे populate नहीं किया (0/missing, जैसे दिन के बिल्कुल पहले
        # ticks जब exchange ने अभी official open publish नहीं किया), तो
        # नीचे का formula (0 - previous_close_price) / previous_close_price
        # compute करेगा, जो एक artificial -100% "gap" है और +3.0 पर clip
        # हो जाएगा -- यानी बिना किसी असली gap के, एक बड़ा gap दिखाई देगा।
        return 0.0
    gap_percent = ((tick_data["open_price"] - tick_data["previous_close_price"])
                   / tick_data["previous_close_price"]) * 100
    return clip_value(-gap_percent, -3.0, 3.0)

def calculate_gap_fade(_symbol_state, tick_data):
    """Calculator #7: gap-fade signal, सिर्फ़ 9:15:00-9:29:59 IST window में
    active होता है (इस system के design contract के हिसाब से -- module
    docstring / README's Signal Modes table देखें: 'Gap (9:15-9:30 window
    only)')। यह ENTRY-side gate है: यह compute_feature_score के composite
    score को और determine_signal_modes के GAP_FADE qualification को feed
    करता है, दोनों को सिर्फ़ इस window के अंदर एक NEW entry ही trigger
    करना चाहिए। window के बाहर यह unrestricted value के बजाय जान-बूझकर
    0.0 return करता है -- देखें calculate_gap_fade_unrestricted ऊपर,
    क्यों EXIT path को एक अलग (clock से zero न होने वाला) metric चाहिए,
    न कि इस function का output दोबारा इस्तेमाल करने का।"""
    local_time = time.localtime(tick_data["timestamp"])
    is_within_gap_window = (local_time.tm_hour == 9
                            and 15 <= local_time.tm_min < 30)
    if not is_within_gap_window:
        return 0.0
    return calculate_gap_fade_unrestricted(tick_data)

def calculate_session_weight(_symbol_state, tick_data):
    """Calculator #8: current tick trading session के किस हिस्से में गिरता
    है, उसके आधार पर composite score पर multiplier लगाता है।
       OPEN_VOL   (09:15-09:45 IST) -> 1.2  (सबसे अच्छी signal quality)
       CLOSE_HOUR (14:45-15:30 IST) -> 1.1  (institutional square-off flow)
       MID_QUIET  (बाकी सब)         -> 0.8  (कम conviction, dampen करो)"""
    local_time = time.localtime(tick_data["timestamp"])
    hour, minute = local_time.tm_hour, local_time.tm_min
    # NOTE: original condition `hour == 9 and minute < 45` थी, जो
    # 09:00-09:44 cover करती थी -- यानी NSE का pre-open session (09:00-
    # 09:15: order collection, matching, और buffer) भी शामिल था, न कि
    # सिर्फ़ documented 09:15-09:45 continuous-trading open window। Pre-
    # open ticks regular continuous trading नहीं हैं और उन्हें भी वही
    # 1.2 OPEN_VOL boost मिल रहा था जो असली post-open ticks को मिलता
    # है। minute >= 15 वाला restriction लगाने से code अपने docstring के
    # अनुरूप हो जाता है।
    if hour == 9 and 15 <= minute < 45:
        return 1.2
    if (hour == 14 and minute >= 45) or (hour == 15 and minute <= 30):
        return 1.1
    return 0.8


# ============================================================
# COMPOSITE SCORE + TRADE CLASSIFIER + WINDOW UPDATER
# ============================================================

def compute_feature_score(symbol_state, tick_data):
    """सभी 9 calculators चलाकर उन्हें [-10, +10] range के एक composite
    score में combine करता है। Return करता है (composite_score,
    relative_volume, gap_fade_raw_score, gap_fade_raw_score_unrestricted):
      - gap_fade_raw_score ENTRY-gated value है (9:15-9:30 IST के बाहर 0),
        composite score के लिए और determine_signal_modes के GAP_FADE
        entry qualification के लिए इस्तेमाल होता है।
      - gap_fade_raw_score_unrestricted वही gap measurement है पर बिना
        time gate के, अलग से publish होता है ताकि broadcaster_loop का
        exit check किसी पहले से खुली GAP_FADE position के लिए एक ऐसा
        metric इस्तेमाल कर सके जो 9:30:00 clock पास होते ही artificially
        zero न हो जाए -- देखें calculate_gap_fade_unrestricted का
        docstring, यह specifically किस false-exit bug को fix करता है।"""
    relative_volume = calculate_relative_volume(symbol_state, tick_data)

    spread_ratio = calculate_spread_ratio(symbol_state, tick_data)
    if spread_ratio > SPREAD_RATIO_MAX:
        # Spread इतना wide है कि इस quote पर भरोसा नहीं -- score को zero
        # कर दो, लेकिन असली relative_volume अभी भी return करो (यह अलग से
        # useful है) और unrestricted gap metric भी (किसी पहले से खुली
        # position के लिए exit check को एक अकेले wide-spread tick से
        # blind नहीं होना चाहिए)।
        return 0.0, relative_volume, 0.0, calculate_gap_fade_unrestricted(tick_data)

    vwap_distance_score      = calculate_vwap_distance(symbol_state, tick_data)
    aggressive_buy_score     = calculate_aggressive_buy_pressure(symbol_state, tick_data)
    absorption_score         = calculate_absorption(symbol_state, tick_data)
    book_imbalance_score     = calculate_book_imbalance(symbol_state, tick_data)
    gap_fade_raw_score       = calculate_gap_fade(symbol_state, tick_data)
    gap_fade_raw_score_unrestricted = calculate_gap_fade_unrestricted(tick_data)
    session_weight           = calculate_session_weight(symbol_state, tick_data)

    # aggressive-buy और absorption को combine करना: अगर उनके sign मेल
    # खाते हैं (same direction), तो redundancy को dampen करें (एक ही
    # direction का confirmation दो बार count नहीं करना चाहिए); अगर वो
    # disagree करते हैं, तो add करें (net directional pressure)।
    if aggressive_buy_score * absorption_score >= 0:
        combined_flow_score = (max(aggressive_buy_score, absorption_score)
                               + 0.4 * min(aggressive_buy_score, absorption_score))
    else:
        combined_flow_score = aggressive_buy_score + absorption_score

    composite_score = clip_value(
        session_weight * (2.0 * vwap_distance_score
                          + 1.5 * combined_flow_score
                          + 1.5 * book_imbalance_score
                          + 1.0 * gap_fade_raw_score),
        -10.0, 10.0)
    return composite_score, relative_volume, gap_fade_raw_score, gap_fade_raw_score_unrestricted

def classify_trade_direction(tick_data, previous_last_traded_price):
    """Lee-Ready trade classification + tick-rule fallback।
    Aggressive buy के लिए +1, aggressive sell के लिए -1, या 0 अगर trade
    को classify नहीं किया जा सकता (price exactly midpoint पर हो और
    previous tick से कोई change न हो)।"""
    if tick_data["best_bid_price"] and tick_data["best_ask_price"]:
        midpoint_price = (tick_data["best_bid_price"] + tick_data["best_ask_price"]) / 2
    else:
        midpoint_price = tick_data["last_traded_price"]

    if tick_data["last_traded_price"] > midpoint_price:
        return 1
    if tick_data["last_traded_price"] < midpoint_price:
        return -1
    if previous_last_traded_price and tick_data["last_traded_price"] > previous_last_traded_price:
        return 1
    if previous_last_traded_price and tick_data["last_traded_price"] < previous_last_traded_price:
        return -1
    return 0

def update_rolling_windows(symbol_state, tick_data):
    """Current tick के लिए symbol_state की हर rolling window को update करता
    है: raw volume windows, trade-classified aggression windows, और
    absorption calculator के लिए short last-traded-price history।"""
    current_timestamp = tick_data["timestamp"]

    if symbol_state.last_cumulative_volume < 0:
        # इस symbol का बिल्कुल पहला tick: baseline set करो, पूरे दिन के
        # cumulative volume को single-tick spike मानने से बचो।
        symbol_state.last_cumulative_volume = tick_data["cumulative_day_volume"]
        delta_volume = 0
    elif tick_data["cumulative_day_volume"] >= symbol_state.last_cumulative_volume:
        delta_volume = (tick_data["cumulative_day_volume"]
                        - symbol_state.last_cumulative_volume)
        symbol_state.last_cumulative_volume = tick_data["cumulative_day_volume"]
    else:
        # नया cumulative volume हमारे मौजूदा watermark से कम है। इसके दो
        # बिल्कुल अलग real-world कारण हो सकते हैं, और उनका OPPOSITE
        # handling चाहिए:
        #
        # (a) Out-of-order/delayed packet: एक network-reordered पुराना
        #     tick एक नए वाले के थोड़ा पीछे आ जाए। reported value और
        #     watermark के बीच gap छोटा होता है (आमतौर पर कुछ सेकंड
        #     worth का volume)। इसे normal update मानने पर watermark
        #     नीचे corrupt हो जाएगा और अगला in-order tick volume को
        #     double-count करेगा (एक पिछले commit में simulation से
        #     verified: watermark=1000, stale tick 980, अगला real tick
        #     1010 -- गलत delta=30 compute हुआ, सही 10 था)। इस case का
        #     fix: tick को reject करो: delta=0, watermark unchanged।
        #
        # (b) असली broker-side counter reset: कुछ broker backends कभी-कभी
        #     mid-day पर किसी symbol का cumulative volume counter reset
        #     कर देते हैं (backend glitch, network artifact नहीं)। अगर
        #     हम यहाँ fix (a) unconditionally लगाएँ, तो एक असली reset
        #     (जैसे watermark=5,000,000 से 1,000 पर reset हुआ) HAR
        #     आगे आने वाले tick को हमेशा के लिए "stale" के तौर पर reject
        #     करेगा, क्योंकि counter को 1,000 से 5,000,000 तक चढ़ना
        #     पड़ेगा तब जाकर एक tick >= check पास कर पाएगा -- चुपचाप
        #     इस symbol के volume-derived signals (RVOL, और उसके नीचे
        #     सब) को पूरे दिन के लिए mute कर देगा। यह failure mode शायद
        #     उस original double-counting bug से भी बुरा है जिसे यह
        #     logic fix करने के लिए लिखा गया था।
        #
        # दोनों में अंतर करने की Heuristic: एक network-reordered packet
        # उतने ही volume तक limited होता है जितना reordering delay के
        # कुछ सेकंडों में plausibly trade हो सकता है -- दिन के total
        # volume के मुक़ाबले कहीं कम। एक असली reset watermark के एक
        # बड़े fraction से drop करता है। नीचे RESET_DROP_FRACTION_THRESHOLD
        # किसी भी 50% से ज़्यादा current watermark वाली drop को reset
        # मानता है (नई value पर rebase करो, इस tick के लिए 0 volume),
        # न कि stale packet (reject, watermark unchanged)।
        drop_amount = symbol_state.last_cumulative_volume - tick_data["cumulative_day_volume"]
        looks_like_a_reset = (
            symbol_state.last_cumulative_volume > 0
            and drop_amount > symbol_state.last_cumulative_volume * RESET_DROP_FRACTION_THRESHOLD)
        if looks_like_a_reset:
            # NOTE: tick_data["cumulative_day_volume"] खुद negative हो
            # सकता है (एक malformed/corrupt feed value -- cumulative
            # traded volume कभी legitimately negative नहीं होता)। ऐसी
            # value पर सीधे watermark rebase करने से last_cumulative_volume
            # < 0 हो जाएगा, जिससे बिल्कुल अगले tick का `if symbol_state.
            # last_cumulative_volume < 0:` guard ऊपर फिर "इस symbol का
            # पहला tick" के तौर पर misfire होगा -- चुपचाप एक नया baseline
            # set होगा (उस अगले tick का real delta खोकर) बजाय volume को
            # normally compute करने के। Rebase target को 0 पर clamp करने
            # से watermark खुद हमेशा non-negative रहेगा, तो वो guard सिर्फ़
            # किसी symbol के असली पहले tick पर true हो सकेगा।
            symbol_state.last_cumulative_volume = max(
                tick_data["cumulative_day_volume"], 0)
        delta_volume = 0

    symbol_state.volume_sum_last_60_seconds.add(current_timestamp, delta_volume)
    symbol_state.volume_sum_last_20_minutes.add(current_timestamp, delta_volume)

    # Trade classification aggressive-buy calculator और absorption calculator
    # दोनों को feed करता है (shared infrastructure)। ज़रूरी: .add() हर
    # tick पर call होता है, चाहे इस tick का contribution zero ही क्यों न
    # हो, क्योंकि पुरानी entries की eviction add() के अंदर होती है। अगर
    # हम .add() सिर्फ़ तब call करते जब कोई aggressive trade हो, तो एक
    # shांत stretch (जिसमें कोई aggressive trade न हो) में eviction कभी
    # चलता ही नहीं, और मिनटों पुराना stale volume "पिछले 30 seconds" के
    # तौर पर पढ़ा जाता -- यह एक असली bug था जो पकड़कर fix हुआ था।
    trade_direction = classify_trade_direction(tick_data, symbol_state.last_traded_price)
    symbol_state.last_traded_price = tick_data["last_traded_price"]

    trade_is_classified = trade_direction != 0 and delta_volume > 0
    signed_volume_contribution = (trade_direction * delta_volume) if trade_is_classified else 0.0
    absolute_volume_contribution = delta_volume if trade_is_classified else 0.0
    symbol_state.aggressive_signed_volume_30s.add(current_timestamp, signed_volume_contribution)
    symbol_state.aggressive_absolute_volume_30s.add(current_timestamp, absolute_volume_contribution)

    price_history = symbol_state.last_traded_price_history_30s
    # एक out-of-order/delayed tick (network jitter, WS reconnect replay)
    # का timestamp deque के back पर पहले से मौजूद newest entry से पुराना
    # हो सकता है। फिर भी उसे back पर append करना deque के chronological
    # ordering invariant को तोड़ देगा -- और calculate_absorption के
    # min/max price lookup (और नीचे का eviction loop, जो सिर्फ़ deque
    # के FRONT को check करता है) दोनों चुपचाप इस invariant पर भरोसा
    # करते हैं। एक stale entry जो out-of-order append हो गई, newer
    # entries के पीछे दबकर अपने 30-second window से भी बहुत बाद तक जीवित
    # रह सकती है, क्योंकि front-only eviction उस तक तब तक कभी नहीं
    # पहुँचता जब तक उसके पहले append हुई सारी entries (chronologically
    # बाद वाली, पर arrival order में पहले वाली) age out नहीं हो जातीं।
    # Simulation से verified: anomalous price वाले एक out-of-order tick
    # ने absorption calculator के price_range_ratio के लिए इस window के
    # min/max को कई ticks तक corrupt रखा, expire होने के काफ़ी बाद तक।
    # deque के current back से पुराना कोई भी tick reject (append न) करने
    # से chronological invariant intact रहता है -- यह वही "out-of-order
    # sample को reject करो" approach है जो इस file में और जगहों पर
    # cumulative-volume watermark के लिए पहले से इस्तेमाल हो रहा है,
    # यहाँ भी उसी underlying reason से लगाया गया है।
    if not price_history or current_timestamp >= price_history[-1][0]:
        price_history.append((current_timestamp, tick_data["last_traded_price"]))
        price_history_cutoff = current_timestamp - 30
        while price_history and price_history[0][0] < price_history_cutoff:
            price_history.popleft()


# ============================================================
# MAIN ENTRY POINT: alpha_engine()
# ============================================================

def alpha_engine():
    """Process B का entry point। Process A से publish हुए ticks को consume
    करता है, हर symbol की rolling state update करता है, 9-calculator
    composite score compute करता है, EMA smoother लगाता है, और result
    score को publish कर देता है ताकि Process C उसे broadcast कर सके।"""
    zmq_context = zmq.Context()   # हर process के लिए fresh context

    tick_subscriber = zmq_context.socket(zmq.SUB)
    tick_subscriber.setsockopt(zmq.RCVHWM, ZMQ_HIGH_WATER_MARK)
    tick_subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    tick_subscriber.connect(ZMQ_TICKS_ADDRESS)

    score_publisher = zmq_context.socket(zmq.PUB)
    score_publisher.setsockopt(zmq.SNDHWM, ZMQ_HIGH_WATER_MARK)
    score_publisher.bind(ZMQ_SCORES_ADDRESS)

    heartbeat_redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    start_heartbeat_thread("B", heartbeat_redis_client)   # kill switch #2

    # multiprocessing.Process.terminate() इस process को SIGTERM भेजता है।
    # अपने handler के बिना, SIGTERM पर Python का default behaviour है
    # तुरंत terminate होना -- launcher की नज़र में terminate()/join()
    # "काम" तो करेगा, लेकिन इस process को अपनी sockets को साफ़ तरीक़े से
    # बंद करने का मौका ही नहीं मिलेगा। यहाँ एक handler register करने से
    # process अपनी main loop से बाहर निकलेगा और normal return से exit
    # होगा, जो एक cleaner shutdown है।
    stop_requested = threading.Event()

    def handle_stop_signal(_signum, _frame):
        stop_requested.set()

    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    # एक सादा, unconditional recv_multipart() हमेशा के लिए block हो जाता है
    # अगर Process A ticks भेजना बंद कर दे (market closed, WS disconnected
    # आदि)। जब तक blocked रहेगा, यह loop अपने periodic stats-print check
    # तक कभी नहीं पहुँच पाएगा, तो console पूरी तरह silent हो जाएगा बिना
    # किसी indication के कि क्यों -- यह देखने में एक crash से fark ही
    # नहीं होगा। एक timeout वाले Poller का इस्तेमाल करने का मतलब है कि
    # loop हर RECEIVE_POLL_TIMEOUT_MS पर कम से कम एक बार wake हो जाता है,
    # tick आया हो या नहीं, तो stats print होते रहते हैं (ticks=0 दिखाकर)
    # और stop signal तुरंत check होता है, न कि अगले tick आने पर।
    RECEIVE_POLL_TIMEOUT_MS = 1000
    poller = zmq.Poller()
    poller.register(tick_subscriber, zmq.POLLIN)

    symbol_state_by_symbol: dict[str, SymbolState] = {}
    tick_count_in_window, last_stats_print_time = 0, time.time()
    print("[B] Alpha Engine up", flush=True)

    while not stop_requested.is_set():
        ready_sockets = dict(poller.poll(timeout=RECEIVE_POLL_TIMEOUT_MS))
        if tick_subscriber not in ready_sockets:
            # Poll timeout में कोई tick नहीं आया। फिर भी नीचे का periodic
            # stats print चलाओ, ताकि silence "ticks=0" के तौर पर दिखे,
            # किसी hang से अलग पहचाना जा सके।
            current_time = time.time()
            if current_time - last_stats_print_time >= 10:
                print(f"[B] ticks=0 (0/s)  syms={len(symbol_state_by_symbol)}  "
                      f"top: -  (no ticks received in last "
                      f"{current_time - last_stats_print_time:.0f}s)", flush=True)
                tick_count_in_window, last_stats_print_time = 0, current_time
            continue

        symbol_bytes, message_payload = tick_subscriber.recv_multipart()
        tick_data = json.loads(message_payload)
        latency_ms = (time.time() - tick_data["timestamp"]) * 1000
        symbol = tick_data["symbol"]

        symbol_state = symbol_state_by_symbol.get(symbol)
        if symbol_state is None:
            symbol_state = symbol_state_by_symbol[symbol] = SymbolState()

        update_rolling_windows(symbol_state, tick_data)
        (raw_composite_score, relative_volume, gap_fade_raw_score,
         gap_fade_raw_score_unrestricted) = compute_feature_score(symbol_state, tick_data)

        # --- Calculator #9: EMA smoother, साथ में time-based peak-amplitude decay ---
        if symbol_state.last_tick_timestamp == 0:
            # इस symbol का बिल्कुल पहला tick: EMA को सीधे initialize करो
            # (यहाँ blend करने के लिए कोई prior smoothed value नहीं है,
            # तो smoothed == raw इस एक tick पर unavoidable है)। लेकिन
            # peak_score_amplitude को 0.0 पर seed करें, NOT abs(raw_
            # composite_score) पर -- market-open ticks अक्सर anomalous
            # होते हैं (illiquid opening quotes, real two-sided trading
            # शुरू होने से पहले का stale/wide first snap-quote), और इस
            # system का peak field हर Telegram alert में सीधे broadcast
            # होता है ("peak=X.XX" के रूप में) मानो यह genuinely sustained
            # momentum को reflect करता है। इसे एक अकेले unsmoothed first
            # sample से seed करने से एक bad opening tick (जैसे spurious
            # +10.0) एक misleadingly high peak को एक मिनट से भी ज़्यादा
            # देर तक broadcast करता रहता था, जबकि smoothed score तो कुछ
            # ही seconds में सामान्य value पर आ जाता है (simulation से
            # verified: 10.0 का एक outlier first tick फिर लगातार 1.0
            # वाले genuine steady ticks -- पुराने seed-from-raw behavior
            # में peak पूरे 60 seconds तक >= 5.0 रहा)। इसके बजाय 0.0 पर
            # seed करने से बिल्कुल अगली line का normal max(decayed_peak,
            # abs(smoothed_ema_score)) logic peak को सिर्फ़ genuinely-
            # observed (पहले से smoothed) values से build करता है, हर
            # tick पर -- तो एक असली, sustained move लगभग तुरंत अपने true
            # peak पर पहुँच जाता है, जबकि एक single-tick anomaly को अब
            # पूरे एक मिनट का decay time work off करने के लिए नहीं मिलता।
            symbol_state.smoothed_ema_score = raw_composite_score
            symbol_state.peak_score_amplitude = 0.0
        elif tick_data["timestamp"] > symbol_state.last_tick_timestamp:
            delta_time_seconds = tick_data["timestamp"] - symbol_state.last_tick_timestamp
            ema_alpha = 1.0 - math.exp(-delta_time_seconds / EMA_TIME_CONSTANT_SECONDS)
            symbol_state.smoothed_ema_score = (
                ema_alpha * raw_composite_score
                + (1 - ema_alpha) * symbol_state.smoothed_ema_score)
            peak_decay_factor = math.exp(
                -delta_time_seconds * math.log(2) / PEAK_SCORE_HALF_LIFE_SECONDS)
            symbol_state.peak_score_amplitude = max(
                symbol_state.peak_score_amplitude * peak_decay_factor,
                abs(symbol_state.smoothed_ema_score))
        # else: इस tick का timestamp पिछले वाले जैसा ही है (एक batched/
        # duplicate tick) -- पिछला smoothed score और peak amplitude
        # unchanged रखें।
        #
        # ज़रूरी: last_tick_timestamp सिर्फ़ आगे बढ़ना चाहिए। एक out-of-
        # order/delayed tick (network jitter, WS reconnect replay) पहले
        # से stored timestamp से EARLIER timestamp के साथ आ सकता है।
        # ऐसे tick के timestamp से last_tick_timestamp को unconditionally
        # overwrite करने से watermark पीछे चला जाएगा; अगला in-order tick
        # फिर उस बहुत-पुराने watermark के मुक़ाबले delta_time_seconds
        # compute करेगा, elapsed time को overstate करेगा और EMA alpha
        # (calculator #9) व peak-amplitude decay factor दोनों को distort
        # कर देगा।
        # Verified via trace: tick1 ts=5, out-of-order tick2 ts=3 (correctly
        # skipped for EMA update above), tick3 ts=6 -- the old unconditional
        # assignment left last_tick_timestamp=3 after tick2, so tick3's
        # delta_time_seconds became 6-3=3 instead of the correct 6-5=1.
        symbol_state.last_tick_timestamp = max(
            symbol_state.last_tick_timestamp, tick_data["timestamp"])

        if latency_ms > LATENCY_WARNING_MS:
            print(f"[B] high lat={latency_ms:.0f}ms sym={symbol}", flush=True)

        score_publisher.send_multipart([symbol_bytes, json.dumps({
            "symbol": symbol,
            "score": symbol_state.smoothed_ema_score,
            "peak": symbol_state.peak_score_amplitude,
            "relative_volume": relative_volume,
            "gap": gap_fade_raw_score,
            # gap_unrestricted: ऊपर वाले "gap" जैसा ही measurement है, पर
            # 9:15-9:30 entry-only time gate के बिना। broadcaster_loop
            # का exit check किसी पहले से TRACKED GAP_FADE position के
            # लिए इस field को इस्तेमाल करता है, न कि "gap" को -- वहाँ
            # time-gated "gap" इस्तेमाल करना एक असली bug था: window
            # end होते ही यह unconditionally zero हो जाता है, जो हर खुली
            # GAP_FADE position के लिए ठीक 9:30:00 पर एक false exit
            # alert force करता था, चाहे underlying gap वाकई closed हुआ
            # हो या नहीं।
            "gap_unrestricted": gap_fade_raw_score_unrestricted,
            "last_traded_price": tick_data["last_traded_price"],
            "timestamp": tick_data["timestamp"],
        }).encode()])

        tick_count_in_window += 1
        current_time = time.time()
        if current_time - last_stats_print_time >= 10:
            top_five_by_absolute_score = sorted(
                symbol_state_by_symbol.items(),
                key=lambda symbol_and_state: -abs(symbol_and_state[1].smoothed_ema_score)
            )[:5]

            def get_last_traded_price(state):
                history = state.last_traded_price_history_30s
                return history[-1][1] if history else 0.0

            summary_line = "  ".join(
                f"{symbol_name}={state.smoothed_ema_score:+.2f}"
                f"(ltp={get_last_traded_price(state):.1f})"
                for symbol_name, state in top_five_by_absolute_score
            ) or "-"
            ticks_per_second = tick_count_in_window / max(current_time - last_stats_print_time, 0.001)
            print(f"[B] ticks={tick_count_in_window} ({ticks_per_second:.0f}/s)  "
                  f"syms={len(symbol_state_by_symbol)}  top: {summary_line}", flush=True)
            tick_count_in_window, last_stats_print_time = 0, current_time

    print("[B] shutting down (signal received)", flush=True)
    # ZMQ sockets का default LINGER=-1 (infinite) होता है: close() तब तक
    # block करेगा जब तक कोई भी outstanding/buffered-but-unsent messages
    # flush नहीं हो जातीं, और नीचे zmq_context.term() तब तक block करता है
    # जब तक context की हर socket close नहीं हो जाती। इन दोनों को मिलाकर,
    # एक unflushed PUB socket (score_publisher) एक slow/absent SUB peer
    # के साथ close() को खुद अनिश्चित काल तक hang करा सकता है -- जो पहले
    # से लगी close-before-term ordering का पूरा मक़सद हरा देता है। LINGER=0
    # set करने से close() unsent messages को तुरंत drop करता है, wait
    # करने के बजाय, जो shutdown के दौरान सही trade-off है (एक signal-
    # only advisory alert जो कभी send ही न हो, वो एक ऐसे process से
    # बहुत कम नुक़सानदायक है जो hang होकर launcher के 5s timeout पर
    # SIGKILL होना पड़े)।
    for zmq_socket in (tick_subscriber, score_publisher):
        try:
            zmq_socket.setsockopt(zmq.LINGER, 0)
        except Exception:
            pass
    tick_subscriber.close()
    score_publisher.close()
    # अलग-अलग sockets close करने से उनके file descriptors release होते
    # हैं, लेकिन underlying zmq.Context (एक C++-level I/O thread pool) अलग
    # है और उसे explicit रूप से कभी tear down नहीं किया गया। term() तब
    # तक block करता है जब तक context के I/O threads साफ़ तरीक़े से बंद
    # नहीं हो जाते; इसके बिना, वो threads (और कोई lingering socket state)
    # इस function के return होने के बाद भी थोड़ी देर alive रह सकती हैं,
    # जो एक ऐसे process के लिए wasted cleanup work है जो वैसे भी exit
    # होने वाला है, पर सस्ता है और सही तरीक़े से करना उचित है।
    zmq_context.term()

