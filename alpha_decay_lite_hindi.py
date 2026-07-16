"""
Alpha-Decay Lite -- केवल signal (सिग्नल) देने वाला NSE cash equity alerting engine.
रीयल-टाइम, per-tick पाइपलाइन:  Angel WS (mode 3)  ->  ZMQ  ->  9 calculators + EMA  ->  Telegram
सिर्फ़ advisory (सलाहकारी) है।  कोई order खुद से place नहीं करता।

नामकरण (Naming) के बारे में एक बात: इस फाइल में हर variable और function
का नाम पूरा और descriptive है (कोई single-letter या समझ में न आने वाला
अबब्रीविएशन नहीं) ताकि code को समझने के लिए इस comment block या किसी
external documentation को बार-बार देखने की ज़रूरत न पड़े।

नोट: यह फाइल alpha_decay_lite.py की हिंदी-कमेंट वाली कॉपी है। code (variables,
functions, string literals जो output में जाते हैं) exactly वैसा ही है जैसा
original में है -- सिर्फ़ comments/docstrings को हिंदी में लिखा गया है ताकि
पढ़ने में आसान हो। दोनों फाइलें एक जैसा ही व्यवहार करती हैं।

================================================================================
READING GUIDE / TABLE OF CONTENTS  (यह फाइल कैसे पढ़ें)
================================================================================
यह फ़ाइल 3 अलग प्रोसेस चलाती है (ऊपर pipeline diagram देखें)। ऊपर से नीचे
लीनियर पढ़ने के बजाय, नीचे दिए गए क्रम में पढ़ना ज़्यादा आसान रहेगा -- हर
हिस्सा काफ़ी हद तक स्वतंत्र (self-contained) है।

सुझाया गया पढ़ने का क्रम:
  1. CONFIGURATION CONSTANTS   -- सब कुछ किन नंबरों पर चलता है, यह समझ लें
  2. ROLLING WINDOW HELPER     -- हर calculator इसी पर बना है, पहले समझना ज़रूरी
  3. PROCESS A (Data Factory)  -- डेटा कहाँ से आता है
  4. PROCESS B (Alpha Engine)  -- 9 calculators + score गणना (सबसे भारी हिस्सा)
  5. PROCESS C (Broadcaster)   -- अलर्ट कब/कैसे भेजे जाते हैं + Telegram बटन
  6. LAUNCHER                  -- सब कुछ शुरू/बंद/restart कैसे होता है

हर सेक्शन नीचे "======" वाली लाइनों से अलग किया गया है -- अपने एडिटर में
Ctrl+F से नीचे दिए गए EXACT TEXT खोजें तो सीधे उस सेक्शन पर पहुँच जाएंगे
(line नंबर भी दिए हैं, पर एडिट होने पर बदल सकते हैं -- टेक्स्ट सर्च ज़्यादा भरोसेमंद है):

    "CONFIGURATION CONSTANTS"                          (कॉन्फ़िगरेशन, लगभग यहीं से शुरू)
    "AMORTIZED-O(1) ROLLING WINDOW HELPER"              (RollingWindowSum क्लास)
    "HEARTBEAT HELPER"                                  (heartbeat थ्रेड हेल्पर)
    "PROCESS A: DATA FACTORY"                           (Angel WebSocket ingest)
    "PROCESS B: ALPHA ENGINE"                           (9 calculators + score)
    "PROCESS C: BROADCASTER + TELEGRAM BOT"             (अलर्ट भेजना)
    "LAUNCHER: self-healing"                            (३ प्रोसेस मैनेज करना)

--------------------------------------------------------------------------
INDEX  (function/class नाम -> संक्षिप्त विवरण, लाइन नंबर सेक्शन के अनुसार क्रम में)
--------------------------------------------------------------------------

[शुरुआत / कॉन्फ़िगरेशन]
  clip_value()                        -- किसी value को [min, max] रेंज में clamp करे
  is_within_trading_hours()           -- क्या अभी NSE का ट्रेडिंग टाइम है (watchdog के लिए)

[ROLLING WINDOW HELPER]
  class RollingWindowSum              -- O(1) amortized rolling sum, out-of-order-safe
  start_heartbeat_thread()            -- हर प्रोसेस की "मैं ज़िंदा हूँ" heartbeat (Redis में)

[PROCESS A: DATA FACTORY  -- Angel WebSocket से टिक डेटा लाना]
  download_scrip_master_with_retry()  -- scrip master JSON डाउनलोड, network retry के साथ
  resolve_nse_equity_tokens()         -- symbol नाम -> Angel token नंबर मैपिंग
  data_factory()                      -- Process A का मुख्य entry point
    ├─ Angel login + JWT auto-refresh timer
    ├─ tick-starvation watchdog (zombie कनेक्शन पकड़ने के लिए)
    ├─ on_tick_received()             -- हर टिक पर चलने वाला callback (नेस्टेड फंक्शन)
    └─ graceful shutdown (SIGTERM/SIGINT handler)

[PROCESS B: ALPHA ENGINE  -- 9 calculators + score गणना]
  class SymbolState                   -- हर symbol की rolling state (एक copy प्रति stock)
  calculate_vwap_distance()           -- Calculator #1: VWAP से कितनी दूर है price
  calculate_relative_volume()         -- Calculator #2: RVOL (आज का वॉल्यूम बनाम औसत)
  calculate_aggressive_buy_pressure() -- Calculator #3: आक्रामक खरीद/बिक्री दबाव
  calculate_absorption()              -- Calculator #4: भारी वॉल्यूम फिर भी price flat (absorption)
  calculate_book_imbalance()          -- Calculator #5: bid/ask quantity असंतुलन
  calculate_spread_ratio()            -- Calculator #6: spread फिल्टर (जो score को 0 कर दे)
  calculate_gap_fade_unrestricted()   -- gap-fade का मूल फॉर्मूला (समय की कोई सीमा नहीं)
  calculate_gap_fade()                -- Calculator #7: वही फॉर्मूला, पर सिर्फ़ 9:15-9:30 में
  calculate_session_weight()          -- Calculator #8: दिन के किस हिस्से में हैं (weight)
  compute_feature_score()             -- सभी 9 calculators को जोड़कर एक composite score बनाना
  classify_trade_direction()          -- Lee-Ready ट्रेड क्लासिफिकेशन (खरीद या बिक्री?)
  update_rolling_windows()            -- हर टिक पर सभी rolling windows अपडेट करना
  alpha_engine()                      -- Process B का मुख्य entry point (इसमें Calculator #9 -
                                          EMA smoother - भी inline लिखा है, अलग function नहीं)

[PROCESS C: BROADCASTER + TELEGRAM BOT  -- अलर्ट भेजना]
  determine_signal_modes()            -- स्कोर/वॉल्यूम/गैप देखकर कौन सा mode (MOMENTUM/
                                          MEAN_REVERSION/GAP_FADE) qualify करता है
  is_entry_cooldown_expired()         -- entry cooldown चेक (per symbol+mode)
  is_exit_cooldown_expired()          -- exit cooldown चेक (per symbol+mode)
  build_entry_alert_text()            -- entry अलर्ट का मैसेज टेक्स्ट बनाना
  build_exit_alert_text()             -- exit अलर्ट का मैसेज टेक्स्ट बनाना
  initialize_telegram_bot()           -- Telegram bot शुरू करना + ACK/SKIP/DONE/HOLD बटन लॉजिक
  shutdown_telegram_bot()             -- Telegram bot बंद करना
  determine_entry_direction()         -- LONG या SHORT? (हर mode के लिए सही तरीका)
  send_entry_alert()                  -- entry अलर्ट भेजना (या bot disabled हो तो stdout पर print)
  send_exit_alert()                   -- exit अलर्ट भेजना
  check_suppression_status()          -- 5 में से कौन सा kill-switch अभी active है, चेक करना
  broadcaster_loop()                  -- Process C का मुख्य लूप (सबसे बड़ा फंक्शन, ध्यान से पढ़ें)
  run_broadcaster_process()           -- asyncio wrapper ताकि multiprocessing इसे चला सके

[LAUNCHER  -- तीनों प्रोसेस शुरू/बंद/restart करना]
  cleanup_ipc_socket_files()          -- पुरानी ZMQ socket फ़ाइलें साफ़ करना
  spawn_all_processes()               -- A, B, C तीनों प्रोसेस शुरू करना
  shutdown_all_processes()            -- तीनों को ग्रेसफुली बंद करना (SIGTERM फिर ज़रूरत हो तो SIGKILL)
  if __name__ == "__main__":          -- मेन लूप: process मरे तो respawn, crash-loop breaker

--------------------------------------------------------------------------
हर बड़े comment block में ये पैटर्न मिलेगा (ये bug-fix history है, ignore मत
करें) -- जहाँ भी "NOTE:", "IMPORTANT:", या "verified via simulation" लिखा
हो, वहाँ किसी असली bug को ढूंढ कर fix किया गया था। वो comment बताता है कि
वो कोड उस particular तरीके से क्यों लिखा गया, ताकि कोई future edit उस bug
को दोबारा introduce न करे।
================================================================================
"""
import os
import sys
import time
import signal
import threading

# time.localtime() के लिए IST को force करें (तेज़ C-level रास्ता + सही timezone,
# चाहे server खुद किसी भी timezone पर सेट हो)।
#
# ज़रूरी प्लेटफ़ॉर्म नोट: time.tzset() सिर्फ़ Unix पर उपलब्ध है
# (Python docs: "Availability: Unix")। Windows पर, अकेला os.environ["TZ"]
# time.localtime() के output को नहीं बदलता, क्योंकि Windows का C runtime
# TZ environment variable को Unix libc की तरह नहीं देखता, और वहाँ
# time.tzset() खुद ही exist नहीं करता। यह deployment सिर्फ़ Linux VPS के
# लिए बना है (README/deploy docs देखें), इसलिए असली इस्तेमाल में यह कोई
# व्यावहारिक समस्या नहीं है -- लेकिन अगर यह script गलती से Windows पर
# चला दी जाए, तो calculate_gap_fade और calculate_session_weight के
# time-of-day windows बिना कोई error दिए ग़लत timezone पर चलेंगे। नीचे
# का check तुरंत fail कर देता है एक साफ़ message के साथ, बजाय इसके कि
# चुपचाप ग़लत signals बनते रहें।
os.environ["TZ"] = "Asia/Kolkata"
if hasattr(time, "tzset"):
    time.tzset()
elif os.name != "posix":
    # यह message जान-बूझकर अंग्रेज़ी में है -- यह startup log में जाता है और
    # remote SSH / VPS console में सही से दिखे इसके लिए ASCII text सबसे safe है।
    print("[STARTUP] FATAL: this script requires time.tzset() (Unix-only) "
          "for correct IST session-window calculations. Detected a "
          "non-Unix platform (Windows?) where TZ env var alone does not "
          "affect time.localtime(). Run this on a Linux/Unix host instead.",
          flush=True)
    sys.exit(1)

import json
import math
import asyncio
import requests
from collections import deque
from dataclasses import dataclass, field
from multiprocessing import Process

import zmq
import zmq.asyncio
import redis
import redis.asyncio as redis_asyncio
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIGURATION CONSTANTS (सभी configuration constants)
# ============================================================
ZMQ_TICKS_ADDRESS   = "ipc:///tmp/alpha_ticks.ipc"    # Process A -> Process B (ticks भेजने के लिए)
ZMQ_SCORES_ADDRESS  = "ipc:///tmp/alpha_scores.ipc"   # Process B -> Process C (scores भेजने के लिए)
REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MUTE_FILE_PATH       = os.getenv("MUTE_FILE", "/tmp/alpha_mute.flag")
TRADING_UNIVERSE     = [symbol.strip() for symbol in
                         os.getenv("UNIVERSE", "RELIANCE,TCS,HDFCBANK").split(",")
                         if symbol.strip()]
                        # हर symbol पर .strip() क्यों: अगर कोई .env में
                        # "RELIANCE, TCS, HDFCBANK" (कॉमा के बाद space, जो
                        # पढ़ने में natural है) लिखे, तो सादा split(",")
                        # इसे [' TCS', ' HDFCBANK'] बना देगा -- हर symbol के
                        # आगे एक space जुड़ जाएगा। Angel के scrip master में
                        # " TCS" नाम का कोई entry नहीं है, इसलिए वो symbols
                        # बिना किसी crash के चुपचाप drop हो जाएंगे -- data
                        # missing होगा और यह देखने में आसानी से नहीं दिखता।
                        # `if symbol.strip()` वाला filter एक और चीज़ रोकता है --
                        # अगर कोई "RELIANCE,TCS," लिख दे (आख़िर में trailing
                        # comma) तो एक ख़ाली-string entry नहीं बनेगी।
SCRIP_MASTER_URL     = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

TELEGRAM_BOT_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Angel SDK के mode-3 SNAP_QUOTE में कुछ SDK versions पर यह देखा गया है कि
# best_5_buy_data और best_5_sell_data output layer पर swap हो जाते हैं।
# .env में ANGEL_BIDASK_SWAPPED=1 सिर्फ़ तभी लगाएँ जब diag_bidask.py
# चलाकर confirm हो जाए कि आपके SDK version में यह swap हो रहा है। यह
# सेटिंग अगर ग़लत लगा दी तो spread filter बिल्कुल टूट जाएगा।
ANGEL_BID_ASK_IS_SWAPPED = os.getenv("ANGEL_BIDASK_SWAPPED", "0").strip() == "1"

SPREAD_RATIO_MAX            = 0.0015   # 0.15% -- इससे ऊपर होने पर score को 0 कर दिया जाता है
EMA_TIME_CONSTANT_SECONDS   = 1.0      # score smoother का tau (calculator #9)

# Cooldowns rate limits नहीं हैं। यह system सिर्फ़ advisory (सलाहकारी) है --
# user तय करता है कि कौन से alert पर action लेना है, इसलिए हर qualifying
# signal deliver होता है। Cooldowns सिर्फ़ इसलिए हैं ताकि एक ही signal
# (symbol + mode) बार-बार न fire हो जब तक score अपने trigger band में है।
COOLDOWN_SECONDS_BY_MODE = {"MOMENTUM": 300, "MEAN_REVERSION": 45, "GAP_FADE": 180}

ZMQ_HIGH_WATER_MARK      = 10000   # ZMQ socket पर max queued messages, इसके बाद drop होंगे
LATENCY_WARNING_MS       = 100     # tick से score तक latency इससे ज़्यादा हो तो warning log करें
MUTE_STATUS_CACHE_SECONDS = 1.0    # Process C mute/heartbeat check को कितनी देर cache करे
RVOL_WARMUP_SECONDS      = 300     # RVOL तभी meaningful है जब 5+ मिनट का history हो
RESET_DROP_FRACTION_THRESHOLD = 0.5   # cumulative volume में >50% की गिरावट को broker-side
                                       # counter reset (rebase) माना जाएगा, न कि stale/out-of-order
                                       # packet (reject) -- update_rolling_windows देखें
PEAK_SCORE_HALF_LIFE_SECONDS = 60.0  # "peak score" amplitude कितनी तेज़ी से decay करे
AUTO_RESTART_AFTER_HOURS = float(os.getenv("AUTO_RESTART_HOURS", "20"))  # Angel JWT ~24-28h में expire होता है, उससे कम रखें
TICK_STARVATION_WATCHDOG_SECONDS = 90   # data_factory में watchdog thread देखें:
                                         # अगर MARKET OPEN के दौरान इतनी देर तक कोई tick न आए तो
                                         # Process A को force-exit करके launcher से respawn करवाएँ --
                                         # यह "zombie" half-open TCP connection से बचाता है (server
                                         # ने FIN/RST नहीं भेजा, तो SDK का blocking connect() call
                                         # कभी return नहीं करता और process OS को हमेशा "alive"
                                         # दिखता रहता है)।

# Process exit codes जिन्हें launcher देखकर तय करता है कि क्या react करे।
# 0 (सादा `return`) और कोई भी unrecognized code को unplanned crash माना जाएगा
# -> respawn होगा और crash-loop breaker में count किया जाएगा।
EXIT_CODE_PLANNED_RESTART = 2   # scheduled JWT-refresh self-exit -> तुरंत respawn, कोई penalty नहीं
EXIT_CODE_FATAL_CONFIG    = 3   # missing/invalid credentials या config -> retry नहीं होगा; तुरंत रुकें
                                # क्योंकि यह error बिना human intervention (.env ठीक किए) खुद
                                # नहीं सुधरेगी, तो crash-loop budget बर्बाद करने का कोई मतलब नहीं

# ---- Kill switches (heartbeats) ----
PROCESS_HEARTBEAT_TTL_SECONDS      = 3   # process-alive heartbeat का TTL (kill switch #2)
DATA_FLOW_HEARTBEAT_TTL_SECONDS    = 5   # data-is-flowing heartbeat का TTL (kill switch #1)
HEARTBEAT_WRITE_INTERVAL_SECONDS   = 1

# ---- Position tracking (ACK'd entries -> exit alerts के लिए) ----
EXIT_ALERT_SCORE_THRESHOLD   = 2.0        # |score| <= इस value -> exit alert भेजें
EXIT_ALERT_COOLDOWN_SECONDS  = 300        # एक ही symbol के लिए exit alerts के बीच minimum time
TRACKED_POSITION_TTL_SECONDS = 6 * 3600   # spec requirement: 6h+ पुरानी stale positions auto-cleanup करें
PENDING_ALERT_TTL_SECONDS    = 24 * 3600  # un-ACK'd alert Redis में कितनी देर actionable रहेगा


def clip_value(value, minimum, maximum):
    """value को [minimum, maximum] range में clamp करता है (inclusive)।"""
    return max(minimum, min(maximum, value))


def is_within_trading_hours(timestamp):
    """NSE के continuous-trading equity session के लिए True return करता है:
    weekdays, 09:15:00-15:30:00 IST। यह जान-बूझकर exchange holidays को
    नहीं जानता (एक static holiday-calendar table हर साल maintain करना
    पड़ेगा, और practical cost शून्य है -- अगर holiday को 'market closed
    but watchdog stays quiet anyway' मान लिया जाए, तो नुक़सान कुछ नहीं
    क्योंकि holiday पर ticks आते ही नहीं)। यह function data_factory के
    tick-starvation watchdog में इस्तेमाल होता है: इस check के बिना,
    watchdog हर रात/weekend पर misfire होगा (जब घंटों तक zero ticks आना
    बिल्कुल normal है), Process A को बार-बार restart करेगा और launcher
    का crash-loop breaker trip कर देगा -- जो फिर sys.exit(1) करके पूरा
    system बंद कर देगा (Process B और C सहित, सिर्फ़ A नहीं)।"""
    local_time = time.localtime(timestamp)
    if local_time.tm_wday >= 5:   # शनिवार=5, रविवार=6
        return False
    hour, minute = local_time.tm_hour, local_time.tm_min
    after_open = (hour > 9) or (hour == 9 and minute >= 15)
    before_close = (hour < 15) or (hour == 15 and minute <= 30)
    return after_open and before_close


# ============================================================
# AMORTIZED-O(1) ROLLING WINDOW HELPER
# (एक trailing time window पर running sum रखने वाला helper)
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
# HEARTBEAT HELPER (Process A, B और C तीनों इसका इस्तेमाल करते हैं)
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
# PROCESS A: DATA FACTORY (Angel SmartWebSocketV2, snap-quote mode 3)
# Angel WebSocket से टिक डेटा लेकर ZMQ पर publish करने वाला process
# ============================================================
def download_scrip_master_with_retry():
    """Angel के scrip master JSON को download करता है, एक छोटे retry/backoff
    loop के साथ। जो सादा requests.get() इसने replace किया, उसमें कोई भी
    exception handling नहीं थी: process startup पर एक transient DNS
    failure या network timeout (जबकि WebSocket connection या कुछ भी और
    try नहीं किया गया था) तुरंत exception raise कर देता और Process A को
    पहले ही call पर crash कर देता। यह crash launcher की नज़र में किसी भी
    दूसरे process death जैसा ही दिखता है -- MAX_RESTARTS_ALLOWED_PER_WINDOW
    में उतना ही count होता है -- तो startup पर कुछ मिनटों की network
    outage पूरे crash-loop budget को बर्बाद कर सकती है सिर्फ़ इस एक
    download पर, और launcher का FATAL shutdown (sys.exit(1), Process B
    और C भी बंद, सिर्फ़ A नहीं) trigger हो सकता है इससे पहले कि network
    को recover होने का मौक़ा भी मिले। एक छोटा bounded retry exponential
    backoff के साथ इसी तरह की transient failures को absorb कर लेता है,
    हर attempt के लिए पूरा process restart cycle चलाए बिना; अगर network
    इस loop के total budget से भी ज़्यादा देर तक down रहे, तो यह अभी भी
    raise करेगा (यहाँ infinite retry नहीं है), इसलिए एक genuinely
    persistent outage पर launcher का पहले वाला crash-loop breaker वैसे
    ही fallback कर के काम करेगा, hang होने की जगह।"""
    max_attempts = 4
    retry_delay_seconds = 3
    last_error = None
    for attempt_number in range(1, max_attempts + 1):
        try:
            return requests.get(SCRIP_MASTER_URL, timeout=60).json()
        except (requests.exceptions.RequestException, ValueError) as download_error:
            last_error = download_error
            print(f"[A] scrip master download failed (attempt {attempt_number}/"
                  f"{max_attempts}): {download_error!r}", flush=True)
            if attempt_number < max_attempts:
                time.sleep(retry_delay_seconds)
                retry_delay_seconds *= 2
    raise last_error


def resolve_nse_equity_tokens(symbol_list):
    """Angel के scrip master JSON को download करता है और हर requested NSE
    equity symbol (जैसे 'RELIANCE') को उसके numeric exchange token से
    match करता है, जो WebSocket subscribe call को चाहिए (symbol नाम नहीं
    चाहिए)। हर मिले हुए symbol के लिए एक {token: symbol} dict return
    करता है।"""
    start_time = time.time()
    print("[A] Downloading scrip master (~4 MB)...", flush=True)
    scrip_master_list = download_scrip_master_with_retry()
    nse_symbol_to_token = {
        entry["symbol"].replace("-EQ", ""): entry["token"]
        for entry in scrip_master_list
        if entry.get("exch_seg") == "NSE" and entry.get("symbol", "").endswith("-EQ")
    }
    resolved_token_to_symbol = {
        nse_symbol_to_token[symbol]: symbol
        for symbol in symbol_list if symbol in nse_symbol_to_token
    }
    missing_symbols = [symbol for symbol in symbol_list if symbol not in nse_symbol_to_token]
    elapsed_seconds = time.time() - start_time
    print(f"[A] Scrip master loaded in {elapsed_seconds:.1f}s, "
          f"resolved {len(resolved_token_to_symbol)}/{len(symbol_list)} symbols", flush=True)
    if missing_symbols:
        print(f"[A] Symbols NOT found: {missing_symbols}", flush=True)
    return resolved_token_to_symbol


def data_factory():
    """Process A का entry point। Angel SmartAPI में login करता है, trading
    universe (symbol list) को exchange tokens में convert करता है,
    market-data WebSocket को snap-quote mode 3 में खोलता है, और आने वाले
    हर tick को ZMQ पर publish कर देता है ताकि Process B उसे consume कर
    सके।"""
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    import pyotp

    angel_api_key      = os.getenv("ANGEL_API_KEY")
    angel_client_code  = os.getenv("ANGEL_CLIENT_CODE")
    angel_password     = os.getenv("ANGEL_PASSWORD")        # 4-digit MPIN, web login password नहीं
    angel_totp_secret  = os.getenv("ANGEL_TOTP_SECRET")

    required_env_vars = {
        "ANGEL_API_KEY": angel_api_key,
        "ANGEL_CLIENT_CODE": angel_client_code,
        "ANGEL_PASSWORD": angel_password,
        "ANGEL_TOTP_SECRET": angel_totp_secret,
    }
    missing_env_vars = [name for name, value in required_env_vars.items() if not value]
    if missing_env_vars:
        print(f"[A] FATAL missing env vars: {missing_env_vars}. Fill .env and restart.")
        os._exit(EXIT_CODE_FATAL_CONFIG)

    print("[A] Logging in to Angel...", flush=True)
    angel_connection = SmartConnect(api_key=angel_api_key)
    try:
        current_totp_code = pyotp.TOTP(angel_totp_secret).now()
    except Exception as totp_error:
        print(f"[A] FATAL invalid TOTP secret: {totp_error}. ANGEL_TOTP_SECRET must be base32.")
        os._exit(EXIT_CODE_FATAL_CONFIG)

    login_session = angel_connection.generateSession(
        angel_client_code, angel_password, current_totp_code)
    if not login_session or not login_session.get("data"):
        error_message = (login_session or {}).get("message", "unknown")
        print(f"[A] FATAL Angel login failed: {error_message}")
        print("     Common fixes:")
        print("       - ANGEL_PASSWORD must be your 4-digit MPIN (not web login password)")
        print("       - ANGEL_TOTP_SECRET must be the full base32 secret (not 6-digit code)")
        print("       - Ensure system time is NTP-synced: `timedatectl`")
        os._exit(EXIT_CODE_FATAL_CONFIG)

    auth_token = login_session["data"]["jwtToken"]
    feed_token = angel_connection.getfeedToken()
    print(f"[A] Login OK, client={angel_client_code}", flush=True)

    # JWT auto-refresh: token expire होने से पहले (~24-28h typical lifetime)
    # एक clean self-exit schedule करें। Launcher का respawn logic इस exit
    # के बाद इस process को अपने आप फिर से शुरू कर देगा।
    if AUTO_RESTART_AFTER_HOURS > 0:
        def exit_for_scheduled_restart():
            print(f"[A] JWT nearing expiry ({AUTO_RESTART_AFTER_HOURS}h up), "
                  f"exiting for restart", flush=True)
            os._exit(EXIT_CODE_PLANNED_RESTART)

        restart_timer = threading.Timer(AUTO_RESTART_AFTER_HOURS * 3600,
                                         exit_for_scheduled_restart)
        restart_timer.daemon = True
        restart_timer.start()

    token_to_symbol_map = resolve_nse_equity_tokens(TRADING_UNIVERSE)
    token_list = list(token_to_symbol_map.keys())

    zmq_context = zmq.Context()   # हर process के लिए fresh context, fork से inherited state से बचाव
    tick_publisher = zmq_context.socket(zmq.PUB)
    tick_publisher.setsockopt(zmq.SNDHWM, ZMQ_HIGH_WATER_MARK)
    tick_publisher.bind(ZMQ_TICKS_ADDRESS)
    time.sleep(1.5)   # SUB peers को connect होने का समय दें (ZMQ "slow joiner" packet loss से बचाव)
    print(f"[A] Data Factory up, {len(token_list)} tokens mode=3", flush=True)

    # Heartbeats: kill switch #1 (data flow हो रहा है या नहीं) + kill switch #2 (process alive है या नहीं)।
    heartbeat_redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    last_tick_received_at = {"timestamp": 0.0}
    last_heartbeat_write_from_tick = {"timestamp": 0.0}   # tick-driven heartbeat write के लिए throttle

    def check_data_is_flowing():
        seconds_since_last_tick = time.time() - last_tick_received_at["timestamp"]
        return "data", seconds_since_last_tick < DATA_FLOW_HEARTBEAT_TTL_SECONDS

    start_heartbeat_thread("A", heartbeat_redis_client, additional_check=check_data_is_flowing)

    # Tick-starvation watchdog: "zombie" half-open TCP connection से बचाव।
    # नीचे websocket_client.connect() एक BLOCKING call है जो इस process
    # के अकेले thread को तब तक रोके रखता है जब तक close_connection() न
    # call हो या underlying socket में error न आ जाए। अगर network चुपचाप
    # connection drop कर दे बिना TCP FIN/RST भेजे (कुछ cloud/VPS network
    # paths में एक असली, काफ़ी common failure mode), तो न तो on_close
    # trigger होगा और न ही on_error -- SDK को पता ही नहीं चलेगा कि
    # connection dead है, तो connect() कभी return नहीं करेगा और यह
    # process हमेशा के लिए वहीं रुका रहेगा, OS की नज़र में पूरी तरह से
    # "alive" (उसके पास live PID है, crash नहीं हुआ, OS-level zombie भी
    # नहीं है)। Launcher का respawn logic सिर्फ़ process.is_alive() के
    # False होने पर react करता है, इसलिए एक hung-but-alive process A
    # उसे दिखाई ही नहीं देगा -- kill switch #1 (alpha:hb:data) heartbeat
    # TTL expire होने पर alerts को सही से suppress कर देगा, लेकिन आज कुछ
    # भी इस process को असल में restart करके recover नहीं करवाएगा; यह
    # चुपचाप mute state में रुका रहेगा जब तक कोई इंसान इसे notice करके
    # manually restart न करे (या ज़्यादा से ज़्यादा, ~20h वाला scheduled
    # JWT-refresh restart इसे eventually cycle कर दे)।
    #
    # यह background thread independently last_tick_received_at को
    # monitor करता है और अगर ticks TICK_STARVATION_WATCHDOG_SECONDS से
    # ज़्यादा देर तक न आएँ, तो os._exit(EXIT_CODE_PLANNED_RESTART)
    # call करता है -- लेकिन सिर्फ़ तब जब is_within_trading_hours() कहे
    # कि market अभी actually ticks भेज रहा होना चाहिए। इस market-hours
    # gate के बिना, यही watchdog हर रात और हर weekend पर misfire होगा
    # (जब zero ticks आना बिल्कुल normal है), Process A को बार-बार बेवजह
    # Angel re-login करवाएगा, और launcher का crash-loop breaker trip कर
    # देगा -- जो फिर sys.exit(1) करके पूरा system बंद कर देगा (Process
    # B और C सहित), जो इस zombie-connection bug से भी बुरा outcome है
    # जिसे यह watchdog fix करने के लिए बनाया गया है।
    #
    # os._exit() (न कि sys.exit()) यहाँ ठीक ऊपर वाले JWT-refresh restart
    # जैसा ही है: यहाँ से clean return possible नहीं है क्योंकि blocking
    # connect() call इस thread के caller को control वापस नहीं देता, तो
    # unwind करने के लिए कोई orderly code path है ही नहीं -- launcher
    # का respawn-on-process-death ही असल में recovery mechanism है,
    # बिल्कुल JWT case की तरह।
    def watch_for_tick_starvation():
        while True:
            time.sleep(15)
            now = time.time()
            if not is_within_trading_hours(now):
                continue
            seconds_since_last_tick = now - last_tick_received_at["timestamp"]
            if (last_tick_received_at["timestamp"] > 0
                    and seconds_since_last_tick > TICK_STARVATION_WATCHDOG_SECONDS):
                print(f"[A] WATCHDOG: no tick in {seconds_since_last_tick:.0f}s during "
                      f"market hours (possible zombie connection) -- forcing restart",
                      flush=True)
                os._exit(EXIT_CODE_PLANNED_RESTART)

    watchdog_thread = threading.Thread(target=watch_for_tick_starvation, daemon=True)
    watchdog_thread.start()

    # SDK का default max_retry_attempt=1 है, यानी पूरी तरह हार मानने से पहले
    # सिर्फ़ एक reconnect attempt। इन values को बढ़ाया है ताकि transient
    # network hiccups के लिए manual restart की ज़रूरत न पड़े। अगर installed
    # SDK version इन keyword arguments को accept नहीं करता, तो defaults पर
    # fallback कर देगा।
    try:
        websocket_client = SmartWebSocketV2(
            auth_token, angel_api_key, angel_client_code, feed_token,
            max_retry_attempt=10, retry_strategy=1,
            retry_delay=5, retry_multiplier=2, retry_duration=60)
    except TypeError:
        print("[A] SDK does not accept retry kwargs, using defaults", flush=True)
        websocket_client = SmartWebSocketV2(
            auth_token, angel_api_key, angel_client_code, feed_token)

    def on_tick_received(_websocket, market_data_message):
        symbol = token_to_symbol_map.get(str(market_data_message.get("token")))
        if not symbol:
            return
        now = time.time()
        last_tick_received_at["timestamp"] = now

        # Kill switch #1 की Redis key (alpha:hb:data) आम तौर पर सिर्फ़
        # background heartbeat thread से refresh होती है, जो हर write के
        # बीच HEARTBEAT_WRITE_INTERVAL_SECONDS (1s) sleep करता है। वह
        # thread tick आने से trigger नहीं होता -- अपने independent schedule
        # पर चलता है। नतीजा: अगर market DATA_FLOW_HEARTBEAT_TTL_SECONDS
        # (5s) से ज़्यादा shांत रहे, तो Redis key expire हो जाती है: और
        # जब एक बिल्कुल नया valid tick आए, तो Process C ~1 और सेकंड तक
        # EXPIRED key ही देखेगा (heartbeat thread के अगले scheduled wake
        # तक), और एक नए valid signal को ग़लत तरीक़े से "no_data" कह कर
        # suppress कर देगा। Simulation से verified: heartbeat thread के
        # अगले wake से 2ms पहले आया tick बाकी के ~1 second तक suppress
        # हो गया।
        # Fix: heartbeat key सीधे यहीं, हर tick पर लिखो -- लेकिन एक second
        # में एक बार throttle के साथ, ताकि hot per-tick path पर Redis
        # round-trip न जुड़े। इससे "data resumed" और "heartbeat key उसे
        # reflect करे" के बीच का gap milliseconds में आ जाता है।
        if now - last_heartbeat_write_from_tick["timestamp"] >= HEARTBEAT_WRITE_INTERVAL_SECONDS:
            try:
                heartbeat_redis_client.setex(
                    "alpha:hb:data", DATA_FLOW_HEARTBEAT_TTL_SECONDS, "1")
                last_heartbeat_write_from_tick["timestamp"] = now
            except Exception:
                pass   # background heartbeat thread वैसे भी catch up कर लेगा

        best_5_buy_levels  = market_data_message.get("best_5_buy_data") or []
        best_5_sell_levels = market_data_message.get("best_5_sell_data") or []
        # अगर SDK buy/sell labels swap कर रहा है (diag_bidask.py से
        # confirmed), तो यहाँ swap करके compensate कर देते हैं।
        if ANGEL_BID_ASK_IS_SWAPPED:
            best_5_buy_levels, best_5_sell_levels = best_5_sell_levels, best_5_buy_levels

        # NOTE: dict.get(key, default) default सिर्फ़ तभी return करता है
        # जब key ABSENT हो dict से। अगर broker का feed key को explicit
        # JSON null के साथ भेजे (Python के json module से None बनकर parse
        # होता है), तो .get(key, 0) None return करेगा, न कि 0 -- और
        # None / 100.0 करने पर TypeError raise होगा, इस callback को crash
        # कर देगा (और यह SDK के अपने thread के अंदर चलता है, इसलिए पूरा
        # process crash हो सकता है)। हर lookup को `(... or 0)` में wrap
        # करने से missing key, explicit None, और 0/empty value तीनों को
        # division से पहले 0 में बदल दिया जाता है, ताकि feed में एक अकेला
        # malformed field पूरे Process A को न ले डूबे। यह हर उस field
        # पर लगाया गया है जिसे नीचे 100.0 से divide किया जाता है (price
        # और VWAP fields); quantity fields (total_buy_quantity आदि) divide
        # नहीं होतीं इसलिए वो इस specific failure mode को share नहीं
        # करतीं, लेकिन consistency के लिए वो भी wrap कर दी हैं।
        tick_data = {
            "symbol": symbol,
            "timestamp": time.time(),
            "last_traded_price": (market_data_message.get("last_traded_price") or 0) / 100.0,
            # NOTE: सिर्फ़ `if best_5_buy_levels` check करना केवल EMPTY
            # list से बचाता है; यह इस case से नहीं बचाता कि पहला element
            # एक ऐसा dict हो जिसमें "price" key missing हो (जैसे {} जैसा
            # malformed level)। ऐसे case में best_5_buy_levels[0]["price"]
            # KeyError raise करेगा, callback crash कर देगा (जो SDK के
            # अपने thread पर चलता है)। सीधे indexing के बजाय .get("price")
            # इस्तेमाल करने से malformed-level case भी None पर degrade
            # हो जाता है, बाकी हर field की तरह जो इस dict में missing
            # values के लिए hardened है -- लेकिन .get("price", 0) default
            # 0 सिर्फ़ तभी देता है जब "price" KEY ABSENT हो। अगर level
            # dict में key मौजूद है पर उसकी value EXPLICIT JSON null है
            # (Python में None, ठीक वही failure mode जो ऊपर top-level
            # fields के लिए fix हुआ है), तो .get("price", 0) None return
            # करेगा, न कि 0, और None / 100.0 फिर TypeError raise करेगा,
            # callback फिर crash हो जाएगा। `(... or 0)` में wrap करना --
            # वही pattern जो हर top-level field पर लगा है -- missing key,
            # explicit None, और numeric 0 तीनों को division से पहले 0 में
            # बदल देता है, इस nested case को उसी तरह close कर देता है।
            "best_bid_price": ((best_5_buy_levels[0].get("price") or 0) / 100.0) if best_5_buy_levels else 0.0,
            "best_ask_price": ((best_5_sell_levels[0].get("price") or 0) / 100.0) if best_5_sell_levels else 0.0,
            "cumulative_day_volume": market_data_message.get("volume_trade_for_the_day") or 0,
            "daily_vwap": (market_data_message.get("average_traded_price") or 0) / 100.0,
            # total_buy_quantity / total_sell_quantity: Angel के WHOLE-
            # BOOK aggregates -- इस symbol के किसी भी price level पर
            # pending हर buy/sell order को count करते हैं, दूर पड़े
            # retail limit orders भी शामिल हैं (जैसे LTP ₹1100 पर है
            # और कोई ₹900 पर buy order डाले हुए है -- वो लगभग कभी
            # trade नहीं होगा)। calculate_book_imbalance के purpose के
            # लिए ये fields misleading हैं: एक perfectly balanced near-
            # price book पर भी ये strongly bullish (या bearish)
            # imbalance report कर सकते हैं सिर्फ़ इसलिए कि एक side पर
            # far-off "wish list" orders का long tail है।
            # Simulation से verified: near-price top-5 balanced 4000/4000,
            # far-off retail 4,50,000 buys LTP से 20% नीचे, whole-book
            # formula से imbalance +0.90 (score +1.80) आया vs सही 0.00
            # top-5 formula से।
            # tick_data में as-is रखे गए हैं backward compatibility के
            # लिए (external tools या कोई future calculator whole-book
            # value चाहे सकता है), पर meaningful "immediate liquidity
            # imbalance" signal top5_buy_quantity / top5_sell_quantity
            # नीचे है, जो calculate_book_imbalance actually पढ़ता है।
            "total_buy_quantity": market_data_message.get("total_buy_quantity") or 0,
            "total_sell_quantity": market_data_message.get("total_sell_quantity") or 0,
            # Top-5-level quantity sums: यह actionable near-price order-
            # book imbalance measure है। Angel पहले से 5 levels of depth
            # mode-3 SNAP_QUOTE में भेजता है (best_5_buy_data /
            # best_5_sell_data), तो ये sums हम locally compute कर लेते
            # हैं zero extra API cost पर। Same `(x or 0)` null-safety
            # pattern जो हर जगह इस tick_data dict में है, per level
            # apply करके एक malformed level dict (empty {}, missing
            # "quantity" key, या explicit JSON null value) से इस
            # callback को crash न होने देना।
            "top5_buy_quantity":  sum((lvl.get("quantity") or 0) for lvl in best_5_buy_levels[:5]),
            "top5_sell_quantity": sum((lvl.get("quantity") or 0) for lvl in best_5_sell_levels[:5]),
            "open_price": (market_data_message.get("open_price_of_the_day") or 0) / 100.0,
            "previous_close_price": (market_data_message.get("closed_price") or 0) / 100.0,
        }
        try:
            tick_publisher.send_multipart(
                [symbol.encode(), json.dumps(tick_data).encode()], zmq.DONTWAIT)
        except zmq.Again:
            pass   # backpressure में यह tick drop कर दो (publisher का high-water-mark पहुँच गया)

    websocket_client.on_data = on_tick_received
    websocket_client.on_open = lambda _websocket: websocket_client.subscribe(
        "adl", 3, [{"exchangeType": 1, "tokens": token_list}])
    websocket_client.on_error = lambda _websocket, error: print(
        f"[A] WS error: {error}", flush=True)
    websocket_client.on_close = lambda _websocket: print(
        "[A] WS closed (SDK will reconnect)", flush=True)

    # websocket_client.connect() एक BLOCKING call है -- यह SDK का अपना
    # event loop इसी thread पर चलाता है और connection close होने तक
    # return नहीं करता (यह verified है: process घंटों तक alive रहकर
    # ticks publish करता रहता है, इस point के नीचे किसी और loop के
    # बिना)। इसी वजह से, यहाँ एक SIGTERM handler सिर्फ़ एक flag set
    # नहीं कर सकता जिसे कोई `while` loop check करे (ऐसा loop है ही
    # नहीं -- connect() खुद thread पर बैठा है); उसे actively
    # close_connection() call करना पड़ेगा ताकि connect() return कर सके,
    # जिसके बाद नीचे का cleanup normal तरीक़े से चल सके।
    def handle_stop_signal(_signum, _frame):
        print("[A] shutdown signal received, closing WebSocket...", flush=True)
        try:
            websocket_client.close_connection()
        except Exception as close_error:
            print(f"[A] WS close error during shutdown: {close_error!r}", flush=True)

    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    websocket_client.connect()   # blocks here until close_connection() is called or the SDK gives up

    print("[A] shutting down (WebSocket connection closed)", flush=True)
    tick_publisher.close()
    zmq_context.term()


# ============================================================
# PROCESS B: ALPHA ENGINE
# 9 calculators + EMA smoother, tick से feature score बनाना
# ============================================================
@dataclass
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


# ---- 9 calculators। हर एक amortized O(1) per tick है। ----

def calculate_vwap_distance(_symbol_state, tick_data):
    """Calculator #1: last-traded-price की daily VWAP से percentage दूरी।
    चूंकि daily VWAP cumulative है, इसकी 60-second slope दोपहर तक लगभग
    zero हो जाती है -- इसलिए VWAP से दूरी ही meaningful mean-reversion
    signal है, slope नहीं।"""
    if not tick_data["daily_vwap"] or not tick_data["last_traded_price"]:
        return 0.0
    percent_distance = ((tick_data["last_traded_price"] - tick_data["daily_vwap"])
                         / tick_data["daily_vwap"] * 100)
    return clip_value(percent_distance * 5.0, -2.0, 2.0)   # 0.4% दूर -> +-2.0 (capped)


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
    tail unreachable prices पर।"""
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
    return 1.0   # missing quote data को "spread too wide" मानो (fail safe)


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
# Calculator #9 EMA smoother है, जो alpha_engine() के main loop के अंदर inline लगाया गया है।


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


# ============================================================
# PROCESS C: BROADCASTER + TELEGRAM BOT
# अलर्ट कब भेजने हैं, कैसे भेजने हैं, और Telegram बटन handling
# ============================================================
def determine_signal_modes(composite_score, relative_volume, gap_fade_raw_score=0.0):
    """Current score/volume/gap combination किस-किस alert mode के लिए
    qualify करता है, यह तय करता है:
       GAP_FADE       : |gap_fade_raw_score| >= 2.0
                         (सिर्फ़ 9:15-9:30 IST में nonzero, calculate_gap_fade
                          के gating से)
       MOMENTUM       : |composite_score| >= 8 और relative_volume >= 1.5
       MEAN_REVERSION : |composite_score| >= 3 और relative_volume < 2.0,
                        AND MOMENTUM पहले से match नहीं हुआ (elif के जरिए
                        MOMENTUM के साथ mutually exclusive, न कि किसी
                        upper score bound से -- नीचे inline note देखें
                        क्यों upper bound हटाया गया)

    हर मोड जो अलग से qualify करता है, उन सबकी LIST return करता है, NOT
    कोई single priority-ordered choice। इस function के एक पुराने version
    ने पहले GAP_FADE check किया था और match होते ही तुरंत return कर दिया
    था, जिससे एक modest gap (जैसे gap=2.1, बस अपने 2.0 threshold से ज़रा
    ऊपर) एक साथ हो रहे एक बहुत ज़्यादा strong MOMENTUM signal को पूरी
    तरह से निगल जाता (जैसे composite_score=9.5, relative_volume=5.0) --
    simulation से verified: वो exact combination सिर्फ़ "GAP_FADE" return
    करती थी, MOMENTUM signal को चुपचाप discard कर देती थी।

    GAP_FADE gap_fade_raw_score से evaluate होता है, जो composite score
    से independent value है (देखें calculate_gap_fade) -- इसलिए यह
    MOMENTUM/MEAN_REVERSION का "same underlying signal at different
    strengths" नहीं है, यह एक genuinely distinct measurement है जो
    उन दोनों में से किसी एक के साथ legitimately coexist कर सकता है।
    MOMENTUM और MEAN_REVERSION खुद construction से mutually exclusive
    हैं (उनकी |composite_score| ranges overlap नहीं करतीं: >=8 vs [3, 8)),
    तो एक ही समय पर दोनों में से ज़्यादा से ज़्यादा एक ही qualify कर
    सकता है -- सिर्फ़ GAP_FADE अलग से उनमें से किसी एक के साथ co-occur
    कर सकता है। Caller (broadcaster_loop) पहले से हर (symbol, mode) पर
    अलर्ट track करता है independent cooldowns और independent tracked-
    position keys के साथ, इसलिए यहाँ multiple modes return करने के लिए
    किसी नई infrastructure की ज़रूरत नहीं -- यह बस उस mode को discard
    करना बंद करता है जिसे बाकी हर जगह पहले से independently handle
    करने का system तैयार था।"""
    qualifying_modes = []
    if abs(gap_fade_raw_score) >= 2.0:
        qualifying_modes.append("GAP_FADE")
    absolute_score = abs(composite_score)
    if absolute_score >= 8 and relative_volume >= 1.5:
        qualifying_modes.append("MOMENTUM")
    # NOTE: यह पहले `elif 3 <= absolute_score < 8 and relative_volume
    # < 2.0` था, जिसने एक "dead zone" बनाया था: एक score जैसे 9.5
    # (MOMENTUM के अपने >=8 threshold से काफ़ी ऊपर) साथ में
    # relative_volume=1.2 (MOMENTUM के >=1.5 volume requirement से बस
    # थोड़ा नीचे) दोनों branches में fail होता -- MOMENTUM पर volume की
    # वजह से miss हो जाता, और MEAN_REVERSION पर भी miss होता क्योंकि
    # उसका "< 8" upper bound उतना ऊँचा score exclude कर देता। ऐसा
    # signal, जो एक असामान्य तौर पर strong price move दिखा रहा है,
    # चुपचाप पूरी तरह drop हो जाता -- ठीक वैसे low-volume/high-score
    # situations में (जैसे spoofing, illiquid circuit-adjacent move)
    # जहाँ यह system को blind नहीं होना चाहिए। Simulation से verified:
    # composite_score=9.5, relative_volume=1.2 पर पुरानी condition से
    # [] return होता था। "< 8" upper bound हटाने का मतलब है कि कोई भी
    # score जो MOMENTUM के volume gate पर fail हो, वो fall through होने
    # बजाय पूरी तरह से fall through कर जाने के, MEAN_REVERSION के तौर पर
    # evaluate होगा -- MOMENTUM और MEAN_REVERSION एक-दूसरे से mutually
    # exclusive बने रहते हैं (`elif` अभी भी MOMENTUM की अपनी condition
    # match न होने पर gated है), बस fallback path पर score<8 वाला extra
    # restriction नहीं है।
    elif absolute_score >= 3 and relative_volume < 2.0:
        qualifying_modes.append("MEAN_REVERSION")
    return qualifying_modes


async def is_entry_cooldown_expired(redis_client, symbol, mode):
    """(symbol, mode) के लिए एक cooldown key atomically set करता है, लेकिन
    सिर्फ़ तभी जब वह पहले से मौजूद न हो। True (cooldown "expired"/available)
    सिर्फ़ उस call पर return करता है जिसने key को successfully set किया --
    cooldown window के अंदर हर बाद वाला call False return करेगा।"""
    cooldown_key = f"alpha:cd:{symbol}:{mode}"
    was_newly_set = await redis_client.set(
        cooldown_key, "1", ex=COOLDOWN_SECONDS_BY_MODE[mode], nx=True)
    return bool(was_newly_set)


async def is_exit_cooldown_expired(redis_client, symbol, mode):
    """is_entry_cooldown_expired जैसा ही pattern, लेकिन exit alerts के
    लिए। (symbol, mode) से key बनती है -- tracked-position key जैसी ही --
    ताकि एक ही symbol पर different modes में concurrently tracked
    positions (जैसे RELIANCE GAP_FADE और RELIANCE MOMENTUM दोनों ACK'd)
    को independent exit-alert cooldowns मिलें, न कि एक ही share करें।"""
    cooldown_key = f"alpha:exitcd:{symbol}:{mode}"
    was_newly_set = await redis_client.set(
        cooldown_key, "1", ex=EXIT_ALERT_COOLDOWN_SECONDS, nx=True)
    return bool(was_newly_set)


def build_entry_alert_text(score_data, mode):
    """एक entry alert message के लिए human-readable text बनाता है।"""
    direction = determine_entry_direction(score_data, mode)

    stop_loss_text_by_mode = {
        "MOMENTUM": "1.5%", "MEAN_REVERSION": "0.5%", "GAP_FADE": "gap-based"}
    stop_loss_text = stop_loss_text_by_mode[mode]

    gap_extra_text = (f"  gap={score_data['gap']:+.2f}"
                      if mode == "GAP_FADE" and "gap" in score_data else "")

    return (f"🔔 [{mode}] {score_data['symbol']} {direction}\n"
            f"score={score_data['score']:+.2f}  "
            f"peak={score_data['peak']:.2f}  "
            f"ltp={score_data['last_traded_price']:.2f}{gap_extra_text}\n"
            f"suggested SL: {stop_loss_text}\n"
            f"advisory only — user executes manually")


def build_exit_alert_text(symbol, tracked_position, current_score, current_last_traded_price):
    """एक exit alert message के लिए human-readable text बनाता है, जो तब
    fire होता है जब किसी ACK'd position का score exit threshold की तरफ
    (या उसके पार) चला गया हो। current_score वो metric है जिसने वाकई इस
    position के mode के लिए exit decide किया (GAP_FADE के लिए gap score,
    बाकी हर mode के लिए composite score) -- देखें caller (send_exit_alert)
    क्यों यह matter करता है।"""
    # entry time पर saved direction को prefer करें (हर mode के लिए सही,
    # GAP_FADE भी शामिल)। Fall back सिर्फ़ उन positions के लिए, जो इस
    # field के add होने से पहले बनी थीं -- score_at_entry के लिए भी
    # .get() इस्तेमाल किया है ताकि एक malformed/partial Redis record
    # gracefully default पर degrade हो जाए, KeyError raise न हो।
    direction = tracked_position.get(
        "direction", "LONG" if tracked_position.get("score_at_entry", 0.0) > 0 else "SHORT")
    entry_price = tracked_position.get("ltp_entry", 0)
    profit_loss_percent = (
        (current_last_traded_price - entry_price) / entry_price * 100
        if entry_price else 0.0)
    if direction == "SHORT":
        profit_loss_percent = -profit_loss_percent

    # Redis-stored dict field पढ़ने पर हर जगह .get() default के साथ,
    # direct indexing नहीं -- एक tracked_position dict में कोई field
    # missing हो सकती है (जैसे यह इस code के किसी पुराने version से
    # लिखी गई हो जब वो field add नहीं हुई थी, या Redis data manually
    # edit हुआ हो), और direct tracked_position['field'] access
    # KeyError raise करके इस exit-alert path को crash कर देगा gracefully
    # degrade होने के बजाय।
    mode_text = tracked_position.get("mode", "UNKNOWN")
    score_at_entry = tracked_position.get("score_at_entry", 0.0)

    return (f"⚠️ EXIT — {symbol} ({mode_text} {direction})\n"
            f"score decayed to {current_score:+.2f} "
            f"(entry {score_at_entry:+.2f})\n"
            f"ltp {current_last_traded_price:.2f} "
            f"(entry {entry_price:.2f}, delta {profit_loss_percent:+.2f}%)\n"
            f"consider closing position")


# ---- Telegram bot state (module-level ताकि send helpers running app तक पहुँच सकें) ----
TELEGRAM_BOT_STATE = {"application": None, "entry_keyboard_builder": None, "exit_keyboard_builder": None}


async def initialize_telegram_bot(redis_client_async):
    """python-telegram-bot Application को set up करता है, button callback
    handler को register करता है, और एक startup ping भेजता है। अगर
    TELEGRAM_BOT_TOKEN या TELEGRAM_CHAT_ID missing हो, या library
    import/init fail हो, तो bot disabled रहेगा और सारे alerts stdout
    पर print होंगे।"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[C] TELEGRAM_TOKEN/CHAT_ID missing — bot disabled, alerts to stdout", flush=True)
        return

    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        from telegram.ext import Application, CallbackQueryHandler
    except ImportError as import_error:
        print(f"[C] python-telegram-bot import failed: {import_error}", flush=True)
        return

    async def handle_button_callback(update, context):
        callback_query = update.callback_query
        try:
            await callback_query.answer()
        except Exception:
            pass
        try:
            callback_parts = callback_query.data.split(":", 2)
            action = callback_parts[0]
            symbol = callback_parts[1]
            mode = callback_parts[2] if len(callback_parts) > 2 else ""
            redis_client = context.application.bot_data["redis"]

            # NOTE: edit_message_text() एक reply_markup parameter accept
            # करता है जो एक ही API call में message के inline keyboard
            # को replace कर देता है (या None पास करने पर हटा देता है)।
            # दो calls की जगह एक call करने से (एक पुराने version में
            # पहले edit_message_reply_markup और फिर edit_message_text
            # अलग-अलग call होते थे) हर button press पर Telegram API का
            # usage आधा हो जाता है, और दोनों calls के बीच की 'Message
            # is not modified' race भी टल जाती है।
            #
            # हम message.text_html पढ़ते हैं (HTML-escaped: '&','<','>'
            # '&amp;','&lt;','&gt;' बन जाते हैं) और नीचे हर edit_message_
            # text call पर parse_mode="HTML" pass करते हैं ताकि Telegram
            # वाकई इन entities को interpret करे, न कि उन्हें literal text
            # के तौर पर दिखाए। text_html इस्तेमाल करना बिना parse_mode=
            # "HTML" के (जैसा इस code के एक पुराने version में हुआ था)
            # यूज़र को raw HTML-escaped entities दिखा देगा, जैसे "S&P"
            # की जगह "S&amp;P", जिस moment कोई alert text इन characters
            # में से एक भी contain करे। आज के alert text में ऐसे characters
            # नहीं हैं, पर यह escaping को matching parse_mode के साथ pair
            # कर देता है ताकि आगे कुछ बदले तो भी सही रहे, बजाय एक latent
            # bug छोड़ने के जो अगले किसी को झेलना पड़े जो build_entry_
            # alert_text edit करे।
            original_text_html = callback_query.message.text_html

            if action == "ACK":
                pending_info = await redis_client.get(
                    f"alpha:pending:{callback_query.message.message_id}")
                if pending_info:
                    # NOTE: tracked-position key में MODE भी शामिल है,
                    # सिर्फ़ symbol नहीं (alpha:pos:{symbol}:{mode}, न कि
                    # alpha:pos:{symbol})। एक symbol-only key का मतलब
                    # होगा कि एक ही symbol पर DIFFERENT mode में दूसरा
                    # signal ACK करने पर (जैसे RELIANCE GAP_FADE ACK
                    # किया, फिर बाद में RELIANCE MOMENTUM ACK किया) पहली
                    # position चुपचाप overwrite हो जाएगी -- उसका exit
                    # tracking हमेशा के लिए खो जाएगा, क्योंकि पुरानी key
                    # को और कुछ reference नहीं करता। Mode include करने
                    # से दोनों positions independently track हो पाती हैं।
                    await redis_client.setex(
                        f"alpha:pos:{symbol}:{mode}", TRACKED_POSITION_TTL_SECONDS, pending_info)
                    await redis_client.delete(
                        f"alpha:pending:{callback_query.message.message_id}")
                    await callback_query.edit_message_text(
                        text=f"{original_text_html}\n\n✅ ACK'd — tracking for exit",
                        parse_mode="HTML", reply_markup=None)
                else:
                    await callback_query.edit_message_text(
                        text=f"{original_text_html}\n\n"
                             f"✅ ACK'd (details expired after 24h)",
                        parse_mode="HTML", reply_markup=None)

            elif action == "SKIP":
                skip_log_key = f"alpha:skip:{time.strftime('%Y%m%d')}"
                await redis_client.lpush(skip_log_key, json.dumps(
                    {"symbol": symbol, "mode": mode, "timestamp": time.time()}))
                await redis_client.expire(skip_log_key, 30 * 24 * 3600)
                await callback_query.edit_message_text(
                    text=f"{original_text_html}\n\n⏭️ SKIPPED (logged)",
                    parse_mode="HTML", reply_markup=None)

            elif action == "DONE":
                await redis_client.delete(f"alpha:pos:{symbol}:{mode}")
                # DONE पर इस (symbol, mode) के लिए entry cooldown को फिर
                # से re-arm कर दो। इसके बिना, अगर entry के बाद इतना असली
                # समय बीत चुका है कि original entry cooldown naturally
                # expire हो चुका है (जैसे user ने MOMENTUM position घंटे
                # भर hold की, पर उसका cooldown सिर्फ़ 300s था), तो tracked
                # position को delete करने से re-entry नहीं रुकेगी: अगर
                # score अभी भी trigger band में है, तो बिल्कुल अगले tick
                # पर उसी position के लिए एक नया entry alert तुरंत fire
                # हो जाएगा जिसे user ने अभी बंद किया। एक fresh cooldown
                # set करने से user को manually trade बंद करने के बाद
                # एक deliberate cool-down period मिल जाता है, तब तक उस
                # (symbol, mode) signal से दोबारा alert नहीं आएगा।
                await redis_client.set(
                    f"alpha:cd:{symbol}:{mode}", "1",
                    ex=COOLDOWN_SECONDS_BY_MODE.get(mode, EXIT_ALERT_COOLDOWN_SECONDS))
                await callback_query.edit_message_text(
                    text=f"{original_text_html}\n\n✔️ DONE — position closed",
                    parse_mode="HTML", reply_markup=None)

            elif action == "HOLD":
                await redis_client.setex(
                    f"alpha:exitcd:{symbol}:{mode}", EXIT_ALERT_COOLDOWN_SECONDS * 2, "1")
                await callback_query.edit_message_text(
                    text=f"{original_text_html}\n\n⏳ HOLDING — exit cooldown extended",
                    parse_mode="HTML", reply_markup=None)
        except Exception as button_error:
            print(f"[C] button handler err: {button_error!r}", flush=True)

    try:
        telegram_application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        telegram_application.add_handler(CallbackQueryHandler(handle_button_callback))
        telegram_application.bot_data["redis"] = redis_client_async
        await telegram_application.initialize()
        await telegram_application.start()
        await telegram_application.updater.start_polling(drop_pending_updates=True)

        TELEGRAM_BOT_STATE["application"] = telegram_application
        TELEGRAM_BOT_STATE["entry_keyboard_builder"] = lambda symbol, mode: InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ ACK",  callback_data=f"ACK:{symbol}:{mode}"),
            InlineKeyboardButton("⏭️ SKIP", callback_data=f"SKIP:{symbol}:{mode}"),
        ]])
        TELEGRAM_BOT_STATE["exit_keyboard_builder"] = lambda symbol, mode: InlineKeyboardMarkup([[
            InlineKeyboardButton("✔️ DONE",      callback_data=f"DONE:{symbol}:{mode}"),
            InlineKeyboardButton("⏳ HOLD MORE", callback_data=f"HOLD:{symbol}:{mode}"),
        ]])

        try:
            await telegram_application.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID, text="🟢 Alpha-Decay Lite bot online")
        except Exception as ping_error:
            print(f"[C] startup ping failed: {ping_error!r}", flush=True)

        print(f"[C] Telegram bot up (chat={TELEGRAM_CHAT_ID})", flush=True)
    except Exception as init_error:
        print(f"[C] Telegram bot init failed: {init_error!r} — alerts to stdout only", flush=True)


async def shutdown_telegram_bot():
    """Telegram bot के polling updater को gracefully रोकता है और
    Application को shut down करता है, अगर एक successfully initialized
    हुआ था।"""
    telegram_application = TELEGRAM_BOT_STATE["application"]
    if not telegram_application:
        return
    try:
        await telegram_application.updater.stop()
        await telegram_application.stop()
        await telegram_application.shutdown()
    except Exception as shutdown_error:
        print(f"[C] TG shutdown err: {shutdown_error!r}", flush=True)


def determine_entry_direction(score_data, mode):
    """किसी entry की LONG/SHORT direction के लिए single source of truth।
    GAP_FADE के लिए, meaningful direction gap के अपने sign से आती है
    (composite score के sign से नहीं, जो अलग हो सकता है)। बाकी हर mode
    के लिए, composite score का sign ही direction है। यह दोनों जगह
    इस्तेमाल होता है: entry alert text बनाते समय AND pending position
    को persist करते समय, ताकि बाद में exit alert same direction को
    reliably वापस पढ़ सके, न कि score_at_entry से re-derive करे (जो
    GAP_FADE के लिए गलत होगा, क्योंकि gap-fade का score और उसकी trade
    direction हमेशा एक ही sign की नहीं होतीं)।"""
    if mode == "GAP_FADE":
        return "LONG" if score_data.get("gap", 0.0) > 0 else "SHORT"
    return "LONG" if score_data["score"] > 0 else "SHORT"


async def send_entry_alert(redis_client_async, score_data, mode):
    """एक entry alert भेजता है (या bot disabled हो तो print करता है)
    ACK/SKIP buttons के साथ, और pending alert की details को Redis में
    persist कर देता है ताकि ACK handler बाद में उन्हें लुक-अप कर सके --
    restart होने के बाद भी।"""
    alert_text = build_entry_alert_text(score_data, mode)
    telegram_application = TELEGRAM_BOT_STATE["application"]

    if telegram_application is None:
        print(f"[TG STUB ENTRY]\n{alert_text}\n---", flush=True)
        return

    try:
        sent_message = await telegram_application.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=alert_text,
            reply_markup=TELEGRAM_BOT_STATE["entry_keyboard_builder"](score_data["symbol"], mode))

        pending_position_data = {
            "symbol": score_data["symbol"],
            "mode": mode,
            "direction": determine_entry_direction(score_data, mode),
            "score_at_entry": score_data["score"],
            "peak": score_data["peak"],
            "ltp_entry": score_data["last_traded_price"],
            "entry_timestamp": time.time(),
        }
        await redis_client_async.setex(
            f"alpha:pending:{sent_message.message_id}",
            PENDING_ALERT_TTL_SECONDS, json.dumps(pending_position_data))
    except Exception as send_error:
        print(f"[C] TG entry send failed: {send_error!r}", flush=True)


async def send_exit_alert(score_data, tracked_position, displayed_score):
    """एक exit alert भेजता है (या bot disabled हो तो print करता है)
    DONE/HOLD MORE buttons के साथ, किसी पहले ACK'd position के लिए।
    displayed_score वो metric है (composite score या gap score) जिसने
    वाकई exit decide किया -- देखें broadcaster_loop में caller -- ताकि
    message text real trigger से match करे, न कि हमेशा composite score
    even for a GAP_FADE exit that was decided from the gap score."""
    alert_text = build_exit_alert_text(
        score_data["symbol"], tracked_position, displayed_score,
        score_data["last_traded_price"])
    telegram_application = TELEGRAM_BOT_STATE["application"]

    if telegram_application is None:
        print(f"[TG STUB EXIT]\n{alert_text}\n---", flush=True)
        return

    try:
        await telegram_application.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=alert_text,
            reply_markup=TELEGRAM_BOT_STATE["exit_keyboard_builder"](
                score_data["symbol"], tracked_position.get("mode", "")))
    except Exception as send_error:
        print(f"[C] TG exit send failed: {send_error!r}", flush=True)


async def check_suppression_status(redis_client_async):
    """सभी suppression conditions को priority order में check करता है और
    (is_suppressed, reason_string) return करता है। Caller की ज़िम्मेदारी
    है कि इस result को कुछ समय के लिए cache करे ताकि हर आने वाले score
    message पर Redis/disk hit न हो।

    disk check (os.path.exists) asyncio.to_thread के जरिए चलता है, सीधे
    call नहीं होता, क्योंकि यह एक synchronous/blocking system call है।
    एक typical /tmp tmpfs पर यह microseconds में पूरा होता है, तो
    practical impact negligible है, पर asyncio coroutine के अंदर blocking
    I/O सीधे call करना अच्छी practice नहीं है, इसलिए यहाँ avoid किया है।"""
    try:
        # NOTE: Python में bool(some_string) हर non-empty string के लिए
        # True होता है, "0" और "false" भी शामिल -- Redis हमेशा strings
        # return करता है (या None अगर key absent हो), कभी Python booleans
        # नहीं। bool(await redis_client_async.get("alpha:mute")) इसलिए
        # एक ऐसी key जो explicit रूप से "0" पर set हो ("not muted" का
        # मतलब है) को muted के तौर पर treat करेगा, जो इस kill switch के
        # लिए documented fail-safe OR semantics का उल्टा है। specific
        # string "1" (जो value यह codebase mute करते समय हमेशा लिखता है)
        # से compare करने से यह trap टल जाता है; एक key जो absent (None)
        # हो या "1" के अलावा कुछ भी set हो, not-muted मानी जाती है।
        mute_value = await redis_client_async.get("alpha:mute")
        if mute_value == "1":
            return True, "muted"
        if await asyncio.to_thread(os.path.exists, MUTE_FILE_PATH):
            return True, "muted"
        if not await redis_client_async.get("alpha:hb:B"):
            return True, "engine_dead"
        if not await redis_client_async.get("alpha:hb:data"):
            return True, "no_data"
    except Exception:
        # Redis खुद unreachable है। पहले यहाँ यह होता था:
        #   mute_file_exists = await asyncio.to_thread(os.path.exists, MUTE_FILE_PATH)
        #   return mute_file_exists, "muted"
        # जो एक safety-critical function के लिए fail-OPEN bug है: अगर
        # Redis down है AND disk mute file भी exist नहीं करती, तो यह
        # (False, "muted") return करता -- मतलब "NOT suppressed" -- ठीक
        # उसी moment जब ऊपर की heartbeat checks (kill switches #1 और #2:
        # Process B alive है, data flow हो रहा है) में से कोई भी चल ही
        # नहीं सकती, क्योंकि वे सब उसी unreachable Redis पर depend करती
        # हैं। Trace से verified: एक Redis outage जिसमें disk flag भी
        # न हो, चुपचाप alerts fire होने देता था बिना किसी live safety
        # check के -- यह ठीक उलटा है जो एक kill switch के लिए चाहिए।
        # इसकी जगह fail SAFE: Redis outage को खुद एक suppression
        # condition मानो (unconditionally True return करो), और disk
        # check का इस्तेमाल सिर्फ़ ज़्यादा specific reason report करने
        # के लिए करो अगर वो actually set हो।
        mute_file_exists = await asyncio.to_thread(os.path.exists, MUTE_FILE_PATH)
        return True, ("muted" if mute_file_exists else "redis_down")
    return False, ""


SCORE_RECEIVE_TIMEOUT_SECONDS = 1.0   # broadcaster loop बिना data के भी इतनी देर में wake हो जाए


async def broadcaster_loop():
    """Process C का main loop। Process B के publish हुए scores को consume
    करता है, suppression check करता है (mute/heartbeats), tracked
    positions के लिए exit alerts fire करता है जिनका score decay हो गया,
    नए qualifying signals के लिए entry alerts fire करता है (per-signal
    cooldowns respect करते हुए), और observability के लिए समय-समय पर
    counters का summary print करता है।"""
    zmq_context = zmq.asyncio.Context()
    score_subscriber = zmq_context.socket(zmq.SUB)
    score_subscriber.setsockopt(zmq.RCVHWM, ZMQ_HIGH_WATER_MARK)
    score_subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    score_subscriber.connect(ZMQ_SCORES_ADDRESS)

    # multiprocessing.Process.terminate() SIGTERM भेजता है। एक asyncio
    # event loop के अंदर उसका सबसे साफ़ जवाब है एक asyncio-native signal
    # handler जो एक stop Event set करे, ताकि नीचे का main while-loop
    # उसे check करके `finally` block से exit हो (जो Telegram bot को साफ़
    # तरीक़े से बंद करता है), न कि process को mid-flight kill कर दिया जाए
    # जब Telegram/Redis I/O pending हो।
    stop_requested = asyncio.Event()
    running_loop = asyncio.get_running_loop()
    try:
        for signal_number in (signal.SIGTERM, signal.SIGINT):
            running_loop.add_signal_handler(signal_number, stop_requested.set)
    except NotImplementedError:
        # add_signal_handler कुछ platforms पर उपलब्ध नहीं है (जैसे Windows);
        # process अभी भी launcher के force-kill fallback से आने वाले
        # SIGKILL पर react करेगा, बस clean shutdown path नहीं मिलेगा।
        pass

    redis_client_async = redis_asyncio.from_url(
        REDIS_URL, decode_responses=True,
        health_check_interval=30, socket_keepalive=True)

    # Process C का अपना liveness heartbeat (Process A और B जैसा symmetric)।
    heartbeat_redis_client_sync = redis.from_url(REDIS_URL, decode_responses=True)
    start_heartbeat_thread("C", heartbeat_redis_client_sync)

    await initialize_telegram_bot(redis_client_async)

    stats_counters = {
        "scores_received": 0, "no_qualifying_mode": 0, "muted": 0,
        "no_data": 0, "engine_dead": 0, "cooldown_blocked": 0,
        "entry_alerts_sent": 0, "exit_alerts_sent": 0, "redis_errors": 0,
    }
    last_stats_print_time = time.time()
    cached_suppression_state = {"is_suppressed": False, "reason": "", "checked_at": 0.0}
    print("[C] Broadcaster up", flush=True)

    try:
        while not stop_requested.is_set():
            try:
                # एक सादा, unconditional recv_multipart() हमेशा के लिए
                # block हो जाता है अगर Process B publish करना बंद कर दे
                # (market closed, engine down, आदि)। जब तक blocked रहेगा,
                # यह loop नीचे के periodic stats-print check तक कभी नहीं
                # पहुँचेगा, तो console silent हो जाएगा बिना "no_data"
                # suppression के किसी visible indication के -- यह देखने
                # में crash से अलग नहीं होगा। recv को asyncio.wait_for()
                # में wrap करने का मतलब है कि loop हर SCORE_RECEIVE_TIMEOUT_
                # SECONDS पर कम से कम एक बार wake हो जाता है, incoming
                # messages ज़ीरो होने पर भी, तो stats print होते रहते
                # हैं और stop signal तुरंत check होता है।
                try:
                    symbol_bytes, message_payload = await asyncio.wait_for(
                        score_subscriber.recv_multipart(),
                        timeout=SCORE_RECEIVE_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    current_time = time.time()
                    if current_time - last_stats_print_time >= 15:
                        print(f"[C] {stats_counters} (no scores received in last "
                              f"{current_time - last_stats_print_time:.0f}s)", flush=True)
                        stats_counters = dict.fromkeys(stats_counters, 0)
                        last_stats_print_time = current_time
                    continue

                score_data = json.loads(message_payload)
                stats_counters["scores_received"] += 1

                # cached suppression check को ज़्यादा से ज़्यादा एक second में एक बार refresh करो।
                current_wall_time = time.time()
                if current_wall_time - cached_suppression_state["checked_at"] > MUTE_STATUS_CACHE_SECONDS:
                    is_suppressed, suppression_reason = await check_suppression_status(redis_client_async)
                    cached_suppression_state["is_suppressed"] = is_suppressed
                    cached_suppression_state["reason"] = suppression_reason
                    cached_suppression_state["checked_at"] = current_wall_time

                if cached_suppression_state["is_suppressed"]:
                    reason = cached_suppression_state["reason"]
                    stats_counters[reason] = stats_counters.get(reason, 0) + 1
                else:
                    # 1) पहले ACK'd positions के लिए exit alerts, जिनका
                    # score या तो zero की तरफ decay हो गया OR entry
                    # direction के विरुद्ध जोरदार reverse हो गया। सिर्फ़
                    # abs(current_score) <= threshold check करना पर्याप्त
                    # नहीं है: अगर एक LONG entry का score बहुत sharply
                    # negative हो जाए (जैसे entry पर +8.5 -> trend
                    # reversal पर -6.0), abs(-6.0) = 6.0 <= 2.0 नहीं है,
                    # तो plain-abs check कभी exit alert नहीं भेजेगा जबकि
                    # position अब user के खिलाफ़ जोरदार move कर रही है।
                    # नीचे का check तब fire होता है जब score LONG के लिए
                    # exit threshold से गिर जाए, या SHORT के लिए ऊपर
                    # जाए -- यह दोनों cases cover करता है: "decayed to
                    # flat" और "reversed against us"।
                    #
                    # Positions per (symbol, mode) track होती हैं, सिर्फ़
                    # per symbol नहीं, इसलिए एक ही symbol पर different
                    # modes में independent ACK'd positions एक साथ हो
                    # सकती हैं (जैसे RELIANCE GAP_FADE और RELIANCE
                    # MOMENTUM दोनों active)। इस symbol के लिए हर mode
                    # की tracked-position key check करें।
                    #
                    # modes_with_active_position नीचे भी दोबारा इस्तेमाल
                    # होता है, entry-alert section (part 2) में, ताकि
                    # किसी mode के लिए brand-new entry alert generate करना
                    # SKIP कर दिया जाए अगर उसकी पहले से एक position इस
                    # symbol पर track हो रही है। इसके बिना, एक position
                    # जो अपने (बहुत छोटे) entry cooldown से ज़्यादा देर तक
                    # hold की गई हो -- जैसे एक MOMENTUM position ACK
                    # हुई और 20 मिनट तक hold की गई जबकि उसकी 300s वाली
                    # entry cooldown key naturally expire हो चुकी हो --
                    # अगले qualifying tick पर एक बिल्कुल fresh, duplicate
                    # entry alert मिलेगा, जबकि user अभी भी original
                    # position hold कर रहा है।
                    modes_with_active_position = set()
                    for candidate_mode in COOLDOWN_SECONDS_BY_MODE:
                        try:
                            tracked_position_json = await redis_client_async.get(
                                f"alpha:pos:{score_data['symbol']}:{candidate_mode}")
                        except Exception:
                            tracked_position_json = None
                            stats_counters["redis_errors"] += 1

                        if not tracked_position_json:
                            continue

                        modes_with_active_position.add(candidate_mode)
                        tracked_position = json.loads(tracked_position_json)
                        position_direction = tracked_position.get(
                            "direction",
                            "LONG" if tracked_position.get("score_at_entry", 0.0) > 0 else "SHORT")
                        position_mode = tracked_position.get("mode", candidate_mode)

                        # GAP_FADE की direction GAP के sign से derive होती
                        # है, composite score के sign से नहीं (देखें
                        # determine_entry_direction) -- दोनों legitimately
                        # disagree कर सकती हैं (जैसे एक bullish gap-fade
                        # entry, बाकी 8 calculators से आने वाले bearish
                        # composite score के साथ coexist कर सकती है)। तो
                        # GAP_FADE के लिए हमें exit condition को
                        # gap_fade_raw_score के मुक़ाबले evaluate करना है
                        # (वही metric जिसने entry trigger किया), NOT
                        # composite score। यहाँ composite score इस्तेमाल
                        # करना एक असली bug था: entry के बिल्कुल अगले
                        # tick पर एक exit alert fire हो सकता था, क्योंकि
                        # composite score पहले से GAP_FADE entry के
                        # trigger range के अंदर था ही नहीं।
                        if position_mode == "GAP_FADE":
                            # यहाँ gap_unrestricted इस्तेमाल करो, NOT gap।
                            # "gap" entry-only time-gated metric है जिसे
                            # calculate_gap_fade unconditionally zero कर
                            # देता है 9:15-9:30 window end होते ही --
                            # यहाँ उसे पढ़ना हर खुली GAP_FADE position
                            # को ठीक 9:30:00 पर force-exit कर देगा, चाहे
                            # underlying gap वाकई close हुआ हो या नहीं
                            # (simulation से verified: एक असली 2.5% gap
                            # पर entry, price बिल्कुल unchanged, फिर भी
                            # जैसे ही clock 9:30:00 पर पहुँचा, "gap"
                            # इस्तेमाल करने पर should_exit=True हो जाता
                            # था)। window gate का मतलब है कि क्या NEW
                            # entry fire होनी चाहिए; इसका मतलब यह नहीं
                            # है कि क्या EXISTING position का underlying
                            # gap वाकई close हो गया, जो एक real-world
                            # price fact है, clock fact नहीं।
                            current_exit_metric = score_data.get("gap_unrestricted", 0.0)
                            if position_direction == "LONG":
                                should_exit = current_exit_metric <= EXIT_ALERT_SCORE_THRESHOLD
                            else:
                                should_exit = current_exit_metric >= -EXIT_ALERT_SCORE_THRESHOLD
                        else:
                            current_exit_metric = score_data["score"]
                            if position_direction == "LONG":
                                should_exit = current_exit_metric <= EXIT_ALERT_SCORE_THRESHOLD
                            else:
                                should_exit = current_exit_metric >= -EXIT_ALERT_SCORE_THRESHOLD

                        if should_exit:
                            try:
                                if await is_exit_cooldown_expired(
                                        redis_client_async, score_data["symbol"], position_mode):
                                    # जो metric वाकई इस exit को trigger
                                    # किया (GAP_FADE के लिए gap score,
                                    # बाकी के लिए composite score) उसे
                                    # pass करो, ताकि Telegram message वही
                                    # number report करे जिस पर decision
                                    # हुआ, न कि हमेशा composite score
                                    # दिखाए जब GAP_FADE exit असल में gap
                                    # score से decide हुआ हो।
                                    await send_exit_alert(
                                        score_data, tracked_position, current_exit_metric)
                                    stats_counters["exit_alerts_sent"] += 1
                            except Exception as exit_error:
                                stats_counters["redis_errors"] += 1
                                print(f"[C] exit err: {exit_error!r}", flush=True)

                    # 2) नए qualifying signals के लिए entry alerts।
                    # कोई global rate limiting नहीं है -- यह एक advisory
                    # system है, user तय करता है कि किस alert पर action
                    # लेना है। सिर्फ़ per-(symbol, mode) cooldown लगता है,
                    # ताकि एक ही signal बार-बार duplicate न भेजा जाए।
                    #
                    # determine_signal_modes() एक LIST return करता है, न
                    # कि single priority-ordered mode: GAP_FADE MOMENTUM/
                    # MEAN_REVERSION से genuinely distinct measurement है
                    # (gap_fade_raw_score से compute होता है, composite
                    # score से independent) और उनमें से किसी के साथ
                    # legitimately co-occur कर सकता है। एक पुराने version
                    # ने पहले match पर fixed GAP_FADE > MOMENTUM >
                    # MEAN_REVERSION order में सिर्फ़ पहला return किया था,
                    # जिसका मतलब है कि एक modest gap (जैसे 2.1, बस 2.0
                    # threshold से थोड़ा ऊपर) एक साथ हो रहे बहुत strong
                    # MOMENTUM signal को चुपचाप discard कर देता था --
                    # simulation से verified (score=9.5, rvol=5.0, gap=2.1
                    # पर सिर्फ़ "GAP_FADE" return हुआ)। यहाँ हर qualifying
                    # mode पर iterate करके, हर एक की cooldown/tracked-
                    # position state independently evaluate करना इसे बिना
                    # कुछ discard किए fix कर देता है, क्योंकि बाकी इस
                    # codebase में (cooldown keys, tracked-position keys,
                    # ऊपर वाला exit-alert loop) पहले से सब कुछ (symbol,
                    # mode) से key करता है।
                    qualifying_modes = determine_signal_modes(
                        score_data["score"], score_data["relative_volume"],
                        score_data.get("gap", 0.0))
                    if not qualifying_modes:
                        stats_counters["no_qualifying_mode"] += 1
                    for signal_mode in qualifying_modes:
                        # अगर इस (symbol, mode) की पहले से actively tracked
                        # position है, तो entry generation पूरी तरह skip
                        # कर दो -- ऊपर modes_with_active_position देखें।
                        # अकेला entry cooldown इसके लिए काफ़ी नहीं है: वो
                        # पूरी तरह time-based है (जैसे MOMENTUM के लिए
                        # 300s) और उसे यह पता नहीं होता कि position अभी
                        # open है या नहीं, तो एक position जो अपने cooldown
                        # window से ज़्यादा देर hold की गई हो, अगले
                        # qualifying tick पर एक duplicate, unsolicited
                        # fresh entry alert पा जाती, भले ही user अभी भी
                        # original hold कर रहा हो।
                        if signal_mode in modes_with_active_position:
                            stats_counters["position_already_tracked"] = (
                                stats_counters.get("position_already_tracked", 0) + 1)
                            continue
                        try:
                            if not await is_entry_cooldown_expired(
                                    redis_client_async, score_data["symbol"], signal_mode):
                                stats_counters["cooldown_blocked"] += 1
                            else:
                                stats_counters["entry_alerts_sent"] += 1
                                await send_entry_alert(
                                    redis_client_async, score_data, signal_mode)
                        except Exception as entry_error:
                            stats_counters["redis_errors"] += 1
                            print(f"[C] entry err: {entry_error!r}", flush=True)

                current_time = time.time()
                if current_time - last_stats_print_time >= 15:
                    suppression_tag = (f" suppressed={cached_suppression_state['reason']}"
                                       if cached_suppression_state["is_suppressed"] else "")
                    print(f"[C] {stats_counters}{suppression_tag}", flush=True)
                    stats_counters = dict.fromkeys(stats_counters, 0)
                    last_stats_print_time = current_time
            except asyncio.CancelledError:
                raise
            except Exception as loop_error:
                print(f"[C] loop err: {loop_error!r}", flush=True)
                await asyncio.sleep(0.1)   # tight error loop से बचाव
    finally:
        await shutdown_telegram_bot()
        # async Redis connection pool को explicitly close करो और ZMQ
        # context को terminate करो। इसके बिना, redis.asyncio अपने
        # connection pool की underlying sockets को open छोड़ देता है जब
        # तक Python का garbage collector client को finalize न कर दे (जो
        # एक ऐसे process के लिए जो इस point के बाद तुरंत exit हो रहा है,
        # कभी cleanly नहीं हो सकता), और stderr पर "Unclosed client
        # session" / "Unclosed connector" warning आएगी।
        try:
            await redis_client_async.aclose()
        except Exception as redis_close_error:
            print(f"[C] redis close err: {redis_close_error!r}", flush=True)

        # CRITICAL ordering requirement: zmq_context.term() तब तक block
        # करता है (हमेशा के लिए hang) जब तक उस context से बनी हर socket
        # close नहीं हो जाती। score_subscriber को इस term() call से पहले
        # कभी explicitly close नहीं किया गया था (यह एक पिछले commit में
        # जोड़ा गया था) -- यह socket के eventually garbage-collect होने
        # पर निर्भर था, जिसका term() सारे pyzmq versions में reliably
        # wait नहीं करता। पहले एक explicit close() के बिना, term()
        # indefinitely block हो सकता है, और launcher सिर्फ़ अपने 5-second
        # SIGKILL fallback से recover होगा -- मतलब यह process कभी
        # gracefully shutdown नहीं होगा, term() add करने का पूरा उद्देश्य
        # हरा जाएगा। इसलिए पहले socket को close करो, फिर context को
        # terminate करो।
        #
        # close() से पहले LINGER=0: default LINGER=-1 (infinite) का मतलब
        # है कि close() खुद block हो सकता है किसी buffered-but-unsent
        # messages को flush करने के इंतज़ार में, जो -- term() के हर socket
        # close होने तक block करने के साथ मिलकर -- ठीक उसी तरह का
        # shutdown hang दोबारा introduce कर देगा जिसे यह close-before-
        # term ordering रोकने के लिए लिखी गई थी। score_subscriber एक SUB
        # socket है (सिर्फ़ receive करता है, zmq.SUBSCRIBE के जरिए), तो
        # इसका खुद का outbound buffer normally सिर्फ़ subscription-filter
        # handshake तक limited होता है, पर यहाँ LINGER=0 set करने से
        # किसी भी pyzmq/libzmq version internals में hang होने की
        # possibility खत्म हो जाती है, zero behavioral cost पर, क्योंकि
        # shutdown में यह socket वैसे भी discard हो रही है।
        try:
            score_subscriber.setsockopt(zmq.LINGER, 0)
        except Exception:
            pass
        try:
            score_subscriber.close()
        except Exception as socket_close_error:
            print(f"[C] score_subscriber close err: {socket_close_error!r}", flush=True)
        try:
            zmq_context.term()
        except Exception as zmq_close_error:
            print(f"[C] zmq context term err: {zmq_close_error!r}", flush=True)


def run_broadcaster_process():
    """एक entry point wrapper ताकि Process C async broadcaster_loop को
    multiprocessing.Process के अंदर चला सके (जो एक plain sync callable
    की उम्मीद करता है)।"""
    asyncio.run(broadcaster_loop())


# ============================================================
# LAUNCHER: self-healing (auto-respawn) + graceful shutdown
# तीनों processes को शुरू/बंद/restart करने वाला हिस्सा
# ============================================================
# ipc:// URIs से raw filesystem paths derive करो ताकि नीचे का cleanup
# logic उन्हें दोबारा hardcode न करे।
IPC_SOCKET_FILE_PATHS = [
    address[len("ipc://"):]
    for address in (ZMQ_TICKS_ADDRESS, ZMQ_SCORES_ADDRESS)
    if address.startswith("ipc://")
]
MAX_RESTARTS_ALLOWED_PER_WINDOW = 6      # crash-loop breaker
RESTART_COUNTING_WINDOW_SECONDS = 3600


def cleanup_ipc_socket_files():
    """Stale ZMQ ipc:// socket files को हटाता है। यह ज़रूरी है क्योंकि
    UNIX domain sockets असल में disk पर files होती हैं जिन्हें kernel
    तब automatically remove नहीं करता जब कोई process बिना उन्हें
    cleanly close किए मर जाए (जैसे SIGKILL से) -- ऐसे में उसी path पर
    अगली bind() attempt "Address already in use" error देगी।"""
    for socket_path in IPC_SOCKET_FILE_PATHS:
        try:
            if os.path.exists(socket_path):
                os.remove(socket_path)
                print(f"[launcher] removed stale ipc socket: {socket_path}", flush=True)
        except Exception as cleanup_error:
            print(f"[launcher] ipc cleanup warn ({socket_path}): {cleanup_error!r}", flush=True)


def spawn_all_processes():
    """A, B, और C के लिए fresh Process objects बनाकर start करता है, और
    process handles की list return करता है।"""
    process_list = [
        Process(target=data_factory,           name="A-DataFactory"),
        Process(target=alpha_engine,           name="B-AlphaEngine"),
        Process(target=run_broadcaster_process, name="C-Broadcaster"),
    ]
    for process in process_list:
        process.start()
    return process_list


def shutdown_all_processes(process_list, shutdown_reason):
    """process_list में हर process को gracefully terminate करता है:
    SIGTERM भेजता है, सारे processes के exit होने के लिए 5 seconds तक
    wait करता है, फिर बाक़ी बचे stragglers को SIGKILL से force-kill
    करता है। अंत में कोई भी leftover ZMQ ipc socket files साफ़ करता है।"""
    print(f"\n[launcher] shutdown: {shutdown_reason}", flush=True)
    for process in process_list:
        if process.is_alive():
            process.terminate()

    shutdown_deadline = time.time() + 5
    for process in process_list:
        remaining_seconds = max(0.1, shutdown_deadline - time.time())
        process.join(timeout=remaining_seconds)

    for process in process_list:
        if process.is_alive():
            print(f"[launcher] force-kill {process.name}", flush=True)
            process.kill()
            process.join(timeout=2)

    cleanup_ipc_socket_files()


if __name__ == "__main__":
    cleanup_ipc_socket_files()   # defensive: पिछले ungraceful exit से बची sockets clear करो
    running_processes = spawn_all_processes()

    stop_event = threading.Event()
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signal_number, lambda *_args: stop_event.set())

    recent_restart_timestamps = deque()   # crash-loop breaker: recent restarts के timestamps

    try:
        while not stop_event.is_set():
            dead_processes = [process for process in running_processes if not process.is_alive()]

            if dead_processes:
                exit_codes_by_process_name = {
                    process.name: process.exitcode for process in dead_processes}

                # EXIT_CODE_FATAL_CONFIG का मतलब है कि एक process एक
                # non-retryable error से hit हुआ (missing/invalid .env
                # credentials, bad TOTP secret, Angel login rejected,
                # आदि)। Respawn करने से बस वही failure repeat होगा जब तक
                # crash-loop breaker trip नहीं हो जाता, ~2 मिनट बर्बाद
                # होंगे और log identical error messages से भर जाएगा एक
                # ऐसी problem के लिए जो सिर्फ़ इंसान के .env edit करने
                # से ठीक होगी। इसलिए तुरंत रुक जाओ।
                has_fatal_config_error = any(
                    code == EXIT_CODE_FATAL_CONFIG for code in exit_codes_by_process_name.values())
                if has_fatal_config_error:
                    shutdown_all_processes(
                        running_processes,
                        f"fatal config error, not retrying: {exit_codes_by_process_name}")
                    print("[launcher] FATAL: a process exited due to invalid configuration "
                          "or credentials (see the error above). Fix .env and restart manually "
                          "-- not retrying automatically.", flush=True)
                    sys.exit(1)

                # Exit code 2 हमारा अपना "planned restart" signal है (जैसे
                # data_factory का scheduled JWT-refresh self-exit)।
                is_planned_restart = any(
                    code == EXIT_CODE_PLANNED_RESTART for code in exit_codes_by_process_name.values())
                shutdown_reason = ("planned restart (JWT refresh)" if is_planned_restart
                                  else f"process died: {exit_codes_by_process_name}")
                shutdown_all_processes(running_processes, shutdown_reason)

                current_time = time.time()
                recent_restart_timestamps.append(current_time)
                while (recent_restart_timestamps
                       and current_time - recent_restart_timestamps[0] > RESTART_COUNTING_WINDOW_SECONDS):
                    recent_restart_timestamps.popleft()

                if len(recent_restart_timestamps) > MAX_RESTARTS_ALLOWED_PER_WINDOW:
                    print(f"[launcher] FATAL: {len(recent_restart_timestamps)} restarts in the "
                          f"last {RESTART_COUNTING_WINDOW_SECONDS / 3600:.0f}h -- crash loop "
                          f"suspected (check credentials/.env/network). "
                          f"Exiting without further retry.", flush=True)
                    sys.exit(1)

                print("[launcher] respawning all processes...", flush=True)
                time.sleep(2)   # tight crash loop से बचाव के लिए short pause
                running_processes = spawn_all_processes()
                continue

            time.sleep(1)

        shutdown_all_processes(running_processes, "signal received")
    except KeyboardInterrupt:
        shutdown_all_processes(running_processes, "KeyboardInterrupt")
