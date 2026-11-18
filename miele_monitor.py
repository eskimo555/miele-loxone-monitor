#!/usr/bin/env python3
"""
miele_monitor.py - standalone Miele washer/dryer monitor (no ioBroker needed).

What it does
  * Talks directly to the Miele 3rd Party API (cloud) with OAuth2 + automatic
    token refresh. Tokens are stored in a JSON file (chmod 600).
  * Listens to Miele Server-Sent Events (push), so it reacts within seconds and
    does not hammer the API with polling.
  * Sends the same Loxone virtual-input calls as the old cron script:
        /dev/sps/io/<washer|dryer>_status/<status text>
        /dev/sps/io/switch_<washer|dryer>/on      (≈5 min before the end)
  * Powers the machine off when the programme is finished, retries if the
    first attempt fails, and checks that it really went off.
  * Runs forever (systemd), reconnects with back-off on any error.

Usage
  python3 miele_monitor.py --login     one-time: get OAuth tokens
  python3 miele_monitor.py --list      show your Miele devices and status
  python3 miele_monitor.py --dry-run   run, but don't power off / call Loxone
  python3 miele_monitor.py             normal run (used by systemd)

Config: /home/pi/scripts/config.ini (override with --config)
  [Loxone]  server, user, pass                       (same as before)
  [Miele]   client_id, client_secret                 (from developer.miele.com)
            redirect_uri   (must match your Miele client registration)
            scope          (optional, default below)
            oauth          new | legacy               (default: new)
            auth_token     (optional, old access token used only as fallback
                            until --login has been done)
  [Devices] washer = <fabNumber of washer>   (see --list)
            dryer  = <fabNumber of dryer>
"""

import argparse
import configparser
import json
import logging
import os
import secrets
import signal
import sys
import threading
import time
from urllib.parse import quote, urlencode, urlparse, parse_qs

import requests

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------
API_BASE = "https://api.mcs3.miele.com/v1"
OAUTH = {
    # Current Miele OAuth provider
    "new": {
        "auth": "https://auth.domestic.miele-iot.com/partner/realms/mcs/protocol/openid-connect/auth",
        "token": "https://auth.domestic.miele-iot.com/partner/realms/mcs/protocol/openid-connect/token",
    },
    # Legacy provider (Miele says it will be phased out)
    "legacy": {
        "auth": "https://api.mcs3.miele.com/thirdparty/login/",
        "token": "https://api.mcs3.miele.com/thirdparty/token/",
    },
}
DEFAULT_CONFIG = "/home/pi/scripts/config.ini"
DEFAULT_TOKEN_FILE = "/home/pi/scripts/miele_tokens.json"
DEFAULT_SCOPE = "mcs_thirdparty_read mcs_thirdparty_write"
LANGUAGE = "en"  # keeps status texts "In use" / "Finished" / "Off" like before

# Miele status.value_raw codes
ST_OFF = 1
ST_RUNNING = 5
ST_PAUSE = 6
ST_FINISHED = 7  # "End programmed" / "Finished"

HTTP_TIMEOUT = (10, 20)          # connect, read (normal API / Loxone calls)
SSE_TIMEOUT = (10, 300)          # read timeout for the event stream
WARN_MINUTES = 5                 # announce when <= 5 minutes left
POWEROFF_RETRY_SEC = 120         # wait between power-off attempts
POWEROFF_MAX_ATTEMPTS = 5
STATUS_RESEND_SEC = 600          # resend unchanged status text to Loxone every 10 min

log = logging.getLogger("miele")
stop_event = threading.Event()
DRY_RUN = False


# --------------------------------------------------------------------------
# OAuth token handling
# --------------------------------------------------------------------------
class TokenStore:
    def __init__(self, path, client_id, client_secret, token_url, fallback_token=None):
        self.path = path
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.fallback_token = fallback_token
        self.lock = threading.Lock()
        self.data = {}
        if os.path.exists(path):
            with open(path) as f:
                self.data = json.load(f)

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=2)
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)  # atomic: never a half-written token file

    def store(self, tok):
        """Store a token response from the OAuth server."""
        self.data["access_token"] = tok["access_token"]
        # Some servers rotate the refresh token, keep the old one if none is sent
        if tok.get("refresh_token"):
            self.data["refresh_token"] = tok["refresh_token"]
        self.data["expires_at"] = time.time() + int(tok.get("expires_in", 3600))
        self._save()

    def refresh(self):
        with self.lock:
            rt = self.data.get("refresh_token")
            if not rt:
                raise RuntimeError("No refresh_token stored - run with --login first")
            log.info("Refreshing Miele access token")
            r = requests.post(self.token_url, timeout=HTTP_TIMEOUT, data={
                "grant_type": "refresh_token",
                "refresh_token": rt,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            })
            if r.status_code != 200:
                raise RuntimeError("Token refresh failed: HTTP %s %s" % (r.status_code, r.text[:200]))
            self.store(r.json())

    def get(self):
        if not self.data.get("access_token"):
            if self.fallback_token:
                return self.fallback_token
            raise RuntimeError("No Miele token - run with --login first")
        # refresh 10 minutes before expiry
        if self.data.get("refresh_token") and time.time() > self.data.get("expires_at", 0) - 600:
            self.refresh()
        return self.data["access_token"]

    def headers(self, extra=None):
        h = {"Authorization": "Bearer " + self.get(), "Accept": "application/json"}
        if extra:
            h.update(extra)
        return h


# --------------------------------------------------------------------------
# Miele API
# --------------------------------------------------------------------------
class MieleAPI:
    def __init__(self, tokens):
        self.tokens = tokens

    def _request(self, method, path, **kw):
        """HTTP call with one automatic token refresh on 401."""
        url = API_BASE + path
        for attempt in (1, 2):
            r = requests.request(method, url, headers=self.tokens.headers(
                {"Content-Type": "application/json"} if method == "PUT" else None),
                timeout=HTTP_TIMEOUT, **kw)
            if r.status_code == 401 and attempt == 1 and self.tokens.data.get("refresh_token"):
                self.tokens.refresh()
                continue
            r.raise_for_status()
            return r
        return r

    def devices(self):
        return self._request("GET", "/devices", params={"language": LANGUAGE}).json()

    def device(self, dev_id):
        return self._request("GET", "/devices/%s" % dev_id, params={"language": LANGUAGE}).json()

    def actions(self, dev_id):
        return self._request("GET", "/devices/%s/actions" % dev_id).json()

    def power_off(self, dev_id):
        self._request("PUT", "/devices/%s/actions" % dev_id, data=json.dumps({"powerOff": True}))


def sse_events(resp):
    """Minimal Server-Sent-Events parser. Yields (event, data)."""
    resp.encoding = "utf-8"
    event, data = None, []
    for line in resp.iter_lines(decode_unicode=True):
        if stop_event.is_set():
            return
        if line is None:
            continue
        if line == "":
            if data:
                yield event or "message", "\n".join(data)
            event, data = None, []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "event":
            event = value
        elif field == "data":
            data.append(value)


# --------------------------------------------------------------------------
# Loxone
# --------------------------------------------------------------------------
class Loxone:
    def __init__(self, server, user, password):
        self.base = "http://%s/dev/sps/io" % server
        self.auth = (user, password)

    def send(self, name, value, retries=3):
        url = "%s/%s/%s" % (self.base, quote(name, safe=""), quote(value, safe=":()"))
        if DRY_RUN:
            log.info("[dry-run] Loxone %s = %r", name, value)
            return True
        for i in range(retries):
            try:
                r = requests.get(url, auth=self.auth, timeout=(5, 10))
                r.raise_for_status()
                log.debug("Loxone %s = %r -> %s", name, value, r.status_code)
                return True
            except requests.RequestException as e:
                log.warning("Loxone call %s failed (%d/%d): %s", name, i + 1, retries, e)
                time.sleep(2)
        return False


# --------------------------------------------------------------------------
# Per-appliance logic
# --------------------------------------------------------------------------
class Appliance:
    def __init__(self, name, dev_id, api, loxone):
        self.name = name
        self.id = dev_id
        self.api = api
        self.lox = loxone
        self.lock = threading.Lock()
        self.status = None
        self.last_text = None
        self.last_text_sent = 0
        self.armed = False          # program seen with > WARN_MINUTES left
        self.warned = False
        self.poweroff_attempts = 0
        self.last_poweroff = 0

    @staticmethod
    def _remaining(state):
        rt = state.get("remainingTime")
        try:
            h, m = int(rt[0]), int(rt[1])
            if h < 0 or m < 0:
                raise ValueError
            return h, m
        except (TypeError, ValueError, IndexError):
            return 0, 0

    def update(self, device):
        """Handle a full device object (from GET /devices or an SSE event)."""
        state = device.get("state") or {}
        st = state.get("status") or {}
        status = st.get("value_raw")
        status_txt = st.get("value_localized") or str(status)
        program = (state.get("ProgramID") or {}).get("value_localized") or ""
        h, m = self._remaining(state)
        left = h * 60 + m

        with self.lock:
            if status != self.status:
                log.info("%s: status %s -> %s (%s)", self.name, self.status, status, status_txt)
            self.status = status

            # 1. Status text to Loxone (same format as the old script)
            time_txt = "%d:%02d to go (%s)" % (h, m, program) if status != ST_OFF else ""
            text = "%s %s" % (status_txt, time_txt)
            now = time.time()
            if text != self.last_text or now - self.last_text_sent > STATUS_RESEND_SEC:
                if self.lox.send("%s_status" % self.name, text):
                    self.last_text = text
                    self.last_text_sent = now

            # 2. "Almost done" switch, once per programme
            if status == ST_RUNNING:
                if left > WARN_MINUTES:
                    self.armed = True
                elif self.armed and not self.warned:
                    log.info("%s: %d min left, triggering switch_%s", self.name, left, self.name)
                    if self.lox.send("switch_%s" % self.name, "on"):
                        self.warned = True
            elif status not in (ST_PAUSE,):
                self.armed = False
                self.warned = False

            # 3. Power-off bookkeeping
            if status == ST_OFF and self.poweroff_attempts:
                log.info("%s: confirmed off after %d attempt(s)", self.name, self.poweroff_attempts)
            if status != ST_FINISHED:
                self.poweroff_attempts = 0

        if status == ST_FINISHED:
            self.try_power_off()

    def try_power_off(self):
        with self.lock:
            if self.status != ST_FINISHED:
                return
            if self.poweroff_attempts >= POWEROFF_MAX_ATTEMPTS:
                return
            if time.time() - self.last_poweroff < POWEROFF_RETRY_SEC:
                return
            self.poweroff_attempts += 1
            self.last_poweroff = time.time()
            attempt = self.poweroff_attempts

        try:
            allowed = self.api.actions(self.id).get("powerOff")
            if not allowed:
                log.warning("%s: finished but Miele reports powerOff not allowed now (attempt %d)",
                            self.name, attempt)
                return
            if DRY_RUN:
                log.info("[dry-run] %s: would power off now", self.name)
                return
            self.api.power_off(self.id)
            log.info("%s program is finished, power-off sent (attempt %d)", self.name, attempt)
        except Exception as e:
            log.error("%s: power-off attempt %d failed: %s", self.name, attempt, e)
        if attempt >= POWEROFF_MAX_ATTEMPTS:
            log.error("%s: giving up power-off after %d attempts", self.name, attempt)


# --------------------------------------------------------------------------
# Main loops
# --------------------------------------------------------------------------
def dispatch(appliances, devices):
    for dev_id, dev in devices.items():
        app = appliances.get(dev_id)
        if app:
            try:
                app.update(dev)
            except Exception:
                log.exception("Error handling update for %s", app.name)


def retry_loop(appliances, api):
    """Safety net: retries power-off and keeps tokens fresh even when no events arrive."""
    while not stop_event.wait(60):
        try:
            api.tokens.get()
        except Exception as e:
            log.error("Token check failed: %s", e)
        for app in appliances.values():
            if app.status == ST_FINISHED:
                # re-read state first: maybe it already went off
                try:
                    app.update(api.device(app.id))
                except Exception as e:
                    log.error("%s: state refresh failed: %s", app.name, e)


def event_loop(appliances, api):
    backoff = 5
    while not stop_event.is_set():
        try:
            # full sync first, so nothing is missed while disconnected
            dispatch(appliances, api.devices())
            log.info("Connecting to Miele event stream")
            with requests.get(API_BASE + "/devices/all/events", stream=True, timeout=SSE_TIMEOUT,
                              headers=api.tokens.headers({"Accept": "text/event-stream",
                                                          "Accept-Language": LANGUAGE})) as r:
                if r.status_code == 401:
                    api.tokens.refresh()
                    continue
                r.raise_for_status()
                backoff = 5
                for event, data in sse_events(r):
                    if event == "devices":
                        dispatch(appliances, json.loads(data))
                    elif event == "ping":
                        log.debug("ping")
            log.warning("Event stream closed by server, reconnecting")
        except Exception as e:
            log.error("Event loop error: %s - retry in %ds", e, backoff)
            stop_event.wait(backoff)
            backoff = min(backoff * 2, 300)


def do_login(cfg, tokens, oauth):
    m = cfg["Miele"]
    redirect = m.get("redirect_uri", "").strip()
    if not redirect:
        sys.exit("Set redirect_uri in [Miele] (must match your Miele client registration)")
    state = secrets.token_urlsafe(16)
    params = {"response_type": "code", "client_id": m["client_id"],
              "redirect_uri": redirect, "state": state}
    scope = m.get("scope", DEFAULT_SCOPE).strip()
    if scope:
        params["scope"] = scope
    print("\n1. Open this URL in a browser and log in with your Miele account:\n")
    print(oauth["auth"] + "?" + urlencode(params))
    print("\n2. After login you are redirected (the page may fail to load - that's OK).")
    url = input("3. Paste the full URL from the address bar here:\n> ").strip()
    q = parse_qs(urlparse(url).query)
    if q.get("state", [None])[0] != state:
        sys.exit("State mismatch - did you paste the right URL?")
    code = q.get("code", [None])[0]
    if not code:
        sys.exit("No ?code= in that URL")
    r = requests.post(oauth["token"], timeout=HTTP_TIMEOUT, data={
        "grant_type": "authorization_code", "code": code, "redirect_uri": redirect,
        "client_id": m["client_id"], "client_secret": m["client_secret"]})
    if r.status_code != 200:
        sys.exit("Token request failed: HTTP %s %s" % (r.status_code, r.text[:300]))
    tokens.store(r.json())
    print("Tokens saved to", tokens.path)


def main():
    global DRY_RUN
    p = argparse.ArgumentParser(description="Miele washer/dryer monitor")
    p.add_argument("--config", default=DEFAULT_CONFIG)
    p.add_argument("--tokens", default=DEFAULT_TOKEN_FILE)
    p.add_argument("--login", action="store_true", help="one-time OAuth login")
    p.add_argument("--list", action="store_true", help="list devices and exit")
    p.add_argument("--dry-run", action="store_true", help="no power-off, no Loxone calls")
    p.add_argument("--debug", action="store_true")
    a = p.parse_args()
    DRY_RUN = a.dry_run

    logging.basicConfig(level=logging.DEBUG if a.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    cfg = configparser.ConfigParser(interpolation=None)  # passwords may contain %
    if not cfg.read(a.config):
        sys.exit("Cannot read config %s" % a.config)
    m = cfg["Miele"]
    oauth = OAUTH[m.get("oauth", "new").strip()]
    tokens = TokenStore(a.tokens, m.get("client_id", ""), m.get("client_secret", ""),
                        oauth["token"], fallback_token=m.get("auth_token") or None)
    api = MieleAPI(tokens)

    if a.login:
        do_login(cfg, tokens, oauth)
        return
    if a.list:
        for dev_id, d in api.devices().items():
            ident, st = d.get("ident", {}), d.get("state", {})
            print("%s  %-20s %-15s %s" % (dev_id, ident.get("type", {}).get("value_localized"),
                                         ident.get("deviceName") or "-",
                                         st.get("status", {}).get("value_localized")))
        return

    if not tokens.data.get("refresh_token"):
        log.warning("No refresh token stored, using auth_token from config. "
                    "This stops working when it expires - run --login.")

    lx = cfg["Loxone"]
    loxone = Loxone(lx["server"], lx["user"], lx["pass"])
    if not cfg.has_section("Devices") or not cfg["Devices"]:
        sys.exit("Add a [Devices] section to the config (run --list to find the IDs)")
    devs = cfg["Devices"]
    appliances = {dev_id.strip(): Appliance(name, dev_id.strip(), api, loxone)
                  for name, dev_id in devs.items()}
    log.info("Monitoring %s%s", ", ".join("%s=%s" % (a_.name, a_.id) for a_ in appliances.values()),
             " (DRY RUN)" if DRY_RUN else "")

    def _stop(*_):
        log.info("Stopping")
        stop_event.set()
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    threading.Thread(target=retry_loop, args=(appliances, api), daemon=True).start()
    event_loop(appliances, api)


if __name__ == "__main__":
    main()
