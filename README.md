# miele-loxone-monitor

Standalone monitor for Miele washers and dryers on a Raspberry Pi. No ioBroker needed.

- Talks directly to the official **Miele 3rd Party API** (OAuth2 with automatic token refresh)
- Uses Miele **Server-Sent Events**, so updates arrive within seconds without polling
- Sends status text to **Loxone** virtual inputs (`<device>_status`) and turns on `switch_<device>` about 5 minutes before the end, for example for an announcement through the house
- **Powers the machine off** when the programme is finished (checks that power-off is allowed, retries, confirms)
- Runs as a systemd service and reconnects with back-off when the Miele cloud is unavailable

## Requirements
- Miele appliances connected in the Miele app
- Client ID and secret from <https://developer.miele.com>
- Python 3 with `requests` (`sudo apt install python3-requests`)
- Optional: a Loxone Miniserver with virtual inputs `washer_status`, `dryer_status`, `switch_washer`, `switch_dryer`

## Setup
```bash
mkdir -p /home/pi/scripts && cd /home/pi/scripts
cp config.example.ini config.ini && chmod 600 config.ini   # fill in your values
python3 miele_monitor.py --login     # one-time OAuth login (paste the redirect URL)
python3 miele_monitor.py --list      # find your device IDs, put them in [Devices]
python3 miele_monitor.py --dry-run   # test: logs only, no power-off, no Loxone calls
```

Run as a service:
```bash
sudo cp miele-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now miele-monitor
journalctl -u miele-monitor -f
```

## Notes
- Tokens are stored in `miele_tokens.json` (mode 600). If the log shows `Token refresh failed`, run `--login` again.
- `redirect_uri` must match the redirect URI registered for your Miele client. If the new OAuth provider rejects it, try `oauth = legacy`.
- Don't use the same Miele client credentials in another integration (ioBroker, Home Assistant) at the same time, or the two may invalidate each other's tokens.

## Disclaimer
Not affiliated with Miele. Use at your own risk.
