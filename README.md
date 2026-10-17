# Deriv Supply & Demand Telegram Signal Bot

Signal-only Node.js app using Deriv public market data, Express dashboard, and Telegram Bot API.

## Features
- 33-instrument target list (29 Forex pairs plus AAPL, TSLA, NVDA, AMZN), with automatic lookup of symbols currently available on Deriv.
- 4H and 15M supply/demand zones, approximate open/wick-based order-block logic.
- Live Deriv tick subscriptions send an immediate zone-touch warning.
- A separate alert is sent when a completed 5M candle overlaps the touched zone and shows a directional rejection wick/body.
- Confirmed-entry alert includes the completed candle close as entry, stop-loss beyond the zone, and 1:2 take-profit.
- Telegram alerts and a live dashboard.
- No Deriv login token is required for public market data.

## Important limitations
This is an independent approximation of the behaviour described for the protected JP7FX indicator, not its original algorithm. A zone touch is not proof of reversal. The initial zone scan uses historical candles; live tick price is used to check touches. A restart rebuilds zones and in-memory signal history. Availability and pricing can vary by Deriv account/market. Test on demo and verify all symbols and price formatting before using signals.

The target list is 33 instruments: the first 29 Forex pairs are unchanged, and slots 30–33 are AAPL, TSLA, NVDA, and AMZN. Availability depends on Deriv’s live `active_symbols` response; unavailable instruments are shown as such on the dashboard. Stock symbols may be returned with exchange suffixes such as `.OQ`.

## Files
- `server.js` - API connection, zone logic, signal generation, Telegram notifications.
- `public/index.html` - dashboard.
- `package.json` - dependencies and start command.

## Create Telegram bot
1. In Telegram, open `@BotFather`.
2. Send `/newbot`, follow the prompts, and copy the bot token.
3. Open the new bot and press Start.
4. To obtain your chat ID, send a message to the bot and use:
   `https://api.telegram.org/botYOUR_TOKEN/getUpdates`
   Replace `YOUR_TOKEN` with your token. Find `message.chat.id` in the response.
5. Keep the token private. Do not paste it into code or commit it to GitHub.

For a group, add the bot to the group, send a message in the group, and obtain the group's chat ID from `getUpdates`.

## GitHub setup
1. Create a new repository, e.g. `deriv-supply-demand-telegram-bot`.
2. Upload `server.js`, `package.json`, and the `public` folder containing `index.html`.
3. Do not upload `.env` files or bot tokens.

## Railway setup
1. Create a Railway project and deploy from the GitHub repository.
2. Add these variables in Railway → service → Variables:
   - `TELEGRAM_BOT_TOKEN` = your BotFather token
   - `TELEGRAM_CHAT_ID` = your Telegram user/group chat ID
   - `DERIV_APP_ID` = `1089` (or your own Deriv app ID)
   - `SCAN_INTERVAL_MS` = `300000`
   - `REQUEST_GAP_MS` = `250`
3. Railway should detect Node and run `npm start`.
4. Generate a public domain in Railway's networking/settings.
5. Open the domain. `/health` should return `{"ok":true,...}` and `/` shows the dashboard.
6. Check the dashboard logs for resolved pairs and Telegram send errors.

## How the bot decides a zone
- Demand approximation: a bearish candle followed by a sufficiently strong bullish candle that closes above the previous high.
- Supply approximation: a bullish candle followed by a sufficiently strong bearish candle that closes below the previous low.
- Zone source timeframes: 4H and 15M.
- On a live price touch, send an immediate warning once per zone per cooldown period. This warning is not an entry confirmation.
- After the touch, check the most recently completed 5M candle; if it overlaps the zone and closes with a directional rejection wick/body, send a second confirmed-entry alert. The confirmation window is 30 minutes.
- Confirmed entry uses the completed 5M close. Stop-loss is beyond the zone plus an ATR-based buffer; take-profit is 2R from that entry.
- Confirmation is a rule-based approximation, not a guarantee of reversal or a claim to reproduce a protected indicator.

## Troubleshooting
- **No pairs resolved:** inspect dashboard logs. Deriv may return a different display-name format or a symbol may not be available. Do not manually replace missing pairs with synthetic/OTC instruments.
- **Telegram not sent:** check token/chat ID, start the bot in a direct chat, and inspect logs.
- **Too many requests/timeouts:** increase `REQUEST_GAP_MS` to `400` or `500`, and/or increase `SCAN_INTERVAL_MS` to `600000`.
- **Disconnected:** the WebSocket client reconnects automatically; watch the dashboard logs.
- **No signals:** first confirm active pairs and zones are present. Signals are generated only when a live tick enters a zone; it is not a prediction that price will reverse.

## Risk note
Signals are informational only. Spread, slippage, execution differences, and rapid breakouts can cause losses. Validate on a demo account and backtest before relying on any alert.

## Structure logic update (approximation)

The current build now uses confirmed swing pivots (2 bars left/right), close-based BOS/CHoCH events, and creates candidate order-block zones from the nearest opposite-colour source candle before a qualifying structure break. Zone-touch alerts include the detected event and a basic premium/discount position estimate, plus a simple wick-sweep annotation.

This is an independently implemented approximation based on the public description of JP7FX's protected indicator. It does **not** reproduce the protected source or guarantee identical zones. The full persistent external/internal range lifecycle and the exact Magic M/W and LQ Magic M/W sequence logic are not implemented in this build yet. Swing confirmation is delayed by design. Test with demo data before relying on alerts.
