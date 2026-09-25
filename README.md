# V10.6.2 RENDER WEB FIX

- Fixes Render Web Service when its existing Start Command is `python app.py`.
- `app.py` now keeps the HTTP server alive and listens on Render `$PORT` at `0.0.0.0`.
- `worker.py` remains responsible for live raw capture + 200,000 historical raw blocks.
- Historical 360-rule rebuild stays blocked until the frozen 200,000-block archive is complete.
- Keeps V10.6 one-time dataset reset marker; redeploying this patch does not intentionally reset the same V10.6 dataset again.
- `render.yaml` still recommends Gunicorn for Blueprint/new services, but this patch is compatible with the existing `python app.py` Web Service setting.
