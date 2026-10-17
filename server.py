from pathlib import Path

from aiohttp import web

import config

STATIC = Path(__file__).parent / "static"


@web.middleware
async def guard(request: web.Request, handler):
    """Защита панели: только localhost и только JSON в POST-запросах.
    Это не даёт постороннему сайту в вашем браузере управлять ботом."""
    host = request.headers.get("Host", "").split(":")[0]
    if host not in ("127.0.0.1", "localhost"):
        return web.json_response({"error": "forbidden host"}, status=403)
    if request.method == "POST" and request.content_type != "application/json":
        return web.json_response({"error": "json only"}, status=415)
    return await handler(request)


def create_app(engine) -> web.Application:
    async def index(_):
        return web.FileResponse(STATIC / "index.html")

    async def state(_):
        return web.json_response(engine.snapshot())

    async def get_settings(_):
        return web.json_response({"values": config.settings, "spec": config.SPEC})

    async def post_settings(request):
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"errors": ["Некорректный JSON"]}, status=400)
        if not isinstance(data, dict):
            return web.json_response({"errors": ["Ожидался объект"]}, status=400)
        errors = config.update_settings(data)
        if errors:
            return web.json_response({"errors": errors}, status=400)
        engine.event("info", "Настройки сохранены")
        return web.json_response({"values": config.settings})

    async def reset_settings(_):
        config.reset_settings()
        engine.event("info", "Настройки сброшены к стандартным")
        return web.json_response({"values": config.settings})

    async def control(request):
        try:
            data = await request.json()
        except Exception:
            data = {}
        action = data.get("action") if isinstance(data, dict) else None
        if action == "start":
            engine.start()
        elif action == "stop":
            await engine.stop()
        elif action == "reset_stats":
            engine.reset_stats()
        else:
            return web.json_response({"error": "unknown action"}, status=400)
        return web.json_response({"ok": True, "status": engine.status})

    app = web.Application(middlewares=[guard])
    app.add_routes([
        web.get("/", index),
        web.get("/api/state", state),
        web.get("/api/settings", get_settings),
        web.post("/api/settings", post_settings),
        web.post("/api/settings/reset", reset_settings),
        web.post("/api/control", control),
    ])
    return app
