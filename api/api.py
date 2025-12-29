from aiohttp import web  
from db_connection import get_cus_fio_by_id, init_db, close_db

routes = web.RouteTableDef()

@routes.get("/api/getCusFioById")
async def get_product(request):
    cus_id = request.query.get("cus_id")

    if not cus_id:
        return web.json_response(
            {"error": "cus_id is required"},
            status=400
        )

    product = await get_cus_fio_by_id(cus_id)

    if product is None:
        return web.json_response(
            {"error": "transaction not found"},
            status=404
        )

    return web.json_response(product)


async def on_startup(app):
    await init_db()

async def on_cleanup(app):
    await close_db()

app = web.Application()
app.add_routes(routes)
app.on_startup.append(on_startup)
app.on_cleanup.append(on_cleanup)

web.run_app(app, host="127.0.0.1", port=8000)
