import asyncio
from os.path import join
from os import stat
import yaml
from aiohttp import web
import aiohttp


class AsyncFileStorage:
    def __init__(self, port=5000, data_dir="storage", servers = (), config_path="config.yaml"):
        self._port = port
        self._data_dir = data_dir
        self._servers = servers

    def write_file(self, file_name, data):
        with open(file_name, "w") as file:
            file.write(data)
            return True

    async def fetch(self, url):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return resp.text()
        except:
            pass

    async def api_call(self, server, file_name):
        print(f"API CALL: {server}, {file_name}")
        if await self.fetch("/".join([server, "api", file_name])):
            print(f"API CALL: {server}, {file_name} OK")
            return server
        print(f"API CALL: {server}, {file_name} NOT FOUND")

    async def get_api_handler(self, request: web.Request) -> web.Response:
        file_name = request.match_info.get(join(self._data_dir, 'file_name'))
        file_size = stat(file_name).st_size
        return web.Response(text=str(file_size))

    async def get_file_handler(self, request: web.Request) -> web.Response:
        file_name = request.match_info.get('file_name')
        print(f"REQ: {file_name}")
        file_path = join(self._data_dir, file_name)
        try:
            file = open(file_path, 'r')
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, file.read)
            print(f"GOT IN: {file_path}")
        except FileNotFoundError:
            print(f"NOT FOUND: {file_path}")
            futures = [self.api_call(server, file_name) for server in config["servers"]]
            done, _ = await asyncio.wait(futures, timeout=0.1)
            for future in done:
                if not future.exception() and future.result():
                    data = await self.fetch(future.result())
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, self.write_file, file_path, data)
                    return web.Response(text=data)
            return web.Response(status=404)
        else:
            file.close()
            print(f"NO LOCAL AND REMOTE RETURNING 404")
            return web.Response(text=result)

    def run(self):
        app = web.Application()
        app.add_routes([web.get('/{file_name}', self.get_file_handler),
                        web.get('/api/{file_name}', self.get_api_handler)])
        web.run_app(app, port=self._port) #docs says blocking?(
        # runner = web.AppRunner(app)
        # await runner.setup()
        # site = web.TCPSite(runner, 'localhost', 5000)
        # await site.start()


def load_config(config_path):
    with open(config_path, 'r') as config_file:
        config = yaml.load(config_file)
        servers = []
        for server in config["servers"]:
            servers.append("".join(["http://", str(server["IP"]), ":", str(server["PORT"])]))
        config["servers"] = servers
        print(config)
        return config

if __name__ == "__main__":
    config = load_config("config.yaml")
    afs = AsyncFileStorage(5000, servers=config['servers'])
    afs.run()