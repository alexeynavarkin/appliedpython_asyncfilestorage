import argparse
import asyncio
from aiohttp import web, ClientSession
from os.path import join
from os import stat
from sys import argv
import yaml


class AsyncFileStorage:
    def __init__(self, port=5000, save_files=True ,data_dir="storage", nodes=()):
        self._port = port
        self._data_dir = data_dir
        self._nodes = nodes
        self._save_files = save_files

    def write_file(self, file_name: str, data: str) -> bool:
        with open(file_name, "w") as file:
            file.write(data)
            return True

    async def fetch(self, url: str) -> str:
        async with ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    raise FileNotFoundError

    async def poll_nodes(self, file_name: str) -> list:
        futures = [self.api_call(node_id, file_name) for node_id in range(len(self._nodes))]
        done, _ = await asyncio.wait(futures)
        success_nodes = []
        for future in done:
            if not future.exception():
                success_nodes.append(future.result())
        return success_nodes

    async def api_call(self, node_id: int, file_name: str) -> int:
        get = "/".join([self._nodes[node_id]['url'], "api", file_name])
        print(f"POLLING NODE: '{get}'")
        if await self.fetch(get):
            print(f"POLLING SUCCESS: '{get}'")
            return node_id

    async def download_file(self, node_id: int, file_name: str) -> str:
        get = "/".join([self._nodes[node_id]['url'], file_name])
        print(f"DOWNLOADING FROM: '{get}'")
        data = await self.fetch(get)
        print(f"DOWNLOADED FROM '{get}':\n----\n{data}\n----")
        if self._save_files and self._nodes[node_id]['save_files']:
            print(f"SAVING LOCAL: '{file_name}''")
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.write_file, join(self._data_dir, file_name), data)
        return data

    async def get_api_handler(self, request: web.Request) -> web.Response:
        file_name = request.match_info.get('file_name')
        try:
            file_size = stat(join(self._data_dir, file_name)).st_size
        except FileNotFoundError:
            return web.Response(status=404)
        else:
            return web.Response(text=str(file_size))

    async def get_file_handler(self, request: web.Request) -> web.Response:
        file_name = request.match_info.get('file_name')
        file_path = join(self._data_dir, file_name)
        print(f"REQ: '{file_name}'")

        try:
            file = open(file_path, 'r')
            print(f"FOUND LOCAL: '{file_path}'")
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, file.read)

        except FileNotFoundError:
            print(f"NOT FOUND LOCAL: '{file_path}' POLLING NODES")
            success_nodes = await self.poll_nodes(file_name)
            if success_nodes:
                print(f"FILE '{file_name}' FOUND ON NODES: {success_nodes}")
                data = await self.download_file(success_nodes[0], file_name)
                return web.Response(text=data)
            print(f"NOT FOUND LOCAL AND NODES: '{file_name}' RETURNING 404")
            return web.Response(status=404)

        else:
            file.close()
            return web.Response(text=data)

    def run(self):
        app = web.Application()
        app.add_routes([web.get('/{file_name}', self.get_file_handler),
                        web.get('/api/{file_name}', self.get_api_handler)])
        web.run_app(app, port=self._port) #docs says blocking?(
        # runner = web.AppRunner(app)
        # await runner.setup()
        # site = web.TCPSite(runner, 'localhost', 5000)
        # await site.start()


def chunkify(data, n):
    """
        Method to chunk data, works properly only if len(data) >> chunk_size
        Maybe if multiple servers has target file, split into chunks and download it parallel then concat
    """
    chunk_size = len(data)//n
    return [data[start:start + chunk_size] for start in range(0, len(data), chunk_size)]


def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as config_file:
        config = yaml.load(config_file)
        print(f"Config loaded from: '{config_path}'")
        return config

def parse_args(args):
    parser = argparse.ArgumentParser(description='Async File Storage')
    parser.add_argument('-c',
                        '--config',
                        action="store",
                        type=str,
                        default="config.yaml",
                        help='Config file')
    return parser.parse_args(args)


if __name__ == "__main__":
    args = parse_args(argv[1:])
    config = load_config(args.config)
    afs = AsyncFileStorage(**config)
    afs.run()