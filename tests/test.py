from unittest import TestCase
from requests import get
from time import sleep
import subprocess

class BaseTest(TestCase):

    def setUp(self):
        self.servers = [
                subprocess.Popen(['python', 'async_file_storage.py', '--config', 'tests/tmp/a.yaml']),
                subprocess.Popen(['python', 'async_file_storage.py', '--config', 'tests/tmp/b.yaml']),
                subprocess.Popen(['python', 'async_file_storage.py', '--config', 'tests/tmp/c.yaml']),
            ]
        sleep(0.5)

    def tearDown(self):
        for server in self.servers:
            server.terminate()
            server.wait()

    def test_local(self):
        response = get("http://localhost:5000/a.txt")
        self.assertEqual('This is plain file from NODE A.', response.text)

    def test_download_from_node(self):
        response = get("http://localhost:5000/c.txt")
        self.assertEqual('This is plain file from NODE C.', response.text)

    def test_download_empty_file(self):
        response = get("http://localhost:5000/b.txt")
        self.assertEqual('', response.text)

    def test_404(self):
        response = get("http://localhost:5000/404.txt")
        self.assertEqual(404, response.status_code)


