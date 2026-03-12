from __future__ import annotations

from dataclasses import dataclass

from api.main import main


@dataclass
class FakeServer:
    served: bool = False
    closed: bool = False

    def serve_forever(self) -> None:
        self.served = True
        raise KeyboardInterrupt

    def server_close(self) -> None:
        self.closed = True


def test_standalone_baseline_query_main_starts_server(capsys) -> None:
    server = FakeServer()
    fake_app = object()

    def server_factory(host: str, port: int, app):
        assert host == "127.0.0.1"
        assert port == 9100
        assert app is fake_app
        return server

    exit_code = main(
        ["serve", "--host", "127.0.0.1", "--port", "9100"],
        api_app=fake_app,
        server_factory=server_factory,
    )

    assert exit_code == 0
    assert server.served is True
    assert server.closed is True
    assert (
        capsys.readouterr().out.strip()
        == "Serving standalone baseline query API on http://127.0.0.1:9100"
    )

