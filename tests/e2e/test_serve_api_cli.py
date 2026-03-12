from __future__ import annotations

from dataclasses import dataclass

from cluster_metrics_platform.main import main


@dataclass
class FakeServer:
    served: bool = False
    closed: bool = False

    def serve_forever(self) -> None:
        self.served = True
        raise KeyboardInterrupt

    def server_close(self) -> None:
        self.closed = True


def test_serve_api_cli_starts_server_and_prints_address(capsys) -> None:
    server = FakeServer()
    fake_app = object()

    def server_factory(host: str, port: int, app):
        assert host == "127.0.0.1"
        assert port == 9000
        assert app is fake_app
        return server

    exit_code = main(
        ["serve-api", "--host", "127.0.0.1", "--port", "9000"],
        api_app=fake_app,
        server_factory=server_factory,
    )

    assert exit_code == 0
    assert server.served is True
    assert server.closed is True
    assert (
        capsys.readouterr().out.strip()
        == "Serving dashboard and API on http://127.0.0.1:9000"
    )
