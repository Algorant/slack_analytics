from __future__ import annotations

import socket
import subprocess
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from playwright.sync_api import Error, Page, sync_playwright


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_server(url: str, process: subprocess.Popen[str], timeout: float = 30.0) -> None:
    started_at = time.monotonic()
    while time.monotonic() - started_at < timeout:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(
                "marimo exited before becoming ready\n"
                f"stdout:\n{stdout}\n\nstderr:\n{stderr}"
            )
        try:
            with urlopen(url, timeout=1) as response:
                if 200 <= response.status < 500:
                    return
        except URLError:
            time.sleep(0.25)
    raise AssertionError(f"Timed out waiting for marimo app at {url}")


def _expand_section(page: Page, name: str) -> None:
    try:
        locator = page.get_by_role("button", name=name)
        if locator.count():
            locator.first.click()
            return
    except Error:
        pass
    page.get_by_text(name, exact=True).first.click()


def test_marimo_app_smoke() -> None:
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    process = subprocess.Popen(
        ["uv", "run", "marimo", "run", "app.py", "--headless", "--port", str(port)],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_server(url, process)
        runtime_errors: list[str] = []
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()

            page.on("pageerror", lambda exc: runtime_errors.append(f"pageerror: {exc}"))
            page.on(
                "console",
                lambda msg: runtime_errors.append(f"console error: {msg.text}")
                if msg.type == "error"
                else None,
            )
            page.on(
                "requestfailed",
                lambda req: runtime_errors.append(
                    f"request failed: {req.method} {req.url} -> {req.failure}"
                ),
            )

            page.goto(url, wait_until="networkidle")
            page.get_by_role("heading", name="Slackalytics").wait_for(timeout=15_000)
            page.get_by_role("heading", name="Filters").wait_for(timeout=15_000)

            _expand_section(page, "Reaction personalities")
            page.get_by_text("Top emojis", exact=True).wait_for(timeout=15_000)
            page.get_by_text("Favorite emojis by person", exact=True).wait_for(timeout=15_000)

            _expand_section(page, "Posts by year")
            page.get_by_text("Posts by month (latest 36 months)", exact=True).wait_for(timeout=15_000)

            page.get_by_text("Include bots / system users", exact=True).click()
            page.wait_for_timeout(1500)
            page.get_by_text("Include bots / system users", exact=True).click()
            page.wait_for_timeout(1500)

            browser.close()

        assert not runtime_errors, "marimo runtime errors:\n" + "\n".join(runtime_errors)
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
