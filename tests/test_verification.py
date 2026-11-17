"""产物核对（固件可复现性）API 的端到端测试与 verify 模块单元测试。

运行:
  .venv/bin/pytest tests/test_verification.py -v
"""
from __future__ import annotations

import hashlib
import json
import os
import shlex
from concurrent.futures import ThreadPoolExecutor

import pytest

from app import verify
from test_integration import ServerHarness, _free_port, step, submit, wait_build


# ------------------------------------------------------------- 单元：路径归一化


def test_normalize_rel_path_accepts_and_collapses():
    assert verify.normalize_rel_path("out/fw.bin") == "out/fw.bin"
    assert verify.normalize_rel_path("f.bin") == "f.bin"
    assert verify.normalize_rel_path("a/./b") == "a/b"
    assert verify.normalize_rel_path("a//b") == "a/b"
    assert verify.normalize_rel_path("a/b/") == "a/b"
    assert verify.normalize_rel_path("a/b/../c") == "a/c"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "   ",
        "..",
        "../x",
        "a/../../b",
        "a/b/../..",
        "/etc/passwd",
        "/abs/path",
        " path",
        "path ",
        "a\x00b",
    ],
)
def test_normalize_rel_path_rejects(bad):
    with pytest.raises(ValueError):
        verify.normalize_rel_path(bad)


# ------------------------------------------------------- 单元：无符号链接读取


def test_read_regular_file_ok(tmp_path):
    data = os.urandom(100_000)
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "f.bin").write_bytes(data)
    size, digest = verify.read_regular_file(tmp_path, "sub/f.bin")
    assert size == len(data)
    assert digest == hashlib.sha256(data).hexdigest()


def test_read_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        verify.read_regular_file(tmp_path, "nope.bin")


def test_read_directory_rejected(tmp_path):
    (tmp_path / "d").mkdir()
    with pytest.raises(verify.NotRegularFileError):
        verify.read_regular_file(tmp_path, "d")


def test_read_rejects_symlink_final_component(tmp_path):
    (tmp_path / "real.bin").write_bytes(b"data")
    (tmp_path / "link").symlink_to("real.bin")  # 指向目录内也不行
    with pytest.raises(verify.SymlinkError):
        verify.read_regular_file(tmp_path, "link")


def test_read_rejects_symlink_escaping_workdir(tmp_path):
    (tmp_path / "escape").symlink_to("/etc/hostname")
    with pytest.raises(verify.SymlinkError):
        verify.read_regular_file(tmp_path, "escape")


def test_read_rejects_symlink_intermediate_component(tmp_path):
    (tmp_path / "realdir").mkdir()
    (tmp_path / "realdir" / "f.bin").write_bytes(b"x")
    (tmp_path / "dirlink").symlink_to("realdir")
    with pytest.raises(verify.SymlinkError):
        verify.read_regular_file(tmp_path, "dirlink/f.bin")


def test_read_detects_change_during_read(tmp_path, monkeypatch):
    target = tmp_path / "f.bin"
    target.write_bytes(b"x" * 8192)
    real_read = os.read

    def mutating_read(fd, n):
        chunk = real_read(fd, n)
        # 读取期间改写文件：mtime/ctime 变化必须被捕获
        target.write_bytes(b"y" * 8192)
        return chunk

    monkeypatch.setattr(verify.os, "read", mutating_read)
    with pytest.raises(verify.ChangedDuringRead):
        verify.read_regular_file(tmp_path, "f.bin")


def test_read_detects_truncate_during_read(tmp_path, monkeypatch):
    target = tmp_path / "f.bin"
    target.write_bytes(b"x" * 8192)
    real_read = os.read

    def truncating_read(fd, n):
        chunk = real_read(fd, n)
        target.write_bytes(b"short")
        return chunk

    monkeypatch.setattr(verify.os, "read", truncating_read)
    with pytest.raises(verify.ChangedDuringRead):
        verify.read_regular_file(tmp_path, "f.bin")


# ------------------------------------------------------------ 集成：HTTP 链路


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = ServerHarness(tmp_path_factory.mktemp("verify-data"), _free_port())
    srv.start()
    yield srv
    srv.stop()


def _make_build(client, name: str, content: str, extra: str = "") -> int:
    cmd = f"mkdir -p out && printf '%s' {shlex.quote(content)} > out/fw.bin"
    if extra:
        cmd = f"{cmd} && {extra}"
    bid = submit(client, name, [step("gen", cmd)])
    detail = wait_build(client, bid)
    assert detail["status"] == "succeeded"
    return bid


def _post_verify(client, a: int, b: int, path: str = "out/fw.bin"):
    return client.post(
        "/verifications", json={"build_a": a, "build_b": b, "path": path}
    )


def test_consistent_pair_confirmed_and_queryable(server):
    a = _make_build(server.client, "v-con-a", "firmware-identical")
    b = _make_build(server.client, "v-con-b", "firmware-identical")
    r = _post_verify(server.client, a, b)
    assert r.status_code == 201, r.text
    body = r.json()
    expected = hashlib.sha256(b"firmware-identical").hexdigest()
    lo, hi = sorted((a, b))
    assert body["build_a"] == lo and body["build_b"] == hi  # 升序归一化
    assert body["path"] == "out/fw.bin"
    assert body["a_sha256"] == body["b_sha256"] == expected
    assert body["a_size"] == body["b_size"] == len(b"firmware-identical")
    assert body["consistent"] is True

    got = server.client.get(f"/verifications/{body['id']}")
    assert got.status_code == 200 and got.json() == body
    # 按构建对过滤（顺序无关）
    lst = server.client.get("/verifications", params={"build_a": b, "build_b": a})
    assert [v["id"] for v in lst.json()] == [body["id"]]
    # 单构建过滤与路径过滤
    lst = server.client.get("/verifications", params={"build_a": a})
    assert any(v["id"] == body["id"] for v in lst.json())
    lst = server.client.get("/verifications", params={"path": "out/fw.bin"})
    assert any(v["id"] == body["id"] for v in lst.json())
    assert server.client.get("/verifications/999999").status_code == 404


def test_inconsistent_pair_still_confirmed_with_digests(server):
    a = _make_build(server.client, "v-inc-a", "content-aaa")
    b = _make_build(server.client, "v-inc-b", "content-bbb")
    r = _post_verify(server.client, a, b)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["consistent"] is False
    assert body["a_sha256"] != body["b_sha256"]
    assert {body["a_sha256"], body["b_sha256"]} == {
        hashlib.sha256(b"content-aaa").hexdigest(),
        hashlib.sha256(b"content-bbb").hexdigest(),
    }


def test_concurrent_requests_confirm_exactly_one_result(server):
    a = _make_build(server.client, "v-cc-a", "race-content")
    b = _make_build(server.client, "v-cc-b", "race-content")

    def post(_):
        return _post_verify(server.client, a, b)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(post, range(8)))

    assert all(r.status_code in (200, 201) for r in responses)
    assert sum(1 for r in responses if r.status_code == 201) == 1  # 只确认一份
    ids = {r.json()["id"] for r in responses}
    assert len(ids) == 1
    bodies = {json.dumps(r.json(), sort_keys=True) for r in responses}
    assert len(bodies) == 1  # 所有并发响应内容一致
    lst = server.client.get("/verifications", params={"build_a": a, "build_b": b})
    assert len(lst.json()) == 1


def test_workdir_change_after_confirmation_does_not_rewrite(server):
    a = _make_build(server.client, "v-im-a", "original-content")
    b = _make_build(server.client, "v-im-b", "original-content")
    first = _post_verify(server.client, a, b)
    assert first.status_code == 201 and first.json()["consistent"] is True

    # 确认之后篡改一侧工作目录：结果不得被改写
    victim = server.data_dir / "workdir" / str(a) / "out" / "fw.bin"
    victim.write_text("tampered-content")

    second = _post_verify(server.client, a, b)
    assert second.status_code == 200  # 已存在，返回原确认结果
    assert second.json() == first.json()
    assert second.json()["consistent"] is True
    got = server.client.get(f"/verifications/{first.json()['id']}")
    assert got.json()["consistent"] is True
    assert got.json()["a_sha256"] == first.json()["a_sha256"]


def test_request_validation_and_state_errors(server):
    ok = _make_build(server.client, "v-err-ok", "x")
    fail_bid = submit(server.client, "v-err-fail", [step("bad", "exit 3")])
    assert wait_build(server.client, fail_bid)["status"] == "failed"

    assert _post_verify(server.client, ok, ok).status_code == 400  # 同一构建
    assert _post_verify(server.client, ok, 999999).status_code == 404  # 构建不存在
    r = _post_verify(server.client, ok, fail_bid)
    assert r.status_code == 409 and "成功" in r.json()["detail"]  # 非成功构建

    ok2 = _make_build(server.client, "v-err-ok2", "x")
    for bad_path in ("../secret", "..", "a/../../etc/passwd", "/etc/passwd"):
        r = _post_verify(server.client, ok, ok2, bad_path)
        assert r.status_code == 400, bad_path

    assert _post_verify(server.client, ok, ok2, "out/nope.bin").status_code == 404
    assert _post_verify(server.client, ok, ok2, "out").status_code == 400  # 目录


def test_symlinks_never_participate(server):
    link_setup = (
        "ln -s fw.bin out/link && ln -s /etc/hostname out/escape "
        "&& ln -s out outlink"
    )
    a = _make_build(server.client, "v-ln-a", "real-content", extra=link_setup)
    b = _make_build(server.client, "v-ln-b", "real-content", extra=link_setup)
    for p in ("out/link", "out/escape", "outlink/fw.bin"):
        r = _post_verify(server.client, a, b, p)
        assert r.status_code == 400, p
        assert "符号链接" in r.json()["detail"]
    # 符号链接请求不得留下任何已确认记录
    lst = server.client.get("/verifications", params={"build_a": a, "build_b": b})
    assert lst.json() == []


def test_verification_survives_restart(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("verify-restart")
    port = _free_port()
    srv = ServerHarness(data_dir, port)
    srv.start()
    try:
        a = _make_build(srv.client, "v-rs-a", "persist-across-restart")
        b = _make_build(srv.client, "v-rs-b", "persist-across-restart")
        r = _post_verify(srv.client, a, b)
        assert r.status_code == 201
        snapshot = r.json()
    finally:
        srv.stop()

    srv2 = ServerHarness(data_dir, port)
    srv2.start()
    try:
        got = srv2.client.get(f"/verifications/{snapshot['id']}")
        assert got.status_code == 200
        assert got.json() == snapshot  # 重启后仍可查询且内容不变
        lst = srv2.client.get("/verifications", params={"build_a": a, "build_b": b})
        assert [v["id"] for v in lst.json()] == [snapshot["id"]]
        # 构建/日志等旧行为在重启后依然正常
        assert srv2.client.get(f"/builds/{a}").json()["status"] == "succeeded"
    finally:
        srv2.stop()
