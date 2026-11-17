"""FastAPI 应用：路由、序列化与启动恢复。"""
from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Response

from . import __version__, config, verify
from .db import Database
from .models import (
    BuildCreate,
    BuildOut,
    CancelOut,
    LogOut,
    StepOut,
    VerificationCreate,
    VerificationOut,
)
from .scheduler import Scheduler

db = Database()
scheduler = Scheduler(db)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _step_out(row) -> StepOut:
    return StepOut(
        id=row["step_uid"],
        command=row["command"],
        depends_on=json.loads(row["depends_on"]),
        timeout_seconds=row["timeout_seconds"],
        status=row["status"],
        exit_code=row["exit_code"],
        detail=row["detail"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


async def _build_out(build_id: int) -> BuildOut:
    row = await db.get_build(build_id)
    if row is None:
        raise HTTPException(status_code=404, detail="构建不存在")
    steps = [_step_out(r) for r in await db.get_steps(build_id)]
    return BuildOut(
        id=row["id"],
        name=row["name"],
        status=row["status"],
        cancel_requested=bool(row["cancel_requested"]),
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        detail=row["detail"],
        steps=steps,
    )


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # 顺序不可颠倒：先落库连接，再清空旧构建残留并标记中断，最后才对外服务
    await db.connect()
    app.state.recovery = await scheduler.recover()
    try:
        yield
    finally:
        await db.close()


app = FastAPI(
    title="固件构建流水线 API",
    version=__version__,
    description=(
        "接收最多 12 步的 JSON DAG，在挂载工作目录中执行真实命令；"
        "全局最多两步并行，依赖全成功才启动，失败后代跳过，"
        "超时/取消终止整个进程组，支持逐步状态与日志查询。"
        "另提供产物核对：对两个成功构建的同一相对路径计算大小与 SHA-256，"
        "给出一致结论并持久化，同一构建对同一路径只确认一份结果。"
    ),
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "version": __version__,
        "max_steps": config.MAX_STEPS,
        "max_parallel": config.MAX_PARALLEL,
        "recovery": getattr(app.state, "recovery", None),
    }


@app.post("/builds", response_model=BuildOut, status_code=201)
async def create_build(payload: BuildCreate) -> BuildOut:
    try:
        build_id = await scheduler.submit(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return await _build_out(build_id)


@app.get("/builds", response_model=list[BuildOut])
async def list_builds(limit: int = Query(100, ge=1, le=500)) -> list[BuildOut]:
    rows = await db.list_builds(limit)
    return [await _build_out(r["id"]) for r in rows]


@app.get("/builds/{build_id}", response_model=BuildOut)
async def get_build(build_id: int) -> BuildOut:
    return await _build_out(build_id)


@app.post("/builds/{build_id}/cancel", response_model=CancelOut)
async def cancel_build(build_id: int) -> CancelOut:
    row, message = await scheduler.cancel(build_id)
    if row is None:
        raise HTTPException(status_code=404, detail="构建不存在")
    # 重新读取以拿到 cancel_requested 的真实落库值（终态上的重复取消不会置位）
    fresh = await db.get_build(build_id)
    return CancelOut(
        id=row["id"],
        status=fresh["status"],
        cancel_requested=bool(fresh["cancel_requested"]),
        detail=message,
    )


@app.get("/builds/{build_id}/steps/{step_id}/logs", response_model=LogOut)
async def get_step_log(
    build_id: int,
    step_id: str,
    tail: int = Query(0, ge=0, description="只返回最后 N 行，0 表示返回全部（受字节上限约束）"),
) -> LogOut:
    step = await db.get_step(build_id, step_id)
    if step is None:
        raise HTTPException(status_code=404, detail="构建或步骤不存在")
    path = Path(step["log_path"])
    if not path.exists():
        return LogOut(build_id=build_id, step_id=step_id, status=step["status"],
                      truncated=False, content="")
    # 在线程池读文件，避免大日志阻塞事件循环
    raw = await asyncio.to_thread(path.read_bytes)
    truncated = False
    if len(raw) > config.LOG_MAX_BYTES:
        raw = raw[-config.LOG_MAX_BYTES:]
        truncated = True
    content = raw.decode("utf-8", errors="replace")
    if tail > 0:
        lines = content.splitlines()[-tail:]
        content = "\n".join(lines)
    return LogOut(
        build_id=build_id,
        step_id=step_id,
        status=step["status"],
        truncated=truncated,
        content=content,
    )


# ---------------------------------------------------------------- 产物核对


def _verification_out(row) -> VerificationOut:
    # 响应按构建号升序归一化：build_a 恒为较小号，a_* 为其摘要
    return VerificationOut(
        id=row["id"],
        build_a=row["build_lo"],
        build_b=row["build_hi"],
        path=row["path"],
        a_size=row["lo_size"],
        a_sha256=row["lo_sha256"],
        b_size=row["hi_size"],
        b_sha256=row["hi_sha256"],
        consistent=bool(row["consistent"]),
        created_at=row["created_at"],
    )


async def _read_side(build_id: int, rel: str) -> tuple[int, str]:
    """读取一侧产物并返回 (大小, sha256)；所有失败都转成明确的 HTTP 错误，
    且保证不落库（读取期间变化/读取失败均不确认结果）。"""
    workdir = config.WORKDIR_ROOT / str(build_id)
    try:
        return await asyncio.to_thread(verify.read_regular_file, workdir, rel)
    except verify.SymlinkError as exc:
        raise HTTPException(status_code=400, detail=f"构建 {build_id}: {exc}") from exc
    except verify.NotRegularFileError as exc:
        raise HTTPException(status_code=400, detail=f"构建 {build_id}: {exc}") from exc
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"构建 {build_id} 工作目录内不存在该文件: {rel}",
        ) from exc
    except NotADirectoryError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"构建 {build_id}: 路径中间分量不是目录: {rel}",
        ) from exc
    except verify.ChangedDuringRead as exc:
        raise HTTPException(
            status_code=409, detail=f"构建 {build_id}: {exc}"
        ) from exc
    except OSError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"构建 {build_id}: 文件读取失败({exc})，未确认结果",
        ) from exc


@app.post("/verifications", response_model=VerificationOut, status_code=201)
async def create_verification(
    payload: VerificationCreate, response: Response
) -> VerificationOut:
    """核对两个成功构建的同一相对路径产物。

    - 两个构建号必须不同且均为 ``succeeded``；
    - 路径必须是工作目录内的相对路径，越界路径与符号链接一律拒绝；
    - 任一文件在读取期间变化或读取失败时不确认结果（不落库）；
    - 同一构建对同一路径只确认一份：并发/重复请求返回已确认的结果，
    后续工作目录变化不会改写它。
    """
    if payload.build_a == payload.build_b:
        raise HTTPException(status_code=400, detail="两个构建号必须不同")
    build_lo, build_hi = sorted((payload.build_a, payload.build_b))
    try:
        rel = verify.normalize_rel_path(payload.path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    for bid in (build_lo, build_hi):
        row = await db.get_build(bid)
        if row is None:
            raise HTTPException(status_code=404, detail=f"构建 {bid} 不存在")
        if row["status"] != "succeeded":
            raise HTTPException(
                status_code=409,
                detail=f"构建 {bid} 状态为 {row['status']}，仅成功构建可参与核对",
            )

    # 已确认过的结果直接返回，不再触碰工作目录（后续变化不得改写结论）
    existing = await db.get_verification_by_key(build_lo, build_hi, rel)
    if existing is not None:
        response.status_code = 200
        return _verification_out(existing)

    lo_size, lo_sha = await _read_side(build_lo, rel)
    hi_size, hi_sha = await _read_side(build_hi, rel)
    consistent = lo_size == hi_size and lo_sha == hi_sha

    row = await db.create_verification(
        build_lo, build_hi, rel,
        lo_size, lo_sha, hi_size, hi_sha, consistent, _now(),
    )
    if row is None:
        # 并发请求已抢先确认同一构建对：返回那一份，保证全库唯一
        row = await db.get_verification_by_key(build_lo, build_hi, rel)
        assert row is not None
        response.status_code = 200
    return _verification_out(row)


@app.get("/verifications", response_model=list[VerificationOut])
async def list_verifications(
    build_a: Optional[int] = Query(None, ge=1),
    build_b: Optional[int] = Query(None, ge=1),
    path: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
) -> list[VerificationOut]:
    """查询核对结果。build_a/build_b 同时给出时按构建对过滤（顺序无关），
    只给一个时过滤该构建参与的所有核对。"""
    pair: Optional[tuple[int, int]] = None
    single: Optional[int] = None
    if build_a is not None and build_b is not None:
        if build_a == build_b:
            raise HTTPException(status_code=400, detail="两个构建号必须不同")
        pair = tuple(sorted((build_a, build_b)))
    elif build_a is not None:
        single = build_a
    elif build_b is not None:
        single = build_b
    norm_path: Optional[str] = None
    if path is not None:
        try:
            norm_path = verify.normalize_rel_path(path)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    rows = await db.list_verifications(pair=pair, single=single, path=norm_path, limit=limit)
    return [_verification_out(r) for r in rows]


@app.get("/verifications/{verification_id}", response_model=VerificationOut)
async def get_verification(verification_id: int) -> VerificationOut:
    row = await db.get_verification(verification_id)
    if row is None:
        raise HTTPException(status_code=404, detail="核对记录不存在")
    return _verification_out(row)
