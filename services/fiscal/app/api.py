from __future__ import annotations

from uuid import UUID

import asyncpg
from fastapi import APIRouter, Query, Request, Response

from app.schemas import FiscalDocumentOut, FiscalStatus
from edd_common.api import NotFoundError
from edd_common.runtime import ServiceRuntime

router = APIRouter(prefix="/fiscal-documents", tags=["fiscal"])

FISCAL_COLUMNS = (
    "id, order_id, invoice_id, invoice_number, document_number, access_key, "
    "status, customer_id, total_cents, tax_cents, issued_at, cancelled_at, cancel_reason"
)


def _pool(request: Request) -> asyncpg.Pool:
    runtime: ServiceRuntime = request.app.state.runtime
    return runtime.assert_ready()


@router.get("", response_model=list[FiscalDocumentOut], summary="List fiscal documents")
async def list_fiscal_documents(
    request: Request,
    fiscal_status: FiscalStatus | None = Query(default=None, alias="status"),
    order_id: UUID | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict]:
    conditions: list[str] = []
    args: list[object] = []
    if fiscal_status:
        args.append(fiscal_status.value)
        conditions.append(f"status = ${len(args)}")
    if order_id:
        args.append(order_id)
        conditions.append(f"order_id = ${len(args)}")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    async with _pool(request).acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT {FISCAL_COLUMNS}
            FROM fiscal_documents
            {where}
            ORDER BY issued_at DESC
            LIMIT ${len(args) + 1}
            """,
            *args,
            limit,
        )
    return [dict(row) for row in rows]


@router.get(
    "/by-order/{order_id}",
    response_model=FiscalDocumentOut,
    summary="Get the fiscal document for an order",
)
async def get_fiscal_document_by_order(order_id: UUID, request: Request) -> dict:
    async with _pool(request).acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {FISCAL_COLUMNS} FROM fiscal_documents WHERE order_id = $1",
            order_id,
        )
    if row is None:
        raise NotFoundError(f"No fiscal document for order {order_id}")
    return dict(row)


@router.get(
    "/{document_id}/xml",
    summary="Download the simulated fiscal document XML",
    response_class=Response,
)
async def get_fiscal_document_xml(document_id: UUID, request: Request) -> Response:
    async with _pool(request).acquire() as conn:
        xml_content = await conn.fetchval(
            "SELECT xml_content FROM fiscal_documents WHERE id = $1",
            document_id,
        )
    if xml_content is None:
        raise NotFoundError(f"Fiscal document {document_id} was not found")
    return Response(
        content=xml_content,
        media_type="application/xml",
        headers={"Content-Disposition": f'attachment; filename="{document_id}.xml"'},
    )


@router.get("/{document_id}", response_model=FiscalDocumentOut, summary="Get one fiscal document")
async def get_fiscal_document(document_id: UUID, request: Request) -> dict:
    async with _pool(request).acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {FISCAL_COLUMNS} FROM fiscal_documents WHERE id = $1",
            document_id,
        )
    if row is None:
        raise NotFoundError(f"Fiscal document {document_id} was not found")
    return dict(row)
