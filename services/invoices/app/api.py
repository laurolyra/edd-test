from __future__ import annotations

from uuid import UUID

import asyncpg
from fastapi import APIRouter, Query, Request

from app.schemas import InvoiceListOut, InvoiceOut, InvoiceStatus
from edd_common.api import NotFoundError
from edd_common.runtime import ServiceRuntime

router = APIRouter(prefix="/invoices", tags=["invoices"])

INVOICE_COLUMNS = (
    "id, order_id, invoice_number, customer_id, total_cents, currency, "
    "status, items, issued_at, voided_at, void_reason"
)


def _pool(request: Request) -> asyncpg.Pool:
    runtime: ServiceRuntime = request.app.state.runtime
    return runtime.assert_ready()


@router.get("", response_model=InvoiceListOut, summary="List invoices")
async def list_invoices(
    request: Request,
    invoice_status: InvoiceStatus | None = Query(default=None, alias="status"),
    order_id: UUID | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict:
    conditions: list[str] = []
    args: list[object] = []
    if invoice_status:
        args.append(invoice_status.value)
        conditions.append(f"status = ${len(args)}")
    if order_id:
        args.append(order_id)
        conditions.append(f"order_id = ${len(args)}")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    async with _pool(request).acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT {INVOICE_COLUMNS}
            FROM invoices
            {where}
            ORDER BY issued_at DESC
            LIMIT ${len(args) + 1}
            """,
            *args,
            limit,
        )
    invoices = [_invoice_dict(row) for row in rows]
    return {"items": invoices, "count": len(invoices)}


@router.get(
    "/by-order/{order_id}",
    response_model=InvoiceOut,
    summary="Get the invoice for an order",
)
async def get_invoice_by_order(order_id: UUID, request: Request) -> dict:
    async with _pool(request).acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {INVOICE_COLUMNS} FROM invoices WHERE order_id = $1",
            order_id,
        )
    if row is None:
        raise NotFoundError(f"No invoice for order {order_id}")
    return _invoice_dict(row)


@router.get("/{invoice_id}", response_model=InvoiceOut, summary="Get one invoice")
async def get_invoice(invoice_id: UUID, request: Request) -> dict:
    async with _pool(request).acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {INVOICE_COLUMNS} FROM invoices WHERE id = $1",
            invoice_id,
        )
    if row is None:
        raise NotFoundError(f"Invoice {invoice_id} was not found")
    return _invoice_dict(row)


def _invoice_dict(row: asyncpg.Record) -> dict:
    result = dict(row)
    result["items"] = [dict(item) for item in row["items"]]
    return result
