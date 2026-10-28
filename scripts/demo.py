"""End-to-end demo. Run after `make up` and `make dev` (in another terminal).

It exercises:
  1. the happy path: order -> stock -> invoice -> fiscal -> completed,
  2. stock rejection and compensation,
  3. recommendations built from completed orders.
"""

from __future__ import annotations

import sys
import time
from typing import Any

import httpx

ORDERS = "http://localhost:8001"
STOCK = "http://localhost:8002"
INVOICES = "http://localhost:8003"
FISCAL = "http://localhost:8004"
RECOMMENDATIONS = "http://localhost:8005"

SERVICES = {
    "orders": ORDERS,
    "stock": STOCK,
    "invoices": INVOICES,
    "fiscal": FISCAL,
    "recommendations": RECOMMENDATIONS,
}


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def pretty(value: Any) -> None:
    import json

    print(json.dumps(value, indent=2, default=str))


def wait_for_services(timeout: float = 45.0) -> None:
    deadline = time.time() + timeout
    with httpx.Client(timeout=2.0) as client:
        for name, url in SERVICES.items():
            while True:
                try:
                    response = client.get(f"{url}/health")
                    if response.status_code == 200:
                        print(f"  {name:<16} ready")
                        break
                except httpx.HTTPError:
                    pass
                if time.time() > deadline:
                    raise SystemExit(
                        f"{name} did not become ready. Start the stack with `make dev`."
                    )
                time.sleep(0.5)


def wait_for_order(
    client: httpx.Client,
    order_id: str,
    statuses: set[str],
    timeout: float = 30.0,
) -> dict:
    deadline = time.time() + timeout
    last: dict = {}
    while time.time() < deadline:
        last = client.get(f"{ORDERS}/orders/{order_id}").json()
        if last.get("status") in statuses:
            return last
        time.sleep(0.3)
    pretty(last)
    raise SystemExit(f"Order {order_id} did not reach {statuses} in time")


def wait_until(description: str, check: Any, timeout: float = 20.0) -> Any:
    """Read models are eventually consistent; wait for the projection to catch up."""

    deadline = time.time() + timeout
    while time.time() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.2)
    raise SystemExit(f"Timed out waiting for {description}")


def create_order(client: httpx.Client, customer_id: str, items: list[dict]) -> dict:
    response = client.post(
        f"{ORDERS}/orders",
        json={"customer_id": customer_id, "currency": "usd", "items": items},
    )
    response.raise_for_status()
    return response.json()


def main() -> None:
    print("Checking services...")
    wait_for_services()

    with httpx.Client(timeout=10.0) as client:
        # Make the demo repeatable: add stock in case earlier runs consumed it.
        for product_id in ("p-100", "p-101"):
            client.post(f"{STOCK}/stock/{product_id}/restock", json={"quantity": 20})

        section("1. Happy path: POST /orders")
        order = create_order(
            client,
            "alice",
            [
                {"product_id": "p-100", "quantity": 1, "unit_price_cents": 12900},
                {"product_id": "p-101", "quantity": 1, "unit_price_cents": 24900},
            ],
        )
        print(f"order_id={order['id']}  status={order['status']}")
        print("The HTTP request returned immediately. Everything below happens asynchronously.")

        completed = wait_for_order(client, order["id"], {"COMPLETED", "CANCELLED"})
        print(f"\nFinal order status: {completed['status']}")
        pretty(
            {
                "id": completed["id"],
                "status": completed["status"],
                "stock_reserved_at": completed["stock_reserved_at"],
                "invoice_issued_at": completed["invoice_issued_at"],
                "fiscal_issued_at": completed["fiscal_issued_at"],
                "completed_at": completed["completed_at"],
            }
        )

        section("2. Read models produced by other services")
        # The order is complete at this point, but stock still has to consume
        # order.completed and move RESERVED -> COMMITTED. This is eventual
        # consistency in action, not a bug.
        reservation = wait_until(
            "stock to commit the reservation",
            lambda: (
                res
                if (res := client.get(f"{STOCK}/reservations/{order['id']}").json()).get("status")
                == "COMMITTED"
                else None
            ),
        )
        invoice = client.get(f"{INVOICES}/invoices/by-order/{order['id']}").json()
        fiscal = client.get(f"{FISCAL}/fiscal-documents/by-order/{order['id']}").json()
        print(f"invoice:     {invoice['invoice_number']} ({invoice['status']})")
        print(f"fiscal doc:  {fiscal['document_number']} ({fiscal['status']})")
        print(f"reservation: {reservation['status']}")
        print(f"XML endpoint: {FISCAL}/fiscal-documents/{fiscal['id']}/xml")

        section("3. Stock rejection: order more than available")
        rejected = create_order(
            client,
            "charlie",
            [{"product_id": "p-102", "quantity": 999, "unit_price_cents": 39900}],
        )
        rejected = wait_for_order(client, rejected["id"], {"COMPLETED", "CANCELLED"})
        print(f"status={rejected['status']}")
        print(f"reason={rejected['cancellation_reason']}")
        reservation = client.get(f"{STOCK}/reservations/{rejected['id']}").json()
        print(f"reservation={reservation['status']}")

        section("4. Recommendations from co-purchase history")
        # Alice bought p-100 + p-101 together, so bob buying p-100 should
        # receive p-101 as a "bought together" recommendation.
        bob_order = create_order(
            client,
            "bob",
            [{"product_id": "p-100", "quantity": 1, "unit_price_cents": 12900}],
        )
        wait_for_order(client, bob_order["id"], {"COMPLETED", "CANCELLED"})

        # Recommendation is a projection: wait until bob's purchase has been
        # consumed before asking for a recommendation.
        recommendations = wait_until(
            "the recommendations projection to catch up",
            lambda: (
                payload
                if "p-100"
                in (payload := client.get(f"{RECOMMENDATIONS}/recommendations/bob").json()).get(
                    "based_on", []
                )
                else None
            ),
        )
        pretty(recommendations)

        section("5. Cancellation and compensation")
        cancellable = create_order(
            client,
            "dave",
            [{"product_id": "p-103", "quantity": 1, "unit_price_cents": 7900}],
        )
        # Cancel immediately. The saga still has to settle: stock may reserve
        # and release, and the invoice may be issued and voided (or skipped
        # entirely if the cancellation marker wins the race).
        response = client.post(
            f"{ORDERS}/orders/{cancellable['id']}/cancel",
            json={"reason": "changed my mind"},
        )
        response.raise_for_status()
        wait_for_order(client, cancellable["id"], {"CANCELLED"})
        print(f"cancel response status={response.json()['status']}")

        def compensation_finished() -> tuple[str, str, str] | None:
            inv = client.get(f"{INVOICES}/invoices/by-order/{cancellable['id']}")
            res = client.get(f"{STOCK}/reservations/{cancellable['id']}")
            doc = client.get(f"{FISCAL}/fiscal-documents/by-order/{cancellable['id']}")
            invoice_status = inv.json()["status"] if inv.status_code == 200 else "none"
            reservation_status = res.json()["status"] if res.status_code == 200 else "none"
            fiscal_status = doc.json()["status"] if doc.status_code == 200 else "none"
            finished = (
                invoice_status in {"VOIDED", "none"}
                and reservation_status == "RELEASED"
                and fiscal_status in {"CANCELLED", "none"}
            )
            return (invoice_status, reservation_status, fiscal_status) if finished else None

        invoice_status, reservation_status, fiscal_status = wait_until(
            "the compensation flow to finish", compensation_finished
        )
        print(f"stock reservation: {reservation_status}")
        print(f"invoice:           {invoice_status}")
        print(f"fiscal document:   {fiscal_status}")
        print("Compensation is eventual: each service reacted to order.cancelled.")

        section("Where to look next")
        print("  Kafka UI:          http://localhost:8080")
        print(f"  Orders OpenAPI:    {ORDERS}/docs")
        print(f"  Stock OpenAPI:     {STOCK}/docs")
        print(f"  Invoice OpenAPI:   {INVOICES}/docs")
        print(f"  Fiscal OpenAPI:    {FISCAL}/docs")
        print(f"  Recomm. OpenAPI:   {RECOMMENDATIONS}/docs")
        print("\nDemo complete.")


if __name__ == "__main__":
    try:
        main()
    except httpx.HTTPError as exc:
        print(f"\nHTTP error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
