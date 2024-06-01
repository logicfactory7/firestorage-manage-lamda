from dataclasses import dataclass


@dataclass()
class Item:
    name: str
    price: int
    quantity: int


@dataclass()
class Customer:
    name: str
    postal_code: str
    address: str


@dataclass()
class Company:
    name: str
    postal_code: str
    address: str
    invoice_number: int
    registration_number: str
    bank_name: str
    exp: str
    url: str


@dataclass()
class Invoice:
    issued_date: str
    customer: Customer
    company: Company
    items: list[Item]
    total: int


class InvoiceService:

    def get(self) -> Invoice:
        customer = Customer(
            name="株式会社サンプル",
            postal_code="123-4567",
            address="東京都新宿区1-2-3"
        )
        company = Company(
            name="ロジックファクトリー株式会社",
            postal_code="153-0052",
            address="東京都目黒区祐天寺2-8-8",
            invoice_number=123,
            registration_number="ABC123",
            bank_name="サンプル銀行",
            exp="サンプル支店",
            url="https://firestorage.jp"
        )
        items = [
            Item(name="サンプル商品1", price=1000, quantity=2),
            Item(name="サンプル商品2", price=2000, quantity=1)
        ]
        total = sum(item.price * item.quantity for item in items)
        return Invoice(
            issued_date="2024年4月1日",
            customer=customer,
            company=company,
            items=items,
            total=total
        )
