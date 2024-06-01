from dataclasses import asdict
from typing import Final
from jinja2 import Template, Environment, FileSystemLoader

from invoice_service import Invoice


def number_format(price: int) -> str:
    return "{:,}".format(price)


class HtmlService():
    def __init__(self):
        self._env: Final = Environment(loader=FileSystemLoader("./templates"))
        self._env.filters["number_format"] = number_format

    def render_invoice(self, invoice: Invoice, body) -> str:
        #template = self._env.get_template("invoice.html")
        template = self._env.from_string(source=body)
        params = asdict(invoice)
        html = template.render(params)
        return html
