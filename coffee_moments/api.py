from __future__ import unicode_literals
import frappe
import erpnext
import json
from frappe.desk.reportview import get_match_cond, get_filters_cond
from frappe.utils import nowdate, getdate
from collections import defaultdict
from erpnext.stock.get_item_details import _get_item_tax_template
from frappe.utils import unique


@frappe.whitelist()
@frappe.validate_and_sanitize_search_inputs
def price_list_query(doctype, txt, searchfield, start, page_len, filters, as_dict=False):
	conditions = []

	return frappe.get_all("Price List", fields={name}, cached=True)