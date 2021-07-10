# Copyright (c) 2013, Bantoo and Saudi BTI and contributors
# For license information, please see license.txt

from __future__ import unicode_literals

import frappe
import datetime
from frappe.utils import flt, cint, getdate, now, date_diff
from erpnext.stock.utils import update_included_uom_in_report, is_reposting_item_valuation_in_progress
from frappe import _
from erpnext.stock.doctype.serial_no.serial_no import get_serial_nos
from six import iteritems

def execute(filters=None):
	is_reposting_item_valuation_in_progress()
	include_uom = filters.get("include_uom")
	columns = get_columns()
	items = get_items(filters)
	sl_entries = get_stock_ledger_entries(filters, items)
	slee = get_stock_ledger_entries2(filters, items)
	item_details = get_item_details(items, sl_entries, include_uom, filters)
	opening_row = get_opening_balance(filters, columns)
	precision = cint(frappe.db.get_single_value("System Settings", "float_precision"))
	
	
	if filters.price_list:
		prices = frappe.get_all('Item Price', filters={'selling': 1, 'buying': 0, 'price_list': filters.price_list}, fields=['item_code', 'price_list_rate', 'price_list'])
	else:
		prices = frappe.get_all('Item Price', filters={'selling': 1, 'buying': 0, 'price_list': "Standard Selling"}, fields=['item_code', 'price_list_rate', 'price_list'])
	

	data = []
	conversion_factors = []
	if opening_row:
		data.append(opening_row)

	actual_qty = stock_value = 0

	iwb_map = get_item_warehouse_map(filters, slee)
	item_map = get_item_details2(items, slee, filters)

	data2 = {}
	conversion_factors = {}

	_func = lambda x: x[1]
	qty_dict = {}
	for (company, item, warehouse) in sorted(iwb_map):
		if item_map.get(item):
			qty_dict[(company, item, warehouse)] = iwb_map[(company, item, warehouse)]

			report_data = {
				'item_code': item,
				'warehouse': warehouse,
				'company': company,
			}
			report_data.update(qty_dict)

	available_serial_nos = {}
	for sle in sl_entries:
		item_detail = item_details[sle.item_code]
		sle.update(item_detail)

		if filters.get("batch_no"):
			actual_qty += flt(sle.actual_qty, precision)
			stock_value += sle.stock_value_difference

			if sle.voucher_type == 'Stock Reconciliation' and not sle.actual_qty:
				actual_qty = sle.qty_after_transaction
				stock_value = sle.stock_value

			sle.update({
				"qty_after_transaction": actual_qty,
				"stock_value": stock_value
			})

		sle.update({
			"in_qty": max(sle.actual_qty, 0),
			"out_qty": min(sle.actual_qty, 0),
			"opening_qty": report_data[(sle.company, sle.item_code, sle.warehouse)].opening_qty,
			"opening_val": report_data[(sle.company, sle.item_code, sle.warehouse)].opening_val,
			"bal_qty": report_data[(sle.company, sle.item_code, sle.warehouse)].bal_qty,
			"bal_val": report_data[(sle.company, sle.item_code, sle.warehouse)].bal_val
		})
		
		for price in prices:
			if price.item_code == sle.item_code:
				sle.update({"price": price.price_list_rate})
		
		sle.setdefault('price', 0)
	

		if sle.serial_no:
			update_available_serial_nos(available_serial_nos, sle)				
		data.append(sle)
			
		if include_uom:
			conversion_factors.append(item_detail.conversion_factor)
		

	update_included_uom_in_report(columns, data, include_uom, conversion_factors)
	return columns, data

def get_conditions(filters):
	conditions = ""
	if not filters.get("from_date"):
		frappe.throw(_("'From Date' is required"))

	if filters.get("to_date"):
		conditions += " and sle.posting_date <= %s" % frappe.db.escape(filters.get("to_date"))
	else:
		frappe.throw(_("'To Date' is required"))

	if filters.get("company"):
		conditions += " and sle.company = %s" % frappe.db.escape(filters.get("company"))

	if filters.get("warehouse"):
		warehouse_details = frappe.db.get_value("Warehouse",
			filters.get("warehouse"), ["lft", "rgt"], as_dict=1)
		if warehouse_details:
			conditions += " and exists (select name from `tabWarehouse` wh \
				where wh.lft >= %s and wh.rgt <= %s and sle.warehouse = wh.name)"%(warehouse_details.lft,
				warehouse_details.rgt)

	if filters.get("warehouse_type") and not filters.get("warehouse"):
		conditions += " and exists (select name from `tabWarehouse` wh \
			where wh.warehouse_type = '%s' and sle.warehouse = wh.name)"%(filters.get("warehouse_type"))

	return conditions

def get_stock_ledger_entries2(filters, items):
	item_conditions_sql = ''
	if items:
		item_conditions_sql = ' and sle.item_code in ({})'\
			.format(', '.join(frappe.db.escape(i, percent=False) for i in items))

	conditions = get_conditions(filters)

	return frappe.db.sql("""
		select
			sle.item_code, warehouse, sle.posting_date, sle.actual_qty, sle.valuation_rate,
			sle.company, sle.voucher_type, sle.qty_after_transaction, sle.stock_value_difference,
			sle.item_code as name, sle.voucher_no, sle.stock_value, sle.batch_no
		from
			`tabStock Ledger Entry` sle force index (posting_sort_index)
		where sle.docstatus < 2 %s %s
		and is_cancelled = 0
		order by sle.posting_date, sle.posting_time, sle.creation, sle.actual_qty""" % #nosec
		(item_conditions_sql, conditions), as_dict=1)

def get_item_details2(items, sle, filters):
	item_details = {}
	if not items:
		items = list(set(d.item_code for d in sle))

	if not items:
		return item_details

	cf_field = cf_join = ""
	if filters.get("include_uom"):
		cf_field = ", ucd.conversion_factor"
		cf_join = "left join `tabUOM Conversion Detail` ucd on ucd.parent=item.name and ucd.uom=%s" \
			% frappe.db.escape(filters.get("include_uom"))

	res = frappe.db.sql("""
		select
			item.name, item.item_name, item.description, item.item_group, item.brand, item.stock_uom %s
		from
			`tabItem` item
			%s
		where
			item.name in (%s)
	""" % (cf_field, cf_join, ','.join(['%s'] *len(items))), items, as_dict=1)

	for item in res:
		item_details.setdefault(item.name, item)

	if filters.get('show_variant_attributes', 0) == 1:
		variant_values = get_variant_values_for(list(item_details))
		item_details = {k: v.update(variant_values.get(k, {})) for k, v in iteritems(item_details)}

	return item_details

def get_item_warehouse_map(filters, sle):
	iwb_map = {}
	from_date = getdate(filters.get("from_date"))
	to_date = getdate(filters.get("to_date"))

	float_precision = cint(frappe.db.get_default("float_precision")) or 3

	for d in sle:
		key = (d.company, d.item_code, d.warehouse)
		if key not in iwb_map:
			iwb_map[key] = frappe._dict({
				"opening_qty": 0.0, "opening_val": 0.0,
				"in_qty": 0.0, "in_val": 0.0,
				"out_qty": 0.0, "out_val": 0.0,
				"bal_qty": 0.0, "bal_val": 0.0,
				"val_rate": 0.0
			})

		qty_dict = iwb_map[(d.company, d.item_code, d.warehouse)]

		if d.voucher_type == "Stock Reconciliation" and not d.batch_no:
			qty_diff = flt(d.qty_after_transaction) - flt(qty_dict.bal_qty)
		else:
			qty_diff = flt(d.actual_qty)

		value_diff = flt(d.stock_value_difference)

		if d.posting_date < from_date:
			qty_dict.opening_qty += qty_diff
			qty_dict.opening_val += value_diff

		elif d.posting_date >= from_date and d.posting_date <= to_date:
			if flt(qty_diff, float_precision) >= 0:
				qty_dict.in_qty += qty_diff
				qty_dict.in_val += value_diff
			else:
				qty_dict.out_qty += abs(qty_diff)
				qty_dict.out_val += abs(value_diff)

		qty_dict.val_rate = d.valuation_rate
		qty_dict.bal_qty += qty_diff
		qty_dict.bal_val += value_diff

	iwb_map = filter_items_with_no_transactions(iwb_map, float_precision)

	return iwb_map

def filter_items_with_no_transactions(iwb_map, float_precision):
	for (company, item, warehouse) in sorted(iwb_map):
		qty_dict = iwb_map[(company, item, warehouse)]

		no_transactions = True
		for key, val in iteritems(qty_dict):
			val = flt(val, float_precision)
			qty_dict[key] = val
			if key != "val_rate" and val:
				no_transactions = False

		if no_transactions:
			iwb_map.pop((company, item, warehouse))

	return iwb_map



def update_available_serial_nos(available_serial_nos, sle):
	serial_nos = get_serial_nos(sle.serial_no)
	key = (sle.item_code, sle.warehouse)
	if key not in available_serial_nos:
		available_serial_nos.setdefault(key, [])

	existing_serial_no = available_serial_nos[key]
	for sn in serial_nos:
		if sle.actual_qty > 0:
			if sn in existing_serial_no:
				existing_serial_no.remove(sn)
			else:
				existing_serial_no.append(sn)
		else:
			if sn in existing_serial_no:
				existing_serial_no.remove(sn)
			else:
				existing_serial_no.append(sn)

	sle.balance_serial_no = '\n'.join(existing_serial_no)

def get_columns():
	"""
		"opening_qty": report_data[(sle.company, sle.item_code, sle.warehouse)].opening_qty,
		"opening_val": report_data[(sle.company, sle.item_code, sle.warehouse)].opening_val,
		"bal_qty": report_data[(sle.company, sle.item_code, sle.warehouse)].bal_qty,
		"bal_val": report_data[(sle.company, sle.item_code, sle.warehouse)].bal_val
	"""
	columns = [
		{"label": _("Date"), "fieldname": "date", "fieldtype": "Datetime", "width": 150},
		{"label": _("Item"), "fieldname": "item_code", "fieldtype": "Link", "options": "Item", "width": 100},
		{"label": _("Item Name"), "fieldname": "item_name", "width": 100},
		{"label": _("Stock UOM"), "fieldname": "stock_uom", "fieldtype": "Link", "options": "UOM", "width": 90},
		{"label": _("In Qty"), "fieldname": "in_qty", "fieldtype": "Float", "width": 80, "convertible": "qty"},
		{"label": _("Out Qty"), "fieldname": "out_qty", "fieldtype": "Float", "width": 80, "convertible": "qty"},
		{"label": _("Balance Qty"), "fieldname": "qty_after_transaction", "fieldtype": "Float", "width": 100, "convertible": "qty"},
		{"label": _("Selling Price"), "fieldname": "price", "fieldtype": "Currency", "width": 110, "options": "Company:company:default_currency", "convertible": "rate"},
		{"label": _("Voucher #"), "fieldname": "voucher_no", "fieldtype": "Dynamic Link", "options": "voucher_type", "width": 150},
		{"label": _("Warehouse"), "fieldname": "warehouse", "fieldtype": "Link", "options": "Warehouse", "width": 150},
		{"label": _("Item Group"), "fieldname": "item_group", "fieldtype": "Link", "options": "Item Group", "width": 100},
		{"label": _("Brand"), "fieldname": "brand", "fieldtype": "Link", "options": "Brand", "width": 100},
		{"label": _("Description"), "fieldname": "description", "width": 200},
		{"label": _("Incoming Rate"), "fieldname": "incoming_rate", "fieldtype": "Currency", "width": 110, "options": "Company:company:default_currency", "convertible": "rate"},
		{"label": _("Valuation Rate"), "fieldname": "valuation_rate", "fieldtype": "Currency", "width": 110, "options": "Company:company:default_currency", "convertible": "rate"},
		{"label": _("Balance Value"), "fieldname": "stock_value", "fieldtype": "Currency", "width": 110, "options": "Company:company:default_currency"},
		{"label": _("WH Opening Qty"), "fieldname": "opening_qty", "fieldtype": "Float", "width": 100, "convertible": "qty"},
		{"label": _("WH Opening Value"), "fieldname": "opening_val", "fieldtype": "Float", "width": 100, "convertible": "qty"},
		{"label": _("WH Balance Qty"), "fieldname": "bal_qty", "fieldtype": "Float", "width": 100, "convertible": "qty"},
		{"label": _("WH Balance Value"), "fieldname": "bal_val", "fieldtype": "Float", "width": 100, "convertible": "qty"},
		{"label": _("Voucher Type"), "fieldname": "voucher_type", "width": 110},
		{"label": _("Voucher #"), "fieldname": "voucher_no", "fieldtype": "Dynamic Link", "options": "voucher_type", "width": 100},
		{"label": _("Batch"), "fieldname": "batch_no", "fieldtype": "Link", "options": "Batch", "width": 100},
		#{"label": _("Serial No"), "fieldname": "serial_no", "fieldtype": "Link", "options": "Serial No", "width": 100},
		#{"label": _("Balance Serial No"), "fieldname": "balance_serial_no", "width": 100},
		#{"label": _("Project"), "fieldname": "project", "fieldtype": "Link", "options": "Project", "width": 100},
		#{"label": _("Company"), "fieldname": "company", "fieldtype": "Link", "options": "Company", "width": 110}
	]

	return columns


def get_stock_ledger_entries(filters, items):
	item_conditions_sql = ''
	if items:
		item_conditions_sql = 'and sle.item_code in ({})'\
			.format(', '.join(frappe.db.escape(i) for i in items))

	sl_entries = frappe.db.sql("""
		SELECT
			concat_ws(" ", posting_date, posting_time) AS date,
			item_code,
			warehouse,
			actual_qty,
			qty_after_transaction,
			incoming_rate,
			valuation_rate,
			stock_value,
			voucher_type,
			voucher_no,
			batch_no,
			serial_no,
			company,
			project,
			stock_value_difference
		FROM
			`tabStock Ledger Entry` sle
		WHERE
			company = %(company)s
				AND is_cancelled = 0 AND posting_date BETWEEN %(from_date)s AND %(to_date)s
				{sle_conditions}
				{item_conditions_sql}
		ORDER BY
			posting_date asc, posting_time asc, creation asc
		""".format(sle_conditions=get_sle_conditions(filters), item_conditions_sql=item_conditions_sql),
		filters, as_dict=1)

	return sl_entries


def get_items(filters):
	conditions = []
	if filters.get("item_code"):
		conditions.append("item.name=%(item_code)s")
	else:
		if filters.get("brand"):
			conditions.append("item.brand=%(brand)s")
		if filters.get("item_group"):
			conditions.append(get_item_group_condition(filters.get("item_group")))

	items = []
	if conditions:
		items = frappe.db.sql_list("""select name from `tabItem` item where {}"""
			.format(" and ".join(conditions)), filters)
	return items


def get_item_details(items, sl_entries, include_uom, filters):
	item_details = {}
	if not items:
		items = list(set(d.item_code for d in sl_entries))

	if not items:
		return item_details

	cf_field = cf_join = ""
	if include_uom:
		cf_field = ", ucd.conversion_factor"
		cf_join = "left join `tabUOM Conversion Detail` ucd on ucd.parent=item.name and ucd.uom=%s" \
			% frappe.db.escape(include_uom)

	res = frappe.db.sql("""
		select
			item.name, item.item_name, item.description, item.item_group, item.brand, item.stock_uom {cf_field}
		from
			`tabItem` item
			{cf_join}
		where
			item.name in ({item_codes})
	""".format(cf_field=cf_field, cf_join=cf_join, item_codes=','.join(['%s'] *len(items))), items, as_dict=1)
	
	for item in res:
		item_details.setdefault(item.name, item)

	return item_details


def get_sle_conditions(filters):
	conditions = []
	if filters.get("warehouse"):
		warehouse_condition = get_warehouse_condition(filters.get("warehouse"))
		if warehouse_condition:
			conditions.append(warehouse_condition)
	if filters.get("voucher_no"):
		conditions.append("voucher_no=%(voucher_no)s")
	if filters.get("batch_no"):
		conditions.append("batch_no=%(batch_no)s")
	if filters.get("project"):
		conditions.append("project=%(project)s")

	return "and {}".format(" and ".join(conditions)) if conditions else ""


def get_opening_balance(filters, columns):
	if not (filters.item_code and filters.warehouse and filters.from_date):
		return

	from erpnext.stock.stock_ledger import get_previous_sle
	last_entry = get_previous_sle({
		"item_code": filters.item_code,
		"warehouse_condition": get_warehouse_condition(filters.warehouse),
		"posting_date": filters.from_date,
		"posting_time": "00:00:00"
	})

	row = {
		"item_code": _("'Opening'"),
		"qty_after_transaction": last_entry.get("qty_after_transaction", 0),
		"valuation_rate": last_entry.get("valuation_rate", 0),
		"stock_value": last_entry.get("stock_value", 0)
	}

	return row


def get_warehouse_condition(warehouse):
	warehouse_details = frappe.db.get_value("Warehouse", warehouse, ["lft", "rgt"], as_dict=1)
	if warehouse_details:
		return " exists (select name from `tabWarehouse` wh \
			where wh.lft >= %s and wh.rgt <= %s and warehouse = wh.name)"%(warehouse_details.lft,
			warehouse_details.rgt)

	return ''


def get_item_group_condition(item_group):
	item_group_details = frappe.db.get_value("Item Group", item_group, ["lft", "rgt"], as_dict=1)
	if item_group_details:
		return "item.item_group in (select ig.name from `tabItem Group` ig \
			where ig.lft >= %s and ig.rgt <= %s and item.item_group = ig.name)"%(item_group_details.lft,
			item_group_details.rgt)

	return ''
