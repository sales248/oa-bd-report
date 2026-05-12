"""
OA BD Weekly Report Updater
============================
Fetches HubSpot data for the current report week and patches bd-weekly-report.html.

Run manually:
    python update_weekly_report.py

Discover HubSpot contact properties (run once to find your property names):
    python update_weekly_report.py --discover

Requirements:
    pip install requests

Environment variable required:
    HUBSPOT_TOKEN   Your HubSpot Private App token
"""

import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta

import requests

# ── CONFIG ────────────────────────────────────────────────────────────────────

HUBSPOT_TOKEN  = os.environ.get("HUBSPOT_TOKEN", "")
BASE_URL       = "https://api.hubapi.com"
HEADERS        = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}

BD_PIPELINE_ID = "68218158"
PORTAL_ID      = "44390857"
MANILA_TZ      = timezone(timedelta(hours=8))

HTML_FILE      = os.path.join(os.path.dirname(__file__), "bd-weekly-report.html")

# ── HubSpot contact property names (confirmed via API schema discovery) ───────
PROP_LEAD_CATEGORY = "lead_category"   # Source/type of BD contact
PROP_VALIDITY      = "lead_validity"   # Lead validity classification
PROP_LEAD_STATUS   = "hs_lead_status"  # "CONNECTED" means lead replied/engaged

# "BD Lead" is the internal value for the "OA Business Development" lead category.
# Only these contacts are counted for weekly reporting.
OA_BD_VALUE     = "BD Lead"
OA_BD_VALUES    = {OA_BD_VALUE}

# Kept for top-50 pool and paid-deals (broader BD universe)
INBOUND_VALUES  = {"BD Lead"}
OUTBOUND_VALUES = {"OA Outbound", "BizDev Outbound", "Outbound Enterprise", "Duane Test"}
SP_VALUES       = {"SP Lead", "SP Lead Enterprise"}
ALL_BD_VALUES   = INBOUND_VALUES | OUTBOUND_VALUES | SP_VALUES | {"Inbound Transfer"}

# lead_validity values
VALID_STRICT_VALUES  = {"Valid"}
VALID_NI_VALUES      = {"Valid - Not Interested", "Valid - Unreachable"}
SPAM_VALUES          = {"Invalid - Spam"}
JOBSEEKER_VALUES     = {"Invalid - Jobseeker"}
SP_INVALID_VALUES    = {"Invalid - Service Provider", "Invalid - Potential SP"}
UNQUALIFIED_VALUES   = {"Invalid - Unqualified"}

CONNECTED_VALUE = "CONNECTED"  # hs_lead_status value

# Stage IDs
STAGES = {
    "deal_created":      "132946329",
    "dc_outreach":       "244709522",
    "dc_completed":      "222405237",
    "dc_no_show":        "132946331",
    "ac_outreach":       "244520495",
    "ac_no_show":        "133003872",
    "ac_completed":      "132946333",
    "cd_main":           "1029860491",
    "cd_scheduled":      "1053002936",
    "cd_no_show":        "1053002935",
    "cd_completed":      "1053002937",
    "hiring_recruiting": "133348729",
    "closed_won":        "132946334",
    "closed_lost":       "132946335",
    "deal_unqualified":  "991351894",
}

STAGE_LABELS = {
    STAGES["dc_completed"]:      "DC Completed",
    STAGES["dc_no_show"]:        "DC No Show",
    STAGES["ac_outreach"]:       "AC Outreach",
    STAGES["ac_completed"]:      "AC Completed",
    STAGES["ac_no_show"]:        "AC No Show",
    STAGES["cd_main"]:           "CD Main",
    STAGES["cd_scheduled"]:      "CD Scheduled",
    STAGES["cd_no_show"]:        "CD No Show",
    STAGES["cd_completed"]:      "CD Completed",
    STAGES["hiring_recruiting"]:  "Hiring & Recruiting",
    STAGES["closed_won"]:        "Closed Won",
    STAGES["closed_lost"]:       "Closed Lost",
    STAGES["deal_unqualified"]:  "Deal Unqualified",
    STAGES["deal_created"]:      "Deal Created",
}

ACTIVE_STAGES = {v for k, v in STAGES.items()
                 if k not in ("closed_won", "closed_lost", "deal_unqualified")}

# Stages that indicate DC / AC was attended, or Paid Fee reached
DC_ATTENDED_STAGES = frozenset({
    STAGES["dc_completed"],
    STAGES["ac_outreach"], STAGES["ac_no_show"], STAGES["ac_completed"],
    STAGES["cd_main"], STAGES["cd_scheduled"], STAGES["cd_no_show"], STAGES["cd_completed"],
    STAGES["hiring_recruiting"], STAGES["closed_won"],
})
AC_ATTENDED_STAGES = frozenset({
    STAGES["ac_completed"],
    STAGES["cd_main"], STAGES["cd_scheduled"], STAGES["cd_no_show"], STAGES["cd_completed"],
    STAGES["hiring_recruiting"], STAGES["closed_won"],
})
PAID_FEE_STAGES = frozenset({
    STAGES["hiring_recruiting"],
    STAGES["closed_won"],
})

# ── VALIDITY / SOURCE NORMALISATION ──────────────────────────────────────────

def norm_validity(raw):
    v = (raw or "").strip()
    if v in VALID_STRICT_VALUES:  return "valid_strict"
    if v in VALID_NI_VALUES:      return "valid_ni"
    if v in SPAM_VALUES:          return "spam"
    if v in JOBSEEKER_VALUES:     return "jobseeker"
    if v in SP_INVALID_VALUES:    return "service_provider"
    if v in UNQUALIFIED_VALUES:   return "unqualified"
    return "no_validity"

def norm_source(lead_cat):
    v = (lead_cat or "").strip()
    if v in INBOUND_VALUES:  return "inbound"
    if v in OUTBOUND_VALUES: return "outbound"
    if v in SP_VALUES:       return "sp"
    return "other"

def is_connected(lead_status):
    return (lead_status or "").strip() == CONNECTED_VALUE

# ── DATE HELPERS ──────────────────────────────────────────────────────────────

def current_week_range():
    """Return (start_dt, end_dt, week_num, label_dates) for this report week.
    Week ends today (Tuesday) and starts 6 days ago (Wednesday) in Manila time."""
    today = datetime.now(MANILA_TZ)
    end   = today.replace(hour=23, minute=59, second=59, microsecond=0)
    start = (today - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    week_num = start.isocalendar()[1]

    mo = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    if start.month == end.month:
        dates_label = f"{mo[start.month-1]} {start.day}–{end.day}"
    else:
        dates_label = f"{mo[start.month-1]} {start.day}–{mo[end.month-1]} {end.day}"

    return start, end, week_num, dates_label

def to_iso(dt):
    return dt.isoformat()

# ── HUBSPOT HELPERS ──────────────────────────────────────────────────────────

def search_contacts(filters, properties, limit=100):
    """Single contacts search call."""
    r = requests.post(
        f"{BASE_URL}/crm/v3/objects/contacts/search",
        headers=HEADERS,
        json={"filterGroups": [{"filters": filters}], "properties": properties, "limit": limit}
    )
    r.raise_for_status()
    return r.json()

def _search_one_batch(filter_groups, properties):
    """Single paginated contacts search (≤5 filter groups per HubSpot limit)."""
    results, after = [], None
    while True:
        body = {"filterGroups": filter_groups, "properties": properties, "limit": 100}
        if after:
            body["after"] = after
        r = requests.post(f"{BASE_URL}/crm/v3/objects/contacts/search", headers=HEADERS, json=body)
        if not r.ok:
            print(f"  [WARN] contacts search {r.status_code}: {r.text[:200]}")
            break
        data = r.json()
        results.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return results

def search_contacts_by_categories(cat_values, shared_filters, properties):
    """Search contacts matching any lead_category value + shared_filters.
    Batches into groups of 5 to stay within HubSpot's filter group limit."""
    seen, results = set(), []
    for i in range(0, len(cat_values), 5):
        batch = cat_values[i:i+5]
        groups = [
            {"filters": [{"propertyName": PROP_LEAD_CATEGORY, "operator": "EQ", "value": v}] + shared_filters}
            for v in batch
        ]
        for c in _search_one_batch(groups, properties):
            if c["id"] not in seen:
                seen.add(c["id"])
                results.append(c)
    return results

def search_all_deals(filters, properties):
    """Paginate through all matching deals."""
    results, after = [], None
    while True:
        body = {"filterGroups": [{"filters": filters}], "properties": properties, "limit": 200}
        if after:
            body["after"] = after
        r = requests.post(f"{BASE_URL}/crm/v3/objects/deals/search", headers=HEADERS, json=body)
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return results

def get_contact_deal_info(contact_ids):
    """Batch-fetch deal associations for a list of contact IDs (auto-batches ≤100).
    Returns {contact_id: [deal_id, ...]}"""
    if not contact_ids:
        return {}
    results = {}
    for i in range(0, len(contact_ids), 100):
        inputs = [{"id": str(cid)} for cid in contact_ids[i:i+100]]
        r = requests.post(
            f"{BASE_URL}/crm/v4/associations/contacts/deals/batch/read",
            headers=HEADERS, json={"inputs": inputs}
        )
        if r.status_code not in (200, 207):
            continue
        for item in r.json().get("results", []):
            cid = str(item.get("from", {}).get("id", ""))
            results[cid] = [str(a.get("toObjectId", "")) for a in item.get("to", [])]
    return results

def get_deals_by_ids(deal_ids):
    """Fetch deal details for a list of deal IDs (auto-batches ≤100)."""
    if not deal_ids:
        return {}
    props = ["pipeline", "dealstage", "paid_recruitment_date", "dealname", "hs_lastmodifieddate", "amount"]
    result = {}
    for i in range(0, len(deal_ids), 100):
        inputs = [{"id": did} for did in deal_ids[i:i+100]]
        r = requests.post(
            f"{BASE_URL}/crm/v3/objects/deals/batch/read",
            headers=HEADERS, json={"properties": props, "inputs": inputs}
        )
        if r.status_code not in (200, 207):
            continue
        for d in r.json().get("results", []):
            result[str(d["id"])] = d.get("properties", {})
    return result

def fetch_deals_progress(contact_ids):
    """Count BD pipeline deal stages for the given OA BD contact IDs.
    Counts are based on each deal's current stage today."""
    if not contact_ids:
        return {"total": 0, "dcAttended": 0, "acAttended": 0, "paid": 0}

    assoc_map    = get_contact_deal_info(contact_ids)
    all_deal_ids = list({did for ids in assoc_map.values() for did in ids})
    if not all_deal_ids:
        return {"total": 0, "dcAttended": 0, "acAttended": 0, "paid": 0}

    deals = get_deals_by_ids(all_deal_ids)
    total = dc = ac = paid = 0
    for props in deals.values():
        if props.get("pipeline") != BD_PIPELINE_ID:
            continue
        total += 1
        stage  = props.get("dealstage", "")
        if stage in DC_ATTENDED_STAGES: dc   += 1
        if stage in AC_ATTENDED_STAGES: ac   += 1
        if stage in PAID_FEE_STAGES:    paid += 1

    return {"total": total, "dcAttended": dc, "acAttended": ac, "paid": paid}

# ── SCORING ──────────────────────────────────────────────────────────────────

C_SUITE  = {"ceo","coo","cto","cfo","founder","co-founder","owner","president","managing director","md","principal"}
VP_LEVEL = {"vp","vice president","head","chief","director","partner"}
MGR_LEVEL= {"manager","supervisor","lead","team lead","sr.","senior"}

def score_title(title):
    t = (title or "").lower()
    if any(k in t for k in C_SUITE):  return 40
    if any(k in t for k in VP_LEVEL): return 28
    if any(k in t for k in MGR_LEVEL):return 18
    if t:                              return 10
    return 5

def score_email(email):
    e = (email or "").lower()
    free = ("gmail","yahoo","hotmail","outlook","icloud","me.com","aol","proton")
    if any(e.endswith(f) for f in free): return 0
    if "@" in e:                          return 15
    return 0

def score_company(company):
    enterprise = ("fortune","sotheby","berkshire","nhs","lucid motors","fujifilm","patterson")
    c = (company or "").lower()
    if any(k in c for k in enterprise): return 20
    if c and c not in ("(individual)","(independent)","individual","independent"): return 12
    return 2

def score_contact(c_props, has_active_deal, connected):
    s  = score_title(c_props.get("jobtitle",""))
    s += score_email(c_props.get("email",""))
    s += score_company(c_props.get("company",""))
    if has_active_deal and connected: s += 30
    elif has_active_deal:             s += 22
    elif connected:                   s += 15
    return min(s, 100)

def assign_tier(has_active_deal, connected, validity_key, score):
    if has_active_deal and score >= 55: return "CLOSE"
    if connected and not has_active_deal:  return "ADVANCE"
    if validity_key in ("valid_strict","valid_ni"): return "QUALIFY"
    return "PROSPECT"

def build_why(c_props, has_active_deal, connected, tier):
    title   = c_props.get("jobtitle","") or ""
    company = c_props.get("company","") or "(Individual)"
    parts = []
    if title:   parts.append(f"{title} at {company}")
    else:       parts.append(company)
    if has_active_deal and connected: parts.append("connected with active deal — strong close signal")
    elif has_active_deal:             parts.append("active deal in pipeline — needs advancing")
    elif connected:                   parts.append("connected — relationship ready, no deal yet")
    else:                             parts.append("valid BD lead — initial outreach stage")
    return ". ".join(p.capitalize() for p in parts[:2]) + "."

def build_action(tier, has_active_deal, connected):
    if tier == "CLOSE" and has_active_deal:
        return "Follow up on open deal — confirm requirements and timeline to close."
    if tier == "ADVANCE":
        return "Create deal — send capabilities deck and schedule discovery call."
    if tier == "QUALIFY":
        return "Schedule discovery call — explore outsourcing needs and qualify role."
    return "Assign validity first, then personalised outreach to explore business needs."

# ── WEEKLY CONTACTS STATS ─────────────────────────────────────────────────────

def fetch_weekly_contacts(start, end):
    """Fetch OA Business Development contacts (lead_category = BD Lead) created in [start, end]."""
    date_filters = [
        {"propertyName": "createdate", "operator": "GTE", "value": to_iso(start)},
        {"propertyName": "createdate", "operator": "LTE", "value": to_iso(end)},
    ]
    props = [PROP_LEAD_CATEGORY, PROP_VALIDITY, PROP_LEAD_STATUS,
             "firstname","lastname","jobtitle","company","email","phone","hs_country_code"]
    contacts = search_contacts_by_categories(list(OA_BD_VALUES), date_filters, props)
    print(f"  OA Business Development contacts found: {len(contacts)}")

    stats = dict(contacts_total=0, valid_strict=0, valid_ni=0, spam=0,
                 jobseeker=0, service_provider=0, unqualified=0, no_validity=0,
                 connected=0,
                 inbound=0, outbound=0, sp=0, other=0,
                 inbound_valid=0, inbound_connected=0,
                 outbound_valid=0, outbound_connected=0,
                 sp_valid=0, sp_connected=0)

    for c in contacts:
        p        = c.get("properties", {})
        val_key  = norm_validity(p.get(PROP_VALIDITY))
        src_key  = norm_source(p.get(PROP_LEAD_CATEGORY))
        conn     = is_connected(p.get(PROP_LEAD_STATUS))
        is_valid = val_key in ("valid_strict", "valid_ni")

        stats["contacts_total"]  += 1
        stats[val_key]           += 1
        stats[src_key]           += 1
        if conn: stats["connected"] += 1

        if src_key == "inbound":
            if is_valid: stats["inbound_valid"] += 1
            if conn:     stats["inbound_connected"] += 1
        elif src_key == "outbound":
            if is_valid: stats["outbound_valid"] += 1
            if conn:     stats["outbound_connected"] += 1
        elif src_key == "sp":
            if is_valid: stats["sp_valid"] += 1
            if conn:     stats["sp_connected"] += 1

    return stats, [c["id"] for c in contacts]

# ── WEEKLY DEALS COUNT + SA SIGNED ───────────────────────────────────────────

def _date_ms(dt):
    """Convert a date/datetime to midnight-UTC milliseconds for HubSpot date-type filters."""
    from datetime import timezone as _tz
    d = dt.date() if hasattr(dt, "date") else dt
    return int(datetime(d.year, d.month, d.day, tzinfo=_tz.utc).timestamp() * 1000)

def fetch_sa_signed_count(start, end):
    """Count BD pipeline deals where pandadoc_signed date falls within [start, end]."""
    return _fetch_deal_date_count("pandadoc_signed", start, end)

def _fetch_deal_date_count(date_prop, start, end):
    """Count BD pipeline deals where a given date property falls within [start, end]."""
    filters = [
        {"propertyName": "pipeline",  "operator": "EQ",  "value": BD_PIPELINE_ID},
        {"propertyName": date_prop,   "operator": "GTE", "value": str(_date_ms(start))},
        {"propertyName": date_prop,   "operator": "LTE", "value": str(_date_ms(end))},
    ]
    body = {"filterGroups": [{"filters": filters}], "properties": ["hs_object_id"], "limit": 1}
    r = requests.post(f"{BASE_URL}/crm/v3/objects/deals/search", headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json().get("total", 0)

def fetch_dc_deals(start, end):
    """Fetch BD pipeline deals with discovery_call_date in [start, end], returning company list."""
    filters = [
        {"propertyName": "pipeline",            "operator": "EQ",  "value": BD_PIPELINE_ID},
        {"propertyName": "discovery_call_date", "operator": "GTE", "value": str(_date_ms(start))},
        {"propertyName": "discovery_call_date", "operator": "LTE", "value": str(_date_ms(end))},
    ]
    props = ["dealname", "discovery_call_date", "discovery_call_attendance", "dealstage"]
    results, after = [], None
    while True:
        body = {"filterGroups": [{"filters": filters}], "properties": props, "limit": 100}
        if after:
            body["after"] = after
        r = requests.post(f"{BASE_URL}/crm/v3/objects/deals/search", headers=HEADERS, json=body)
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    deals = []
    for d in results:
        p = d.get("properties", {})
        deals.append({
            "id":         d["id"],
            "company":    p.get("dealname", "Unknown"),
            "dcDate":     p.get("discovery_call_date", ""),
            "attendance": p.get("discovery_call_attendance", ""),
            "stage":      STAGE_LABELS.get(p.get("dealstage", ""), p.get("dealstage", "")),
        })
    deals.sort(key=lambda x: x["dcDate"])
    return deals

def fetch_deals_created_count(start, end):
    filters = [
        {"propertyName": "pipeline",   "operator": "EQ",  "value": BD_PIPELINE_ID},
        {"propertyName": "createdate", "operator": "GTE", "value": to_iso(start)},
        {"propertyName": "createdate", "operator": "LTE", "value": to_iso(end)},
    ]
    body = {"filterGroups": [{"filters": filters}], "properties": ["hs_object_id"], "limit": 1}
    r = requests.post(f"{BASE_URL}/crm/v3/objects/deals/search", headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json().get("total", 0)

# ── TOP 50 OUTREACH ───────────────────────────────────────────────────────────

def fetch_top_50():
    """Fetch top BD Lead contacts for the outreach ranking."""
    print("  Fetching top contacts for outreach ranking…")

    # Fetch recent BD contacts (last 90 days for a useful pool)
    now = datetime.now(MANILA_TZ)
    ninety_days_ago = now - timedelta(days=90)
    date_filter = [{"propertyName": "createdate", "operator": "GTE", "value": to_iso(ninety_days_ago)}]
    props = [PROP_LEAD_CATEGORY, PROP_VALIDITY, PROP_LEAD_STATUS,
             "firstname","lastname","jobtitle","company","email","phone","hs_country_code"]
    contacts = search_contacts_by_categories(list(ALL_BD_VALUES), date_filter, props)
    print(f"  Pool: {len(contacts)} BD contacts (90 days)")

    # Get deal associations in batch
    contact_ids = [c["id"] for c in contacts]
    assoc_map   = get_contact_deal_info(contact_ids)

    # Collect all deal IDs and fetch them
    all_deal_ids = list({did for ids in assoc_map.values() for did in ids})
    deals_by_id  = get_deals_by_ids(all_deal_ids) if all_deal_ids else {}

    # Score each contact
    scored = []
    for c in contacts:
        p       = c.get("properties", {})
        val_key = norm_validity(p.get(PROP_VALIDITY))
        conn    = is_connected(p.get(PROP_LEAD_STATUS))

        deal_ids = assoc_map.get(c["id"], [])
        active_deals = [d for did in deal_ids
                        if (d := deals_by_id.get(did))
                        and d.get("pipeline") == BD_PIPELINE_ID
                        and d.get("dealstage") in ACTIVE_STAGES]
        has_active = len(active_deals) > 0

        score = score_contact(p, has_active, conn)
        tier  = assign_tier(has_active, conn, val_key, score)

        fn = (p.get("firstname") or "").strip()
        ln = (p.get("lastname")  or "").strip()
        name = f"{fn} {ln}".strip() or "Unknown"
        country = (p.get("hs_country_code") or "").strip().upper()

        scored.append({
            "id":       c["id"],
            "name":     name,
            "title":    (p.get("jobtitle") or "").strip(),
            "company":  (p.get("company")  or "(Individual)").strip(),
            "email":    (p.get("email")    or "").strip(),
            "phone":    (p.get("phone")    or "").strip(),
            "country":  country,
            "score":    score,
            "tier":     tier,
            "deals":    len(active_deals),
            "conn":     conn,
            "val_key":  val_key,
        })

    scored.sort(key=lambda x: (-x["score"], x["name"]))
    top50 = scored[:50]

    result = []
    for i, c in enumerate(top50, 1):
        result.append({
            "rank":    i,
            "name":    c["name"],
            "title":   c["title"],
            "company": c["company"],
            "email":   c["email"],
            "phone":   c["phone"],
            "country": c["country"],
            "deals":   c["deals"],
            "tier":    c["tier"],
            "score":   c["score"],
            "why":     build_why({"jobtitle": c["title"], "company": c["company"]},
                                  c["deals"] > 0, c["conn"], c["tier"]),
            "action":  build_action(c["tier"], c["deals"] > 0, c["conn"]),
            "hsId":    c["id"],
        })
    return result

# ── MONTHLY CONTACTS ─────────────────────────────────────────────────────────

def fetch_monthly_contacts(num_months=6):
    """Fetch OA Business Development contact counts for each of the last N calendar months."""
    now     = datetime.now(MANILA_TZ)
    mo_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    props   = [PROP_LEAD_CATEGORY, PROP_VALIDITY, PROP_LEAD_STATUS]
    results = []

    for i in range(num_months - 1, -1, -1):
        offset = now.month - 1 - i          # Python floor-div handles negatives correctly
        y      = now.year + offset // 12
        m      = offset % 12 + 1

        month_start = datetime(y, m, 1, 0, 0, 0, tzinfo=MANILA_TZ)
        if m == 12:
            month_end = datetime(y + 1, 1, 1, tzinfo=MANILA_TZ) - timedelta(seconds=1)
        else:
            month_end = datetime(y, m + 1, 1, tzinfo=MANILA_TZ) - timedelta(seconds=1)
        is_partial = (i == 0)
        if is_partial:
            month_end = now.replace(hour=23, minute=59, second=59, microsecond=0)

        label = f"{mo_names[m-1]} {y}"
        print(f"    {label}...", end=" ", flush=True)

        date_filters = [
            {"propertyName": "createdate", "operator": "GTE", "value": to_iso(month_start)},
            {"propertyName": "createdate", "operator": "LTE", "value": to_iso(month_end)},
        ]
        contacts = search_contacts_by_categories(list(OA_BD_VALUES), date_filters, props)

        total     = len(contacts)
        valid_c   = sum(1 for c in contacts
                        if norm_validity(c.get("properties",{}).get(PROP_VALIDITY))
                        in ("valid_strict","valid_ni"))
        connected = sum(1 for c in contacts
                        if is_connected(c.get("properties",{}).get(PROP_LEAD_STATUS)))
        print(f"{total} contacts", end=" ", flush=True)

        contact_ids  = [c["id"] for c in contacts]
        deals_prog   = fetch_deals_progress(contact_ids)
        sa_signed    = fetch_sa_signed_count(month_start, month_end)
        dc_count     = _fetch_deal_date_count("discovery_call_date", month_start, month_end)
        ac_count     = _fetch_deal_date_count("alignment_call_date",  month_start, month_end)
        print(f"| {deals_prog['total']} deals | {sa_signed} SA | {dc_count} DC | {ac_count} AC")

        results.append({
            "key":         f"{y}-{m:02d}",
            "label":       label,
            "year":        y,
            "month":       m,
            "partial":     is_partial,
            "contacts":    total,
            "valid":       valid_c,
            "connected":   connected,
            "validRate":   round(valid_c / total * 100, 1) if total else 0.0,
            "connectRate": round(connected / total * 100, 1) if total else 0.0,
            "dealProgress": deals_prog,
            "saSigned":    sa_signed,
            "dcCount":     dc_count,
            "acCount":     ac_count,
        })

    return results


# ── PAID DEALS ────────────────────────────────────────────────────────────────

def fetch_paid_deals():
    """Fetch deals with lead_source = 'Outbound Paid' and classify into HOT/ADVANCING/STALLED/CLOSED."""
    filters = [
        {"propertyName": "pipeline",    "operator": "EQ", "value": BD_PIPELINE_ID},
        {"propertyName": "lead_source", "operator": "EQ", "value": "Outbound Paid"},
    ]
    props = ["dealname","dealstage","lead_source","paid_recruitment_date","hs_lastmodifieddate",
             "amount","hs_is_closed_won","hs_probability"]
    deals = search_all_deals(filters, props)
    print(f"  Outbound Paid deals found: {len(deals)}")

    now = datetime.now(MANILA_TZ)
    groups = {"HOT": [], "ADVANCING": [], "STALLED": [], "CLOSED": []}

    for d in deals:
        p      = d.get("properties", {})
        stage  = p.get("dealstage", "")
        lmod   = p.get("hs_lastmodifieddate", "")
        name   = p.get("dealname", "Unknown Deal")
        prob   = int(float(p.get("hs_probability") or 0) * 100) or None

        try:
            lmod_dt  = datetime.fromisoformat(lmod.replace("Z","+00:00"))
            age_days = (now - lmod_dt.astimezone(MANILA_TZ)).days
        except Exception:
            age_days = 0

        stage_label = STAGE_LABELS.get(stage, stage)

        entry = {
            "id":      d["id"],
            "company": name,
            "role":    "",
            "stage":   stage_label,
            "prob":    prob,
            "ageDays": age_days,
            "lastMod": lmod[:10] if lmod else "",
            "action":  "",
        }

        if stage == STAGES["ac_completed"]:
            entry["action"] = "Schedule closing call — highest urgency in pipeline"
            groups["HOT"].append(entry)
        elif stage in (STAGES["dc_completed"], STAGES["ac_outreach"]):
            entry["action"] = "Advance to next stage"
            groups["ADVANCING"].append(entry)
        elif stage in (STAGES["dc_no_show"], STAGES["ac_no_show"], STAGES["deal_unqualified"]):
            entry["action"] = "Re-engage or reschedule missed call"
            groups["STALLED"].append(entry)
        elif stage in (STAGES["closed_won"], STAGES["hiring_recruiting"]):
            entry["action"] = "Monitor progress — deal closed, in hiring stage"
            entry["closedWon"] = True
            groups["CLOSED"].append(entry)
        elif stage == STAGES["closed_lost"]:
            entry["action"] = "Request loss reason for learnings"
            entry["closedWon"] = False
            groups["CLOSED"].append(entry)
        else:
            entry["action"] = "Review and advance"
            groups["ADVANCING"].append(entry)

    return {"lastQueried": now.strftime("%Y-%m-%d"), "groups": groups}

# ── HTML PATCH ────────────────────────────────────────────────────────────────

def update_html(week_num, week_data, paid_deals_data, monthly_data, today):
    print(f"  Patching {HTML_FILE}…")
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    # 1. Parse existing JSON
    m = re.search(r'<script id="report-data" type="application/json">(.*?)</script>',
                  html, re.DOTALL)
    if not m:
        raise RuntimeError("Could not find report-data script tag in HTML")
    report = json.loads(m.group(1))

    # 2. Update meta
    report["meta"]["lastUpdated"] = today.strftime("%Y-%m-%d")

    # 3. Add / overwrite this week
    report["weeks"][str(week_num)] = week_data

    # 4. Overwrite paidDeals
    report["paidDeals"] = paid_deals_data

    # 5. Overwrite monthly contacts
    report["monthly"] = monthly_data

    new_json = json.dumps(report, indent=2, ensure_ascii=False, default=str)
    new_tag  = f'<script id="report-data" type="application/json">\n{new_json}\n</script>'
    html     = html[:m.start()] + new_tag + html[m.end():]

    # 5. Update meta data-last-updated
    html = re.sub(r'content="[\d-]+"(\s+/>|>)\s*(?=\s*<meta name="data-cycle")',
                  f'content="{today.strftime("%Y-%m-%d")}"\\1\n  ', html)

    # 7. Update header week badge initial text
    wk_label  = f"W{week_num}"
    dates_str = week_data["dates"]
    year_str  = str(week_data["year"])
    html = re.sub(
        r'(<div class="week-badge" id="currentWeekLabel">)[^<]*(</div>)',
        rf'\g<1>{wk_label} &middot; {dates_str}, {year_str}\g<2>',
        html
    )

    # 8. Update sidebar "This Week" label
    html = re.sub(
        r'(<div class="sidebar-label">This Week \()W\d+(\)</div>)',
        rf'\g<1>{wk_label}\g<2>',
        html
    )

    # 9. Rebuild week navigator tabs — keep existing + add new if missing
    #    Find the week-tabs div and regenerate its contents
    def make_tabs(weeks_dict):
        sorted_keys = sorted(weeks_dict.keys(), key=int)
        tabs = []
        for k in sorted_keys:
            w = weeks_dict[k]
            active = " active" if int(k) == week_num else ""
            tabs.append(
                f'<button class="week-tab{active}" data-week="{k}" '
                f'onclick="selectWeek({k}, this)">'
                f'{w["label"]} {w["dates"]}</button>'
            )
        return "\n        ".join(tabs)

    tab_html = make_tabs(report["weeks"])
    html = re.sub(
        r'(<div class="week-tabs">)\s*(.*?)\s*(</div>)',
        rf'\g<1>\n        {tab_html}\n      \g<3>',
        html, flags=re.DOTALL, count=1
    )

    # 10. Update nextUpdateLabel static value (JS will also recalculate this)
    nxt = today + timedelta(days=7)
    mo  = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    nxt_label = f"Next update: {mo[nxt.month-1]} {nxt.day}, {nxt.year}"
    html = re.sub(r'Next update: \w+ \d+, \d+', nxt_label, html)

    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  HTML updated: {HTML_FILE}")

# ── DISCOVER MODE ─────────────────────────────────────────────────────────────

def discover_properties():
    print("\n=== DISCOVERING HUBSPOT CONTACT PROPERTIES ===\n")
    r = requests.get(f"{BASE_URL}/crm/v3/properties/contacts", headers=HEADERS)
    r.raise_for_status()
    props = r.json().get("results", [])

    validity_cands, source_cands, connected_cands = [], [], []
    for p in props:
        if p.get("groupName") == "contactinformation" and not p.get("hidden"):
            continue  # skip purely standard fields
        name  = p.get("name","")
        label = p.get("label","")
        ftype = p.get("fieldType","")
        opts  = [o.get("label","").lower() for o in p.get("options",[])]
        opts_str = " | ".join(opts)

        if any(v in opts_str for v in ["valid","spam","job seeker","unqualified"]):
            validity_cands.append((name, label, opts_str))
        if any(v in opts_str for v in ["inbound","outbound"]):
            source_cands.append((name, label, opts_str))
        if ftype == "booleancheckbox" and "connect" in name.lower():
            connected_cands.append((name, label))

    print("── VALIDITY CANDIDATES (set PROP_VALIDITY to one of these) ──")
    for n,l,o in validity_cands:
        print(f"  name={n!r:40s}  label={l!r}  options: {o}")

    print("\n── SOURCE/TYPE CANDIDATES (set PROP_SOURCE_TYPE) ──")
    for n,l,o in source_cands:
        print(f"  name={n!r:40s}  label={l!r}  options: {o}")

    print("\n── CONNECTED CANDIDATES (set PROP_CONNECTED) ──")
    for n,l in connected_cands:
        print(f"  name={n!r:40s}  label={l!r}")

    print("\n─────────────────────────────────────────────")
    print("Edit PROP_VALIDITY, PROP_SOURCE_TYPE, PROP_CONNECTED at the top of this script.")

# ── BACKFILL ─────────────────────────────────────────────────────────────────

def backfill_missing_fields():
    """Patch all weeks in the HTML that are missing saSigned / dcCount / acCount."""
    print(f"\n[BACKFILL] Reading {HTML_FILE}…")
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()
    m = re.search(r'<script id="report-data" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not m:
        raise RuntimeError("Could not find report-data script tag")
    report = json.loads(m.group(1))

    patched = 0
    for wk_str, d in sorted(report["weeks"].items(), key=lambda x: int(x[0])):
        if "saSigned" in d and "dcCount" in d and "acCount" in d:
            print(f"  W{wk_str}: already complete, skipping")
            continue

        start = datetime.fromisoformat(d["startDate"]).replace(
            hour=0, minute=0, second=0, tzinfo=MANILA_TZ)
        end   = datetime.fromisoformat(d["endDate"]).replace(
            hour=23, minute=59, second=59, tzinfo=MANILA_TZ)
        print(f"  W{wk_str}: fetching missing fields ({d['startDate']} to {d['endDate']})…", flush=True)

        d["saSigned"] = fetch_sa_signed_count(start, end)
        dc_deals      = fetch_dc_deals(start, end)
        d["dcCount"]  = len(dc_deals)
        d["dcDeals"]  = dc_deals
        d["acCount"]  = _fetch_deal_date_count("alignment_call_date", start, end)

        print(f"    SA={d['saSigned']} DC={d['dcCount']} AC={d['acCount']}")
        patched += 1

    if patched == 0:
        print("  Nothing to backfill.")
        return

    new_json = json.dumps(report, indent=2, ensure_ascii=False, default=str)
    new_tag  = f'<script id="report-data" type="application/json">\n{new_json}\n</script>'
    html     = html[:m.start()] + new_tag + html[m.end():]
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Backfilled {patched} week(s). HTML saved.")

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    if "--discover" in sys.argv:
        if not HUBSPOT_TOKEN:
            print("ERROR: set HUBSPOT_TOKEN before running --discover")
            sys.exit(1)
        discover_properties()
        return

    if "--backfill" in sys.argv:
        if not HUBSPOT_TOKEN:
            print("ERROR: set HUBSPOT_TOKEN before running --backfill")
            sys.exit(1)
        backfill_missing_fields()
        return

    if not HUBSPOT_TOKEN:
        print("ERROR: HUBSPOT_TOKEN environment variable not set.")
        print("  PowerShell: $env:HUBSPOT_TOKEN = 'pat-na1-...'")
        print("  Run --discover to find your contact property names.")
        sys.exit(1)

    today = datetime.now(MANILA_TZ)
    print(f"\n[{today.isoformat()}] OA BD Weekly Report Updater")
    print("-" * 55)

    start, end, week_num, dates_label = current_week_range()
    print(f"  Week {week_num}: {dates_label} ({start.date()} to {end.date()})")

    # Weekly contacts
    print("\n[1/5] Fetching weekly contact stats…")
    stats, weekly_contact_ids = fetch_weekly_contacts(start, end)

    # Weekly deals progress
    print(f"\n[2/5] Fetching weekly deal progress ({len(weekly_contact_ids)} contacts)…")
    weekly_deals_progress = fetch_deals_progress(weekly_contact_ids)
    print(f"  Deals: {weekly_deals_progress['total']} total | "
          f"{weekly_deals_progress['dcAttended']} DC | "
          f"{weekly_deals_progress['acAttended']} AC | "
          f"{weekly_deals_progress['paid']} Paid")

    # Weekly deal metrics
    print("\n[3/5] Counting weekly deal metrics…")
    deals_count = fetch_deals_created_count(start, end)
    sa_signed   = fetch_sa_signed_count(start, end)
    dc_deals    = fetch_dc_deals(start, end)
    dc_count    = len(dc_deals)
    ac_count    = _fetch_deal_date_count("alignment_call_date", start, end)
    print(f"  Deals created: {deals_count} | SA signed: {sa_signed} | DC: {dc_count} | AC: {ac_count}")

    # Top 50 outreach
    print("\n[4/5] Building top 50 outreach list…")
    top_accounts = fetch_top_50()
    print(f"  Top accounts: {len(top_accounts)}")

    # Paid deals
    print("\n[5/5] Fetching Outbound Paid deals + monthly data…")
    paid_deals = fetch_paid_deals()

    # Monthly contacts
    print("\n[5/5] Fetching monthly contact stats (last 6 months)…")
    monthly_data = fetch_monthly_contacts(num_months=6)
    print(f"  Monthly data: {len(monthly_data)} months fetched")

    # Build week object
    valid_total = stats["valid_strict"] + stats["valid_ni"]
    week_data = {
        "label":    f"W{week_num}",
        "dates":    dates_label,
        "year":     today.year,
        "startDate": start.strftime("%Y-%m-%d"),
        "endDate":   end.strftime("%Y-%m-%d"),
        "contacts": stats["contacts_total"],
        "valid":    valid_total,
        "validStrict": stats["valid_strict"],
        "validNI":     stats["valid_ni"],
        "connected":   stats["connected"],
        "qualified":   valid_total,
        "deals":       deals_count,
        "dealsAll":    deals_count,
        "spam":              stats["spam"],
        "jobseekers":        stats["jobseeker"],
        "serviceProviders":  stats["service_provider"],
        "unqualified":       stats["unqualified"],
        "noValidity":        stats["no_validity"],
        "inbound":           stats["inbound"],
        "outbound":          stats["outbound"],
        "sp":                stats["sp"],
        "other":             stats["other"],
        "inboundValid":      stats["inbound_valid"],
        "inboundConnected":  stats["inbound_connected"],
        "outboundValid":     stats["outbound_valid"],
        "outboundConnected": stats["outbound_connected"],
        "spValid":           stats["sp_valid"],
        "spConnected":       stats["sp_connected"],
        "topAccounts":       top_accounts,
        "dealProgress":      weekly_deals_progress,
        "saSigned":          sa_signed,
        "dcCount":           dc_count,
        "dcDeals":           dc_deals,
        "acCount":           ac_count,
    }

    # Patch HTML
    print("\n[Patching HTML]")
    update_html(week_num, week_data, paid_deals, monthly_data, today)

    print(f"\nDone - Week {week_num} ({dates_label}) committed to HTML.")
    print(f"  Next update: next Tuesday\n")


if __name__ == "__main__":
    main()
