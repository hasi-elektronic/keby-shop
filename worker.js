// Keby Worker v4 — Modern Admin + Full Product CRUD + Image Management
// Secrets come from environment bindings (no hardcoded credentials)

var PAYPAL_API = "https://api-m.paypal.com";

var CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization"
};

var rateLimitMap = new Map();
function rl(ip, max = 60, windowMs = 60000) {
  const now = Date.now();
  const arr = (rateLimitMap.get(ip) || []).filter(t => now - t < windowMs);
  if (arr.length >= max) return false;
  arr.push(now);
  rateLimitMap.set(ip, arr);
  return true;
}

function sanitize(s, max = 200) {
  return String(s || "").replace(/[^\w\-.@ ğüşöçıĞÜŞÖÇİ]/g, "").slice(0, max);
}

function jsonResp(data, status = 200, extra = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...CORS, "Content-Type": "application/json", ...extra }
  });
}

function errResp(message, status = 400) {
  return jsonResp({ success: false, error: message }, status);
}

function getDefaultProducts() {
  return [
    {
      id: "olivenoel-750ml", slug: "produkt-olivenoel-750ml",
      active: true, sort: 1,
      name_tr: "Sızma Zeytinyağı 750ml", name_de: "Natives Olivenöl Extra 750ml",
      price: 14.99, mwst: "7%", stock: 100,
      short_tr: "El yapımı cam şişe.", short_de: "Handgefertigte Glasflasche.",
      desc_tr: "", desc_de: "",
      images: [], thumb: ""
    },
    {
      id: "kanister-5l", slug: "produkt-kanister-5l",
      active: true, sort: 2,
      name_tr: "Sızma Zeytinyağı 5L Bidon", name_de: "Natives Olivenöl Extra 5L Kanister",
      price: 59.90, mwst: "7%", stock: 50,
      short_tr: "Ekonomik seçim.", short_de: "Wirtschaftliche Wahl.",
      desc_tr: "", desc_de: "",
      images: [], thumb: ""
    },
    {
      id: "oliven-schwarz", slug: "produkt-oliven-schwarz",
      active: true, sort: 3,
      name_tr: "Yağlı Siyah Zeytin 900g", name_de: "Schwarze Oliven in Öl 900g",
      price: 14.99, mwst: "19%", stock: 80,
      short_tr: "Sofra zeytini.", short_de: "Tafeloliven.",
      desc_tr: "", desc_de: "",
      images: [], thumb: ""
    }
  ];
}

async function getProducts(env) {
  const obj = await env.KEBY_R2.get("keby/data/products.json");
  if (!obj) return getDefaultProducts();
  try { return JSON.parse(await obj.text()); }
  catch { return getDefaultProducts(); }
}

async function putProducts(env, products) {
  await env.KEBY_R2.put(
    "keby/data/products.json",
    JSON.stringify(products, null, 2),
    { httpMetadata: { contentType: "application/json" } }
  );
}

async function getOrders(env) {
  const obj = await env.KEBY_R2.get("keby/data/orders.json");
  if (!obj) return [];
  try { return JSON.parse(await obj.text()); } catch { return []; }
}
async function putOrders(env, orders) {
  await env.KEBY_R2.put(
    "keby/data/orders.json",
    JSON.stringify(orders),
    { httpMetadata: { contentType: "application/json" } }
  );
}

// ============ INVOICE HELPERS ============
// Counter format: { "2026": 5, "2025": 142 }  → year-based ardışık numaralandırma (§14 UStG)
async function getInvoiceCounter(env) {
  const obj = await env.KEBY_R2.get("keby/data/invoice-counter.json");
  if (!obj) return {};
  try { return JSON.parse(await obj.text()); } catch { return {}; }
}
async function nextInvoiceNumber(env) {
  const counter = await getInvoiceCounter(env);
  const year = new Date().getFullYear();
  const next = (counter[year] || 0) + 1;
  counter[year] = next;
  await env.KEBY_R2.put(
    "keby/data/invoice-counter.json",
    JSON.stringify(counter),
    { httpMetadata: { contentType: "application/json" } }
  );
  // Format: 2026-0001
  return year + "-" + String(next).padStart(4, "0");
}
async function getInvoice(env, orderId) {
  const obj = await env.KEBY_R2.get("keby/invoices/" + orderId + ".json");
  if (!obj) return null;
  try { return JSON.parse(await obj.text()); } catch { return null; }
}
async function putInvoice(env, orderId, data) {
  await env.KEBY_R2.put(
    "keby/invoices/" + orderId + ".json",
    JSON.stringify(data, null, 2),
    { httpMetadata: { contentType: "application/json" } }
  );
}


// ============ STOCK HELPERS ============

async function getStockMovements(env) {
  const obj = await env.KEBY_R2.get("keby/data/stock_movements.json");
  if (!obj) return [];
  try { return JSON.parse(await obj.text()); } catch { return []; }
}

async function saveStockMovements(env, movements) {
  // Son 500 hareketi sakla (eski olanları temizle)
  const trimmed = movements.slice(-500);
  await env.KEBY_R2.put(
    "keby/data/stock_movements.json",
    JSON.stringify(trimmed),
    { httpMetadata: { contentType: "application/json" } }
  );
}

// Stok düş (sipariş tamamlandığında)
async function deductStock(env, items, orderId) {
  if (!items || !items.length) return;
  const products = await getProducts(env);
  const movements = await getStockMovements(env);
  let changed = false;

  for (const item of items) {
    const qty = parseInt(item.qty || item.quantity || 1);
    // id ile eşleş, yoksa isimle
    const idx = products.findIndex(p =>
      (item.id && p.id === item.id) ||
      (item.productId && p.id === item.productId) ||
      p.name === item.name
    );
    if (idx === -1) continue;

    const before = products[idx].stock || 0;
    const after = Math.max(0, before - qty);
    products[idx].stock = after;
    changed = true;

    movements.push({
      id: Date.now() + "_" + idx,
      date: new Date().toISOString(),
      productId: products[idx].id,
      productName: products[idx].name,
      type: "abgang",
      qty: -qty,
      before,
      after,
      reason: "Bestellung " + orderId,
      orderId
    });
  }

  if (changed) {
    await putProducts(env, products);
    await saveStockMovements(env, movements);
  }
}

// Manuel stok düzeltme
async function adjustStock(env, productId, qty, reason, type) {
  const products = await getProducts(env);
  const movements = await getStockMovements(env);

  const idx = products.findIndex(p => p.id == productId);
  if (idx === -1) return { success: false, error: "Produkt nicht gefunden" };

  const before = products[idx].stock || 0;
  const after = Math.max(0, before + qty);
  products[idx].stock = after;

  movements.push({
    id: Date.now() + "_" + idx,
    date: new Date().toISOString(),
    productId: products[idx].id,
    productName: products[idx].name,
    type: type || (qty > 0 ? "zugang" : "abgang"),
    qty,
    before,
    after,
    reason: reason || "Manuelle Korrektur"
  });

  await putProducts(env, products);
  await saveStockMovements(env, movements);
  return { success: true, before, after, product: products[idx].name };
}

// ============ LIEFERSCHEIN (İRSALİYE) HELPERS ============
// Counter format: { "2026": 5 } → LS-2026-0001
async function getLieferscheinCounter(env) {
  const obj = await env.KEBY_R2.get("keby/data/lieferschein-counter.json");
  if (!obj) return {};
  try { return JSON.parse(await obj.text()); } catch { return {}; }
}
async function nextLieferscheinNumber(env) {
  const counter = await getLieferscheinCounter(env);
  const year = new Date().getFullYear();
  const next = (counter[year] || 0) + 1;
  counter[year] = next;
  await env.KEBY_R2.put(
    "keby/data/lieferschein-counter.json",
    JSON.stringify(counter),
    { httpMetadata: { contentType: "application/json" } }
  );
  return "LS-" + year + "-" + String(next).padStart(4, "0");
}
async function getLieferschein(env, lsId) {
  const obj = await env.KEBY_R2.get("keby/lieferscheine/" + lsId + ".json");
  if (!obj) return null;
  try { return JSON.parse(await obj.text()); } catch { return null; }
}
async function putLieferschein(env, lsId, data) {
  await env.KEBY_R2.put(
    "keby/lieferscheine/" + lsId + ".json",
    JSON.stringify(data, null, 2),
    { httpMetadata: { contentType: "application/json" } }
  );
}
async function listAllLieferscheine(env) {
  const list = await env.KEBY_R2.list({ prefix: "keby/lieferscheine/", limit: 1000 });
  const items = [];
  for (const obj of list.objects) {
    if (!obj.key.endsWith(".json")) continue;
    try {
      const r = await env.KEBY_R2.get(obj.key);
      if (r) items.push(JSON.parse(await r.text()));
    } catch (e) { /* skip */ }
  }
  items.sort((a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0));
  return items;
}

// Lieferschein PDF HTML (yazdırılabilir, ödeme bilgisi YOK)
function generateLieferscheinHTML(ls) {
  const items = (ls.items || []).map(i => {
    const qty = i.qty || i.quantity || 1;
    return `<tr>
      <td style="padding:8px;border-bottom:1px solid #e0e0e0">${qty}</td>
      <td style="padding:8px;border-bottom:1px solid #e0e0e0">${(i.name || i.title || '').replace(/[<>]/g, '')}</td>
      <td style="padding:8px;border-bottom:1px solid #e0e0e0;color:#666;font-size:0.85em">${(i.sku || i.artikel_nr || '—').replace(/[<>]/g, '')}</td>
    </tr>`;
  }).join('');

  const totalQty = (ls.items || []).reduce((s, i) => s + (i.qty || i.quantity || 1), 0);
  const c = ls.customer || {};
  const addr = c.address || {};
  const addrLine = [addr.line1, addr.line2].filter(Boolean).join(', ');
  const cityLine = [addr.postal_code, addr.city].filter(Boolean).join(' ');
  const dateStr = (ls.delivery_date || ls.created_at || '').slice(0, 10).split('-').reverse().join('.');

  return `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Lieferschein ${ls.number}</title>
<style>
  @page { size: A4; margin: 18mm; }
  body { font-family: -apple-system, 'Segoe UI', Roboto, sans-serif; color: #1a1a1a; max-width: 720px; margin: 0 auto; padding: 20px; line-height: 1.5; }
  .head { display: flex; justify-content: space-between; align-items: flex-start; padding-bottom: 16px; border-bottom: 2px solid #2a4a1a; margin-bottom: 24px; }
  .logo { font-family: 'Cormorant Garamond', serif; font-size: 30px; font-weight: 600; color: #2a4a1a; letter-spacing: 0.02em; }
  .logo-sub { font-size: 11px; color: #888; letter-spacing: 0.1em; text-transform: uppercase; margin-top: 2px; }
  .meta { text-align: right; font-size: 0.85em; }
  .meta-row { margin: 2px 0; }
  .meta-label { color: #888; display: inline-block; width: 100px; text-align: right; }
  .meta-val { font-weight: 500; }
  h1 { font-family: 'Cormorant Garamond', serif; font-size: 28px; font-weight: 500; color: #2a4a1a; margin: 24px 0 8px; }
  .number { font-size: 1.05em; font-family: monospace; color: #555; }
  .addr-block { background: #faf7f0; padding: 14px 18px; border-radius: 6px; margin: 18px 0; }
  .addr-label { font-size: 0.72em; color: #888; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }
  .addr-name { font-weight: 600; font-size: 1.05em; }
  table { width: 100%; border-collapse: collapse; margin: 18px 0; }
  th { background: #2a4a1a; color: white; padding: 10px 8px; text-align: left; font-size: 0.85em; font-weight: 500; }
  th:first-child { width: 80px; }
  th:last-child { width: 160px; }
  .total-row { background: #f7f4ee; }
  .total-row td { padding: 10px 8px; font-weight: 600; }
  .footer { margin-top: 60px; padding-top: 16px; border-top: 1px dashed #ccc; font-size: 0.8em; color: #666; }
  .signature { margin-top: 40px; display: flex; justify-content: space-between; gap: 40px; }
  .sign-box { flex: 1; border-bottom: 1px solid #999; padding-bottom: 4px; padding-top: 50px; font-size: 0.78em; color: #888; }
  .sign-label { margin-top: 4px; }
  .notes { background: #fffaf0; border-left: 3px solid #c4a24a; padding: 10px 14px; border-radius: 4px; margin: 16px 0; font-size: 0.88em; }
  .badge { display: inline-block; background: #f0f0f0; color: #555; padding: 2px 8px; border-radius: 4px; font-size: 0.75em; }
  @media print { body { padding: 0; } .no-print { display: none; } }
</style></head>
<body>
  <div class="head">
    <div>
      <div class="logo">Keby</div>
      <div class="logo-sub">Premium Olivenöl</div>
      <div style="font-size:0.78em;color:#666;margin-top:8px">
        Kader Kayihan · Hauptstraße 12 · 71665 Vaihingen/Enz<br>
        info@keby.shop · USt-IdNr DE358426450
      </div>
    </div>
    <div class="meta">
      <div class="meta-row"><span class="meta-label">Lieferschein-Nr:</span> <span class="meta-val">${ls.number}</span></div>
      <div class="meta-row"><span class="meta-label">Lieferdatum:</span> <span class="meta-val">${dateStr}</span></div>
      ${ls.invoice_ref ? `<div class="meta-row"><span class="meta-label">Rechnung-Ref:</span> <span class="meta-val">${ls.invoice_ref}</span></div>` : ''}
      ${ls.order_ref ? `<div class="meta-row"><span class="meta-label">Bestell-Ref:</span> <span class="meta-val">${ls.order_ref}</span></div>` : ''}
    </div>
  </div>

  <h1>Lieferschein <span class="number">${ls.number}</span></h1>

  <div class="addr-block">
    <div class="addr-label">Lieferadresse</div>
    <div class="addr-name">${(c.company_name || c.name || '').replace(/[<>]/g, '')}</div>
    ${c.company_name && c.name ? `<div>${c.name.replace(/[<>]/g, '')}</div>` : ''}
    ${addrLine ? `<div>${addrLine.replace(/[<>]/g, '')}</div>` : ''}
    ${cityLine ? `<div>${cityLine.replace(/[<>]/g, '')}</div>` : ''}
    ${addr.country && addr.country !== 'DE' ? `<div>${addr.country}</div>` : ''}
  </div>

  <table>
    <thead>
      <tr>
        <th>Menge</th>
        <th>Bezeichnung</th>
        <th>Artikel-Nr</th>
      </tr>
    </thead>
    <tbody>
      ${items}
      <tr class="total-row">
        <td>${totalQty}</td>
        <td>Gesamt-Stückzahl</td>
        <td></td>
      </tr>
    </tbody>
  </table>

  ${ls.notes ? `<div class="notes"><strong>Hinweis:</strong> ${ls.notes.replace(/[<>]/g, '')}</div>` : ''}

  <div style="font-size:0.85em;color:#555;margin-top:20px">
    Die Ware wurde wie oben aufgeführt geliefert. Bitte prüfen Sie die Lieferung auf Vollständigkeit und Beschädigungen.
    <br>Reklamationen sind innerhalb von 7 Tagen geltend zu machen.
  </div>

  <div class="signature">
    <div class="sign-box">
      <div class="sign-label">Datum, Unterschrift Lieferant</div>
    </div>
    <div class="sign-box">
      <div class="sign-label">Datum, Unterschrift Empfänger</div>
    </div>
  </div>

  <div class="footer">
    <strong>Keby — Premium Olivenöl</strong> · Inh. Kader Kayihan · Hauptstraße 12, 71665 Vaihingen/Enz<br>
    Tel: +49 159 06342354 · info@keby.shop · USt-IdNr DE358426450 · keby.shop
    <br><span class="badge">Dies ist ein Lieferschein, keine Rechnung</span>
  </div>
</body></html>`;
}

// Müşteri tarafı için: orderId + email kombinasyonundan token üret
// Token: SHA-256(orderId + ":" + email_lowercase + ":" + ADMIN_PASSWORD).slice(0,16)
async function makeInvoiceToken(env, orderId, email) {
  const norm = (email || "").trim().toLowerCase();
  const data = orderId + ":" + norm + ":" + (env.ADMIN_PASSWORD || "");
  const encoder = new TextEncoder();
  const buf = await crypto.subtle.digest("SHA-256", encoder.encode(data));
  const bytes = new Uint8Array(buf);
  let hex = "";
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i].toString(16).padStart(2, "0");
  }
  return hex.slice(0, 16);
}

// Mevcut faturayı getir veya YENİ oluştur (lazy generation — admin/customer ortak kullanır)
async function getOrCreateInvoice(env, orderId) {
  let invoice = await getInvoice(env, orderId);
  if (invoice) return { invoice, isNew: false };

  const orders = await getOrders(env);
  const order = orders.find(o => o.id === orderId);
  if (!order) return { invoice: null, isNew: false, error: "Sipariş bulunamadı" };

  const number = await nextInvoiceNumber(env);
  const now = new Date();
  const orderDate = order.date ? new Date(order.date) : now;

  let customer = {
    name: order.name || "",
    email: order.email || "",
    phone: order.phone || "",
    address: order.addressDetail || null,
    address_text: order.address || ""
  };

  // Stripe canlı veri çek
  if (order.payment === "stripe" && order.stripePaymentId && env.STRIPE_SECRET_KEY) {
    try {
      const chRes = await fetch(
        "https://api.stripe.com/v1/charges?payment_intent=" + order.stripePaymentId + "&limit=1&expand[]=data.customer",
        { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
      );
      const chData = await chRes.json();
      const ch = chData.data && chData.data[0];
      if (ch) {
        const billing = ch.billing_details || {};
        const shipping = ch.shipping;
        const cust = (typeof ch.customer === 'object' && ch.customer) ? ch.customer : null;
        const addr = (shipping && shipping.address) || billing.address || (cust && cust.address) || {};

        if (!customer.name || customer.name === "Stripe Müşteri") {
          customer.name = (shipping && shipping.name) || billing.name || (cust && cust.name) || customer.name;
        }
        if (!customer.email) customer.email = billing.email || (cust && cust.email) || ch.receipt_email || "";
        if (!customer.phone) customer.phone = billing.phone || (shipping && shipping.phone) || (cust && cust.phone) || "";
        if (!customer.address || !customer.address.line1) {
          customer.address = {
            line1: addr.line1 || "",
            line2: addr.line2 || "",
            city: addr.city || "",
            postal_code: addr.postal_code || "",
            state: addr.state || "",
            country: addr.country || ""
          };
          customer.address_text = [addr.line1, addr.line2, [addr.postal_code, addr.city].filter(Boolean).join(' '), addr.country].filter(Boolean).join(', ');
        }
      }
    } catch (e) { /* ignore */ }
  }

  // Items normalize
  let items = order.items;
  if (typeof items === 'string') {
    try { items = JSON.parse(items); } catch { items = []; }
  }
  if (!Array.isArray(items)) items = [];

  // Ürün listesi (artikel_nr lookup için)
  const productsList = await getProducts(env);
  const mappedItems = items.map(it => {
    const productId = it.productId || it.id || "";
    const p = productsList.find(pp => pp.id === productId || (pp.name_de||pp.name) === (it.name||it.title));
    return {
      artikel_nr: p?.artikel_nr || "",
      product_id: productId || (p?.id || ""),
      name: it.name || it.title || (p?.name_de || p?.name) || it.product || productId || "—",
      qty: parseFloat(it.qty || it.quantity || it.count || 1),
      price: parseFloat(it.price || it.amount || 0)
    };
  });

  invoice = {
    number,
    order_id: orderId,
    order_ref: order.ref || orderId,
    issued_at: now.toISOString(),
    order_date: orderDate.toISOString(),
    payment: order.payment || "",
    payment_id: order.stripePaymentId || order.paypalTxId || "",
    customer,
    items: mappedItems,
    subtotal: items.reduce((s, it) => s + (parseFloat(it.price||0) * parseFloat(it.qty || it.quantity || 1)), 0),
    shipping_fee: parseFloat(order.shipping_fee || 0),
    total: parseFloat(order.total || 0),
    currency: "EUR",
    tax_mode: "kleinunternehmer",
    tax_note: "Gemäß § 19 UStG wird keine Umsatzsteuer ausgewiesen."
  };

  await putInvoice(env, orderId, invoice);

  order.invoice_number = number;
  order.invoice_issued_at = invoice.issued_at;
  await putOrders(env, orders);

  // GoBD: Otomatik Geldfluss Einnahme kaydı
  try {
    await autoAddInvoiceEinnahme(env, invoice);
  } catch (e) { /* geldfluss hata fatal değil */ }

  return { invoice, isNew: true };
}

// ============================================================
// BILANZ — Hesaplama helper'ı (JSON ve PDF endpoint'leri kullanır)
// ============================================================
async function computeBilanz(env, year) {
  const lastYear = year - 1;
  const data = await getGeldfluss(env);
  const labels = data.category_labels || {};
  const allEntries = (data.entries || []).filter(e => !e.voided_by && e.date);

  const yearEntries = allEntries.filter(e => e.date.startsWith(year + "-"));
  const lastYearEntries = allEntries.filter(e => e.date.startsWith(lastYear + "-"));

  function calcTotals(entries) {
    let ein = 0, aus = 0;
    const catEin = {}, catAus = {};
    const monthly = {};
    for (let m = 1; m <= 12; m++) {
      monthly[String(m).padStart(2, '0')] = { einnahmen: 0, ausgaben: 0 };
    }
    for (const e of entries) {
      const a = parseFloat(e.amount) || 0;
      const cat = e.category || "sonstige";
      const label = labels[cat] || cat;
      const month = e.date.slice(5, 7);
      if (e.type === "einnahme") {
        ein += a;
        catEin[label] = (catEin[label] || 0) + a;
        if (monthly[month]) monthly[month].einnahmen += a;
      } else if (e.type === "ausgabe") {
        aus += a;
        catAus[label] = (catAus[label] || 0) + a;
        if (monthly[month]) monthly[month].ausgaben += a;
      }
    }
    return { ein, aus, catEin, catAus, monthly };
  }

  const cur = calcTotals(yearEntries);
  const prev = calcTotals(lastYearEntries);

  const now = new Date();
  const isCurrentYear = year === now.getFullYear();
  const currentMonth = isCurrentYear ? String(now.getMonth() + 1).padStart(2, '0') : '12';
  const thisMonthEntries = yearEntries.filter(e => e.date.slice(5, 7) === currentMonth);
  const thisMonth = calcTotals(thisMonthEntries);

  const prevMonthNum = parseInt(currentMonth) - 1;
  let lastMonthEntries;
  if (prevMonthNum < 1) {
    lastMonthEntries = lastYearEntries.filter(e => e.date.slice(5, 7) === '12');
  } else {
    const pmStr = String(prevMonthNum).padStart(2, '0');
    lastMonthEntries = yearEntries.filter(e => e.date.slice(5, 7) === pmStr);
  }
  const lastMonth = calcTotals(lastMonthEntries);

  // Müşteri bakiyeleri
  const customers = await getCustomers(env);
  let openInvoiceCount = 0;
  let openInvoiceTotal = 0;
  const topCustomers = [];
  for (const c of customers) {
    const balance = await calculateCustomerBalance(env, c.id, c);
    if (balance.balance > 0.01) {
      openInvoiceCount++;
      openInvoiceTotal += balance.balance;
    }
    if (balance.total_invoiced > 0) {
      topCustomers.push({
        id: c.id,
        name: c.customer_type === 'b2b' ? (c.company_name || c.name || c.email) : (c.name || c.email),
        type: c.customer_type || 'b2c',
        total_invoiced: balance.total_invoiced,
        total_paid: balance.total_paid,
        balance: balance.balance
      });
    }
  }
  topCustomers.sort((a, b) => b.total_invoiced - a.total_invoiced);

  const runningBalance = [];
  let cumulative = 0;
  for (let m = 1; m <= 12; m++) {
    const key = String(m).padStart(2, '0');
    const mData = cur.monthly[key];
    cumulative += (mData.einnahmen - mData.ausgaben);
    runningBalance.push({
      month: key,
      einnahmen: mData.einnahmen,
      ausgaben: mData.ausgaben,
      saldo: mData.einnahmen - mData.ausgaben,
      cumulative
    });
  }

  const topCatEin = Object.entries(cur.catEin)
    .sort((a, b) => b[1] - a[1])
    .map(([label, amount]) => ({ label, amount, percent: cur.ein > 0 ? amount / cur.ein : 0 }));
  const topCatAus = Object.entries(cur.catAus)
    .sort((a, b) => b[1] - a[1])
    .map(([label, amount]) => ({ label, amount, percent: cur.aus > 0 ? amount / cur.aus : 0 }));

  const KU_LIMIT = 22000;
  const kuPercent = cur.ein / KU_LIMIT;
  const kuStatus = cur.ein <= KU_LIMIT ? 'unter' : 'über';

  return {
    year,
    kpi: {
      year_einnahmen: cur.ein,
      year_ausgaben: cur.aus,
      year_saldo: cur.ein - cur.aus,
      year_einnahmen_prev: prev.ein,
      year_ausgaben_prev: prev.aus,
      year_saldo_prev: prev.ein - prev.aus,
      month_einnahmen: thisMonth.ein,
      month_ausgaben: thisMonth.aus,
      month_saldo: thisMonth.ein - thisMonth.aus,
      last_month_einnahmen: lastMonth.ein,
      last_month_ausgaben: lastMonth.aus,
      last_month_saldo: lastMonth.ein - lastMonth.aus,
      open_invoice_count: openInvoiceCount,
      open_invoice_total: openInvoiceTotal
    },
    monthly: runningBalance,
    monthly_prev: Object.entries(prev.monthly).map(([month, d]) => ({
      month, einnahmen: d.einnahmen, ausgaben: d.ausgaben
    })),
    categories: {
      einnahmen: topCatEin,
      ausgaben: topCatAus
    },
    top_customers: topCustomers,
    kleinunternehmer: {
      limit: KU_LIMIT,
      current: cur.ein,
      percent: kuPercent,
      status: kuStatus,
      remaining: Math.max(0, KU_LIMIT - cur.ein)
    },
    transaction_count: yearEntries.length,
    generated_at: new Date().toISOString()
  };
}

// HTML escape utility
function bilanzEscape(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function bilanzFmtEur(n) {
  return '€ ' + (Number(n) || 0).toFixed(2).replace('.', ',').replace(/\B(?=(\d{3})+(?!\d))/g, '.').replace(/(\d+)\.(\d{2})$/, '$1,$2');
}

function bilanzFmtPct(n) {
  return (n * 100).toFixed(1) + '%';
}

// PDF için Bilanz raporu HTML (vergi danışmanı için profesyonel görünüm)
function renderBilanzPDFHTML(b, companyInfo) {
  const monthNames = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember'];
  const k = b.kpi;
  const ku = b.kleinunternehmer;
  const yearSaldoColor = k.year_saldo >= 0 ? '#16a34a' : '#dc2626';

  // Pct değişim
  const eDiff = k.year_einnahmen_prev > 0 ? ((k.year_einnahmen - k.year_einnahmen_prev) / k.year_einnahmen_prev) * 100 : 0;
  const aDiff = k.year_ausgaben_prev > 0 ? ((k.year_ausgaben - k.year_ausgaben_prev) / k.year_ausgaben_prev) * 100 : 0;
  const sDiff = k.year_saldo_prev !== 0 ? ((k.year_saldo - k.year_saldo_prev) / Math.abs(k.year_saldo_prev)) * 100 : 0;

  const monthlyRows = b.monthly.map((m, i) => `
    <tr>
      <td>${monthNames[i]}</td>
      <td class="r">${bilanzFmtEur(m.einnahmen)}</td>
      <td class="r">${bilanzFmtEur(m.ausgaben)}</td>
      <td class="r" style="font-weight:600;color:${m.saldo >= 0 ? '#16a34a' : '#dc2626'}">${bilanzFmtEur(m.saldo)}</td>
      <td class="r" style="color:${m.cumulative >= 0 ? '#1e40af' : '#dc2626'}">${bilanzFmtEur(m.cumulative)}</td>
    </tr>
  `).join('');

  const totalEin = b.monthly.reduce((s, m) => s + m.einnahmen, 0);
  const totalAus = b.monthly.reduce((s, m) => s + m.ausgaben, 0);

  const catEinRows = b.categories.einnahmen.map(c => `
    <tr>
      <td>${bilanzEscape(c.label)}</td>
      <td class="r">${bilanzFmtEur(c.amount)}</td>
      <td class="r">${bilanzFmtPct(c.percent)}</td>
    </tr>
  `).join('') || '<tr><td colspan="3" style="text-align:center;color:#888">Keine Einträge</td></tr>';

  const catAusRows = b.categories.ausgaben.map(c => `
    <tr>
      <td>${bilanzEscape(c.label)}</td>
      <td class="r">${bilanzFmtEur(c.amount)}</td>
      <td class="r">${bilanzFmtPct(c.percent)}</td>
    </tr>
  `).join('') || '<tr><td colspan="3" style="text-align:center;color:#888">Keine Einträge</td></tr>';

  const customerRows = b.top_customers.slice(0, 15).map(c => `
    <tr>
      <td>${bilanzEscape(c.name)}</td>
      <td>${c.type.toUpperCase()}</td>
      <td class="r">${bilanzFmtEur(c.total_invoiced)}</td>
      <td class="r">${bilanzFmtEur(c.total_paid)}</td>
      <td class="r" style="font-weight:600;color:${c.balance > 0.01 ? '#dc2626' : '#16a34a'}">${bilanzFmtEur(c.balance)}</td>
    </tr>
  `).join('') || '<tr><td colspan="5" style="text-align:center;color:#888">Keine Kunden</td></tr>';

  const ci = companyInfo || {};
  const generatedDate = new Date(b.generated_at).toLocaleString('de-DE', {
    day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit'
  });

  let kuColor = '#16a34a';
  if (ku.percent > 0.8) kuColor = '#ea580c';
  if (ku.percent > 1.0) kuColor = '#dc2626';

  return `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<title>Bilanz ${b.year}</title>
<style>
  @page { size: A4; margin: 14mm; }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif; color: #1a1a1a; font-size: 10pt; line-height: 1.5; margin: 0; }
  h1 { font-size: 24pt; margin: 0 0 0.3em 0; color: #2a4a1a; font-weight: 700; }
  h2 { font-size: 14pt; margin: 1.5em 0 0.5em 0; color: #2a4a1a; padding-bottom: 0.2em; border-bottom: 2px solid #e8e3d6; page-break-after: avoid; }
  h3 { font-size: 11pt; margin: 1em 0 0.4em 0; color: #444; page-break-after: avoid; }
  table { width: 100%; border-collapse: collapse; margin: 0.5em 0; font-size: 9.5pt; }
  th, td { padding: 6px 8px; text-align: left; border-bottom: 1px solid #e8e3d6; }
  th { background: #faf7f0; color: #555; font-size: 8.5pt; text-transform: uppercase; letter-spacing: 0.04em; font-weight: 600; }
  td.r, th.r { text-align: right; font-variant-numeric: tabular-nums; }
  .cover { padding: 80mm 0 0 0; text-align: center; page-break-after: always; }
  .cover-title { font-size: 36pt; color: #2a4a1a; font-weight: 700; margin: 0 0 0.3em 0; }
  .cover-sub { font-size: 16pt; color: #6b6b5a; margin-bottom: 2em; }
  .cover-meta { margin-top: 3em; font-size: 11pt; color: #555; }
  .cover-meta strong { color: #2a4a1a; }
  .header-band { background: linear-gradient(90deg, #2a4a1a 0%, #3d6b2a 100%); color: white; padding: 12px 16px; margin: 0 -14mm 1.5em -14mm; padding-left: 14mm; padding-right: 14mm; }
  .header-band h1 { color: white; margin: 0; font-size: 18pt; }
  .header-band .sub { color: rgba(255,255,255,0.85); font-size: 10pt; margin-top: 0.2em; }
  .kpi-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin: 1em 0; }
  .kpi-card { border: 1px solid #e8e3d6; border-radius: 8px; padding: 12px 14px; background: #faf7f0; }
  .kpi-card.full { grid-column: 1 / -1; }
  .kpi-label { font-size: 8.5pt; color: #6b6b5a; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }
  .kpi-value { font-size: 16pt; font-weight: 700; margin-top: 4px; }
  .kpi-sub { font-size: 8.5pt; color: #888; margin-top: 4px; }
  .ku-bar-wrap { background: #f0ebe0; border-radius: 6px; height: 14px; overflow: hidden; margin: 8px 0; }
  .ku-bar { height: 100%; background: ${kuColor}; }
  .footer { font-size: 8pt; color: #888; text-align: center; margin-top: 2em; padding-top: 1em; border-top: 1px solid #e8e3d6; }
  .signatures { margin-top: 4em; display: grid; grid-template-columns: 1fr 1fr; gap: 4em; }
  .sig-line { border-bottom: 1px solid #555; padding-bottom: 4em; }
  .sig-label { font-size: 9pt; color: #666; margin-top: 6px; }
  .summary-box { background: #f8f4ea; border-left: 4px solid #2a4a1a; padding: 12px 16px; margin: 1em 0; }
  .pct-up { color: #16a34a; font-weight: 600; }
  .pct-down { color: #dc2626; font-weight: 600; }
  tfoot td { font-weight: 700; background: #faf7f0; border-top: 2px solid #2a4a1a; }
  .text-muted { color: #888; }
  .page-break { page-break-before: always; }
  ul.summary-list { margin: 0; padding-left: 20px; }
  ul.summary-list li { margin-bottom: 4px; }
</style>
</head>
<body>

<!-- KAPAK SAYFASI -->
<div class="cover">
  <div class="cover-title">Bilanz ${b.year}</div>
  <div class="cover-sub">Einnahmen-Überschuss-Rechnung</div>

  <div class="cover-meta">
    <div style="font-size:14pt;font-weight:600;margin-bottom:0.5em">${bilanzEscape(ci.name || 'Keby Shop')}</div>
    ${ci.address ? `<div>${bilanzEscape(ci.address)}</div>` : ''}
    ${ci.tax_id ? `<div>USt-IdNr.: ${bilanzEscape(ci.tax_id)}</div>` : ''}
    <div style="margin-top:2em">Berichtszeitraum: <strong>01.01.${b.year} – 31.12.${b.year}</strong></div>
    <div>Erstellt am: <strong>${generatedDate}</strong></div>
    <div style="margin-top:1.5em;color:#6b6b5a;font-size:9pt">Hinweis: Steuerregelung nach § 19 UStG (Kleinunternehmer)</div>
  </div>
</div>

<!-- ZUSAMMENFASSUNG -->
<div class="header-band">
  <h1>📊 Zusammenfassung ${b.year}</h1>
  <div class="sub">Einnahmen, Ausgaben und Gewinn/Verlust auf einen Blick</div>
</div>

<div class="kpi-grid">
  <div class="kpi-card">
    <div class="kpi-label">Einnahmen</div>
    <div class="kpi-value" style="color:#16a34a">${bilanzFmtEur(k.year_einnahmen)}</div>
    <div class="kpi-sub">
      Vorjahr: ${bilanzFmtEur(k.year_einnahmen_prev)}
      ${eDiff !== 0 ? `<span class="${eDiff >= 0 ? 'pct-up' : 'pct-down'}"> (${eDiff >= 0 ? '+' : ''}${eDiff.toFixed(1)}%)</span>` : ''}
    </div>
  </div>

  <div class="kpi-card">
    <div class="kpi-label">Ausgaben</div>
    <div class="kpi-value" style="color:#dc2626">${bilanzFmtEur(k.year_ausgaben)}</div>
    <div class="kpi-sub">
      Vorjahr: ${bilanzFmtEur(k.year_ausgaben_prev)}
      ${aDiff !== 0 ? `<span class="${aDiff >= 0 ? 'pct-down' : 'pct-up'}"> (${aDiff >= 0 ? '+' : ''}${aDiff.toFixed(1)}%)</span>` : ''}
    </div>
  </div>

  <div class="kpi-card full" style="background:linear-gradient(90deg,#f8f4ea 0%, #faf7f0 100%);border-left:4px solid ${yearSaldoColor}">
    <div class="kpi-label">Gewinn / Verlust ${b.year}</div>
    <div class="kpi-value" style="color:${yearSaldoColor};font-size:22pt">${bilanzFmtEur(k.year_saldo)}</div>
    <div class="kpi-sub">
      Vorjahr: ${bilanzFmtEur(k.year_saldo_prev)}
      ${sDiff !== 0 ? `<span class="${sDiff >= 0 ? 'pct-up' : 'pct-down'}"> (${sDiff >= 0 ? '+' : ''}${sDiff.toFixed(1)}%)</span>` : ''}
    </div>
  </div>
</div>

<div class="summary-box">
  <h3 style="margin-top:0">📌 Wichtige Kennzahlen</h3>
  <ul class="summary-list">
    <li><strong>Anzahl der Buchungen:</strong> ${b.transaction_count}</li>
    <li><strong>Offene Forderungen:</strong> ${bilanzFmtEur(k.open_invoice_total)} (${k.open_invoice_count} Rechnungen)</li>
    <li><strong>Aktive Kunden mit Umsatz:</strong> ${b.top_customers.length}</li>
    <li><strong>Anzahl Einnahmenkategorien:</strong> ${b.categories.einnahmen.length}</li>
    <li><strong>Anzahl Ausgabenkategorien:</strong> ${b.categories.ausgaben.length}</li>
  </ul>
</div>

<h3>§ 19 UStG — Kleinunternehmer-Status</h3>
<div class="kpi-card">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
    <div><strong>Umsatz ${b.year}:</strong> ${bilanzFmtEur(ku.current)} von ${bilanzFmtEur(ku.limit)} (${bilanzFmtPct(ku.percent)})</div>
    <div style="color:${kuColor};font-weight:600">${ku.status === 'unter' ? '✓ unter Grenze' : '⚠ über Grenze'}</div>
  </div>
  <div class="ku-bar-wrap"><div class="ku-bar" style="width:${Math.min(100, ku.percent * 100).toFixed(1)}%"></div></div>
  <div class="kpi-sub">
    ${ku.remaining > 0
      ? `Verbleibender Spielraum: ${bilanzFmtEur(ku.remaining)}`
      : 'Grenze überschritten — Wechsel zur Regelbesteuerung erforderlich'}
  </div>
</div>

<!-- MONATLICHE AUFSTELLUNG -->
<div class="page-break"></div>
<div class="header-band">
  <h1>📅 Monatliche Aufstellung ${b.year}</h1>
  <div class="sub">Einnahmen, Ausgaben, Saldo und kumulativer Stand pro Monat</div>
</div>

<table>
  <thead>
    <tr>
      <th>Monat</th>
      <th class="r">Einnahmen</th>
      <th class="r">Ausgaben</th>
      <th class="r">Saldo</th>
      <th class="r">Kumulativ</th>
    </tr>
  </thead>
  <tbody>
    ${monthlyRows}
  </tbody>
  <tfoot>
    <tr>
      <td>Gesamt ${b.year}</td>
      <td class="r" style="color:#16a34a">${bilanzFmtEur(totalEin)}</td>
      <td class="r" style="color:#dc2626">${bilanzFmtEur(totalAus)}</td>
      <td class="r" style="color:${totalEin - totalAus >= 0 ? '#16a34a' : '#dc2626'}">${bilanzFmtEur(totalEin - totalAus)}</td>
      <td class="r"></td>
    </tr>
  </tfoot>
</table>

<!-- KATEGORİLER -->
<div class="page-break"></div>
<div class="header-band">
  <h1>🏷️ Kategorien-Übersicht</h1>
  <div class="sub">Einnahmen und Ausgaben nach Kategorie sortiert</div>
</div>

<h3 style="color:#16a34a">Einnahmen nach Kategorie</h3>
<table>
  <thead>
    <tr>
      <th>Kategorie</th>
      <th class="r">Betrag</th>
      <th class="r">Anteil</th>
    </tr>
  </thead>
  <tbody>${catEinRows}</tbody>
  <tfoot>
    <tr>
      <td>Gesamt</td>
      <td class="r">${bilanzFmtEur(k.year_einnahmen)}</td>
      <td class="r">100,0%</td>
    </tr>
  </tfoot>
</table>

<h3 style="color:#dc2626;margin-top:2em">Ausgaben nach Kategorie</h3>
<table>
  <thead>
    <tr>
      <th>Kategorie</th>
      <th class="r">Betrag</th>
      <th class="r">Anteil</th>
    </tr>
  </thead>
  <tbody>${catAusRows}</tbody>
  <tfoot>
    <tr>
      <td>Gesamt</td>
      <td class="r">${bilanzFmtEur(k.year_ausgaben)}</td>
      <td class="r">100,0%</td>
    </tr>
  </tfoot>
</table>

<!-- TOP KUNDEN -->
${b.top_customers.length > 0 ? `
<div class="page-break"></div>
<div class="header-band">
  <h1>🏆 Top-Kunden ${b.year}</h1>
  <div class="sub">Kunden nach Gesamtumsatz sortiert</div>
</div>

<table>
  <thead>
    <tr>
      <th>Kunde</th>
      <th>Typ</th>
      <th class="r">Fakturiert</th>
      <th class="r">Bezahlt</th>
      <th class="r">Saldo</th>
    </tr>
  </thead>
  <tbody>${customerRows}</tbody>
</table>

${b.top_customers.length > 15 ? `<div class="text-muted" style="margin-top:8px;font-size:9pt">Zeigt die 15 größten Kunden. Insgesamt ${b.top_customers.length} Kunden mit Umsatz.</div>` : ''}
` : ''}

<!-- SIGNATURES + FOOTER -->
<div class="page-break"></div>
<div class="header-band">
  <h1>📝 Bestätigung</h1>
  <div class="sub">Unterschrift und Bestätigung der Vollständigkeit</div>
</div>

<div class="summary-box">
  <p style="margin:0">Hiermit bestätige ich, dass die in diesem Bericht aufgeführten Daten vollständig und nach bestem Wissen und Gewissen erstellt wurden. Alle Buchungen entsprechen den Vorgaben der GoBD (Grundsätze zur ordnungsmäßigen Führung und Aufbewahrung von Büchern, Aufzeichnungen und Unterlagen in elektronischer Form).</p>
</div>

<div class="signatures">
  <div>
    <div class="sig-line"></div>
    <div class="sig-label">Ort, Datum</div>
  </div>
  <div>
    <div class="sig-line"></div>
    <div class="sig-label">Unterschrift Geschäftsinhaber/in</div>
  </div>
</div>

<div class="signatures">
  <div>
    <div class="sig-line"></div>
    <div class="sig-label">Ort, Datum</div>
  </div>
  <div>
    <div class="sig-line"></div>
    <div class="sig-label">Steuerberater/in (optional)</div>
  </div>
</div>

<div class="footer">
  Erstellt mit Keby Buchhaltung am ${generatedDate}<br>
  Dieser Bericht ist eine zusammenfassende Übersicht und ersetzt keine offizielle Steuererklärung.<br>
  Für die Steuererklärung beim Finanzamt nutzen Sie bitte die DATEV-CSV oder EÜR-Excel-Datei.
</div>

</body>
</html>`;
}

// PDF üret (Browser Rendering API çağrısı)
async function generateInvoicePDF(env, invoice) {
  if (!env.BROWSER_RENDERING_TOKEN || !env.CF_ACCOUNT_ID) {
    throw new Error("Browser Rendering yapılandırılmamış");
  }
  const html = renderInvoicePDFHTML(invoice);
  const res = await fetch(
    "https://api.cloudflare.com/client/v4/accounts/" + env.CF_ACCOUNT_ID + "/browser-rendering/pdf",
    {
      method: "POST",
      headers: {
        Authorization: "Bearer " + env.BROWSER_RENDERING_TOKEN,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        html,
        pdfOptions: {
          format: "a4",
          printBackground: true,
          margin: {
            top: "12mm",
            bottom: "12mm",
            left: "12mm",
            right: "12mm"
          }
        }
      })
    }
  );
  if (!res.ok) {
    const errText = await res.text();
    throw new Error("PDF API HTTP " + res.status + ": " + errText.slice(0, 200));
  }
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("application/json")) {
    const errText = await res.text();
    throw new Error("PDF API JSON döndü: " + errText.slice(0, 200));
  }
  return await res.arrayBuffer();
}

async function getCoupons(env) {
  try {
    const obj = await env.KEBY_R2.get("keby/data/coupons.json");
    if (!obj) return [];
    return JSON.parse(await obj.text());
  } catch (e) { return []; }
}

async function saveCoupons(env, coupons) {
  await env.KEBY_R2.put("keby/data/coupons.json",
    JSON.stringify(coupons),
    { httpMetadata: { contentType: "application/json" } }
  );
}

// Kargo URL üretici — büyük firmalar için otomatik tracking linki
function buildTrackingUrl(carrier, trackingNumber) {
  const tn = encodeURIComponent(trackingNumber);
  const urls = {
    "DHL":     `https://www.dhl.de/de/privatkunden/pakete-empfangen/verfolgen.html?piececode=${tn}`,
    "DPD":     `https://tracking.dpd.de/status/de_DE/parcel/${tn}`,
    "Hermes":  `https://www.myhermes.de/empfangen/sendungsverfolgung/sendungsinformation/#${tn}`,
    "GLS":     `https://gls-group.eu/DE/de/paketverfolgung?match=${tn}`,
    "UPS":     `https://www.ups.com/track?tracknum=${tn}`,
    "FedEx":   `https://www.fedex.com/fedextrack/?trknbr=${tn}`,
    "Post AT": `https://www.post.at/sv/sendungssuche?snr=${tn}`,
    "Post CH": `https://service.post.ch/ekp-web/ui/entry/search/${tn}`,
    "Andere":  null
  };
  return urls[carrier] || null;
}

// Müşteriye gönderilen tracking email HTML şablonu
function buildTrackingEmailHtml(order) {
  const trackingUrl = order.tracking_url;
  const carrier = order.carrier || "DHL";
  const tn = order.tracking_number;
  const ref = order.ref || order.id;
  const customerName = order.name || order.billing_name || "Kund*in";
  const items = (order.items || []).map(i =>
    `<tr><td style="padding:6px 12px;border-bottom:1px solid #f0ebe0">${i.name || i.productId}</td>
     <td style="padding:6px 12px;border-bottom:1px solid #f0ebe0;text-align:right">× ${i.qty}</td></tr>`
  ).join("");

  return `<!DOCTYPE html>
<html lang="de">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f5f1e8;font-family:-apple-system,Helvetica,Arial,sans-serif">
<div style="max-width:600px;margin:32px auto;background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 16px rgba(0,0,0,0.08)">

  <!-- Header -->
  <div style="background:#2a4a1a;padding:28px 32px;text-align:center">
    <div style="color:#e8d49a;font-size:2rem;margin-bottom:8px">📦</div>
    <h1 style="color:white;margin:0;font-size:1.4rem;font-weight:500">Ihr Paket ist unterwegs!</h1>
    <p style="color:rgba(255,255,255,0.8);margin:8px 0 0;font-size:0.9rem">Bestellung ${ref}</p>
  </div>

  <!-- Content -->
  <div style="padding:32px">
    <p style="color:#333;font-size:1rem;margin:0 0 24px">Hallo ${customerName},</p>
    <p style="color:#555;line-height:1.6;margin:0 0 24px">
      Ihre Bestellung wurde versandt und ist auf dem Weg zu Ihnen! 
      Mit der Sendungsnummer unten können Sie Ihr Paket jederzeit verfolgen.
    </p>

    <!-- Tracking Box -->
    <div style="background:#f5f1e8;border-radius:8px;padding:20px;margin-bottom:24px;text-align:center;border:2px solid #e8e3d6">
      <div style="font-size:0.75rem;text-transform:uppercase;letter-spacing:0.1em;color:#6b6b5a;margin-bottom:8px">Sendungsnummer · ${carrier}</div>
      <div style="font-size:1.3rem;font-weight:600;color:#2a4a1a;letter-spacing:0.05em;font-family:monospace">${tn}</div>
      ${trackingUrl ? `
      <a href="${trackingUrl}" style="display:inline-block;margin-top:14px;background:#2a4a1a;color:white;text-decoration:none;padding:10px 24px;border-radius:6px;font-size:0.88rem;font-weight:500">
        Sendung verfolgen →
      </a>` : ""}
    </div>

    <!-- Items -->
    ${items ? `
    <div style="margin-bottom:24px">
      <div style="font-size:0.75rem;text-transform:uppercase;letter-spacing:0.1em;color:#6b6b5a;margin-bottom:8px">Bestellte Artikel</div>
      <table style="width:100%;border-collapse:collapse;font-size:0.9rem">
        ${items}
      </table>
    </div>` : ""}

    <p style="color:#888;font-size:0.85rem;line-height:1.6;margin:0">
      Bei Fragen zu Ihrer Bestellung antworten Sie einfach auf diese E-Mail oder schreiben Sie an 
      <a href="mailto:info@keby.shop" style="color:#2a4a1a">info@keby.shop</a>.
    </p>
  </div>

  <!-- Footer -->
  <div style="background:#faf7f0;padding:20px 32px;text-align:center;border-top:1px solid #e8e3d6">
    <p style="margin:0;font-size:0.8rem;color:#888">
      <strong style="color:#2a4a1a">Keby Shop</strong> · Naturprodukte aus der Türkei<br>
      <a href="https://keby.shop" style="color:#2a4a1a;text-decoration:none">keby.shop</a> ·
      <a href="https://keby.shop/impressum.html" style="color:#888;text-decoration:none">Impressum</a> ·
      <a href="https://keby.shop/datenschutz.html" style="color:#888;text-decoration:none">Datenschutz</a>
    </p>
  </div>
</div>
</body></html>`;
}

async function getCustomers(env) {
  const obj = await env.KEBY_R2.get("keby/data/customers.json");
  if (!obj) return [];
  try { return JSON.parse(await obj.text()); } catch { return []; }
}
async function putCustomers(env, customers) {
  await env.KEBY_R2.put(
    "keby/data/customers.json",
    JSON.stringify(customers),
    { httpMetadata: { contentType: "application/json" } }
  );
}

// Yeni müşteri ID üret
function generateCustomerId() {
  return "cust_" + Date.now().toString(36) + Math.random().toString(36).substring(2, 8);
}

// Tüm faturaları listele (R2'den keby/invoices/ prefix tarayarak)
async function listAllInvoices(env) {
  const list = await env.KEBY_R2.list({ prefix: "keby/invoices/", limit: 1000 });
  const invoices = [];
  for (const obj of list.objects) {
    if (!obj.key.endsWith(".json")) continue;
    try {
      const r = await env.KEBY_R2.get(obj.key);
      if (r) {
        const inv = JSON.parse(await r.text());
        invoices.push(inv);
      }
    } catch (e) { /* skip */ }
  }
  // Tarihe göre sırala (yeni önce)
  invoices.sort((a, b) => {
    const da = new Date(a.issued_at || 0).getTime();
    const db = new Date(b.issued_at || 0).getTime();
    return db - da;
  });
  return invoices;
}

// ═══════════════ CARİ HESAP (Müşteri bakiyesi) ═══════════════
// Bir müşterinin: toplam faturalanmış - toplam ödenmiş = bakiye

async function calculateCustomerBalance(env, customerId, customerInfo) {
  const norm = (s) => (s || "").trim().toLowerCase();
  const custEmail = norm(customerInfo?.email);
  const custName = norm(customerInfo?.name);

  const allInvoices = await listAllInvoices(env);
  // Bu müşteriye ait faturaları bul
  const myInvoices = allInvoices.filter(inv => {
    if (inv.stornoed_at) return false;  // Stornoed faturalar bakiyede yok
    if (inv.customer_id === customerId) return true;
    if (!inv.customer) return false;
    if (custEmail && norm(inv.customer.email) === custEmail) return true;
    if (custName && norm(inv.customer.name) === custName) return true;
    return false;
  });

  const totalInvoiced = myInvoices.reduce((s, inv) => s + (inv.total || 0), 0);

  // Ödemeler customer.payments[] içinde
  const payments = (customerInfo?.payments || []).filter(p => !p.voided);
  const totalPaid = payments.reduce((s, p) => s + (p.amount || 0), 0);

  // Online ödemeler (Stripe/PayPal) zaten kapalı sayılır — fatura "paid" işaretli mi?
  // Manuel müşteriler için: invoice.payment.status veya customer.payments
  // Basit kural: invoice.payment?.status === "paid" → o tutar ödendi sayılır
  let onlinePaid = 0;
  for (const inv of myInvoices) {
    const status = inv.payment?.status;
    if (status === "paid" || status === "succeeded" || status === "completed") {
      onlinePaid += (inv.total || 0);
    }
  }

  const grandTotalPaid = totalPaid + onlinePaid;
  const balance = totalInvoiced - grandTotalPaid;

  return {
    invoice_count: myInvoices.length,
    total_invoiced: Math.round(totalInvoiced * 100) / 100,
    total_paid_manual: Math.round(totalPaid * 100) / 100,
    total_paid_online: Math.round(onlinePaid * 100) / 100,
    total_paid: Math.round(grandTotalPaid * 100) / 100,
    balance: Math.round(balance * 100) / 100,
    invoices: myInvoices.map(inv => ({
      number: inv.number,
      issued_at: inv.issued_at,
      total: inv.total,
      payment_status: inv.payment?.status || "unknown",
      order_id: inv.order_id
    })),
    payments: payments
  };
}

// Müşteriye ödeme kaydı ekle
function addCustomerPayment(customer, payment) {
  if (!customer.payments) customer.payments = [];
  const newPayment = {
    id: "pay_" + Date.now().toString(36) + Math.random().toString(36).substring(2, 8),
    date: payment.date || new Date().toISOString().slice(0, 10),
    amount: parseFloat(payment.amount) || 0,
    method: payment.method || "manual",  // "bar", "ueberweisung", "manual", etc.
    invoice_ref: payment.invoice_ref || null,
    note: payment.note || "",
    created_at: new Date().toISOString(),
    voided: false
  };
  customer.payments.push(newPayment);
  return newPayment;
}

// ═══════════════ GELDFLUSS-BUCH ═══════════════
// GoBD-konform Einnahmen/Ausgaben defteri

async function getGeldfluss(env) {
  try {
    const r = await env.KEBY_R2.get("keby/data/geldfluss.json");
    if (!r) return { entries: [], counters: {} };
    const data = JSON.parse(await r.text());
    return {
      entries: Array.isArray(data.entries) ? data.entries : [],
      counters: data.counters || {}
    };
  } catch (e) {
    return { entries: [], counters: {} };
  }
}

async function saveGeldfluss(env, data) {
  await env.KEBY_R2.put(
    "keby/data/geldfluss.json",
    JSON.stringify(data, null, 2),
    { httpMetadata: { contentType: "application/json" } }
  );
}

function generateGeldflussId(type, year, counters) {
  const c = counters[year] || { einnahme: 0, ausgabe: 0 };
  const newCount = (c[type] || 0) + 1;
  const prefix = type === "einnahme" ? "E" : "A";
  return `${prefix}-${year}-${String(newCount).padStart(4, '0')}`;
}

// Fatura kaydedildiğinde otomatik Einnahme ekle
async function autoAddInvoiceEinnahme(env, invoice) {
  const data = await getGeldfluss(env);

  // Aynı fatura için zaten kayıt var mı kontrol et (duplicate önleme)
  const existing = data.entries.find(e =>
    e.type === "einnahme" &&
    e.ref_type === "invoice" &&
    e.ref_id === invoice.order_id &&
    !e.voided_by
  );
  if (existing) return existing;

  const dateStr = (invoice.order_date || invoice.issued_at || new Date().toISOString()).slice(0, 10);
  const year = dateStr.slice(0, 4);

  if (!data.counters[year]) data.counters[year] = { einnahme: 0, ausgabe: 0 };
  const id = generateGeldflussId("einnahme", year, data.counters);
  data.counters[year].einnahme++;

  const customerName = invoice.customer?.name || "—";
  const entry = {
    id,
    type: "einnahme",
    date: dateStr,
    amount: parseFloat(invoice.total) || 0,
    description: `Verkauf — ${customerName}`,
    category: "verkauf",
    payment_method: invoice.payment || "bar",
    ref_type: "invoice",
    ref_id: invoice.order_id,
    ref_number: invoice.number,
    beleg_url: null,
    notes: "",
    created_at: new Date().toISOString(),
    created_by: "auto",
    immutable: true,
    voided_by: null
  };

  data.entries.push(entry);
  await saveGeldfluss(env, data);
  return entry;
}

// Storno yapıldığında otomatik Ausgabe (negatif Einnahme düzeltmesi)
async function autoAddStornoAusgabe(env, originalInvoice, stornoInvoice) {
  const data = await getGeldfluss(env);

  // Orijinal Einnahme'yi bul ve voided_by işaretle
  const originalEntry = data.entries.find(e =>
    e.type === "einnahme" &&
    e.ref_type === "invoice" &&
    e.ref_id === originalInvoice.order_id &&
    !e.voided_by
  );

  const dateStr = new Date().toISOString().slice(0, 10);
  const year = dateStr.slice(0, 4);
  if (!data.counters[year]) data.counters[year] = { einnahme: 0, ausgabe: 0 };
  const id = generateGeldflussId("ausgabe", year, data.counters);
  data.counters[year].ausgabe++;

  const stornoEntry = {
    id,
    type: "ausgabe",
    date: dateStr,
    amount: Math.abs(parseFloat(originalInvoice.total) || 0),
    description: `Storno — Rechnung ${originalInvoice.number}`,
    category: "storno",
    payment_method: originalInvoice.payment || "bar",
    ref_type: "invoice",
    ref_id: stornoInvoice.order_id,
    ref_number: stornoInvoice.number,
    beleg_url: null,
    notes: `Stornierung der Rechnung ${originalInvoice.number}`,
    created_at: new Date().toISOString(),
    created_by: "auto",
    immutable: true,
    voided_by: null
  };

  if (originalEntry) {
    originalEntry.voided_by = id;
  }

  data.entries.push(stornoEntry);
  await saveGeldfluss(env, data);
  return stornoEntry;
}

const GELDFLUSS_CATEGORIES = {
  einnahme: ["verkauf", "sonstige_einnahme"],
  ausgabe: ["wareneinkauf", "versand", "verpackung", "marketing", "buerobedarf", "steuern", "miete", "storno", "sonstiges"]
};

const GELDFLUSS_CATEGORY_LABELS = {
  verkauf: "Verkauf",
  sonstige_einnahme: "Sonstige Einnahme",
  wareneinkauf: "Wareneinkauf",
  versand: "Versand & Logistik",
  verpackung: "Etiketten & Verpackung",
  marketing: "Marketing & Werbung",
  buerobedarf: "Bürobedarf",
  steuern: "Steuern & Gebühren",
  miete: "Miete & Nebenkosten",
  storno: "Storno",
  sonstiges: "Sonstiges"
};

var worker_default = {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    const ip = request.headers.get("CF-Connecting-IP") || "unknown";

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: CORS });
    }

    // ============================================================
    // AUTH: Session token sistemi (UUID, 8 saat, R2'de store)
    // ============================================================

    // IP bazlı rate limit — login endpoint için (R2 counter, 1 dakika window)
    async function checkRateLimit(ip, maxAttempts = 10, windowSec = 60) {
      const key = `keby/auth/ratelimit/${ip.replace(/[:.]/g, '_')}.json`;
      let data = { count: 0, window_start: Date.now() };
      try {
        const obj = await env.KEBY_R2.get(key);
        if (obj) {
          const raw = await obj.text();
          data = JSON.parse(raw);
        }
      } catch (e) {}

      const now = Date.now();
      // Window sıfırla
      if (now - data.window_start > windowSec * 1000) {
        data = { count: 0, window_start: now };
      }

      data.count++;
      // Güncelle (non-blocking, TTL simulation — eski kayıtları silmiyoruz ama window reset ediyor)
      await env.KEBY_R2.put(key, JSON.stringify(data), {
        httpMetadata: { contentType: "application/json" }
      });

      return {
        allowed: data.count <= maxAttempts,
        count: data.count,
        limit: maxAttempts,
        remaining: Math.max(0, maxAttempts - data.count),
        retry_after: windowSec
      };
    }

    // Session oluştur — UUID + R2'de sakla
    async function createSession(env) {
      const sessionId = crypto.randomUUID();
      const expiresAt = Date.now() + 8 * 60 * 60 * 1000; // 8 saat
      const sessionData = { id: sessionId, created_at: Date.now(), expires_at: expiresAt };
      await env.KEBY_R2.put(
        `keby/auth/sessions/${sessionId}.json`,
        JSON.stringify(sessionData),
        { httpMetadata: { contentType: "application/json" } }
      );
      return sessionId;
    }

    // Session doğrula
    async function validateSession(env, sessionId) {
      if (!sessionId || sessionId.length < 10) return false;
      // Eski btoa token — backward compat (yeni session sistemi deploy edilmeden önce girişler için)
      if (sessionId === btoa(env.ADMIN_PASSWORD)) return true;
      try {
        const obj = await env.KEBY_R2.get(`keby/auth/sessions/${sessionId}.json`);
        if (!obj) return false;
        const session = JSON.parse(await obj.text());
        if (Date.now() > session.expires_at) {
          // Süresi dolmuş — sil
          await env.KEBY_R2.delete(`keby/auth/sessions/${sessionId}.json`).catch(() => {});
          return false;
        }
        return true;
      } catch (e) {
        return false;
      }
    }

    // Session sil (logout)
    async function deleteSession(env, sessionId) {
      if (!sessionId) return;
      try {
        await env.KEBY_R2.delete(`keby/auth/sessions/${sessionId}.json`);
      } catch (e) {}
    }

    // Token'ı Authorization header'dan çıkar
    const auth = request.headers.get("Authorization") || "";
    const tokenFromHeader = auth.startsWith("Bearer ") ? auth.slice(7) : "";

    // isAdmin: session validate (async)
    const isAdmin = tokenFromHeader ? await validateSession(env, tokenFromHeader) : false;

    // ============ IMAGE PROXY (R2 → public) ============
    if (path.startsWith("/img/")) {
      const raw = path.replace("/img/", "");
      const key = raw.includes("/") ? raw : "keby/" + raw;
      const obj = await env.KEBY_R2.get(key);
      if (!obj) return new Response("Not found", { status: 404 });

      // Check If-None-Match for 304 response
      const clientEtag = request.headers.get("If-None-Match");
      const objEtag = '"' + obj.etag + '"';
      if (clientEtag === objEtag) {
        return new Response(null, { status: 304, headers: CORS });
      }

      const h = new Headers(CORS);
      const ct = obj.httpMetadata?.contentType || (
        key.endsWith(".mov") ? "video/quicktime" :
        key.endsWith(".mp4") ? "video/mp4" :
        key.endsWith(".png") ? "image/png" :
        key.endsWith(".webp") ? "image/webp" : "image/jpeg"
      );
      h.set("Content-Type", ct);
      // ETag tied to R2 object version → browser auto-revalidates when image changes
      h.set("ETag", objEtag);
      // Short cache with must-revalidate so updates show immediately
      h.set("Cache-Control", "public, max-age=300, must-revalidate");
      h.set("Last-Modified", new Date(obj.uploaded).toUTCString());
      if (ct.startsWith("video/")) h.set("Accept-Ranges", "bytes");
      return new Response(obj.body, { headers: h });
    }

    // ============ PUBLIC PRODUCTS ============
    if (request.method === "GET" && path === "/api/products") {
      const products = await getProducts(env);
      return jsonResp({ success: true, products });
    }

    // ============ ADMIN: LOGIN CHECK ============
    if (request.method === "POST" && path === "/api/admin/login") {
      // Rate limit: IP bazlı 10 deneme / dakika
      // CF-Connecting-IP: Cloudflare'in gerçek client IP'si (dışarıdan spoof edilemez)
      const clientIP = request.headers.get("CF-Connecting-IP") ||
                       (request.cf && request.cf.ip) ||
                       "global";  // Gerçek Cloudflare edge'de her zaman dolu gelir
      const rl = await checkRateLimit(clientIP, 10, 60);
      if (!rl.allowed) {
        const headers = new Headers(CORS);
        headers.set("Retry-After", String(rl.retry_after));
        headers.set("X-RateLimit-Limit", String(rl.limit));
        headers.set("X-RateLimit-Remaining", "0");
        return new Response(
          JSON.stringify({ success: false, error: "Zu viele Versuche. Bitte warten.", retry_after: rl.retry_after }),
          { status: 429, headers }
        );
      }

      const { password } = await request.json().catch(() => ({}));
      if (password !== env.ADMIN_PASSWORD) {
        return errResp("Ungültiges Passwort", 401);
      }

      // Şifre doğru — session oluştur
      const sessionId = await createSession(env);
      return jsonResp({
        success: true,
        token: sessionId,
        expires_in: 8 * 60 * 60  // saniye
      });
    }

    // ============ ADMIN: LOGOUT ============
    if (request.method === "POST" && path === "/api/admin/logout") {
      await deleteSession(env, tokenFromHeader);
      return jsonResp({ success: true });
    }

    if (request.method === "GET" && path === "/api/admin/check") {
      return jsonResp({ success: isAdmin });
    }

    // ============ ADMIN: PRODUCTS ============
    // Full CRUD
    if (request.method === "POST" && path === "/api/admin/products" && isAdmin) {
      // Legacy: batch save entire list
      const { products } = await request.json();
      await putProducts(env, products);
      return jsonResp({ success: true });
    }

    // Create new product
    if (request.method === "POST" && path === "/api/admin/product/create" && isAdmin) {
      const body = await request.json();
      const products = await getProducts(env);
      const newProduct = {
        id: body.id || ("prod-" + Date.now()),
        slug: body.slug || ("produkt-" + (body.id || Date.now())),
        active: body.active !== false,
        sort: body.sort || (products.length + 1),
        name_tr: body.name_tr || "",
        name_de: body.name_de || "",
        price: parseFloat(body.price) || 0,
        mwst: body.mwst || "19%",
        stock: parseInt(body.stock) || 0,
        short_tr: body.short_tr || "",
        short_de: body.short_de || "",
        desc_tr: body.desc_tr || "",
        desc_de: body.desc_de || "",
        images: Array.isArray(body.images) ? body.images : [],
        thumb: body.thumb || (body.images && body.images[0]) || ""
      };
      if (products.find(p => p.id === newProduct.id)) {
        return errResp("Bu ID zaten mevcut: " + newProduct.id);
      }
      products.push(newProduct);
      await putProducts(env, products);
      return jsonResp({ success: true, product: newProduct });
    }

    // Update single product
    if (request.method === "PUT" && path.startsWith("/api/admin/product/") && isAdmin) {
      const id = decodeURIComponent(path.replace("/api/admin/product/", ""));
      const body = await request.json();
      const products = await getProducts(env);
      const idx = products.findIndex(p => p.id === id);
      if (idx === -1) return errResp("Ürün bulunamadı: " + id, 404);
      products[idx] = { ...products[idx], ...body, id: products[idx].id };
      if (typeof body.price !== "undefined") products[idx].price = parseFloat(body.price) || 0;
      if (typeof body.stock !== "undefined") products[idx].stock = parseInt(body.stock) || 0;
      await putProducts(env, products);
      return jsonResp({ success: true, product: products[idx] });
    }

    // Delete product
    if (request.method === "DELETE" && path.startsWith("/api/admin/product/") && isAdmin) {
      const id = decodeURIComponent(path.replace("/api/admin/product/", ""));
      const products = await getProducts(env);
      const filtered = products.filter(p => p.id !== id);
      if (filtered.length === products.length) return errResp("Ürün bulunamadı: " + id, 404);
      await putProducts(env, filtered);
      return jsonResp({ success: true });
    }

    // ============ ADMIN: ORDERS ============
    if (request.method === "GET" && (path === "/api/orders" || path === "/api/admin/orders")) {
      if (path === "/api/admin/orders" && !isAdmin) return errResp("Unauthorized", 401);
      const orders = await getOrders(env);
      return jsonResp({ success: true, orders });
    }

    // POST /api/admin/orders/recover-payment — Payment Intent ID ile sipariş kurtarma
    if (request.method === "POST" && path === "/api/admin/orders/recover-payment" && isAdmin) {
      try {
        const { payment_intent_id } = await request.json();
        if (!payment_intent_id) return errResp("payment_intent_id gerekli");

        const orders = await getOrders(env);
        const exists = orders.find(o => o.stripePaymentId === payment_intent_id);
        if (exists) return jsonResp({ success: true, order: exists, already_exists: true });

        // Payment intent çek
        const piRes = await fetch(
          `https://api.stripe.com/v1/payment_intents/${payment_intent_id}?expand[]=customer&expand[]=payment_method`,
          { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
        );
        const pi = await piRes.json();
        if (pi.error) return errResp("Stripe: " + pi.error.message, 404);

        // Checkout session bul (varsa)
        const sessListRes = await fetch(
          `https://api.stripe.com/v1/checkout/sessions?payment_intent=${payment_intent_id}&limit=1&expand[]=data.line_items&expand[]=data.customer`,
          { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
        );
        const sessList = await sessListRes.json();
        const session = sessList.data?.[0];

        // Billing details
        const billing = pi.charges?.data?.[0]?.billing_details || {};
        const shipping = pi.shipping || session?.shipping_details || {};
        const addr = shipping.address || billing.address || {};
        const customerEmail = billing.email || session?.customer_details?.email || pi.receipt_email || "";
        const customerName = shipping.name || billing.name || session?.customer_details?.name || "Stripe Müşteri";

        // Items
        let items = [];
        try {
          const meta = pi.metadata || session?.metadata || {};
          if (meta.items_json) items = JSON.parse(meta.items_json);
          else if (meta.items) items = JSON.parse(meta.items);
          else if (session?.line_items?.data) {
            items = session.line_items.data.map(li => ({
              name: li.description || "Ürün",
              qty: li.quantity,
              price: ((li.amount_total || 0) / 100) / li.quantity
            }));
          }
        } catch (e) {}

        const isPaid = pi.status === "succeeded";
        const payMethod = pi.payment_method_types?.[0] || "card";

        const newOrder = {
          id: "KB-" + Date.now(),
          ref: "KB-" + new Date().getFullYear() + "-" + String(orders.length + 1).padStart(3, "0"),
          stripeSessionId: session?.id || null,
          stripePaymentId: payment_intent_id,
          date: new Date(pi.created * 1000).toISOString(),
          status: isPaid ? "bezahlt" : (payMethod === "sepa_debit" ? "zahlung_ausstehend" : "offen"),
          payment: "stripe",
          payment_method: payMethod,
          email: customerEmail,
          name: customerName,
          phone: billing.phone || "",
          address: {
            line1: addr.line1 || "",
            line2: addr.line2 || "",
            city: addr.city || "",
            postal_code: addr.postal_code || "",
            country: addr.country || "DE"
          },
          items,
          total: ((pi.amount || 0) / 100).toFixed(2),
          currency: (pi.currency || "eur").toUpperCase(),
          coupon: (pi.metadata || session?.metadata)?.coupon_code || null,
          notes: "♻️ Retroaktif kurtarıldı"
        };

        orders.push(newOrder);
        await putOrders(env, orders);
        return jsonResp({ success: true, order: newOrder });
      } catch (e) {
        return errResp("Recover: " + e.message, 500);
      }
    }

    // POST /api/admin/orders/recover-session — Stripe session'dan siparişi kurtarma
    if (request.method === "POST" && path === "/api/admin/orders/recover-session" && isAdmin) {
      try {
        const { session_id } = await request.json();
        if (!session_id) return errResp("session_id gerekli");

        // Stripe'tan session çek
        const sessRes = await fetch(
          `https://api.stripe.com/v1/checkout/sessions/${session_id}?expand[]=line_items&expand[]=customer&expand[]=payment_intent`,
          { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
        );
        const session = await sessRes.json();
        if (session.error) return errResp("Stripe: " + session.error.message, 404);

        const orders = await getOrders(env);
        // Zaten kayıtlı mı?
        const exists = orders.find(o =>
          o.stripeSessionId === session.id || o.stripePaymentId === session.payment_intent
        );
        if (exists) return jsonResp({ success: true, order: exists, already_exists: true });

        const details = session.customer_details || {};
        const shipping = session.shipping_details || session.shipping || {};
        const addr = (shipping.address) || details.address || {};

        let items = [];
        try {
          const meta = session.metadata || {};
          if (meta.items_json) items = JSON.parse(meta.items_json);
          else if (meta.items) items = JSON.parse(meta.items);
          else if (session.line_items?.data) {
            items = session.line_items.data.map(li => ({
              name: li.description || "Ürün",
              qty: li.quantity,
              price: ((li.amount_total || 0) / 100) / li.quantity
            }));
          }
        } catch (e) {}

        const newOrder = {
          id: "KB-" + Date.now(),
          ref: "KB-" + new Date().getFullYear() + "-" + String(orders.length + 1).padStart(3, "0"),
          stripeSessionId: session.id,
          stripePaymentId: session.payment_intent,
          date: new Date().toISOString(),
          status: session.payment_status === "paid" ? "bezahlt" : "zahlung_ausstehend",
          payment: "stripe",
          payment_method: session.payment_method_types?.[0] || "card",
          email: details.email || "",
          name: shipping.name || details.name || "Stripe Müşteri",
          phone: details.phone || "",
          address: {
            line1: addr.line1 || "",
            line2: addr.line2 || "",
            city: addr.city || "",
            postal_code: addr.postal_code || "",
            country: addr.country || "DE"
          },
          items,
          total: ((session.amount_total || 0) / 100).toFixed(2),
          currency: (session.currency || "eur").toUpperCase(),
          coupon: session.metadata?.coupon_code || null,
          notes: "♻️ Retroaktif kurtarıldı"
        };

        orders.push(newOrder);
        await putOrders(env, orders);
        return jsonResp({ success: true, order: newOrder, recovered: true });
      } catch (e) {
        return errResp("Recover: " + e.message, 500);
      }
    }
    if (request.method === "POST" && path === "/api/admin/orders" && isAdmin) {
      const { orders } = await request.json();
      await putOrders(env, orders);
      return jsonResp({ success: true });
    }

    // ═══════════════════════════════════════════════════
    // VERSAND & TRACKING
    // ═══════════════════════════════════════════════════

    // POST /api/admin/orders/:id/tracking — tracking ekle/güncelle
    if (request.method === "POST" && path.match(/^\/api\/admin\/orders\/[^/]+\/tracking$/) && isAdmin) {
      try {
        const orderId = path.split("/")[4];
        const body = await request.json();
        const { tracking_number, carrier, tracking_url, send_email } = body;

        if (!tracking_number) return errResp("tracking_number zorunlu");

        const orders = await getOrders(env);
        const order = orders.find(o => o.id === orderId);
        if (!order) return errResp("Sipariş bulunamadı", 404);

        // Tracking bilgilerini güncelle
        order.tracking_number = tracking_number.trim();
        order.carrier = carrier || "DHL";
        order.tracking_url = tracking_url || buildTrackingUrl(carrier || "DHL", tracking_number.trim());
        order.tracking_added_at = new Date().toISOString();
        order.status = order.status === "neu" ? "gonderildi" : (order.status || "gonderildi");

        await putOrders(env, orders);

        // Müşteriye tracking maili gönder
        let emailSent = false;
        if (send_email !== false && order.email && env.RESEND_API_KEY) {
          try {
            const emailHtml = buildTrackingEmailHtml(order);
            const emailRes = await fetch("https://api.resend.com/emails", {
              method: "POST",
              headers: {
                Authorization: "Bearer " + env.RESEND_API_KEY,
                "Content-Type": "application/json"
              },
              body: JSON.stringify({
                from: "Keby <noreply@machbar24.com>",
                to: [order.email],
                subject: `Ihr Paket ist unterwegs 📦 — Bestellung ${order.ref || order.id}`,
                html: emailHtml
              })
            });
            const emailData = await emailRes.json();
            emailSent = !emailData.statusCode || emailData.statusCode < 400;
          } catch (e) {
            console.error("Tracking email:", e);
          }
        }

        return jsonResp({
          success: true,
          order_id: orderId,
          tracking_number: order.tracking_number,
          carrier: order.carrier,
          tracking_url: order.tracking_url,
          email_sent: emailSent
        });
      } catch (e) {
        return errResp("Tracking: " + e.message, 500);
      }
    }

    // GET /api/track/:trackingNumber — müşteri self-service tracking (public)
    if (request.method === "GET" && path.startsWith("/api/track/")) {
      try {
        const tn = decodeURIComponent(path.replace("/api/track/", "")).trim();
        if (!tn) return errResp("Tracking number gerekli", 400);

        const orders = await getOrders(env);
        const order = orders.find(o => o.tracking_number === tn);

        if (!order) return errResp("Tracking number bulunamadı", 404);

        // Müşteriye sadece gerekli bilgileri döndür (hassas veriler hariç)
        return jsonResp({
          success: true,
          order_ref: order.ref || order.id,
          status: order.status,
          carrier: order.carrier,
          tracking_number: order.tracking_number,
          tracking_url: order.tracking_url,
          tracking_added_at: order.tracking_added_at,
          items: (order.items || []).map(i => ({ name: i.name, qty: i.qty }))
        });
      } catch (e) {
        return errResp("Track: " + e.message, 500);
      }
    }


    if (request.method === "GET" && path === "/api/customers") {
      const customers = await getCustomers(env);
      return jsonResp({ success: true, customers });
    }
    if (request.method === "POST" && path === "/api/admin/customers" && isAdmin) {
      const { customers } = await request.json();
      await putCustomers(env, customers);
      return jsonResp({ success: true });
    }

    // Tek müşteri ekle
    if (request.method === "POST" && path === "/api/admin/customers/create" && isAdmin) {
      try {
        const body = await request.json();
        const customers = await getCustomers(env);

        const newCustomer = {
          id: body.id || generateCustomerId(),
          name: (body.name || "").trim(),
          email: (body.email || "").trim().toLowerCase(),
          phone: (body.phone || "").trim(),
          address: {
            line1: (body.address?.line1 || "").trim(),
            line2: (body.address?.line2 || "").trim(),
            postal_code: (body.address?.postal_code || "").trim(),
            city: (body.address?.city || "").trim(),
            country: body.address?.country || "DE"
          },
          ust_idnr: (body.ust_idnr || "").trim(),
          customer_type: body.customer_type || "b2c",  // "b2c" | "b2b"
          company_name: (body.company_name || "").trim(),
          payment_terms_days: parseInt(body.payment_terms_days) || 14,
          payments: [],
          notes: body.notes || "",
          source: body.source || "direct",
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        };

        if (!newCustomer.name) return errResp("Ad gerekli");

        // Aynı email varsa güncelle, yoksa ekle
        const idx = newCustomer.email
          ? customers.findIndex(c => (c.email || "").toLowerCase() === newCustomer.email)
          : -1;
        if (idx >= 0) {
          customers[idx] = { ...customers[idx], ...newCustomer, id: customers[idx].id };
          await putCustomers(env, customers);
          return jsonResp({ success: true, customer: customers[idx], updated: true });
        } else {
          customers.unshift(newCustomer);
          await putCustomers(env, customers);
          return jsonResp({ success: true, customer: newCustomer, created: true });
        }
      } catch (e) {
        return errResp("Müşteri oluşturulamadı: " + e.message);
      }
    }

    // Müşteri güncelle
    if (request.method === "PUT" && path.startsWith("/api/admin/customers/") && !path.endsWith("/timeline") && isAdmin) {
      try {
        const id = decodeURIComponent(path.replace("/api/admin/customers/", ""));
        const body = await request.json();
        const customers = await getCustomers(env);
        const idx = customers.findIndex(c => c.id === id);
        if (idx < 0) return errResp("Müşteri bulunamadı", 404);

        customers[idx] = {
          ...customers[idx],
          name: body.name !== undefined ? body.name : customers[idx].name,
          email: body.email !== undefined ? (body.email || "").toLowerCase() : customers[idx].email,
          phone: body.phone !== undefined ? body.phone : customers[idx].phone,
          address: body.address !== undefined ? body.address : customers[idx].address,
          ust_idnr: body.ust_idnr !== undefined ? body.ust_idnr : customers[idx].ust_idnr,
          customer_type: body.customer_type !== undefined ? body.customer_type : (customers[idx].customer_type || "b2c"),
          company_name: body.company_name !== undefined ? body.company_name : customers[idx].company_name,
          payment_terms_days: body.payment_terms_days !== undefined ? parseInt(body.payment_terms_days) || 14 : (customers[idx].payment_terms_days || 14),
          notes: body.notes !== undefined ? body.notes : customers[idx].notes,
          updated_at: new Date().toISOString()
        };
        await putCustomers(env, customers);
        return jsonResp({ success: true, customer: customers[idx] });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // Müşteri sil
    if (request.method === "DELETE" && path.startsWith("/api/admin/customers/") && isAdmin) {
      try {
        const id = decodeURIComponent(path.replace("/api/admin/customers/", ""));
        const customers = await getCustomers(env);
        const filtered = customers.filter(c => c.id !== id);
        if (filtered.length === customers.length) return errResp("Müşteri bulunamadı", 404);
        await putCustomers(env, filtered);
        return jsonResp({ success: true });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // ═══ MÜŞTERİ CARİ HESAP — Bakiye ═══
    if (request.method === "GET" && path.match(/^\/api\/admin\/customers\/[^/]+\/balance$/) && isAdmin) {
      try {
        const id = decodeURIComponent(path.split("/")[4]);
        const customers = await getCustomers(env);
        const customer = customers.find(c => c.id === id);
        if (!customer) return errResp("Müşteri bulunamadı", 404);
        const balance = await calculateCustomerBalance(env, id, customer);
        return jsonResp({ success: true, customer, balance });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // ═══ MÜŞTERİ CARİ HESAP — Ödeme kaydı ekle ═══
    if (request.method === "POST" && path.match(/^\/api\/admin\/customers\/[^/]+\/payments$/) && isAdmin) {
      try {
        const id = decodeURIComponent(path.split("/")[4]);
        const body = await request.json();
        if (!body.amount || isNaN(parseFloat(body.amount))) return errResp("Tutar geçerli değil");
        const customers = await getCustomers(env);
        const idx = customers.findIndex(c => c.id === id);
        if (idx < 0) return errResp("Müşteri bulunamadı", 404);
        const newPayment = addCustomerPayment(customers[idx], body);
        customers[idx].updated_at = new Date().toISOString();
        await putCustomers(env, customers);
        return jsonResp({ success: true, payment: newPayment, customer: customers[idx] });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // ═══ MÜŞTERİ CARİ HESAP — Ödeme iptal (storno) ═══
    if (request.method === "POST" && path.match(/^\/api\/admin\/customers\/[^/]+\/payments\/[^/]+\/void$/) && isAdmin) {
      try {
        const parts = path.split("/");
        const customerId = decodeURIComponent(parts[4]);
        const paymentId = decodeURIComponent(parts[6]);
        const customers = await getCustomers(env);
        const idx = customers.findIndex(c => c.id === customerId);
        if (idx < 0) return errResp("Müşteri bulunamadı", 404);
        const pay = (customers[idx].payments || []).find(p => p.id === paymentId);
        if (!pay) return errResp("Ödeme bulunamadı", 404);
        pay.voided = true;
        pay.voided_at = new Date().toISOString();
        customers[idx].updated_at = new Date().toISOString();
        await putCustomers(env, customers);
        return jsonResp({ success: true, payment: pay });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // Müşteri timeline (tüm fatura ve siparişleri)
    if (request.method === "GET" && path.startsWith("/api/admin/customers/") && path.endsWith("/timeline") && isAdmin) {
      try {
        const id = decodeURIComponent(path.replace("/api/admin/customers/", "").replace("/timeline", ""));
        const customers = await getCustomers(env);
        const customer = customers.find(c => c.id === id);
        if (!customer) return errResp("Müşteri bulunamadı", 404);

        const norm = (s) => (s||"").trim().toLowerCase();
        const custEmail = norm(customer.email);
        const custName = norm(customer.name);

        // İlgili faturaları bul
        const invoices = await listAllInvoices(env);
        const matchingInvoices = invoices.filter(inv => {
          if (!inv.customer) return false;
          if (custEmail && norm(inv.customer.email) === custEmail) return true;
          if (inv.customer_id && inv.customer_id === id) return true;
          // Fallback: isim eşleşmesi (online siparişlerde email yoksa)
          if (custName && norm(inv.customer.name) === custName) return true;
          return false;
        });

        // İlgili siparişleri bul (henüz fatura kesilmemiş)
        const orders = await getOrders(env);
        const matchingOrders = orders.filter(o => {
          if (custEmail && norm(o.email) === custEmail) return true;
          if (custName && norm(o.name) === custName) return true;
          return false;
        });

        // Timeline event'leri birleştir + sırala
        const events = [];
        for (const inv of matchingInvoices) {
          events.push({
            type: "invoice",
            date: inv.issued_at,
            number: inv.number,
            order_id: inv.order_id,
            total: inv.total,
            payment: inv.payment,
            items: inv.items,
            stornoed: inv.stornoed_at ? true : false,
            storno_for: inv.storno_for || null,
            source: inv.source || (inv.order_id?.startsWith("manual_") ? "manual" : "online")
          });
        }
        for (const ord of matchingOrders) {
          // Sadece fatura'sı yoksa eklemeye gerek (zaten invoice timeline'da var)
          const hasInvoice = matchingInvoices.find(inv => inv.order_id === ord.id);
          if (!hasInvoice) {
            events.push({
              type: "order",
              date: ord.date,
              order_id: ord.id,
              ref: ord.ref,
              total: ord.total,
              status: ord.status,
              payment: ord.payment,
              items: ord.items
            });
          }
        }
        events.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

        // İstatistikler
        const totalSpent = matchingInvoices
          .filter(i => !i.stornoed_at)
          .reduce((s, i) => s + (parseFloat(i.total) || 0), 0);

        return jsonResp({
          success: true,
          customer,
          events,
          stats: {
            invoice_count: matchingInvoices.filter(i => !i.stornoed_at).length,
            order_count: matchingOrders.length,
            total_spent: totalSpent,
            first_purchase: events.length ? events[events.length - 1].date : null,
            last_purchase: events.length ? events[0].date : null
          }
        });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // ============ ADMIN: BUCHHALTUNG / INVOICES ============

    // Tüm faturalar listesi (online + manuel karışık)
    // ═══════════════ GELDFLUSS-BUCH ═══════════════

    // GET /api/admin/geldfluss?from=2026-01-01&to=2026-12-31&type=einnahme&category=...
    if (request.method === "GET" && path === "/api/admin/geldfluss" && isAdmin) {
      try {
        const data = await getGeldfluss(env);
        const from = url.searchParams.get("from");
        const to = url.searchParams.get("to");
        const typeFilter = url.searchParams.get("type");
        const categoryFilter = url.searchParams.get("category");
        const paymentFilter = url.searchParams.get("payment");

        let entries = [...data.entries];
        if (from) entries = entries.filter(e => e.date >= from);
        if (to) entries = entries.filter(e => e.date <= to);
        if (typeFilter) entries = entries.filter(e => e.type === typeFilter);
        if (categoryFilter) entries = entries.filter(e => e.category === categoryFilter);
        if (paymentFilter) entries = entries.filter(e => e.payment_method === paymentFilter);

        // Date desc + id desc
        entries.sort((a, b) => {
          if (a.date !== b.date) return b.date.localeCompare(a.date);
          return b.id.localeCompare(a.id);
        });

        // Toplam hesapla
        const totals = {
          einnahmen: 0,
          ausgaben: 0,
          saldo: 0,
          count: entries.length
        };
        for (const e of entries) {
          if (e.voided_by) continue;
          if (e.type === "einnahme") totals.einnahmen += e.amount;
          else totals.ausgaben += e.amount;
        }
        totals.saldo = totals.einnahmen - totals.ausgaben;

        // Kategori breakdown (gider)
        const byCategory = {};
        for (const e of entries) {
          if (e.voided_by) continue;
          const key = e.type + ":" + e.category;
          if (!byCategory[key]) byCategory[key] = 0;
          byCategory[key] += e.amount;
        }

        return jsonResp({
          success: true,
          entries,
          totals,
          by_category: byCategory,
          categories: GELDFLUSS_CATEGORIES,
          category_labels: GELDFLUSS_CATEGORY_LABELS
        });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // POST /api/admin/geldfluss — manuel eintrag ekle (genelde Ausgabe)
    if (request.method === "POST" && path === "/api/admin/geldfluss" && isAdmin) {
      try {
        const body = await request.json();
        const type = body.type === "ausgabe" ? "ausgabe" : "einnahme";
        const date = (body.date || "").trim();
        const amount = Math.abs(parseFloat(body.amount) || 0);
        const description = (body.description || "").trim();
        const category = (body.category || "sonstiges").trim();
        const payment_method = (body.payment_method || "bar").trim();
        const notes = (body.notes || "").trim();
        const beleg_url = body.beleg_url || null;

        if (!date) return errResp("Datum erforderlich");
        if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return errResp("Datum muss YYYY-MM-DD sein");
        if (amount <= 0) return errResp("Betrag muss > 0 sein");
        if (!description) return errResp("Beschreibung erforderlich");
        if (!GELDFLUSS_CATEGORIES[type].includes(category)) {
          return errResp("Ungültige Kategorie für " + type);
        }

        const data = await getGeldfluss(env);
        const year = date.slice(0, 4);
        if (!data.counters[year]) data.counters[year] = { einnahme: 0, ausgabe: 0 };
        const id = generateGeldflussId(type, year, data.counters);
        data.counters[year][type]++;

        const entry = {
          id,
          type,
          date,
          amount,
          description,
          category,
          payment_method,
          ref_type: "manual",
          ref_id: null,
          ref_number: null,
          beleg_url,
          notes,
          created_at: new Date().toISOString(),
          created_by: "admin",
          immutable: true,
          voided_by: null
        };

        data.entries.push(entry);
        await saveGeldfluss(env, data);

        return jsonResp({ success: true, entry });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // POST /api/admin/geldfluss/:id/void — eintrag iptal (storno-buchung)
    // GoBD: orijinal silinmez, yeni karşıt eintrag eklenir
    if (request.method === "POST" && /^\/api\/admin\/geldfluss\/[^/]+\/void$/.test(path) && isAdmin) {
      try {
        const idToVoid = decodeURIComponent(path.split("/")[4]);
        const body = await request.json().catch(() => ({}));
        const reason = (body.reason || "").trim();
        if (!reason) return errResp("Begründung erforderlich (GoBD)");

        const data = await getGeldfluss(env);
        const orig = data.entries.find(e => e.id === idToVoid);
        if (!orig) return errResp("Eintrag nicht gefunden", 404);
        if (orig.voided_by) return errResp("Eintrag bereits storniert");
        if (orig.created_by === "auto" && orig.ref_type === "invoice") {
          return errResp("Auto-Buchungen aus Rechnungen können nicht direkt storniert werden — bitte die Rechnung selbst stornieren");
        }

        const year = new Date().toISOString().slice(0, 4);
        if (!data.counters[year]) data.counters[year] = { einnahme: 0, ausgabe: 0 };
        // Karşıt tip (einnahme → ausgabe, ausgabe → einnahme)
        const counterType = orig.type === "einnahme" ? "ausgabe" : "einnahme";
        const id = generateGeldflussId(counterType, year, data.counters);
        data.counters[year][counterType]++;

        const voidEntry = {
          id,
          type: counterType,
          date: new Date().toISOString().slice(0, 10),
          amount: orig.amount,
          description: `Storno-Buchung — ${orig.description}`,
          category: "storno",
          payment_method: orig.payment_method,
          ref_type: "void",
          ref_id: orig.id,
          ref_number: null,
          beleg_url: null,
          notes: reason,
          created_at: new Date().toISOString(),
          created_by: "admin",
          immutable: true,
          voided_by: null
        };

        orig.voided_by = id;
        data.entries.push(voidEntry);
        await saveGeldfluss(env, data);

        return jsonResp({ success: true, void_entry: voidEntry, original: orig });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // POST /api/admin/geldfluss/sync-invoices — mevcut tüm faturaları Einnahme olarak senkronize et
    // (one-time migration, idempotent — tekrar çağrılabilir)
    if (request.method === "POST" && path === "/api/admin/geldfluss/sync-invoices" && isAdmin) {
      try {
        const invoices = await listAllInvoices(env);
        let added = 0;
        let skipped = 0;
        for (const inv of invoices) {
          if (inv.stornoed_at) {
            skipped++;
            continue;
          }
          // Storno-Rechnung'ları einnahme yapma (negatif tutarlı, asıl Einnahme'nin storno'su)
          if (inv.storno_for) {
            skipped++;
            continue;
          }
          const before = (await getGeldfluss(env)).entries.length;
          await autoAddInvoiceEinnahme(env, inv);
          const after = (await getGeldfluss(env)).entries.length;
          if (after > before) added++; else skipped++;
        }
        return jsonResp({ success: true, added, skipped, total: invoices.length });
      } catch (e) {
        return errResp(e.message);
      }
    }

    if (request.method === "GET" && path === "/api/admin/invoices" && isAdmin) {
      try {
        const invoices = await listAllInvoices(env);
        // Compact list — full data değil, listeleme için
        const list = invoices.map(inv => ({
          number: inv.number,
          order_id: inv.order_id,
          order_ref: inv.order_ref,
          issued_at: inv.issued_at,
          customer_name: inv.customer?.name || "—",
          customer_email: inv.customer?.email || "",
          total: inv.total,
          payment: inv.payment,
          source: inv.source || (inv.order_id?.startsWith("manual_") ? "manual" : "online"),
          stornoed_at: inv.stornoed_at || null,
          storno_for: inv.storno_for || null
        }));
        return jsonResp({ success: true, invoices: list });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // ═══════════════ EÜR EXPORT (Einnahmen-Überschuss-Rechnung) ═══════════════
    // GET /api/admin/export/eur?year=2026 → JSON (frontend XLSX'e çevirir)
    // ═══════════════ BILANZ DASHBOARD ═══════════════
    // GET /api/admin/bilanz?year=2026 — KPI + grafik verileri tek seferde
    if (request.method === "GET" && path === "/api/admin/bilanz" && isAdmin) {
      try {
        const year = parseInt(url.searchParams.get("year")) || new Date().getFullYear();
        const b = await computeBilanz(env, year);
        // Top 10 müşteri ile sınırla (frontend dashboard için)
        return jsonResp({
          success: true,
          ...b,
          top_customers: b.top_customers.slice(0, 10),
          categories: {
            einnahmen: b.categories.einnahmen.slice(0, 8),
            ausgaben: b.categories.ausgaben.slice(0, 8)
          }
        });
      } catch (e) {
        return errResp("Bilanz: " + e.message, 500);
      }
    }

    // ═══════════════ BILANZ PDF ═══════════════
    // GET /api/admin/bilanz/pdf?year=2026 — PDF rapor (Steuerberater için)
    if (request.method === "GET" && path === "/api/admin/bilanz/pdf" && isAdmin) {
      try {
        if (!env.BROWSER_RENDERING_TOKEN || !env.CF_ACCOUNT_ID) {
          return errResp("Browser Rendering yapılandırılmamış", 500);
        }
        const year = parseInt(url.searchParams.get("year")) || new Date().getFullYear();
        const b = await computeBilanz(env, year);

        // Firma bilgisi (memory'den, settings R2 key)
        let companyInfo = { name: 'Keby Shop', address: '', tax_id: '' };
        try {
          const settings = await env.R2.get('keby/data/settings.json');
          if (settings) {
            const s = JSON.parse(await settings.text());
            companyInfo = {
              name: s.company_name || s.shop_name || 'Keby Shop',
              address: s.company_address || s.address || '',
              tax_id: s.tax_id || s.ust_idnr || ''
            };
          }
        } catch (e) { /* settings yoksa default */ }

        const html = renderBilanzPDFHTML(b, companyInfo);

        const res = await fetch(
          "https://api.cloudflare.com/client/v4/accounts/" + env.CF_ACCOUNT_ID + "/browser-rendering/pdf",
          {
            method: "POST",
            headers: {
              Authorization: "Bearer " + env.BROWSER_RENDERING_TOKEN,
              "Content-Type": "application/json"
            },
            body: JSON.stringify({
              html,
              pdfOptions: {
                format: "a4",
                printBackground: true,
                margin: { top: "12mm", bottom: "12mm", left: "12mm", right: "12mm" }
              }
            })
          }
        );

        if (!res.ok) {
          const errText = await res.text();
          return errResp("PDF API HTTP " + res.status + ": " + errText.slice(0, 300), 500);
        }
        const ct = res.headers.get("content-type") || "";
        if (ct.includes("application/json")) {
          const errText = await res.text();
          return errResp("PDF API JSON döndü: " + errText.slice(0, 300), 500);
        }

        const pdf = await res.arrayBuffer();
        const filename = `Bilanz_${year}_${(companyInfo.name || 'Keby').replace(/[^a-z0-9]/gi, '_')}.pdf`;

        return new Response(pdf, {
          status: 200,
          headers: {
            "Content-Type": "application/pdf",
            "Content-Disposition": `attachment; filename="${filename}"`,
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*"
          }
        });
      } catch (e) {
        return errResp("Bilanz PDF: " + e.message, 500);
      }
    }

    // ═══════════════ EÜR EXPORT ═══════════════
    if (request.method === "GET" && path === "/api/admin/export/eur" && isAdmin) {
      try {
        const year = parseInt(url.searchParams.get("year")) || new Date().getFullYear();
        const data = await getGeldfluss(env);

        // Yıl filtresi
        const yearStart = `${year}-01-01`;
        const yearEnd = `${year}-12-31`;
        const entries = data.entries.filter(e => {
          if (e.voided_by) return false;  // Voided'ları sayma
          if (!e.date) return false;
          return e.date >= yearStart && e.date <= yearEnd;
        });

        // Kategori bazlı toplama
        const labels = data.category_labels || {};
        const einnahmenByCat = {};
        const ausgabenByCat = {};
        let totalEinnahmen = 0;
        let totalAusgaben = 0;

        for (const e of entries) {
          const amount = parseFloat(e.amount) || 0;
          const cat = e.category || "sonstige";
          const label = labels[cat] || cat;
          if (e.type === "einnahme") {
            einnahmenByCat[label] = (einnahmenByCat[label] || 0) + amount;
            totalEinnahmen += amount;
          } else if (e.type === "ausgabe") {
            ausgabenByCat[label] = (ausgabenByCat[label] || 0) + amount;
            totalAusgaben += amount;
          }
        }

        // Aylık dağılım
        const monthlyData = {};
        for (let m = 1; m <= 12; m++) {
          const key = String(m).padStart(2, '0');
          monthlyData[key] = { einnahmen: 0, ausgaben: 0 };
        }
        for (const e of entries) {
          const month = e.date.slice(5, 7);
          if (monthlyData[month]) {
            const a = parseFloat(e.amount) || 0;
            if (e.type === "einnahme") monthlyData[month].einnahmen += a;
            else if (e.type === "ausgabe") monthlyData[month].ausgaben += a;
          }
        }

        // Detaylı liste (her hareket için satır)
        const transactions = entries.map(e => ({
          datum: e.date,
          beleg_nr: e.ref_number || e.id,
          beschreibung: e.description,
          kategorie: labels[e.category] || e.category,
          einnahme: e.type === "einnahme" ? parseFloat(e.amount) || 0 : 0,
          ausgabe: e.type === "ausgabe" ? parseFloat(e.amount) || 0 : 0,
          zahlungsart: e.payment_method || "",
          notizen: e.notes || "",
          beleg_vorhanden: e.beleg ? "Ja" : "Nein"
        })).sort((a, b) => a.datum.localeCompare(b.datum));

        return jsonResp({
          success: true,
          year,
          summary: {
            total_einnahmen: Math.round(totalEinnahmen * 100) / 100,
            total_ausgaben: Math.round(totalAusgaben * 100) / 100,
            saldo: Math.round((totalEinnahmen - totalAusgaben) * 100) / 100,
            entry_count: entries.length,
            kleinunternehmer_grenze: 22000,  // 2024+ § 19 UStG
            ueber_grenze: totalEinnahmen > 22000
          },
          einnahmen_by_category: einnahmenByCat,
          ausgaben_by_category: ausgabenByCat,
          monthly: monthlyData,
          transactions
        });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // ═══════════════ DATEV EXPORT (Buchungsstapel CSV) ═══════════════
    // GET /api/admin/export/datev?from=2026-01-01&to=2026-12-31 → CSV download
    // Format: DATEV Format I (Buchungsstapel) — Steuerberater'ın yazılımına import için
    if (request.method === "GET" && path === "/api/admin/export/datev" && isAdmin) {
      try {
        const fromDate = url.searchParams.get("from") || `${new Date().getFullYear()}-01-01`;
        const toDate = url.searchParams.get("to") || `${new Date().getFullYear()}-12-31`;
        const data = await getGeldfluss(env);
        const labels = data.category_labels || {};

        const entries = data.entries.filter(e => {
          if (e.voided_by) return false;
          if (!e.date) return false;
          return e.date >= fromDate && e.date <= toDate;
        }).sort((a, b) => a.date.localeCompare(b.date));

        // DATEV Buchungsstapel Format (basitleştirilmiş)
        // Standart sütunlar:
        // Umsatz | Soll/Haben-Kennzeichen | WKZ Umsatz | Kurs | Basis-Umsatz | WKZ Basis-Umsatz |
        // Konto | Gegenkonto | BU-Schlüssel | Belegdatum | Belegfeld 1 | Belegfeld 2 | Skonto |
        // Buchungstext | Postensperre | Diverse Adressnummer | Geschäftspartnerbank |
        // Sachverhalt | Zinssperre | Beleglink | ...

        // Basit DATEV mapping:
        // - Einnahme: Soll = Bank/Kasse, Haben = Erlöse (8400)
        // - Ausgabe: Soll = Aufwand kategori, Haben = Bank/Kasse (1200)
        // SKR03 hesap planı (Kleinunternehmer için yaygın)

        const accountMap = {
          // Einnahmen
          "verkauf": "8400",        // Erlöse
          "online_verkauf": "8400",
          "ladenverkauf": "8400",
          "grosshandel": "8400",
          "sonstige_einnahmen": "8410",
          "storno": "8400",
          // Ausgaben
          "wareneingang": "3200",   // Wareneingang
          "verpackung": "4910",     // Büromaterial / Verpackung
          "versand": "4910",
          "buero": "4980",          // Bürobedarf
          "marketing": "4600",      // Werbekosten
          "reise": "4670",          // Reisekosten
          "miete": "4210",          // Miete
          "telefon_internet": "4920",
          "software": "4920",       // EDV-Wartung
          "fahrzeug": "4530",       // Kfz-Kosten
          "fortbildung": "4945",
          "berater": "4955",        // Rechts-/Beratungskosten
          "bank_gebuehren": "4970",
          "stripe_gebuehr": "4970",
          "paypal_gebuehr": "4970",
          "sonstige_ausgaben": "4990"
        };

        const paymentAccountMap = {
          "bar": "1000",            // Kasse
          "ec_karte": "1361",       // Geldtransit / Bank
          "ueberweisung": "1200",   // Bank
          "stripe": "1361",         // Bank/PayPal Konto
          "paypal": "1361",
          "online": "1361",
          "manual": "1200"
        };

        // CSV header (Windows-1252 / DATEV-uyumlu sütun başlıkları)
        const headers = [
          "Umsatz", "Soll/Haben", "Konto", "Gegenkonto", "Belegdatum",
          "Belegfeld 1", "Buchungstext", "Steuersatz"
        ];

        const rows = [headers];

        for (const e of entries) {
          const amount = (parseFloat(e.amount) || 0).toFixed(2).replace('.', ',');
          const dateDE = e.date.slice(8, 10) + e.date.slice(5, 7); // DDMM
          const fullDate = e.date.slice(8, 10) + "." + e.date.slice(5, 7) + "." + e.date.slice(0, 4);
          const beleg = e.ref_number || e.id;
          const text = (e.description || "").replace(/[;"\r\n]/g, ' ').slice(0, 60);

          let konto, gegenkonto, soll_haben;
          if (e.type === "einnahme") {
            // Einnahme: Bank Soll, Erlöse Haben (S = Soll perspektifinden)
            konto = paymentAccountMap[e.payment_method] || "1200";
            gegenkonto = accountMap[e.category] || "8400";
            soll_haben = "S";  // Bank borçlandı
          } else {
            // Ausgabe: Aufwand Soll, Bank Haben
            konto = accountMap[e.category] || "4990";
            gegenkonto = paymentAccountMap[e.payment_method] || "1200";
            soll_haben = "S";  // Aufwand borçlandı
          }

          rows.push([
            amount,
            soll_haben,
            konto,
            gegenkonto,
            fullDate,
            beleg,
            text,
            "0"  // Kleinunternehmer = no Umsatzsteuer
          ]);
        }

        // CSV string (Excel/DATEV Almanya: ; ayraç, virgül decimal, Windows-1252)
        const csv = rows.map(r =>
          r.map(c => {
            const s = String(c || "");
            if (s.includes(";") || s.includes('"') || s.includes("\n")) {
              return '"' + s.replace(/"/g, '""') + '"';
            }
            return s;
          }).join(";")
        ).join("\r\n");

        // BOM + Windows-1252 yerine UTF-8-BOM (Excel açar)
        const bom = "\uFEFF";

        return new Response(bom + csv, {
          headers: {
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": `attachment; filename="DATEV_${fromDate}_${toDate}.csv"`,
            "Access-Control-Allow-Origin": "*"
          }
        });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // ═══════════════ AI VISION — Belge köşe tespiti ═══════════════
    // POST /api/admin/ocr/corners — image → 4 köşe (normalize 0-1)
    if (request.method === "POST" && path === "/api/admin/ocr/corners" && isAdmin) {
      try {
        if (!env.AI) return errResp("AI binding yok", 500);

        const formData = await request.formData();
        const file = formData.get("file");
        if (!file || !file.size) return errResp("Dosya yok");
        const ct = file.type || "";
        if (!ct.startsWith("image/")) return errResp("Sadece resim");

        // Image dimensions için ek bilgi (frontend gönderir)
        const imgW = parseInt(formData.get("width")) || 0;
        const imgH = parseInt(formData.get("height")) || 0;

        // Base64
        const buffer = await file.arrayBuffer();
        const bytes = new Uint8Array(buffer);
        let binary = "";
        const chunkSize = 8192;
        for (let i = 0; i < bytes.length; i += chunkSize) {
          binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize));
        }
        const b64 = btoa(binary);
        const dataUrl = `data:${ct};base64,${b64}`;

        const messages = [
          {
            role: "system",
            content: "Du bist ein Computer-Vision-Experte. Du analysierst Fotos von Dokumenten/Belegen und identifizierst die 4 Eckpunkte des Papiers. Antworte AUSSCHLIESSLICH mit gültigem JSON, ohne Markdown, ohne Erklärungen."
          },
          {
            role: "user",
            content: `Analysiere dieses Foto. Identifiziere die VIER ECKPUNKTE des sichtbaren Dokuments/Belegs/Papiers (das Hauptobjekt im Bild). Gib die Koordinaten als NORMALISIERTE Werte zwischen 0.0 und 1.0 zurück (0,0 = oben links, 1,1 = unten rechts).

Antworte mit diesem JSON (keine Backticks, kein Markdown):

{
  "found": true|false,
  "confidence": 0.0-1.0,
  "corners": {
    "top_left":     {"x": 0.0-1.0, "y": 0.0-1.0},
    "top_right":    {"x": 0.0-1.0, "y": 0.0-1.0},
    "bottom_right": {"x": 0.0-1.0, "y": 0.0-1.0},
    "bottom_left":  {"x": 0.0-1.0, "y": 0.0-1.0}
  }
}

Wichtig:
- Identifiziere die ECHTEN Ecken des Dokuments, nicht den Bildrand
- Wenn das Dokument abgeschnitten ist (Ecken außerhalb des Bildes), nutze die Bildränder als Approximation
- Bei mehreren Dokumenten: nimm das größte/zentralste
- Wenn KEIN Dokument sichtbar: found=false, alle Werte 0
- Antworte NUR JSON, nichts anderes`
          }
        ];

        let aiResponse;
        try {
          aiResponse = await env.AI.run("@cf/meta/llama-3.2-11b-vision-instruct", {
            messages,
            image: dataUrl,
            max_tokens: 256
          });
        } catch (e) {
          return errResp("AI: " + e.message, 500);
        }

        let text = aiResponse.response || aiResponse.result?.response || "";
        if (typeof text !== "string") text = JSON.stringify(text);
        text = text.trim();
        if (text.startsWith("```")) {
          text = text.replace(/^```(?:json)?\s*/i, "").replace(/```\s*$/i, "").trim();
        }
        const jsonStart = text.indexOf("{");
        const jsonEnd = text.lastIndexOf("}");
        if (jsonStart < 0 || jsonEnd < 0) {
          return jsonResp({ success: false, error: "Geçersiz AI yanıtı", raw: text.slice(0, 200) });
        }
        let parsed;
        try {
          parsed = JSON.parse(text.slice(jsonStart, jsonEnd + 1));
        } catch (e) {
          return jsonResp({ success: false, error: "JSON hatası", raw: text.slice(0, 200) });
        }

        if (!parsed.found || !parsed.corners) {
          return jsonResp({ success: true, found: false, confidence: parsed.confidence || 0 });
        }

        const c = parsed.corners;
        // Normalize değerleri image piksel'e çevir (frontend için)
        const result = {
          found: true,
          confidence: parsed.confidence || 0.5,
          corners_normalized: {
            top_left:     c.top_left,
            top_right:    c.top_right,
            bottom_right: c.bottom_right,
            bottom_left:  c.bottom_left
          }
        };

        if (imgW > 0 && imgH > 0) {
          result.corners_pixel = {
            top_left:     { x: Math.round(c.top_left.x * imgW),     y: Math.round(c.top_left.y * imgH) },
            top_right:    { x: Math.round(c.top_right.x * imgW),    y: Math.round(c.top_right.y * imgH) },
            bottom_right: { x: Math.round(c.bottom_right.x * imgW), y: Math.round(c.bottom_right.y * imgH) },
            bottom_left:  { x: Math.round(c.bottom_left.x * imgW),  y: Math.round(c.bottom_left.y * imgH) }
          };
        }

        return jsonResp({ success: true, ...result });
      } catch (e) {
        return errResp("Corner detect: " + e.message, 500);
      }
    }

    // ═══════════════ OCR — Beleg verisini llama vision ile çıkar ═══════════════
    // POST /api/admin/ocr/beleg — multipart image → AI parse → JSON {amount, date, vendor, ...}
    if (request.method === "POST" && path === "/api/admin/ocr/beleg" && isAdmin) {
      try {
        if (!env.AI) {
          return errResp("AI binding mevcut değil. Worker'a [ai] binding ekleyin.", 500);
        }

        const formData = await request.formData();
        const file = formData.get("file");
        if (!file || !file.size) return errResp("Dosya yok");
        if (file.size > 10 * 1024 * 1024) return errResp("Dosya çok büyük");

        // PDF değilse - llama vision sadece resim alır
        const ct = file.type || "";
        if (!ct.startsWith("image/")) {
          return errResp("OCR için resim gerekli (JPG/PNG/WEBP). PDF için manuel giriş yapın.");
        }

        // Image'ı base64'e çevir
        const buffer = await file.arrayBuffer();
        const bytes = new Uint8Array(buffer);
        let binary = "";
        const chunkSize = 8192;  // büyük dosyalarda spread overflow
        for (let i = 0; i < bytes.length; i += chunkSize) {
          binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize));
        }
        const b64 = btoa(binary);
        const dataUrl = `data:${ct};base64,${b64}`;

        // Llama Vision'a gönder — yapılandırılmış JSON çıktısı iste
        const messages = [
          {
            role: "system",
            content: `Du bist ein Experte für die Erkennung von deutschen Geschäftsbelegen, Rechnungen und Quittungen. Antworte AUSSCHLIESSLICH mit gültigem JSON, ohne Markdown-Backticks, ohne Erklärungen, ohne Vortext. Wenn ein Wert nicht erkennbar ist, verwende null.`
          },
          {
            role: "user",
            content: `Analysiere diesen Beleg/Rechnung und gib EIN JSON-Objekt zurück mit folgenden Feldern:

{
  "amount": <Gesamtbetrag (Brutto) als Zahl in Euro, z.B. 45.99>,
  "date": "<Belegdatum im Format YYYY-MM-DD>",
  "vendor": "<Name des Verkäufers / Firma, z.B. 'DHL', 'Amazon EU', 'REWE'>",
  "invoice_number": "<Beleg-/Rechnungsnummer, falls sichtbar, sonst null>",
  "category_hint": "<eine dieser Kategorien: 'wareneingang', 'verpackung', 'versand', 'buero', 'marketing', 'reise', 'miete', 'telefon_internet', 'software', 'fahrzeug', 'fortbildung', 'berater', 'bank_gebuehren', 'sonstige_ausgaben'>",
  "currency": "<EUR, USD, etc.>",
  "language": "<de, en, tr>",
  "confidence": <0.0 bis 1.0, wie sicher die Erkennung ist>
}

Wichtig:
- Bei mehreren Beträgen: nimm den Gesamtbetrag (Brutto, mit MwSt)
- Datum im ISO-Format YYYY-MM-DD
- Vendor ist der Aussteller des Belegs (oben auf der Rechnung)
- Kategorie wähle anhand der Art der Ausgabe (Wareneinkauf, Büromaterial, Software-Abo, etc.)
- Antworte NUR mit dem JSON-Objekt, nichts anderes`
          }
        ];

        let aiResponse;
        try {
          aiResponse = await env.AI.run("@cf/meta/llama-3.2-11b-vision-instruct", {
            messages,
            image: dataUrl,
            max_tokens: 512
          });
        } catch (e) {
          return errResp("AI hatası: " + e.message, 500);
        }

        // AI'dan gelen text'i parse et
        let text = aiResponse.response || aiResponse.result?.response || "";
        if (typeof text !== "string") text = JSON.stringify(text);

        // JSON'u çıkar (bazen markdown wrap olabilir)
        text = text.trim();
        if (text.startsWith("```")) {
          text = text.replace(/^```(?:json)?\s*/i, "").replace(/```\s*$/i, "").trim();
        }
        // İlk { ile başla, son } ile bitir
        const jsonStart = text.indexOf("{");
        const jsonEnd = text.lastIndexOf("}");
        if (jsonStart < 0 || jsonEnd < 0) {
          return jsonResp({
            success: false,
            error: "AI yanıtı JSON formatında değil",
            raw_response: text.slice(0, 500)
          });
        }
        const jsonText = text.slice(jsonStart, jsonEnd + 1);

        let parsed;
        try {
          parsed = JSON.parse(jsonText);
        } catch (e) {
          return jsonResp({
            success: false,
            error: "JSON parse hatası: " + e.message,
            raw_response: jsonText.slice(0, 500)
          });
        }

        // Validate + normalize
        const result = {
          amount: typeof parsed.amount === "number" ? parsed.amount
                : (parsed.amount ? parseFloat(String(parsed.amount).replace(",", ".")) || null : null),
          date: parsed.date && /^\d{4}-\d{2}-\d{2}$/.test(parsed.date) ? parsed.date : null,
          vendor: parsed.vendor || null,
          invoice_number: parsed.invoice_number || null,
          category_hint: parsed.category_hint || "sonstige_ausgaben",
          currency: parsed.currency || "EUR",
          language: parsed.language || "de",
          confidence: typeof parsed.confidence === "number" ? parsed.confidence : 0.5
        };

        return jsonResp({
          success: true,
          ocr: result,
          raw: parsed  // debug
        });
      } catch (e) {
        return errResp("OCR hatası: " + e.message, 500);
      }
    }

    // ═══════════════ BELEGERFASSUNG (Beleg PDF/Foto — Einnahme + Ausgabe) ═══════════════
    // POST /api/admin/geldfluss/:id/belege — Beleg ekle (multipart, çoklu yükleme destekli)
    if (request.method === "POST" && path.match(/^\/api\/admin\/geldfluss\/[^/]+\/belege$/) && isAdmin) {
      try {
        const id = decodeURIComponent(path.split("/")[4]);
        const data = await getGeldfluss(env);
        const idx = data.entries.findIndex(e => e.id === id);
        if (idx < 0) return errResp("Eintrag bulunamadı", 404);

        const formData = await request.formData();
        const files = formData.getAll("file");  // Çoklu dosya destekli (input multiple)
        if (!files || files.length === 0) return errResp("Dosya yok");

        const allowed = ["application/pdf", "image/jpeg", "image/png", "image/webp", "image/heic", "image/heif"];
        const newBelege = [];

        for (const file of files) {
          if (!file || !file.size) continue;
          if (file.size > 10 * 1024 * 1024) {
            return errResp(`"${file.name}" çok büyük (max 10MB)`);
          }
          const ct = file.type || "application/octet-stream";
          if (!allowed.includes(ct)) {
            return errResp(`Format desteklenmiyor: ${ct}`);
          }

          const ext = ct === "application/pdf" ? "pdf"
                    : ct === "image/jpeg" ? "jpg"
                    : ct === "image/png" ? "png"
                    : ct === "image/webp" ? "webp"
                    : ct === "image/heic" ? "heic"
                    : ct === "image/heif" ? "heif" : "bin";

          // Unique beleg ID — çoklu beleg için
          const belegId = "beleg_" + Date.now().toString(36) + Math.random().toString(36).substring(2, 8);
          const r2Key = `keby/belege/${id}/${belegId}.${ext}`;
          const buffer = await file.arrayBuffer();
          await env.KEBY_R2.put(r2Key, buffer, {
            httpMetadata: { contentType: ct }
          });

          newBelege.push({
            id: belegId,
            r2_key: r2Key,
            content_type: ct,
            size: file.size,
            filename: file.name || `beleg-${belegId}.${ext}`,
            uploaded_at: new Date().toISOString()
          });
        }

        // Mevcut belege array'ine ekle (eski tek `beleg` formatından migrate et)
        if (!data.entries[idx].belege) {
          data.entries[idx].belege = [];
          // Eski tek `beleg` field'ı varsa array'e taşı
          if (data.entries[idx].beleg) {
            const old = data.entries[idx].beleg;
            data.entries[idx].belege.push({
              id: "beleg_legacy_" + idx,
              r2_key: old.r2_key,
              content_type: old.content_type,
              size: old.size,
              filename: old.filename,
              uploaded_at: old.uploaded_at
            });
            delete data.entries[idx].beleg;
          }
        }
        data.entries[idx].belege.push(...newBelege);
        await saveGeldfluss(env, data);

        return jsonResp({ success: true, entry: data.entries[idx], added: newBelege.length });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // GET /api/admin/geldfluss/:id/belege/:belegId — Tek beleg göster
    if (request.method === "GET" && path.match(/^\/api\/admin\/geldfluss\/[^/]+\/belege\/[^/]+$/) && isAdmin) {
      try {
        const parts = path.split("/");
        const id = decodeURIComponent(parts[4]);
        const belegId = decodeURIComponent(parts[6]);
        const data = await getGeldfluss(env);
        const entry = data.entries.find(e => e.id === id);
        if (!entry) return errResp("Eintrag bulunamadı", 404);

        let beleg;
        if (entry.belege) beleg = entry.belege.find(b => b.id === belegId);
        // Eski uyum: belegId === "legacy" ve eski single beleg varsa
        if (!beleg && entry.beleg && (belegId === "legacy" || belegId === entry.beleg.r2_key)) {
          beleg = entry.beleg;
        }
        if (!beleg) return errResp("Beleg bulunamadı", 404);

        const obj = await env.KEBY_R2.get(beleg.r2_key);
        if (!obj) return errResp("Dosya R2'de yok", 404);

        return new Response(obj.body, {
          headers: {
            "Content-Type": beleg.content_type,
            "Content-Disposition": `inline; filename="${beleg.filename}"`,
            "Cache-Control": "private, max-age=3600",
            "Access-Control-Allow-Origin": "*"
          }
        });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // DELETE /api/admin/geldfluss/:id/belege/:belegId — Tek beleg sil
    if (request.method === "DELETE" && path.match(/^\/api\/admin\/geldfluss\/[^/]+\/belege\/[^/]+$/) && isAdmin) {
      try {
        const parts = path.split("/");
        const id = decodeURIComponent(parts[4]);
        const belegId = decodeURIComponent(parts[6]);
        const data = await getGeldfluss(env);
        const idx = data.entries.findIndex(e => e.id === id);
        if (idx < 0) return errResp("Eintrag bulunamadı", 404);

        const entry = data.entries[idx];
        let r2Key = null;
        if (entry.belege) {
          const bIdx = entry.belege.findIndex(b => b.id === belegId);
          if (bIdx >= 0) {
            r2Key = entry.belege[bIdx].r2_key;
            entry.belege.splice(bIdx, 1);
          }
        }
        // Eski single beleg
        if (!r2Key && entry.beleg && (belegId === "legacy" || belegId === entry.beleg.r2_key)) {
          r2Key = entry.beleg.r2_key;
          delete entry.beleg;
        }
        if (!r2Key) return errResp("Beleg bulunamadı", 404);

        try { await env.KEBY_R2.delete(r2Key); } catch(e) {}
        await saveGeldfluss(env, data);
        return jsonResp({ success: true });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // ═══ GERIYE UYUM: Eski tek beleg endpoint'leri (deprecated ama çalışır) ═══
    // POST /api/admin/geldfluss/:id/beleg — Eski tek beleg (yeni endpoint'e proxy)
    if (request.method === "POST" && path.match(/^\/api\/admin\/geldfluss\/[^/]+\/beleg$/) && isAdmin) {
      try {
        const id = decodeURIComponent(path.split("/")[4]);
        const data = await getGeldfluss(env);
        const idx = data.entries.findIndex(e => e.id === id);
        if (idx < 0) return errResp("Eintrag bulunamadı", 404);

        const formData = await request.formData();
        const file = formData.get("file");
        if (!file || !file.size) return errResp("Dosya yok");
        if (file.size > 10 * 1024 * 1024) return errResp("Dosya çok büyük (max 10MB)");

        const allowed = ["application/pdf", "image/jpeg", "image/png", "image/webp", "image/heic", "image/heif"];
        const ct = file.type || "application/octet-stream";
        if (!allowed.includes(ct)) return errResp("Format desteklenmiyor");

        const ext = ct === "application/pdf" ? "pdf" : ct === "image/jpeg" ? "jpg" : ct === "image/png" ? "png" : ct === "image/webp" ? "webp" : "bin";
        const belegId = "beleg_" + Date.now().toString(36) + Math.random().toString(36).substring(2, 8);
        const r2Key = `keby/belege/${id}/${belegId}.${ext}`;
        const buffer = await file.arrayBuffer();
        await env.KEBY_R2.put(r2Key, buffer, { httpMetadata: { contentType: ct } });

        if (!data.entries[idx].belege) {
          data.entries[idx].belege = [];
          if (data.entries[idx].beleg) {
            const old = data.entries[idx].beleg;
            data.entries[idx].belege.push({
              id: "beleg_legacy_" + idx, r2_key: old.r2_key, content_type: old.content_type,
              size: old.size, filename: old.filename, uploaded_at: old.uploaded_at
            });
            delete data.entries[idx].beleg;
          }
        }
        data.entries[idx].belege.push({
          id: belegId, r2_key: r2Key, content_type: ct, size: file.size,
          filename: file.name || `beleg-${belegId}.${ext}`,
          uploaded_at: new Date().toISOString()
        });
        await saveGeldfluss(env, data);
        return jsonResp({ success: true, entry: data.entries[idx] });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // GET /api/admin/geldfluss/:id/beleg — Eski tek beleg (ilkini döndürür)
    if (request.method === "GET" && path.match(/^\/api\/admin\/geldfluss\/[^/]+\/beleg$/) && isAdmin) {
      try {
        const id = decodeURIComponent(path.split("/")[4]);
        const data = await getGeldfluss(env);
        const entry = data.entries.find(e => e.id === id);
        if (!entry) return errResp("Eintrag bulunamadı", 404);

        // Önce yeni belege[0], yoksa eski single beleg
        let beleg = (entry.belege && entry.belege.length > 0) ? entry.belege[0] : entry.beleg;
        if (!beleg) return errResp("Beleg bulunamadı", 404);

        const obj = await env.KEBY_R2.get(beleg.r2_key);
        if (!obj) return errResp("Dosya R2'de yok", 404);
        return new Response(obj.body, {
          headers: {
            "Content-Type": beleg.content_type,
            "Content-Disposition": `inline; filename="${beleg.filename}"`,
            "Cache-Control": "private, max-age=3600",
            "Access-Control-Allow-Origin": "*"
          }
        });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // DELETE eski tek beleg
    if (request.method === "DELETE" && path.match(/^\/api\/admin\/geldfluss\/[^/]+\/beleg$/) && isAdmin) {
      try {
        const id = decodeURIComponent(path.split("/")[4]);
        const data = await getGeldfluss(env);
        const idx = data.entries.findIndex(e => e.id === id);
        if (idx < 0) return errResp("Eintrag bulunamadı", 404);
        const entry = data.entries[idx];
        // Tüm belegleri temizle
        const allBelege = [...(entry.belege || [])];
        if (entry.beleg) allBelege.push(entry.beleg);
        for (const b of allBelege) {
          try { await env.KEBY_R2.delete(b.r2_key); } catch(e) {}
        }
        delete entry.beleg;
        delete entry.belege;
        await saveGeldfluss(env, data);
        return jsonResp({ success: true });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // Manuel fatura oluştur (mağaza/telefon satışı için)
    // VORSCHAU: Manuel fatura önizleme (numara verilmez, kaydedilmez, sadece PDF)
    if (request.method === "POST" && path === "/api/admin/invoices/preview" && isAdmin) {
      try {
        const body = await request.json();

        // Müşteri verisini oluştur (DB'ye yazmadan)
        let customer;
        if (body.customer_id) {
          const customers = await getCustomers(env);
          customer = customers.find(c => c.id === body.customer_id);
          if (!customer) return errResp("Müşteri bulunamadı", 404);
        } else if (body.customer) {
          customer = {
            id: "preview",
            name: (body.customer.name || "").trim(),
            email: (body.customer.email || "").trim(),
            phone: (body.customer.phone || "").trim(),
            address: body.customer.address || {},
            ust_idnr: (body.customer.ust_idnr || "").trim()
          };
          if (!customer.name) return errResp("Müşteri adı gerekli");
        } else {
          return errResp("customer_id veya customer gerekli");
        }

        // Items normalize
        const items = (body.items || []).map(it => ({
          artikel_nr: (it.artikel_nr || "").trim(),
          product_id: it.product_id || "",
          name: (it.name || it.description || "—").trim(),
          qty: parseFloat(it.qty || it.quantity || 1),
          price: parseFloat(it.price || 0)
        })).filter(it => it.name && it.name !== "—");
        if (items.length === 0) return errResp("En az 1 pozisyon gerekli");

        const subtotal = items.reduce((s, it) => s + (it.price * it.qty), 0);
        const total = parseFloat(body.total !== undefined ? body.total : subtotal);
        const now = new Date();
        const orderDate = body.order_date ? new Date(body.order_date) : now;

        // Geçici fatura objesi — PDF render için
        const invoice = {
          number: "VORSCHAU",
          order_id: "preview",
          order_ref: "VORSCHAU",
          issued_at: now.toISOString(),
          order_date: orderDate.toISOString(),
          payment: body.payment || "bar",
          payment_id: "",
          customer: {
            id: customer.id,
            name: customer.name,
            email: customer.email || "",
            phone: customer.phone || "",
            address: customer.address || {},
            address_text: [
              customer.address?.line1,
              customer.address?.line2,
              [customer.address?.postal_code, customer.address?.city].filter(Boolean).join(' '),
              customer.address?.country
            ].filter(Boolean).join(', ')
          },
          items,
          subtotal,
          shipping_fee: 0,
          total,
          currency: "EUR",
          tax_mode: "kleinunternehmer",
          tax_note: "Gemäß § 19 UStG wird keine Umsatzsteuer ausgewiesen.",
          source: "preview",
          notes: body.notes || ""
        };

        // PDF üret ve döndür
        const pdfBuffer = await generateInvoicePDF(env, invoice);
        return new Response(pdfBuffer, {
          headers: {
            ...CORS,
            "Content-Type": "application/pdf",
            "Content-Disposition": 'inline; filename="Rechnung_Vorschau.pdf"',
            "Cache-Control": "no-store"
          }
        });
      } catch (e) {
        return errResp("Vorschau hatası: " + e.message);
      }
    }

    // ═══════════════ LIEFERSCHEIN (İRSALİYE) ═══════════════
    // GET /api/admin/lieferscheine — Tüm lieferscheinleri listele
    if (request.method === "GET" && path === "/api/admin/lieferscheine" && isAdmin) {
      try {
        const items = await listAllLieferscheine(env);
        return jsonResp({
          success: true,
          lieferscheine: items.map(l => ({
            id: l.id,
            number: l.number,
            customer_name: l.customer?.company_name || l.customer?.name || "—",
            delivery_date: l.delivery_date,
            created_at: l.created_at,
            invoice_ref: l.invoice_ref || null,
            order_ref: l.order_ref || null,
            item_count: (l.items || []).length,
            total_qty: (l.items || []).reduce((s, i) => s + (i.qty || i.quantity || 1), 0)
          }))
        });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // POST /api/admin/lieferscheine — Yeni Lieferschein oluştur
    // Body: { customer_id?, customer?, items[], delivery_date?, notes?, order_ref?, invoice_ref? }
    if (request.method === "POST" && path === "/api/admin/lieferscheine" && isAdmin) {
      try {
        const body = await request.json();

        // Müşteri belirleme: customer_id varsa DB'den, yoksa body.customer
        let customer = null;
        let customer_id = null;
        if (body.customer_id) {
          const customers = await getCustomers(env);
          customer = customers.find(c => c.id === body.customer_id);
          if (!customer) return errResp("Müşteri bulunamadı", 404);
          customer_id = body.customer_id;
        } else if (body.customer && body.customer.name) {
          customer = body.customer;
        } else {
          return errResp("Müşteri bilgisi gerekli (customer_id veya customer)");
        }

        if (!Array.isArray(body.items) || body.items.length === 0) {
          return errResp("En az 1 ürün gerekli");
        }

        // Items normalize
        const items = body.items.map(i => ({
          name: (i.name || i.title || "").trim(),
          sku: (i.sku || i.artikel_nr || "").trim(),
          qty: parseInt(i.qty || i.quantity || 1) || 1
        })).filter(i => i.name);

        if (items.length === 0) return errResp("Geçerli ürün yok");

        const number = await nextLieferscheinNumber(env);
        const id = "ls_" + Date.now().toString(36) + Math.random().toString(36).substring(2, 6);

        const ls = {
          id,
          number,
          customer_id,
          customer: {
            name: customer.name,
            company_name: customer.company_name || "",
            email: customer.email || "",
            phone: customer.phone || "",
            address: customer.address || {}
          },
          items,
          delivery_date: body.delivery_date || new Date().toISOString().slice(0, 10),
          notes: body.notes || "",
          order_ref: body.order_ref || null,
          invoice_ref: body.invoice_ref || null,
          created_at: new Date().toISOString(),
          created_by: "admin"
        };

        await putLieferschein(env, id, ls);
        return jsonResp({ success: true, lieferschein: ls });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // GET /api/admin/lieferscheine/:id — Tek Lieferschein detay (JSON)
    if (request.method === "GET" && path.match(/^\/api\/admin\/lieferscheine\/[^/]+$/) && isAdmin) {
      try {
        const id = decodeURIComponent(path.split("/")[4]);
        const ls = await getLieferschein(env, id);
        if (!ls) return errResp("Lieferschein bulunamadı", 404);
        return jsonResp({ success: true, lieferschein: ls });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // DELETE /api/admin/lieferscheine/:id — Sil
    if (request.method === "DELETE" && path.match(/^\/api\/admin\/lieferscheine\/[^/]+$/) && isAdmin) {
      try {
        const id = decodeURIComponent(path.split("/")[4]);
        try {
          await env.KEBY_R2.delete("keby/lieferscheine/" + id + ".json");
        } catch (e) {}
        return jsonResp({ success: true });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // GET /api/admin/lieferscheine/:id/pdf — HTML çıktı (yazdırılabilir)
    if (request.method === "GET" && path.match(/^\/api\/admin\/lieferscheine\/[^/]+\/pdf$/) && isAdmin) {
      try {
        const id = decodeURIComponent(path.split("/")[4]);
        const ls = await getLieferschein(env, id);
        if (!ls) return errResp("Lieferschein bulunamadı", 404);
        const html = generateLieferscheinHTML(ls);
        return new Response(html, {
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-cache",
            "Access-Control-Allow-Origin": "*"
          }
        });
      } catch (e) {
        return errResp(e.message);
      }
    }

    // POST /api/admin/orders/:orderId/lieferschein — Online sipariş'ten otomatik Lieferschein
    if (request.method === "POST" && path.match(/^\/api\/admin\/orders\/[^/]+\/lieferschein$/) && isAdmin) {
      try {
        const orderId = decodeURIComponent(path.split("/")[4]);
        const orders = await getOrders(env);
        const order = orders.find(o => o.id === orderId);
        if (!order) return errResp("Sipariş bulunamadı", 404);

        const items = (order.items || []).map(i => ({
          name: i.name || i.title || "",
          sku: i.sku || i.artikel_nr || "",
          qty: parseInt(i.qty || i.quantity || 1) || 1
        }));

        const number = await nextLieferscheinNumber(env);
        const id = "ls_" + Date.now().toString(36) + Math.random().toString(36).substring(2, 6);

        // Müşteri adresini siparişten al
        const customer = {
          name: order.name || order.customer_name || "",
          company_name: order.company_name || "",
          email: order.email || "",
          phone: order.phone || "",
          address: order.address || {
            line1: order.shipping_line1 || "",
            line2: order.shipping_line2 || "",
            postal_code: order.shipping_zip || order.zip || "",
            city: order.shipping_city || order.city || "",
            country: order.shipping_country || order.country || "DE"
          }
        };

        // Var olan invoice ile bağla
        const invoice = await getInvoice(env, orderId);
        const invoice_ref = invoice ? invoice.number : null;

        const ls = {
          id,
          number,
          customer,
          items,
          delivery_date: new Date().toISOString().slice(0, 10),
          notes: "",
          order_ref: orderId,
          invoice_ref,
          created_at: new Date().toISOString(),
          created_by: "admin"
        };

        await putLieferschein(env, id, ls);
        return jsonResp({ success: true, lieferschein: ls });
      } catch (e) {
        return errResp(e.message);
      }
    }

    if (request.method === "POST" && path === "/api/admin/invoices/manual" && isAdmin) {
      try {
        const body = await request.json();
        // body: { customer_id?, customer? (yeni), items, payment, payment_date, notes, order_date }

        // Müşteri bilgisini al — customer_id verilmiş ya da yeni müşteri data'sı verilmiş
        let customer;
        let customerId = body.customer_id;

        if (customerId) {
          const customers = await getCustomers(env);
          customer = customers.find(c => c.id === customerId);
          if (!customer) return errResp("Müşteri bulunamadı", 404);
        } else if (body.customer) {
          // Yeni müşteri kaydet
          const customers = await getCustomers(env);
          const newCust = {
            id: generateCustomerId(),
            name: (body.customer.name || "").trim(),
            email: (body.customer.email || "").trim().toLowerCase(),
            phone: (body.customer.phone || "").trim(),
            address: body.customer.address || {},
            ust_idnr: (body.customer.ust_idnr || "").trim(),
            notes: body.customer.notes || "",
            source: "direct",
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
          };
          if (!newCust.name) return errResp("Müşteri adı gerekli");

          // Aynı email var mı?
          const idx = newCust.email ? customers.findIndex(c => (c.email||"").toLowerCase() === newCust.email) : -1;
          if (idx >= 0) {
            customer = customers[idx];
            customerId = customer.id;
          } else {
            customers.unshift(newCust);
            await putCustomers(env, customers);
            customer = newCust;
            customerId = newCust.id;
          }
        } else {
          return errResp("customer_id veya customer (yeni) gerekli");
        }

        // Items kontrolü
        const items = (body.items || []).map(it => ({
          artikel_nr: (it.artikel_nr || "").trim(),
          product_id: it.product_id || "",
          name: (it.name || it.description || "—").trim(),
          qty: parseFloat(it.qty || it.quantity || 1),
          price: parseFloat(it.price || 0)
        })).filter(it => it.name && it.name !== "—");
        if (items.length === 0) return errResp("En az 1 pozisyon gerekli");

        const subtotal = items.reduce((s, it) => s + (it.price * it.qty), 0);
        const total = parseFloat(body.total !== undefined ? body.total : subtotal);

        // Fatura oluştur
        const number = await nextInvoiceNumber(env);
        const now = new Date();
        const orderDate = body.order_date ? new Date(body.order_date) : now;
        const orderId = "manual_" + Date.now().toString(36) + "_" + Math.random().toString(36).substring(2, 6);

        const invoice = {
          number,
          order_id: orderId,
          order_ref: number,
          issued_at: now.toISOString(),
          order_date: orderDate.toISOString(),
          payment: body.payment || "bar",
          payment_date: body.payment_date || (body.payment !== "offen" ? now.toISOString() : null),
          payment_id: "",
          customer: {
            id: customerId,
            name: customer.name,
            email: customer.email,
            phone: customer.phone,
            address: customer.address,
            address_text: [
              customer.address?.line1,
              customer.address?.line2,
              [customer.address?.postal_code, customer.address?.city].filter(Boolean).join(' '),
              customer.address?.country
            ].filter(Boolean).join(', ')
          },
          customer_id: customerId,
          items,
          subtotal,
          shipping_fee: 0,
          total,
          currency: "EUR",
          tax_mode: "kleinunternehmer",
          tax_note: "Gemäß § 19 UStG wird keine Umsatzsteuer ausgewiesen.",
          source: "manual",
          notes: body.notes || ""
        };

        await putInvoice(env, orderId, invoice);

        // GoBD: Otomatik Geldfluss Einnahme
        try {
          await autoAddInvoiceEinnahme(env, invoice);
        } catch (e) { /* nicht fatal */ }

        return jsonResp({ success: true, invoice });
      } catch (e) {
        return errResp("Manuel fatura hatası: " + e.message);
      }
    }

    // Fatura iptali (Storno) — yeni eksi tutarlı fatura üretir, orijinali işaretler
    if (request.method === "POST" && path.startsWith("/api/admin/invoices/") && path.endsWith("/storno") && isAdmin) {
      try {
        const orderId = decodeURIComponent(path.replace("/api/admin/invoices/", "").replace("/storno", ""));
        const original = await getInvoice(env, orderId);
        if (!original) return errResp("Orijinal fatura bulunamadı", 404);
        if (original.stornoed_at) return errResp("Bu fatura zaten iptal edilmiş");
        if (original.is_storno) return errResp("Storno-Rechnung'u tekrar storno edemezsiniz");

        const body = await request.json().catch(() => ({}));
        const reason = body.reason || "Stornorechnung";

        // Orijinali işaretle
        original.stornoed_at = new Date().toISOString();
        original.stornoed_by_number = null; // ayarlanacak
        await putInvoice(env, orderId, original);

        // Yeni Stornorechnung oluştur (eksi tutarlı)
        const stornoNumber = await nextInvoiceNumber(env);
        const stornoId = "storno_" + Date.now().toString(36);
        const now = new Date();

        const storno = {
          number: stornoNumber,
          order_id: stornoId,
          order_ref: stornoNumber,
          issued_at: now.toISOString(),
          order_date: now.toISOString(),
          payment: original.payment,
          customer: original.customer,
          customer_id: original.customer_id,
          items: original.items.map(it => ({
            name: "[STORNO] " + it.name,
            qty: it.qty,
            price: -Math.abs(it.price)
          })),
          subtotal: -Math.abs(original.subtotal || 0),
          shipping_fee: -Math.abs(original.shipping_fee || 0),
          total: -Math.abs(original.total || 0),
          currency: "EUR",
          tax_mode: original.tax_mode,
          tax_note: original.tax_note,
          source: "storno",
          is_storno: true,
          storno_for: original.number,
          storno_for_order_id: orderId,
          storno_reason: reason,
          notes: "Stornorechnung zu Rechnung " + original.number
        };

        await putInvoice(env, stornoId, storno);

        // Orijinale geri ata storno numarasını
        original.stornoed_by_number = stornoNumber;
        original.stornoed_by_order_id = stornoId;
        await putInvoice(env, orderId, original);

        // GoBD: Geldfluss'ta orijinal Einnahme'yi voidet, Storno-Ausgabe ekle
        try {
          await autoAddStornoAusgabe(env, original, storno);
        } catch (e) { /* nicht fatal */ }

        return jsonResp({ success: true, storno_number: stornoNumber, storno_invoice: storno });
      } catch (e) {
        return errResp("Storno hatası: " + e.message);
      }
    }

    // ============ IMAGES ============
    // List all images
    if (request.method === "GET" && path === "/api/images") {
      const folder = url.searchParams.get("folder") || "keby";
      const list = await env.KEBY_R2.list({ prefix: folder + "/", limit: 1000 });
      const images = list.objects
        .filter(o => !o.key.includes("/data/"))
        .filter(o => /\.(jpg|jpeg|png|webp|gif|mp4|mov)$/i.test(o.key))
        .map(o => ({
          key: o.key,
          size: o.size,
          modified: o.uploaded,
          url: "/img/" + o.key
        }));
      return jsonResp({ success: true, images, count: images.length });
    }

    // Upload image (multipart form)
    if (request.method === "POST" && path === "/api/admin/upload" && isAdmin) {
      const formData = await request.formData();
      const file = formData.get("file");
      let name = sanitize(formData.get("name") || (file && file.name) || "", 120);
      if (!file) return errResp("Dosya yok");
      if (!name) name = "upload-" + Date.now() + ".jpg";
      name = name
        .replace(/ğ/g, "g").replace(/Ğ/g, "G")
        .replace(/ü/g, "u").replace(/Ü/g, "U")
        .replace(/ş/g, "s").replace(/Ş/g, "S")
        .replace(/ö/g, "o").replace(/Ö/g, "O")
        .replace(/ç/g, "c").replace(/Ç/g, "C")
        .replace(/ı/g, "i").replace(/İ/g, "I")
        .replace(/\s+/g, "-");
      const key = name.startsWith("keby/") ? name : "keby/" + name;
      // Content-type: resim + video destekle
      const ct = file.type || "image/jpeg";
      const allowed = ["image/jpeg","image/png","image/webp","image/gif","image/heic","image/heif","video/mp4","video/webm","video/quicktime","application/octet-stream"];
      if (!allowed.some(a => ct.startsWith(a.split('/')[0] + '/')) && ct !== "application/octet-stream") {
        // Sadece image/* ve video/* izin ver
        if (!ct.startsWith("image/") && !ct.startsWith("video/")) {
          return errResp("Sadece resim ve video dosyaları destekleniyor");
        }
      }
      const maxSize = ct.startsWith("video/") ? 100 * 1024 * 1024 : 10 * 1024 * 1024;
      if (file.size > maxSize) {
        return errResp(ct.startsWith("video/") ? "Video max 100MB" : "Resim max 10MB");
      }
      await env.KEBY_R2.put(key, file.stream(), {
        httpMetadata: { contentType: ct }
      });
      const version = Date.now();
      return jsonResp({
        success: true,
        key,
        version,
        type: ct.startsWith("video/") ? "video" : "image",
        url: "https://keby-api.hguencavdi.workers.dev/img/" + key + "?v=" + version
      });
    }

    // ═══════════════ SLIDES — Hero slider yönetimi ═══════════════
    // GET /api/admin/slides — slide listesi
    if (request.method === "GET" && path === "/api/admin/slides" && isAdmin) {
      try {
        const obj = await env.KEBY_R2.get("keby/data/slides.json");
        if (!obj) {
          // Default slides (mevcut hardcoded'dan)
          const defaults = [
            { id: "s1", type: "image", src: "https://keby-api.hguencavdi.workers.dev/img/keby/keby-zeytin-park.jpg", alt: "Keby 750ml Zeytinyağı", order: 0 },
            { id: "s2", type: "image", src: "https://keby-api.hguencavdi.workers.dev/img/keby/keby-kahvalti-sofrasi.jpg", alt: "Keby Tanıtım", order: 1 },
            { id: "s3", type: "image", src: "https://keby-api.hguencavdi.workers.dev/img/keby/keby-urunler-gunbatimi.jpg", alt: "Keby Ürünler", order: 2 },
            { id: "s4", type: "image", src: "https://keby-api.hguencavdi.workers.dev/img/keby/keby-750ml-bahce.jpg", alt: "Keby Kahvaltı", order: 3 },
            { id: "s5", type: "image", src: "https://keby-api.hguencavdi.workers.dev/img/keby/keby-urunler-tanitim.jpg", alt: "Keby Zeytin", order: 4 }
          ];
          return jsonResp({ success: true, slides: defaults });
        }
        const slides = JSON.parse(await obj.text());
        return jsonResp({ success: true, slides });
      } catch (e) {
        return errResp("Slides: " + e.message, 500);
      }
    }

    // PUT /api/admin/slides — slide listesini kaydet
    if (request.method === "PUT" && path === "/api/admin/slides" && isAdmin) {
      try {
        const body = await request.json();
        if (!Array.isArray(body.slides)) return errResp("slides array gerekli");
        await env.KEBY_R2.put("keby/data/slides.json",
          JSON.stringify(body.slides),
          { httpMetadata: { contentType: "application/json" } }
        );
        return jsonResp({ success: true, count: body.slides.length });
      } catch (e) {
        return errResp("Slides save: " + e.message, 500);
      }
    }

    // Public endpoint — frontend slides için
    // GET /api/slides (no auth)
    if (request.method === "GET" && path === "/api/slides") {
      try {
        const obj = await env.KEBY_R2.get("keby/data/slides.json");
        if (!obj) {
          const defaults = [
            { id: "s1", type: "image", src: "https://keby-api.hguencavdi.workers.dev/img/keby/keby-zeytin-park.jpg", alt: "Keby", order: 0 },
            { id: "s2", type: "image", src: "https://keby-api.hguencavdi.workers.dev/img/keby/keby-kahvalti-sofrasi.jpg", alt: "Keby", order: 1 },
            { id: "s3", type: "image", src: "https://keby-api.hguencavdi.workers.dev/img/keby/keby-urunler-gunbatimi.jpg", alt: "Keby", order: 2 },
            { id: "s4", type: "image", src: "https://keby-api.hguencavdi.workers.dev/img/keby/keby-750ml-bahce.jpg", alt: "Keby", order: 3 },
            { id: "s5", type: "image", src: "https://keby-api.hguencavdi.workers.dev/img/keby/keby-urunler-tanitim.jpg", alt: "Keby", order: 4 }
          ];
          return new Response(JSON.stringify(defaults), {
            headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=60", "Access-Control-Allow-Origin": "*" }
          });
        }
        const slides = await obj.text();
        return new Response(slides, {
          headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=60", "Access-Control-Allow-Origin": "*" }
        });
      } catch (e) {
        return errResp("Slides public: " + e.message, 500);
      }
    }


    // Delete image
    if (request.method === "DELETE" && path.startsWith("/api/admin/image/") && isAdmin) {
      const key = decodeURIComponent(path.replace("/api/admin/image/", ""));
      if (key.includes("/data/")) return errResp("Veri dosyaları silinemez");
      await env.KEBY_R2.delete(key);
      return jsonResp({ success: true });
    }

    // ============ STRIPE ADMIN — müşteri ve ödeme listesi ============
    // Liste — Stripe Charges (gerçek ödeme yapmış kişiler) + Customers (kayıtlı)
    // POST /api/admin/stripe/update-webhook — webhook events güncelle
    if (request.method === "POST" && path === "/api/admin/stripe/update-webhook" && isAdmin) {
      try {
        const { webhook_id, events } = await request.json();
        const body = new URLSearchParams();
        events.forEach(e => body.append("enabled_events[]", e));
        const r = await fetch(`https://api.stripe.com/v1/webhook_endpoints/${webhook_id}`, {
          method: "POST",
          headers: {
            Authorization: "Bearer " + env.STRIPE_SECRET_KEY,
            "Content-Type": "application/x-www-form-urlencoded"
          },
          body: body.toString()
        });
        const d = await r.json();
        if (d.error) return errResp("Stripe: " + d.error.message);
        return jsonResp({ success: true, events: d.enabled_events });
      } catch (e) {
        return errResp("Webhook update: " + e.message, 500);
      }
    }

    if (request.method === "GET" && path === "/api/admin/stripe/customers" && isAdmin) {
      try {
        // Charges üzerinden git — billing_details daima dolu, customer object ise opsiyonel
        const limit = url.searchParams.get("limit") || "50";
        const after = url.searchParams.get("starting_after");
        let qs = "limit=" + limit + "&expand[]=data.customer&expand[]=data.payment_intent";
        if (after) qs += "&starting_after=" + after;

        const res = await fetch("https://api.stripe.com/v1/charges?" + qs, {
          headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY }
        });
        const data = await res.json();
        if (data.error) return errResp("Stripe: " + data.error.message);

        // Müşteri bazlı grupla
        const byEmail = {};
        for (const ch of data.data || []) {
          const bd = ch.billing_details || {};
          const email = bd.email || (ch.customer && ch.customer.email) || "—";
          const key = email.toLowerCase();
          if (!byEmail[key]) {
            byEmail[key] = {
              email,
              name: bd.name || (ch.customer && ch.customer.name) || "—",
              phone: bd.phone || (ch.customer && ch.customer.phone) || "",
              address: bd.address || (ch.customer && ch.customer.shipping && ch.customer.shipping.address) || null,
              shipping: ch.shipping || null,
              customer_id: ch.customer ? ch.customer.id : null,
              total_charges: 0,
              total_amount: 0,
              currency: ch.currency,
              first_charge: ch.created,
              last_charge: ch.created,
              charges: []
            };
          }
          const c = byEmail[key];
          c.total_charges++;
          if (ch.status === "succeeded") c.total_amount += ch.amount;
          if (ch.created < c.first_charge) c.first_charge = ch.created;
          if (ch.created > c.last_charge) c.last_charge = ch.created;
          c.charges.push({
            id: ch.id,
            amount: ch.amount,
            currency: ch.currency,
            status: ch.status,
            created: ch.created,
            paid: ch.paid,
            refunded: ch.refunded,
            description: ch.description,
            payment_method: ch.payment_method_details ? ch.payment_method_details.type : null,
            receipt_url: ch.receipt_url
          });
        }

        const customers = Object.values(byEmail).sort((a, b) => b.last_charge - a.last_charge);
        return jsonResp({
          success: true,
          customers,
          has_more: data.has_more,
          last_id: data.data && data.data.length ? data.data[data.data.length - 1].id : null
        });
      } catch (e) {
        return errResp("Stripe hatası: " + e.message);
      }
    }

    // Tek müşteri detay (email ile)
    if (request.method === "GET" && path === "/api/admin/stripe/charges" && isAdmin) {
      try {
        const email = url.searchParams.get("email");
        if (!email) return errResp("Email gerekli");
        // Stripe arama
        const q = encodeURIComponent(`billing_details['email']:'${email}'`);
        const res = await fetch("https://api.stripe.com/v1/charges/search?query=" + q + "&limit=100", {
          headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY }
        });
        const data = await res.json();
        if (data.error) return errResp("Stripe: " + data.error.message);
        return jsonResp({ success: true, charges: data.data });
      } catch (e) {
        return errResp("Stripe hatası: " + e.message);
      }
    }

    // Tek payment intent detayı — sipariş modal'ı için canlı veri
    if (request.method === "GET" && path === "/api/admin/stripe/payment" && isAdmin) {
      try {
        const pi = url.searchParams.get("pi");
        if (!pi) return errResp("Payment intent ID gerekli");

        // 1) Charge çek (en zengin veri burada)
        const chRes = await fetch(
          "https://api.stripe.com/v1/charges?payment_intent=" + pi + "&limit=1&expand[]=data.customer",
          { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
        );
        const chData = await chRes.json();
        if (chData.error) return errResp("Stripe charge: " + chData.error.message);
        const ch = chData.data && chData.data[0];
        if (!ch) return errResp("Bu ödeme bulunamadı");

        // 2) Payment Intent'i de çek (shipping bilgisi orada olabilir)
        let pintent = null;
        try {
          const piRes = await fetch(
            "https://api.stripe.com/v1/payment_intents/" + pi + "?expand[]=customer",
            { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
          );
          pintent = await piRes.json();
        } catch (e) { /* ignore */ }

        // 3) Customer object'i (varsa)
        const cust = (typeof ch.customer === 'object' && ch.customer) ? ch.customer :
                     (pintent && typeof pintent.customer === 'object' ? pintent.customer : null);

        const billing = ch.billing_details || {};
        // Shipping: önce charge'dan, sonra payment_intent'ten, sonra customer'dan
        const shipping = ch.shipping || (pintent && pintent.shipping) || (cust && cust.shipping) || null;
        // Adres: önce shipping, sonra billing, sonra customer.address
        const addr = (shipping && shipping.address) ||
                     billing.address ||
                     (cust && cust.address) ||
                     {};

        // Name: shipping > billing > customer
        const name = (shipping && shipping.name) || billing.name || (cust && cust.name) || null;
        // Email: billing > customer > receipt_email > pintent.receipt_email
        const email = billing.email ||
                      (cust && cust.email) ||
                      ch.receipt_email ||
                      (pintent && pintent.receipt_email) ||
                      null;
        // Phone: billing > shipping > customer
        const phone = billing.phone ||
                      (shipping && shipping.phone) ||
                      (cust && cust.phone) ||
                      null;

        return jsonResp({
          success: true,
          payment: {
            id: ch.id,
            payment_intent: pi,
            amount: ch.amount,
            currency: ch.currency,
            status: ch.status,
            paid: ch.paid,
            refunded: ch.refunded,
            created: ch.created,
            description: ch.description,
            payment_method: ch.payment_method_details ? ch.payment_method_details.type : null,
            payment_method_details: ch.payment_method_details,
            receipt_url: ch.receipt_url,
            receipt_email: ch.receipt_email,
            customer_id: cust ? cust.id : (typeof ch.customer === 'string' ? ch.customer : null),
            name,
            email,
            phone,
            address: {
              line1: addr.line1 || "",
              line2: addr.line2 || "",
              city: addr.city || "",
              postal_code: addr.postal_code || "",
              state: addr.state || "",
              country: addr.country || ""
            },
            shipping: shipping || null,
            metadata: ch.metadata || {},
            // Veri kaynağı bilgisi (UI için)
            sources: {
              has_billing_address: !!billing.address && !!billing.address.line1,
              has_shipping: !!shipping,
              has_customer: !!cust
            }
          }
        });
      } catch (e) {
        return errResp("Stripe hatası: " + e.message);
      }
    }


    // ============ ADMIN: STOK ============
    if (path === "/api/stock" && request.method === "GET") {
      if (!isAdmin) return errResp("Unauthorized", 401);
      const products = await getProducts(env);
      const movements = await getStockMovements(env);
      const stockData = products.map(p => ({
        id: p.id,
        name: p.name,
        image: p.image || null,
        stock: p.stock || 0,
        min_stock: p.min_stock || 5,
        status: (p.stock || 0) === 0 ? "out" : (p.stock || 0) <= (p.min_stock || 5) ? "low" : "ok"
      }));
      return jsonResp({ success: true, products: stockData, movements: movements.slice(-100).reverse() });
    }

    if (path === "/api/stock/adjust" && request.method === "POST") {
      if (!isAdmin) return errResp("Unauthorized", 401);
      const body = await request.json();
      const { productId, qty, reason, type } = body;
      if (!productId || qty === undefined) return errResp("productId und qty erforderlich", 400);
      const result = await adjustStock(env, productId, parseInt(qty), reason, type);
      if (!result.success) return errResp(result.error, 404);
      return jsonResp({ success: true, ...result });
    }

    if (path === "/api/stock/min" && request.method === "POST") {
      if (!isAdmin) return errResp("Unauthorized", 401);
      const body = await request.json();
      const { productId, min_stock } = body;
      const products = await getProducts(env);
      const idx = products.findIndex(p => p.id == productId);
      if (idx === -1) return errResp("Produkt nicht gefunden", 404);
      products[idx].min_stock = parseInt(min_stock) || 5;
      await putProducts(env, products);
      return jsonResp({ success: true });
    }

    // ============ STRIPE CHECKOUT (embedded — site içinde) ============
    // ═══ SEO: robots.txt + sitemap.xml ═══
    if (path === "/robots.txt") {
      return new Response(
`User-agent: *
Allow: /
Disallow: /admin.html
Disallow: /admin

Sitemap: https://keby.shop/sitemap.xml`,
        { headers: { "Content-Type": "text/plain", "Cache-Control": "public, max-age=86400" } }
      );
    }

    if (path === "/sitemap.xml") {
      return new Response(
`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://keby.shop/</loc><lastmod>2026-04-28</lastmod><changefreq>weekly</changefreq><priority>1.0</priority></url>
  <url><loc>https://keby.shop/produkt-olivenoel-750ml.html</loc><lastmod>2026-04-28</lastmod><changefreq>monthly</changefreq><priority>0.9</priority></url>
  <url><loc>https://keby.shop/produkt-kanister-5l.html</loc><lastmod>2026-04-28</lastmod><changefreq>monthly</changefreq><priority>0.9</priority></url>
  <url><loc>https://keby.shop/produkt-oliven-schwarz.html</loc><lastmod>2026-04-28</lastmod><changefreq>monthly</changefreq><priority>0.9</priority></url>
  <url><loc>https://keby.shop/ueber-uns.html</loc><lastmod>2026-04-28</lastmod><changefreq>monthly</changefreq><priority>0.6</priority></url>
  <url><loc>https://keby.shop/vision.html</loc><lastmod>2026-04-28</lastmod><changefreq>monthly</changefreq><priority>0.5</priority></url>
  <url><loc>https://keby.shop/impressum.html</loc><lastmod>2026-04-28</lastmod><changefreq>yearly</changefreq><priority>0.3</priority></url>
  <url><loc>https://keby.shop/datenschutz.html</loc><lastmod>2026-04-28</lastmod><changefreq>yearly</changefreq><priority>0.3</priority></url>
</urlset>`,
        { headers: { "Content-Type": "application/xml", "Cache-Control": "public, max-age=86400" } }
      );
    }



    // ═══════════════════════════════════════════════════
    // BACKUP SİSTEMİ
    // ═══════════════════════════════════════════════════

    if (request.method === "POST" && path === "/api/admin/backup" && isAdmin) {
      try {
        const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
        const dataFiles = [
          "keby/data/orders.json", "keby/data/invoices.json",
          "keby/data/geldfluss.json", "keby/data/customers.json",
          "keby/data/coupons.json", "keby/data/products.json",
          "keby/data/slides.json", "keby/data/lieferschein-counter.json",
        ];
        const results = [];
        for (const key of dataFiles) {
          try {
            const obj = await env.KEBY_R2.get(key);
            if (!obj) { results.push({ key, status: "skip" }); continue; }
            const content = await obj.text();
            const backupKey = `backups/${ts}/${key.replace("keby/data/", "")}`;
            await env.KEBY_BACKUPS.put(backupKey, content, { httpMetadata: { contentType: "application/json" } });
            results.push({ key, backupKey, status: "ok", size: content.length });
          } catch (e) { results.push({ key, status: "error", error: e.message }); }
        }
        const ok = results.filter(r => r.status === "ok").length;
        await env.KEBY_BACKUPS.put(`backups/${ts}/manifest.json`,
          JSON.stringify({ timestamp: ts, created_at: new Date().toISOString(), files: results, total: ok }, null, 2),
          { httpMetadata: { contentType: "application/json" } }
        );
        return jsonResp({ success: true, timestamp: ts, files_backed_up: ok });
      } catch (e) { return errResp("Backup: " + e.message, 500); }
    }

    if (request.method === "GET" && path === "/api/admin/backups" && isAdmin) {
      try {
        // delimiter yerine manifest.json dosyalarını listele
        const list = await env.KEBY_BACKUPS.list({ prefix: "backups/" });
        const manifests = (list.objects || []).filter(o => o.key.endsWith("/manifest.json"));
        const backups = [];
        for (const obj of manifests) {
          try {
            const ts = obj.key.replace("backups/", "").replace("/manifest.json", "");
            const m = await env.KEBY_BACKUPS.get(obj.key);
            const data = m ? JSON.parse(await m.text()) : {};
            backups.push({
              timestamp: ts,
              created_at: data.created_at || ts,
              files: data.total || "?"
            });
          } catch (e) {}
        }
        return jsonResp({ success: true, backups: backups.reverse() });
      } catch (e) { return errResp("Backup list: " + e.message, 500); }
    }

    if (request.method === "POST" && path.startsWith("/api/admin/backup/restore/") && isAdmin) {
      try {
        const ts = path.replace("/api/admin/backup/restore/", "");
        const files = ["orders.json","invoices.json","geldfluss.json","customers.json","coupons.json","products.json"];
        const results = [];
        for (const file of files) {
          try {
            const obj = await env.KEBY_BACKUPS.get(`backups/${ts}/${file}`);
            if (!obj) { results.push({ file, status: "skip" }); continue; }
            await env.KEBY_R2.put(`keby/data/${file}`, await obj.text(), { httpMetadata: { contentType: "application/json" } });
            results.push({ file, status: "restored" });
          } catch (e) { results.push({ file, status: "error", error: e.message }); }
        }
        return jsonResp({ success: true, timestamp: ts, results });
      } catch (e) { return errResp("Restore: " + e.message, 500); }
    }

    // GET /api/admin/coupons — liste (admin)
    if (request.method === "GET" && path === "/api/admin/coupons" && isAdmin) {
      const coupons = await getCoupons(env);
      return jsonResp({ success: true, coupons });
    }

    // POST /api/admin/coupons — yeni coupon oluştur (admin)
    if (request.method === "POST" && path === "/api/admin/coupons" && isAdmin) {
      try {
        const body = await request.json();
        const { code, type, value, max_uses, expires_at, description, min_order_amount } = body;

        if (!code || !type || !value) return errResp("code, type, value zorunlu");
        if (!["percent", "fixed"].includes(type)) return errResp("type: percent veya fixed");
        if (parseFloat(value) <= 0) return errResp("value > 0 olmalı");
        if (type === "percent" && parseFloat(value) > 100) return errResp("Yüzde max 100");

        // Unique code kontrolü
        const existing = await getCoupons(env);
        if (existing.find(c => c.code.toUpperCase() === code.toUpperCase())) {
          return errResp("Bu kod zaten mevcut");
        }

        // Stripe Coupon oluştur (senkronize)
        let stripeCouponId = null;
        if (env.STRIPE_SECRET_KEY) {
          try {
            const stripeBody = new URLSearchParams();
            stripeBody.append("id", "KEBY_" + code.toUpperCase());
            if (type === "percent") {
              stripeBody.append("percent_off", String(parseFloat(value)));
            } else {
              stripeBody.append("amount_off", String(Math.round(parseFloat(value) * 100)));
              stripeBody.append("currency", "eur");
            }
            stripeBody.append("name", description || code);
            if (max_uses) stripeBody.append("max_redemptions", String(max_uses));
            if (expires_at) stripeBody.append("redeem_by", String(Math.floor(new Date(expires_at).getTime() / 1000)));

            const sRes = await fetch("https://api.stripe.com/v1/coupons", {
              method: "POST",
              headers: {
                Authorization: "Bearer " + env.STRIPE_SECRET_KEY,
                "Content-Type": "application/x-www-form-urlencoded"
              },
              body: stripeBody.toString()
            });
            const sCoupon = await sRes.json();
            if (!sCoupon.error) {
              stripeCouponId = sCoupon.id;
            }
          } catch (e) {
            console.error("Stripe coupon create:", e);
          }
        }

        const newCoupon = {
          id: crypto.randomUUID(),
          code: code.toUpperCase().trim(),
          type,
          value: parseFloat(value),
          max_uses: max_uses ? parseInt(max_uses) : null,
          used_count: 0,
          expires_at: expires_at || null,
          description: description || "",
          min_order_amount: min_order_amount ? parseFloat(min_order_amount) : null,
          active: true,
          stripe_coupon_id: stripeCouponId,
          created_at: new Date().toISOString()
        };

        existing.push(newCoupon);
        await saveCoupons(env, existing);
        return jsonResp({ success: true, coupon: newCoupon });
      } catch (e) {
        return errResp("Coupon create: " + e.message, 500);
      }
    }

    // DELETE /api/admin/coupons/:id — sil (admin)
    if (request.method === "DELETE" && path.startsWith("/api/admin/coupons/") && isAdmin) {
      const id = path.replace("/api/admin/coupons/", "");
      const coupons = await getCoupons(env);
      const idx = coupons.findIndex(c => c.id === id || c.code === id.toUpperCase());
      if (idx === -1) return errResp("Coupon bulunamadı", 404);

      const coupon = coupons[idx];

      // Stripe'ta da sil
      if (coupon.stripe_coupon_id && env.STRIPE_SECRET_KEY) {
        fetch(`https://api.stripe.com/v1/coupons/${coupon.stripe_coupon_id}`, {
          method: "DELETE",
          headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY }
        }).catch(() => {});
      }

      coupons.splice(idx, 1);
      await saveCoupons(env, coupons);
      return jsonResp({ success: true });
    }

    // PATCH /api/admin/coupons/:id — aktif/pasif toggle (admin)
    if (request.method === "PATCH" && path.startsWith("/api/admin/coupons/") && isAdmin) {
      const id = path.replace("/api/admin/coupons/", "");
      const body = await request.json().catch(() => ({}));
      const coupons = await getCoupons(env);
      const coupon = coupons.find(c => c.id === id);
      if (!coupon) return errResp("Coupon bulunamadı", 404);
      if (typeof body.active === "boolean") coupon.active = body.active;
      await saveCoupons(env, coupons);
      return jsonResp({ success: true, coupon });
    }

    // POST /api/coupon/validate — kodu doğrula + indirim hesapla (public, checkout'tan çağrılır)
    if (request.method === "POST" && path === "/api/coupon/validate") {
      try {
        const { code, order_total } = await request.json().catch(() => ({}));
        if (!code) return errResp("Kod gerekli");

        const coupons = await getCoupons(env);
        const coupon = coupons.find(c =>
          c.code === code.toUpperCase().trim() && c.active
        );

        if (!coupon) return errResp("Geçersiz veya bulunamayan kod", 404);

        // Sona erdi mi?
        if (coupon.expires_at && new Date(coupon.expires_at) < new Date()) {
          return errResp("Bu kod süresi dolmuş", 410);
        }

        // Kullanım limiti doldu mu?
        if (coupon.max_uses !== null && coupon.used_count >= coupon.max_uses) {
          return errResp("Bu kod kullanım limitine ulaştı", 410);
        }

        // Min sipariş tutarı kontrolü
        const total = parseFloat(order_total) || 0;
        if (coupon.min_order_amount && total < coupon.min_order_amount) {
          return errResp(`Minimum sipariş tutarı: €${coupon.min_order_amount.toFixed(2)}`, 422);
        }

        // İndirim hesapla
        let discount_amount = 0;
        if (coupon.type === "percent") {
          discount_amount = total * (coupon.value / 100);
        } else {
          discount_amount = Math.min(coupon.value, total); // sabit indirim sepet toplamını aşamaz
        }
        discount_amount = Math.round(discount_amount * 100) / 100;
        const new_total = Math.max(0, total - discount_amount);

        return jsonResp({
          success: true,
          coupon: {
            code: coupon.code,
            type: coupon.type,
            value: coupon.value,
            description: coupon.description,
            stripe_coupon_id: coupon.stripe_coupon_id
          },
          discount_amount,
          new_total,
          original_total: total
        });
      } catch (e) {
        return errResp("Validate: " + e.message, 500);
      }
    }

    // ═══════════════════════════════════════════════════

    // ═══════════════════════════════════════════════════
    // STRIPE CHECKOUT SESSION — coupon desteği ile
    // ═══════════════════════════════════════════════════

    if (request.method === "POST" && path === "/api/stripe/create-checkout-session") {
      try {
        const { items, customerEmail, locale, preferredMethod, couponCode } = await request.json();
        if (!items || items.length === 0) return errResp("Cart is empty");

        // Sipariş toplamını hesapla (kargo için)
        const orderSubtotal = items.reduce((sum, it) => sum + (parseFloat(it.price) || 0) * (parseInt(it.qty) || 1), 0);
        const shippingCost = orderSubtotal >= 120 ? 0 : 5.49;

        const body = new URLSearchParams();
        body.append("mode", "payment");
        body.append("ui_mode", "embedded_page");
        body.append("locale", locale || "de");
        body.append("return_url", "https://keby.shop/checkout.html?stripe_session={CHECKOUT_SESSION_ID}");

        if (customerEmail) body.append("customer_email", customerEmail);

        body.append("billing_address_collection", "required");
        body.append("shipping_address_collection[allowed_countries][]", "DE");
        body.append("shipping_address_collection[allowed_countries][]", "AT");
        body.append("shipping_address_collection[allowed_countries][]", "CH");
        body.append("shipping_address_collection[allowed_countries][]", "NL");
        body.append("shipping_address_collection[allowed_countries][]", "BE");
        body.append("shipping_address_collection[allowed_countries][]", "FR");
        body.append("phone_number_collection[enabled]", "true");

        // Ödeme yöntemleri — explicit liste (embedded_page modu için gerekli)
        const allMethods = ["sepa_debit", "card", "klarna", "paypal", "link"];
        allMethods.forEach(m => body.append("payment_method_types[]", m));

        // Line items
        items.forEach((it, idx) => {
          const name = it.name || it.title || it.productId || "Produkt";
          const price = parseFloat(it.price) || 0;
          const qty = parseInt(it.qty) || 1;
          body.append(`line_items[${idx}][quantity]`, String(qty));
          body.append(`line_items[${idx}][price_data][currency]`, "eur");
          body.append(`line_items[${idx}][price_data][unit_amount]`, String(Math.round(price * 100)));
          body.append(`line_items[${idx}][price_data][product_data][name]`, name);
          if (it.artikel_nr) {
            body.append(`line_items[${idx}][price_data][product_data][metadata][artikel_nr]`, it.artikel_nr);
          }
        });

        body.append("metadata[items_json]", JSON.stringify(items));
        body.append("metadata[source]", "keby_shop_v4_embedded");
        body.append("metadata[subtotal]", orderSubtotal.toFixed(2));
        body.append("metadata[shipping_cost]", shippingCost.toFixed(2));

        // Kargo bedeli — Stripe shipping_options ile
        if (shippingCost > 0) {
          body.append("shipping_options[0][shipping_rate_data][type]", "fixed_amount");
          body.append("shipping_options[0][shipping_rate_data][fixed_amount][amount]", String(Math.round(shippingCost * 100)));
          body.append("shipping_options[0][shipping_rate_data][fixed_amount][currency]", "eur");
          body.append("shipping_options[0][shipping_rate_data][display_name]", "Standardversand (1–3 Werktage)");
          body.append("shipping_options[0][shipping_rate_data][delivery_estimate][minimum][unit]", "business_day");
          body.append("shipping_options[0][shipping_rate_data][delivery_estimate][minimum][value]", "1");
          body.append("shipping_options[0][shipping_rate_data][delivery_estimate][maximum][unit]", "business_day");
          body.append("shipping_options[0][shipping_rate_data][delivery_estimate][maximum][value]", "3");
        } else {
          body.append("shipping_options[0][shipping_rate_data][type]", "fixed_amount");
          body.append("shipping_options[0][shipping_rate_data][fixed_amount][amount]", "0");
          body.append("shipping_options[0][shipping_rate_data][fixed_amount][currency]", "eur");
          body.append("shipping_options[0][shipping_rate_data][display_name]", "Kostenloser Versand (ab 120 €)");
          body.append("shipping_options[0][shipping_rate_data][delivery_estimate][minimum][unit]", "business_day");
          body.append("shipping_options[0][shipping_rate_data][delivery_estimate][minimum][value]", "1");
          body.append("shipping_options[0][shipping_rate_data][delivery_estimate][maximum][unit]", "business_day");
          body.append("shipping_options[0][shipping_rate_data][delivery_estimate][maximum][value]", "3");
        }

        // ── İLK SİPARİŞ KONTROLÜ ──────────────────────────────
        // Müşterinin daha önce siparişi var mı? Yoksa %10 otomatik indirim
        let firstOrderCoupon = null;
        if (customerEmail && !couponCode) {
          try {
            const allOrders = await getOrders(env);
            const prevOrders = allOrders.filter(o =>
              o.email && o.email.toLowerCase() === customerEmail.toLowerCase() &&
              o.status !== "storniert"
            );
            if (prevOrders.length === 0) {
              // İlk sipariş — Stripe'ta WILLKOMMEN10 coupon var mı kontrol et, yoksa oluştur
              const couponId = "WILLKOMMEN10";
              const checkRes = await fetch(`https://api.stripe.com/v1/coupons/${couponId}`, {
                headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY }
              });
              if (checkRes.ok) {
                firstOrderCoupon = couponId;
              } else {
                // Yoksa oluştur
                const createBody = new URLSearchParams();
                createBody.append("id", couponId);
                createBody.append("percent_off", "10");
                createBody.append("duration", "once");
                createBody.append("name", "10% Erstkunden-Rabatt");
                const createRes = await fetch("https://api.stripe.com/v1/coupons", {
                  method: "POST",
                  headers: {
                    Authorization: "Bearer " + env.STRIPE_SECRET_KEY,
                    "Content-Type": "application/x-www-form-urlencoded"
                  },
                  body: createBody.toString()
                });
                if (createRes.ok) firstOrderCoupon = couponId;
              }
            }
          } catch (e) {
            console.error("First order check:", e);
          }
        }
        // ──────────────────────────────────────────────────────
        let appliedCoupon = null;

        // İlk sipariş indirimi — manuel kupon yoksa otomatik uygula
        if (firstOrderCoupon && !couponCode) {
          body.append("discounts[0][coupon]", firstOrderCoupon);
          appliedCoupon = { code: "WILLKOMMEN10", type: "percent", value: 10, auto: true };
          body.append("metadata[coupon_code]", "WILLKOMMEN10");
          body.append("metadata[first_order_discount]", "true");
        }

        if (couponCode) {
          const coupons = await getCoupons(env);
          const coupon = coupons.find(c =>
            c.code === couponCode.toUpperCase().trim() && c.active
          );
          if (coupon &&
            !(coupon.expires_at && new Date(coupon.expires_at) < new Date()) &&
            !(coupon.max_uses !== null && coupon.used_count >= coupon.max_uses)
          ) {
            if (coupon.stripe_coupon_id) {
              // Stripe Coupon — discounts array ile
              body.append("discounts[0][coupon]", coupon.stripe_coupon_id);
            } else if (coupon.type === "percent") {
              // Stripe Coupon yoksa promotion_code benzeri inline uygulama
              // (ad-hoc coupon oluştur)
              const adHocBody = new URLSearchParams();
              adHocBody.append("percent_off", String(coupon.value));
              adHocBody.append("duration", "once");
              adHocBody.append("name", coupon.code);
              const adHocRes = await fetch("https://api.stripe.com/v1/coupons", {
                method: "POST",
                headers: {
                  Authorization: "Bearer " + env.STRIPE_SECRET_KEY,
                  "Content-Type": "application/x-www-form-urlencoded"
                },
                body: adHocBody.toString()
              });
              const adHocCoupon = await adHocRes.json();
              if (!adHocCoupon.error) {
                body.append("discounts[0][coupon]", adHocCoupon.id);
                // Stripe ID'yi güncelle
                coupon.stripe_coupon_id = adHocCoupon.id;
                await saveCoupons(env, coupons);
              }
            }

            // Kullanım sayacını artır (ödeme başladığında, webhook'ta da yapılabilir ama bu daha basit)
            coupon.used_count = (coupon.used_count || 0) + 1;
            await saveCoupons(env, coupons);
            appliedCoupon = { code: coupon.code, type: coupon.type, value: coupon.value };
            body.append("metadata[coupon_code]", coupon.code);
          }
        }

        const res = await fetch("https://api.stripe.com/v1/checkout/sessions", {
          method: "POST",
          headers: {
            Authorization: "Bearer " + env.STRIPE_SECRET_KEY,
            "Content-Type": "application/x-www-form-urlencoded"
          },
          body: body.toString()
        });
        const session = await res.json();
        if (session.error) throw new Error(session.error.message);

        return jsonResp({
          success: true,
          sessionId: session.id,
          clientSecret: session.client_secret,  // embedded mode için kritik
          url: session.url,  // hosted fallback için
          appliedCoupon
        });
      } catch (e) {
        return errResp(e.message, 500);
      }
    }

    // GET checkout session details (success page için sipariş bilgisi)
    if (request.method === "GET" && path.startsWith("/api/stripe/checkout-session/")) {
      try {
        const sessionId = decodeURIComponent(path.replace("/api/stripe/checkout-session/", ""));
        const res = await fetch(
          "https://api.stripe.com/v1/checkout/sessions/" + sessionId + "?expand[]=customer&expand[]=line_items&expand[]=payment_intent",
          { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
        );
        const session = await res.json();
        if (session.error) throw new Error(session.error.message);
        return jsonResp({ success: true, session });
      } catch (e) {
        return errResp(e.message, 500);
      }
    }

    // ============ STRIPE PAYMENT INTENT (legacy, kart modal için) ============
    if (request.method === "POST" && path === "/api/stripe/create-payment-intent") {
      try {
        const { amount, currency = "eur", items, customerEmail } = await request.json();
        const body = new URLSearchParams({
          amount: String(Math.round(amount * 100)),
          currency,
          "metadata[items]": JSON.stringify(items || []),
          "metadata[email]": customerEmail || ""
        });
        // Explicit payment methods — Stripe smart algoritmadan emin olamadığımız durumlarda zorla
        // Hangi yöntemlerin Dashboard'da aktif olduğunu bilmek için tek tek deniyoruz
        body.append("payment_method_types[]", "card");
        body.append("payment_method_types[]", "sepa_debit");
        const res = await fetch("https://api.stripe.com/v1/payment_intents", {
          method: "POST",
          headers: {
            Authorization: "Bearer " + env.STRIPE_SECRET_KEY,
            "Content-Type": "application/x-www-form-urlencoded"
          },
          body: body.toString()
        });
        const pi = await res.json();
        if (pi.error) throw new Error(pi.error.message);
        return jsonResp({
          success: true,
          clientSecret: pi.client_secret,
          paymentIntentId: pi.id
        });
      } catch (e) {
        return errResp(e.message, 500);
      }
    }

    if (request.method === "GET" && path === "/api/stripe/config") {
      return jsonResp({ publishableKey: env.STRIPE_PUBLISHABLE_KEY });
    }

    if (request.method === "POST" && path === "/api/stripe/webhook") {
      try {
        const body = await request.text();
        const sig = request.headers.get("stripe-signature");
        if (sig && env.STRIPE_WEBHOOK_SECRET) {
          const ts = sig.match(/t=(\d+)/)?.[1];
          if (ts && Math.abs(Date.now() / 1000 - parseInt(ts)) > 300) {
            return new Response("Webhook expired", { status: 400, headers: CORS });
          }
        }
        const event = JSON.parse(body);

        // ── checkout.session.completed ──────────────────────────────────
        // Kart: payment_status="paid" → hemen sipariş oluştur + mail
        // SEPA: payment_status="unpaid" → sipariş oluştur, "banka onayı bekleniyor" durumu
        if (event.type === "checkout.session.completed") {
          const session = event.data.object;
          const orders = await getOrders(env);

          // Zaten kaydedildi mi?
          if (!orders.find(o => o.stripeSessionId === session.id || o.stripePaymentId === session.payment_intent)) {
            // Session detaylarını çek (line_items dahil)
            const sessRes = await fetch(
              `https://api.stripe.com/v1/checkout/sessions/${session.id}?expand[]=line_items&expand[]=customer&expand[]=payment_intent`,
              { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
            );
            const sessData = await sessRes.json();

            const isPaid = session.payment_status === "paid";
            const isSepa = session.payment_method_types?.includes("sepa_debit") ||
                           sessData?.payment_intent?.payment_method_types?.includes("sepa_debit");

            // Müşteri bilgileri
            const details = sessData.customer_details || {};
            const shipping = sessData.shipping_details || sessData.shipping || {};
            const addr = shipping.address || details.address || {};

            // Items — metadata'dan
            let items = [];
            try {
              const meta = session.metadata || {};
              if (meta.items_json) items = JSON.parse(meta.items_json);
              else if (meta.items) items = JSON.parse(meta.items);
              else if (sessData.line_items?.data) {
                items = sessData.line_items.data.map(li => ({
                  name: li.description || li.price?.product?.name || "Ürün",
                  qty: li.quantity,
                  price: (li.amount_total / 100) / li.quantity
                }));
              }
            } catch (e) {}

            const newOrder = {
              id: "KB-" + Date.now(),
              ref: "KB-" + new Date().getFullYear() + "-" + String(orders.length + 1).padStart(3, "0"),
              stripeSessionId: session.id,
              stripePaymentId: session.payment_intent,
              date: new Date().toISOString(),
              status: isPaid ? "bezahlt" : "zahlung_ausstehend",
              payment: "stripe",
              payment_method: isSepa ? "sepa_debit" : "card",
              email: details.email || "",
              name: shipping.name || details.name || "Stripe Müşteri",
              phone: details.phone || "",
              address: {
                line1: addr.line1 || "",
                line2: addr.line2 || "",
                city: addr.city || "",
                postal_code: addr.postal_code || "",
                country: addr.country || "DE"
              },
              items,
              total: (session.amount_total / 100).toFixed(2),
              currency: session.currency?.toUpperCase() || "EUR",
              coupon: session.metadata?.coupon_code || null,
              notes: isPaid ? "" : "⏳ SEPA Lastschrift — Bankbestätigung ausstehend (1-3 Werktage)"
            };

            orders.push(newOrder);
            await putOrders(env, orders);

            // Stok düşümü — sadece ödeme tamamlandıysa (kart = hemen, SEPA = banka onayında)
            if (isPaid && newOrder.items && newOrder.items.length) {
              await deductStock(env, newOrder.items, newOrder.ref);
            }

            // Mail gönder
            if (newOrder.email && env.RESEND_API_KEY) {
              const subject = isPaid
                ? `Bestellbestätigung — ${newOrder.ref}`
                : `Bestellung eingegangen — Bankbestätigung ausstehend — ${newOrder.ref}`;

              const statusNote = isPaid ? "" : `
                <div style="background:#fef3c7;border:1px solid #f59e0b;border-radius:8px;padding:12px 16px;margin:16px 0;font-size:13px;color:#92400e">
                  <strong>⏳ SEPA-Lastschrift:</strong> Deine Zahlung wird in 1–3 Werktagen von deiner Bank bestätigt.
                  Du erhältst eine weitere Bestätigung sobald die Zahlung eingegangen ist.
                </div>`;

              const itemRows = items.map(i =>
                `<tr><td style="padding:6px 0;border-bottom:1px solid #f0ebe0">${i.name}</td>
                 <td style="text-align:right;padding:6px 0;border-bottom:1px solid #f0ebe0">× ${i.qty}</td>
                 <td style="text-align:right;padding:6px 0;border-bottom:1px solid #f0ebe0">€${(i.price * i.qty).toFixed(2)}</td></tr>`
              ).join("");

              const html = `<!DOCTYPE html><html><body style="font-family:-apple-system,Helvetica,Arial,sans-serif;background:#f5f1e8;margin:0;padding:0">
<div style="max-width:560px;margin:32px auto;background:white;border-radius:12px;overflow:hidden">
  <div style="background:#2a4a1a;padding:24px 32px">
    <h1 style="color:white;margin:0;font-size:1.3rem;font-weight:500">
      ${isPaid ? "✅ Vielen Dank für deine Bestellung!" : "📋 Bestellung eingegangen"}
    </h1>
    <p style="color:rgba(255,255,255,0.7);margin:6px 0 0;font-size:0.85rem">${newOrder.ref}</p>
  </div>
  <div style="padding:28px 32px">
    <p style="color:#333;margin:0 0 16px">Hallo ${newOrder.name},</p>
    ${statusNote}
    <table style="width:100%;border-collapse:collapse;font-size:14px;margin-bottom:16px">
      ${itemRows}
      <tr><td colspan="2" style="padding:10px 0;font-weight:600">Gesamt</td>
          <td style="text-align:right;font-weight:600">€${newOrder.total}</td></tr>
    </table>
    <p style="font-size:13px;color:#666;margin-top:20px">
      Bei Fragen: <a href="mailto:info@keby.shop" style="color:#2a4a1a">info@keby.shop</a>
    </p>
  </div>
  <div style="background:#faf7f0;padding:16px 32px;text-align:center;font-size:12px;color:#888">
    <strong style="color:#2a4a1a">Keby Shop</strong> · <a href="https://keby.shop" style="color:#2a4a1a">keby.shop</a>
  </div>
</div></body></html>`;

              await fetch("https://api.resend.com/emails", {
                method: "POST",
                headers: { Authorization: "Bearer " + env.RESEND_API_KEY, "Content-Type": "application/json" },
                body: JSON.stringify({
                  from: "Keby <noreply@machbar24.com>",
                  reply_to: "info@keby.shop",
                  to: [newOrder.email],
                  bcc: ["info@keby.shop"],
                  subject,
                  html
                })
              }).catch(e => console.error("Mail:", e));
            }
          }
          return new Response("ok", { headers: CORS });
        }

        // ── checkout.session.async_payment_succeeded ────────────────────
        // SEPA onaylandı → sipariş durumunu güncelle + onay maili gönder
        if (event.type === "checkout.session.async_payment_succeeded") {
          const session = event.data.object;
          const orders = await getOrders(env);
          const order = orders.find(o =>
            o.stripeSessionId === session.id || o.stripePaymentId === session.payment_intent
          );
          if (order) {
            order.status = "bezahlt";
            order.sepa_confirmed_at = new Date().toISOString();
            order.notes = "✅ SEPA bestätigt";
            await putOrders(env, orders);

            // Onay maili
            if (order.email && env.RESEND_API_KEY) {
              await fetch("https://api.resend.com/emails", {
                method: "POST",
                headers: { Authorization: "Bearer " + env.RESEND_API_KEY, "Content-Type": "application/json" },
                body: JSON.stringify({
                  from: "Keby <noreply@machbar24.com>",
                  reply_to: "info@keby.shop",
                  to: [order.email],
                  bcc: ["info@keby.shop"],
                  subject: `✅ Zahlung bestätigt — ${order.ref}`,
                  html: `<div style="font-family:-apple-system,Helvetica,Arial,sans-serif;max-width:500px;margin:32px auto;padding:24px;background:#f0fdf4;border-radius:12px;border:1px solid #86efac">
                    <h2 style="color:#15803d;margin:0 0 12px">✅ Deine Zahlung wurde bestätigt!</h2>
                    <p style="color:#166534">Bestellung <strong>${order.ref}</strong> — €${order.total}</p>
                    <p style="color:#15803d;font-size:13px">Wir bereiten deine Bestellung jetzt für den Versand vor.</p>
                    <p style="font-size:13px;color:#666;margin-top:16px">Keby · <a href="https://keby.shop" style="color:#15803d">keby.shop</a></p>
                  </div>`
                })
              }).catch(e => console.error("SEPA confirm mail:", e));
            }
          }
          return new Response("ok", { headers: CORS });
        }

        // ── checkout.session.async_payment_failed ──────────────────────
        if (event.type === "checkout.session.async_payment_failed") {
          const session = event.data.object;
          const orders = await getOrders(env);
          const order = orders.find(o =>
            o.stripeSessionId === session.id || o.stripePaymentId === session.payment_intent
          );
          if (order) {
            order.status = "zahlung_fehlgeschlagen";
            order.notes = "❌ SEPA Lastschrift fehlgeschlagen";
            await putOrders(env, orders);

            if (order.email && env.RESEND_API_KEY) {
              await fetch("https://api.resend.com/emails", {
                method: "POST",
                headers: { Authorization: "Bearer " + env.RESEND_API_KEY, "Content-Type": "application/json" },
                body: JSON.stringify({
                  from: "Keby <noreply@machbar24.com>",
                  reply_to: "info@keby.shop",
                  to: [order.email],
                  subject: `Zahlung fehlgeschlagen — ${order.ref}`,
                  html: `<div style="font-family:-apple-system,Helvetica,Arial,sans-serif;max-width:500px;margin:32px auto;padding:24px;background:#fef2f2;border-radius:12px;border:1px solid #fca5a5">
                    <h2 style="color:#dc2626">❌ Zahlung fehlgeschlagen</h2>
                    <p style="color:#991b1b">Deine SEPA-Lastschrift für Bestellung <strong>${order.ref}</strong> konnte leider nicht eingezogen werden.</p>
                    <p style="font-size:13px;color:#666">Bitte kontaktiere uns: <a href="mailto:info@keby.shop">info@keby.shop</a></p>
                  </div>`
                })
              }).catch(e => console.error("SEPA fail mail:", e));
            }
          }
          return new Response("ok", { headers: CORS });
        }

        if (event.type === "payment_intent.succeeded") {
          const pi = event.data.object;
          const orders = await getOrders(env);
          if (!orders.find(o => o.stripePaymentId === pi.id)) {
            // Müşteri detayını Stripe'tan çek (charges üzerinden, billing_details için)
            let billing = {};
            let shipping = pi.shipping || null;
            // Items: önce PI metadata, yoksa Checkout session'dan
            let stripeItems = [];
            try {
              if (pi.metadata?.items) {
                stripeItems = JSON.parse(pi.metadata.items);
              } else if (pi.metadata?.items_json) {
                stripeItems = JSON.parse(pi.metadata.items_json);
              } else {
                // Checkout Session'ı bul (PI ile bağlı)
                const sRes = await fetch(
                  "https://api.stripe.com/v1/checkout/sessions?payment_intent=" + pi.id + "&limit=1&expand[]=data.line_items",
                  { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
                );
                const sData = await sRes.json();
                const sess = sData.data && sData.data[0];
                if (sess) {
                  if (sess.metadata?.items_json) {
                    stripeItems = JSON.parse(sess.metadata.items_json);
                  } else if (sess.line_items && sess.line_items.data) {
                    stripeItems = sess.line_items.data.map(li => ({
                      name: li.description || (li.price?.product?.name || "Produkt"),
                      qty: li.quantity || 1,
                      price: (li.price?.unit_amount || 0) / 100
                    }));
                  }
                  // Shipping fallback: Checkout session'da customer_details.address
                  if (!shipping && sess.customer_details) {
                    shipping = {
                      name: sess.customer_details.name || "",
                      phone: sess.customer_details.phone || "",
                      address: sess.shipping_details?.address || sess.customer_details.address || {}
                    };
                  }
                }
              }
            } catch (e) { /* ignore */ }

            let chData = { data: [] };
            try {
              const chRes = await fetch(
                "https://api.stripe.com/v1/charges?payment_intent=" + pi.id + "&limit=1",
                { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
              );
              chData = await chRes.json();
              if (chData.data && chData.data[0]) {
                billing = chData.data[0].billing_details || {};
                if (!shipping) shipping = chData.data[0].shipping;
              }
            } catch (e) {
              // sessizce devam — temel bilgiyle siparişi kaydet
            }

            const addr = (shipping && shipping.address) || billing.address || {};
            const addrTxt = [
              addr.line1,
              addr.line2,
              [addr.postal_code, addr.city].filter(Boolean).join(' '),
              addr.state,
              addr.country
            ].filter(Boolean).join(', ');

            orders.unshift({
              id: "ord_stripe_" + pi.id,
              ref: "KEBY-" + pi.id.slice(-6).toUpperCase(),
              name: (shipping && shipping.name) || billing.name || pi.metadata?.email || "Stripe Müşteri",
              email: billing.email || pi.metadata?.email || "",
              phone: billing.phone || (shipping && shipping.phone) || "",
              address: addrTxt,
              addressDetail: {
                line1: addr.line1 || "",
                line2: addr.line2 || "",
                city: addr.city || "",
                postal_code: addr.postal_code || "",
                state: addr.state || "",
                country: addr.country || ""
              },
              payment: "stripe",
              stripePaymentId: pi.id,
              stripeMethod: chData.data && chData.data[0] && chData.data[0].payment_method_details ? chData.data[0].payment_method_details.type : null,
              stripeStatus: chData.data && chData.data[0] ? chData.data[0].status : "succeeded",
              stripeReceiptUrl: chData.data && chData.data[0] ? chData.data[0].receipt_url : null,
              stripeChargeId: chData.data && chData.data[0] ? chData.data[0].id : null,
              status: "onaylandi",
              items: stripeItems,
              total: pi.amount / 100,
              date: new Date().toISOString()
            });
            await putOrders(env, orders);

            // SEPA stok düşümü — banka onaylandığında
            if (stripeItems && stripeItems.length) {
              const piRef = "KEBY-" + pi.id.slice(-6).toUpperCase();
              await deductStock(env, stripeItems, piRef);
            }

            // === Otomatik Bestellbestätigung E-Mail (PDF eki + Download-Link) ===
            const orderId = "ord_stripe_" + pi.id;
            const customerEmail = billing.email || pi.metadata?.email || "";
            const customerName = (shipping && shipping.name) || billing.name || "Kundin/Kunde";

            if (customerEmail && env.RESEND_API_KEY) {
              try {
                // Lazy-generate fatura
                const invResult = await getOrCreateInvoice(env, orderId);
                const invoice = invResult.invoice;

                // Download token üret
                const dlToken = await makeInvoiceToken(env, orderId, customerEmail);
                const dlUrl = "https://keby-api.hguencavdi.workers.dev/api/invoice/" + encodeURIComponent(orderId) + "/download/" + dlToken;

                // PDF üret (opsiyonel — sessizce başarısız olabilir)
                let pdfAttachment = null;
                try {
                  const pdfBuffer = await generateInvoicePDF(env, invoice);
                  const bytes = new Uint8Array(pdfBuffer);
                  let binary = "";
                  const chunkSize = 8192;
                  for (let i = 0; i < bytes.length; i += chunkSize) {
                    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize));
                  }
                  pdfAttachment = {
                    filename: `Rechnung_${invoice.number}.pdf`,
                    content: btoa(binary)
                  };
                } catch (pdfErr) {
                  // PDF üretilemese bile mail gitsin (sadece link ile)
                }

                // Sipariş özeti
                const orderItems = stripeItems;
                const itemsRows = orderItems.map(it =>
                  `<tr>
                    <td style="padding:8px 12px;border-bottom:1px solid #e8e3d6">${it.productId || it.name || "—"}</td>
                    <td style="padding:8px 12px;border-bottom:1px solid #e8e3d6;text-align:center">${it.qty || 1}</td>
                    <td style="padding:8px 12px;border-bottom:1px solid #e8e3d6;text-align:right">€ ${(parseFloat(it.price)||0).toFixed(2).replace('.', ',')}</td>
                  </tr>`
                ).join("");
                const total = (pi.amount / 100).toFixed(2).replace('.', ',');
                const orderRef = "KEBY-" + pi.id.slice(-6).toUpperCase();

                const subject = `Bestellbestätigung ${orderRef} – Keby`;

                const textBody = `Liebe/r ${customerName},

vielen Dank für Ihre Bestellung bei Keby!

Ihre Bestellung wurde erfolgreich aufgenommen und wird in Kürze versendet.

═══════════════════════════════════
BESTELLDETAILS
═══════════════════════════════════
Bestellnummer: ${orderRef}
Rechnungsnummer: ${invoice.number}
Gesamtbetrag: € ${total}

Ihre Rechnung als PDF herunterladen:
${dlUrl}

Bestellungen online verwalten:
https://keby.shop/meine-bestellungen.html?email=${encodeURIComponent(customerEmail)}&order=${encodeURIComponent(orderRef)}

(Den Link können Sie jederzeit aufrufen — die Rechnung ist auch als PDF im Anhang dieser E-Mail.)

Bei Fragen erreichen Sie uns jederzeit unter info@keby.shop oder +49 159 06342354.

Mit besten Grüßen
Kader Kayihan
Keby — Premium Olivenöl
https://keby.shop`;

                const htmlBody = `
<div style="font-family:'Inter',Arial,sans-serif;color:#2a2a22;line-height:1.6;max-width:560px;margin:0 auto;padding:20px">
  <div style="text-align:center;padding:20px 0;border-bottom:2px solid #3d6b2a;margin-bottom:24px">
    <img src="https://keby-api.hguencavdi.workers.dev/img/keby/logo-new.png" alt="Keby" style="height:54px;display:block;margin:0 auto 8px">
    <div style="color:#6b6b5a;font-size:11px;letter-spacing:2px;text-transform:uppercase">Premium Olivenöl</div>
  </div>

  <h2 style="color:#2a4a1a;font-family:Georgia,serif;font-weight:normal;margin:0 0 16px">Vielen Dank, ${customerName}!</h2>

  <p>Ihre Bestellung wurde erfolgreich aufgenommen und wird in Kürze versendet.</p>

  <div style="background:#faf7f0;border-left:3px solid #3d6b2a;padding:16px 20px;margin:24px 0;border-radius:0 4px 4px 0">
    <div style="font-size:11px;color:#6b6b5a;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Bestellnummer</div>
    <div style="font-size:18px;font-weight:bold;color:#2a4a1a;margin-bottom:12px">${orderRef}</div>
    <div style="font-size:11px;color:#6b6b5a;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Rechnungsnummer</div>
    <div style="font-size:14px;color:#2a4a1a">${invoice.number}</div>
  </div>

  <table style="width:100%;border-collapse:collapse;margin:20px 0;font-size:13px">
    <thead>
      <tr style="background:#2a4a1a;color:white">
        <th style="padding:10px 12px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:1px">Artikel</th>
        <th style="padding:10px 12px;text-align:center;font-size:11px;text-transform:uppercase;letter-spacing:1px">Menge</th>
        <th style="padding:10px 12px;text-align:right;font-size:11px;text-transform:uppercase;letter-spacing:1px">Preis</th>
      </tr>
    </thead>
    <tbody>${itemsRows}</tbody>
    <tfoot>
      <tr>
        <td colspan="2" style="padding:14px 12px;text-align:right;font-weight:bold;background:#faf7f0">Gesamtbetrag:</td>
        <td style="padding:14px 12px;text-align:right;font-weight:bold;background:#faf7f0;color:#2a4a1a;font-size:16px">€ ${total}</td>
      </tr>
    </tfoot>
  </table>

  <div style="text-align:center;margin:32px 0">
    <a href="${dlUrl}" style="display:inline-block;background:#3d6b2a;color:white;padding:14px 28px;border-radius:6px;text-decoration:none;font-weight:500;font-size:14px">📄 Rechnung als PDF herunterladen</a>
    <div style="font-size:11px;color:#6b6b5a;margin-top:10px">Der Link ist jederzeit aufrufbar. Die Rechnung liegt auch als PDF im Anhang.</div>
  </div>

  <div style="background:#faf7f0;border-radius:7px;padding:14px 18px;margin:20px 0;font-size:13px;text-align:center">
    <div style="color:#6b6b5a;margin-bottom:6px">💡 Bestellungen online verwalten</div>
    <a href="https://keby.shop/meine-bestellungen.html?email=${encodeURIComponent(customerEmail)}&order=${encodeURIComponent(orderRef)}" style="color:#3d6b2a;font-weight:500;text-decoration:none">Meine Bestellungen ansehen →</a>
  </div>

  <div style="border-top:1px solid #e8e3d6;padding-top:20px;margin-top:32px;font-size:13px;color:#6b6b5a">
    <p>Bei Fragen erreichen Sie uns gerne:</p>
    <p style="margin:6px 0">
      📧 <a href="mailto:info@keby.shop" style="color:#3d6b2a">info@keby.shop</a><br>
      📞 +49 159 06342354
    </p>
    <p style="margin-top:20px">Mit besten Grüßen<br><strong style="color:#2a4a1a">Kader Kayihan</strong><br>Keby — Premium Olivenöl<br><a href="https://keby.shop" style="color:#3d6b2a">keby.shop</a></p>
  </div>
</div>`;

                const resendBody = {
                  from: "Keby <noreply@machbar24.com>",
                  reply_to: "info@keby.shop",
                  to: [customerEmail],
                  bcc: ["info@keby.shop"],
                  subject,
                  text: textBody,
                  html: htmlBody
                };
                if (pdfAttachment) resendBody.attachments = [pdfAttachment];

                await fetch("https://api.resend.com/emails", {
                  method: "POST",
                  headers: {
                    Authorization: "Bearer " + env.RESEND_API_KEY,
                    "Content-Type": "application/json"
                  },
                  body: JSON.stringify(resendBody)
                });
              } catch (mailErr) {
                // Mail başarısız olsa bile webhook OK döndür (sipariş kaydedildi)
                console.log("Onay maili hatası:", mailErr.message);
              }
            }
          }
        }
        return new Response("ok", { headers: CORS });
      } catch (e) {
        return new Response("Error: " + e.message, { status: 500, headers: CORS });
      }
    }

    // Eski Stripe siparişlerini retroaktif zenginleştir
    if (request.method === "POST" && path === "/api/admin/orders/enrich-stripe" && isAdmin) {
      try {
        const orders = await getOrders(env);
        let enriched = 0;
        let attempted = 0;
        for (const o of orders) {
          if (o.payment !== "stripe" || !o.stripePaymentId) continue;

          // Eksik alan var mı? Hepsi doluysa atla
          const missing = !o.email || !o.address || !o.name || o.name === "Stripe Müşteri";
          if (!missing) continue;

          attempted++;

          try {
            // Hem charge hem payment_intent + customer'dan veri topla
            const chRes = await fetch(
              "https://api.stripe.com/v1/charges?payment_intent=" + o.stripePaymentId + "&limit=1&expand[]=data.customer",
              { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
            );
            const chData = await chRes.json();
            const ch = chData.data && chData.data[0];
            if (!ch) continue;

            // Payment intent'i de çek
            let pintent = null;
            try {
              const piRes = await fetch(
                "https://api.stripe.com/v1/payment_intents/" + o.stripePaymentId,
                { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
              );
              pintent = await piRes.json();
            } catch (e) {}

            const cust = (typeof ch.customer === 'object' && ch.customer) ? ch.customer : null;
            const billing = ch.billing_details || {};
            const shipping = ch.shipping || (pintent && pintent.shipping) || (cust && cust.shipping) || null;
            const addr = (shipping && shipping.address) || billing.address || (cust && cust.address) || {};

            const liveName = (shipping && shipping.name) || billing.name || (cust && cust.name);
            const liveEmail = billing.email || (cust && cust.email) || ch.receipt_email || (pintent && pintent.receipt_email);
            const livePhone = billing.phone || (shipping && shipping.phone) || (cust && cust.phone);

            const addrTxt = [
              addr.line1,
              addr.line2,
              [addr.postal_code, addr.city].filter(Boolean).join(' '),
              addr.state,
              addr.country
            ].filter(Boolean).join(', ');

            let changed = false;
            if (liveName && (!o.name || o.name === "Stripe Müşteri")) { o.name = liveName; changed = true; }
            if (liveEmail && !o.email) { o.email = liveEmail; changed = true; }
            if (livePhone && !o.phone) { o.phone = livePhone; changed = true; }
            if (addrTxt && !o.address) {
              o.address = addrTxt;
              o.addressDetail = {
                line1: addr.line1 || "",
                line2: addr.line2 || "",
                city: addr.city || "",
                postal_code: addr.postal_code || "",
                state: addr.state || "",
                country: addr.country || ""
              };
              changed = true;
            }

            // Stripe meta alanları (UI için)
            if (!o.stripeMethod && ch.payment_method_details) { o.stripeMethod = ch.payment_method_details.type; changed = true; }
            if (!o.stripeStatus) { o.stripeStatus = ch.status; changed = true; }
            if (!o.stripeReceiptUrl && ch.receipt_url) { o.stripeReceiptUrl = ch.receipt_url; changed = true; }
            if (!o.stripeChargeId) { o.stripeChargeId = ch.id; changed = true; }

            if (changed) enriched++;
          } catch (e) {
            // bu siparişi atla
          }
        }
        if (enriched > 0) await putOrders(env, orders);
        return jsonResp({
          success: true,
          enriched,
          attempted,
          total_stripe: orders.filter(o => o.payment === "stripe").length,
          note: enriched < attempted ? "Bazı Stripe ödemelerinde müşteri verisi yok (Apple Pay/Link/hızlı ödeme)" : null
        });
      } catch (e) {
        return errResp("Enrich hatası: " + e.message);
      }
    }

    // ============ INVOICE / RECHNUNG ============
    // Generate or retrieve invoice for an order (admin only) — /email path'i hariç (aşağıda)
    if (request.method === "POST" && path.startsWith("/api/admin/invoice/") && !path.endsWith("/email") && isAdmin) {
      try {
        const orderId = decodeURIComponent(path.replace("/api/admin/invoice/", ""));
        const orders = await getOrders(env);
        const order = orders.find(o => o.id === orderId);
        if (!order) return errResp("Sipariş bulunamadı", 404);

        // Mevcut faturayı kontrol et
        let invoice = await getInvoice(env, orderId);
        let isNew = false;

        if (!invoice) {
          // Yeni fatura — numara ata
          isNew = true;
          const number = await nextInvoiceNumber(env);
          const now = new Date();
          // Lieferdatum: sipariş tarihi
          const orderDate = order.date ? new Date(order.date) : now;

          // Müşteri verisi: önce DB, sonra Stripe (eğer Stripe siparişi ise canlı çek)
          let customer = {
            name: order.name || "",
            email: order.email || "",
            phone: order.phone || "",
            address: order.addressDetail || null,
            address_text: order.address || ""
          };

          // Stripe siparişlerinde canlı veri çek
          if (order.payment === "stripe" && order.stripePaymentId) {
            try {
              const chRes = await fetch(
                "https://api.stripe.com/v1/charges?payment_intent=" + order.stripePaymentId + "&limit=1&expand[]=data.customer",
                { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
              );
              const chData = await chRes.json();
              const ch = chData.data && chData.data[0];
              if (ch) {
                const billing = ch.billing_details || {};
                const shipping = ch.shipping;
                const cust = (typeof ch.customer === 'object' && ch.customer) ? ch.customer : null;
                const addr = (shipping && shipping.address) || billing.address || (cust && cust.address) || {};

                if (!customer.name || customer.name === "Stripe Müşteri") {
                  customer.name = (shipping && shipping.name) || billing.name || (cust && cust.name) || customer.name;
                }
                if (!customer.email) customer.email = billing.email || (cust && cust.email) || ch.receipt_email || "";
                if (!customer.phone) customer.phone = billing.phone || (shipping && shipping.phone) || (cust && cust.phone) || "";
                if (!customer.address || !customer.address.line1) {
                  customer.address = {
                    line1: addr.line1 || "",
                    line2: addr.line2 || "",
                    city: addr.city || "",
                    postal_code: addr.postal_code || "",
                    state: addr.state || "",
                    country: addr.country || ""
                  };
                  customer.address_text = [addr.line1, addr.line2, [addr.postal_code, addr.city].filter(Boolean).join(' '), addr.country].filter(Boolean).join(', ');
                }
              }
            } catch (e) { /* ignore */ }
          }

          // Items normalize
          let items = order.items;
          if (typeof items === 'string') {
            try { items = JSON.parse(items); } catch { items = []; }
          }
          if (!Array.isArray(items)) items = [];

          invoice = {
            number,
            order_id: orderId,
            order_ref: order.ref || orderId,
            issued_at: now.toISOString(),
            order_date: orderDate.toISOString(),
            payment: order.payment || "",
            payment_id: order.stripePaymentId || order.paypalTxId || "",
            customer,
            items: items.map(it => ({
              name: it.name || it.title || it.product || it.productId || "—",
              qty: parseFloat(it.qty || it.quantity || it.count || 1),
              price: parseFloat(it.price || it.amount || 0)
            })),
            subtotal: items.reduce((s, it) => s + (parseFloat(it.price||0) * parseFloat(it.qty || it.quantity || 1)), 0),
            shipping_fee: parseFloat(order.shipping_fee || 0),
            total: parseFloat(order.total || 0),
            currency: "EUR",
            // Kleinunternehmer
            tax_mode: "kleinunternehmer",
            tax_note: "Gemäß § 19 UStG wird keine Umsatzsteuer ausgewiesen."
          };

          await putInvoice(env, orderId, invoice);

          // Sipariş kaydına da invoice numarasını yaz (hızlı erişim için)
          order.invoice_number = number;
          order.invoice_issued_at = invoice.issued_at;
          await putOrders(env, orders);
        }

        return jsonResp({ success: true, invoice, is_new: isNew });
      } catch (e) {
        return errResp("Fatura hatası: " + e.message);
      }
    }

    // Public invoice page (HTML) — paylaşılabilir link
    // Admin için JSON formatında fatura detayı (modal için)
    if (request.method === "GET" && path.startsWith("/api/admin/invoice/") && !path.endsWith("/email") && isAdmin) {
      try {
        const orderId = decodeURIComponent(path.replace("/api/admin/invoice/", ""));
        const invoice = await getInvoice(env, orderId);
        if (!invoice) return errResp("Fatura bulunamadı", 404);
        return jsonResp({ success: true, invoice });
      } catch (e) {
        return errResp(e.message);
      }
    }

    if (request.method === "GET" && path.startsWith("/api/invoice/") && !path.endsWith("/pdf-html") && !path.endsWith("/pdf") && !path.includes("/download/")) {
      try {
        const orderId = decodeURIComponent(path.replace("/api/invoice/", ""));
        const invoice = await getInvoice(env, orderId);
        if (!invoice) {
          return new Response("Fatura bulunamadı", { status: 404, headers: { ...CORS, "Content-Type": "text/plain" } });
        }
        const html = renderInvoiceHTML(invoice);
        return new Response(html, {
          headers: { ...CORS, "Content-Type": "text/html; charset=utf-8" }
        });
      } catch (e) {
        return new Response("Hata: " + e.message, { status: 500, headers: CORS });
      }
    }

    // PDF-friendly version (no @page, no external fonts, no actions bar)
    if (request.method === "GET" && path.startsWith("/api/invoice/") && path.endsWith("/pdf-html")) {
      try {
        const orderId = decodeURIComponent(path.replace("/api/invoice/", "").replace("/pdf-html", ""));
        const invoice = await getInvoice(env, orderId);
        if (!invoice) {
          return new Response("Fatura bulunamadı", { status: 404, headers: { ...CORS, "Content-Type": "text/plain" } });
        }
        const html = renderInvoicePDFHTML(invoice);
        return new Response(html, {
          headers: { ...CORS, "Content-Type": "text/html; charset=utf-8" }
        });
      } catch (e) {
        return new Response("Hata: " + e.message, { status: 500, headers: CORS });
      }
    }

    // PUBLIC: Müşteri PDF indirme — token ile korumalı, lazy generation
    // GET /api/invoice/:orderId/download/:token
    if (request.method === "GET" && path.startsWith("/api/invoice/") && path.includes("/download/")) {
      try {
        const m = path.match(/^\/api\/invoice\/(.+)\/download\/([a-f0-9]+)$/);
        if (!m) return new Response("Ungültiger Link", { status: 400, headers: { ...CORS, "Content-Type": "text/plain; charset=utf-8" } });
        const orderId = decodeURIComponent(m[1]);
        const token = m[2];

        // Sipariş bul → email çek → token doğrula
        const orders = await getOrders(env);
        const order = orders.find(o => o.id === orderId);
        if (!order) return new Response("Bestellung nicht gefunden", { status: 404, headers: { ...CORS, "Content-Type": "text/plain; charset=utf-8" } });

        // Email birden fazla yerden gelebilir — sırayla dene
        let candidateEmails = [order.email].filter(Boolean);

        // Mevcut faturadan email
        const existingInv = await getInvoice(env, orderId);
        if (existingInv && existingInv.customer && existingInv.customer.email) {
          candidateEmails.push(existingInv.customer.email);
        }

        // Stripe siparişiyse Stripe'tan da email çekebiliriz
        if (candidateEmails.length === 0 && order.payment === "stripe" && order.stripePaymentId && env.STRIPE_SECRET_KEY) {
          try {
            const chRes = await fetch(
              "https://api.stripe.com/v1/charges?payment_intent=" + order.stripePaymentId + "&limit=1",
              { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
            );
            const chData = await chRes.json();
            const ch = chData.data && chData.data[0];
            if (ch) {
              const billing = ch.billing_details || {};
              const email = billing.email || ch.receipt_email;
              if (email) candidateEmails.push(email);
            }
          } catch (e) { /* ignore */ }
        }

        // Token eşleşen var mı?
        let validEmail = null;
        for (const em of candidateEmails) {
          const expected = await makeInvoiceToken(env, orderId, em);
          if (expected === token) { validEmail = em; break; }
        }
        if (!validEmail) {
          return new Response("Zugriff verweigert — ungültiger Token", { status: 403, headers: { ...CORS, "Content-Type": "text/plain; charset=utf-8" } });
        }

        // Fatura getir veya oluştur
        const result = await getOrCreateInvoice(env, orderId);
        if (!result.invoice) return new Response(result.error || "Rechnung konnte nicht erstellt werden", { status: 500, headers: { ...CORS, "Content-Type": "text/plain; charset=utf-8" } });

        // PDF üret
        const pdfBuffer = await generateInvoicePDF(env, result.invoice);

        return new Response(pdfBuffer, {
          headers: {
            ...CORS,
            "Content-Type": "application/pdf",
            "Content-Disposition": `inline; filename="Rechnung_${result.invoice.number}.pdf"`,
            "Cache-Control": "private, max-age=3600"
          }
        });
      } catch (e) {
        return new Response("Fehler: " + e.message, { status: 500, headers: { ...CORS, "Content-Type": "text/plain; charset=utf-8" } });
      }
    }

    // PUBLIC: Müşteri sipariş arama (light login — email + order_ref)
    // Rate-limited: IP başı 10/5dk
    if (request.method === "POST" && path === "/api/customer/lookup") {
      try {
        const body = await request.json();
        const email = (body.email || "").trim().toLowerCase();
        const orderRef = (body.order_ref || body.order_no || "").trim().toUpperCase();

        if (!email || !orderRef) return errResp("E-Mail und Bestellnummer erforderlich");
        if (!email.includes("@")) return errResp("Ungültige E-Mail");

        // Orders listesinden ara
        const orders = await getOrders(env);
        const norm = (s) => (s||"").trim().toLowerCase();
        const normRef = (s) => (s||"").trim().toUpperCase();

        // Eşleşme: ref eşleşir (KEBY-XXXXXX) + email eşleşir
        let order = orders.find(o => normRef(o.ref) === orderRef && norm(o.email) === email);

        // Fallback: Stripe siparişlerinde email DB'de yoksa, Stripe'tan çek
        if (!order) {
          const candidate = orders.find(o => normRef(o.ref) === orderRef);
          if (candidate && candidate.payment === "stripe" && candidate.stripePaymentId && env.STRIPE_SECRET_KEY) {
            try {
              const chRes = await fetch(
                "https://api.stripe.com/v1/charges?payment_intent=" + candidate.stripePaymentId + "&limit=1",
                { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
              );
              const chData = await chRes.json();
              const ch = chData.data && chData.data[0];
              if (ch) {
                const stripeEmail = (ch.billing_details?.email || ch.receipt_email || "").toLowerCase();
                if (stripeEmail === email) {
                  order = candidate;
                }
              }
            } catch (e) { /* ignore */ }
          }
        }

        if (!order) {
          // Brute force koruması — her zaman aynı mesaj
          return new Response(JSON.stringify({
            success: false,
            error: "Keine Bestellung gefunden mit dieser E-Mail und Bestellnummer"
          }), { status: 404, headers: { ...CORS, "Content-Type": "application/json" } });
        }

        // Fatura var mı?
        const invoice = await getInvoice(env, order.id);
        let downloadUrl = null;
        let invoiceNumber = null;
        if (invoice) {
          const dlToken = await makeInvoiceToken(env, order.id, email);
          downloadUrl = "https://keby-api.hguencavdi.workers.dev/api/invoice/" + encodeURIComponent(order.id) + "/download/" + dlToken;
          invoiceNumber = invoice.number;
        }

        // Items'ı parse et
        let items = order.items;
        if (typeof items === 'string') {
          try { items = JSON.parse(items); } catch { items = []; }
        }

        // Müşteriye güvenli veri (sadece kendi bilgileri)
        return jsonResp({
          success: true,
          order: {
            ref: order.ref,
            date: order.date,
            status: order.status,
            payment: order.payment,
            total: order.total,
            name: order.name,
            email: order.email,
            address: order.address,
            items: items || [],
            has_invoice: !!invoice,
            invoice_number: invoiceNumber,
            invoice_download_url: downloadUrl
          }
        });
      } catch (e) {
        return errResp("Fehler: " + e.message);
      }
    }

    // PUBLIC: Token üret (frontend success sayfası için)
    // POST /api/invoice-link  body: { order_id, email }
    if (request.method === "POST" && path === "/api/invoice-link") {
      try {
        const body = await request.json();
        const orderId = body.order_id || body.orderId;
        const email = body.email;
        if (!orderId || !email) return errResp("order_id ve email gerekli");

        // Sipariş kontrol
        const orders = await getOrders(env);
        const order = orders.find(o => o.id === orderId);
        if (!order) return errResp("Sipariş bulunamadı", 404);

        // Email eşleşmesi (siparişteki email VEYA Stripe billing email)
        let matchEmail = null;
        const norm = (s) => (s||"").trim().toLowerCase();
        if (order.email && norm(order.email) === norm(email)) matchEmail = order.email;

        if (!matchEmail && order.payment === "stripe" && order.stripePaymentId && env.STRIPE_SECRET_KEY) {
          try {
            const chRes = await fetch(
              "https://api.stripe.com/v1/charges?payment_intent=" + order.stripePaymentId + "&limit=1",
              { headers: { Authorization: "Bearer " + env.STRIPE_SECRET_KEY } }
            );
            const chData = await chRes.json();
            const ch = chData.data && chData.data[0];
            if (ch) {
              const billing = ch.billing_details || {};
              const stripeEmail = billing.email || ch.receipt_email;
              if (stripeEmail && norm(stripeEmail) === norm(email)) matchEmail = stripeEmail;
            }
          } catch (e) { /* ignore */ }
        }

        if (!matchEmail) return errResp("E-Mail-Adresse stimmt nicht überein", 403);

        const token = await makeInvoiceToken(env, orderId, matchEmail);
        const downloadUrl = "https://keby-api.hguencavdi.workers.dev/api/invoice/" + encodeURIComponent(orderId) + "/download/" + token;

        return jsonResp({ success: true, download_url: downloadUrl, token });
      } catch (e) {
        return errResp("Hata: " + e.message);
      }
    }

    // Email gönder PDF eki ile (admin) - PDF Browser Rendering ile sunucu tarafında üretilir
    if (request.method === "POST" && path.startsWith("/api/admin/invoice/") && path.endsWith("/email") && isAdmin) {
      try {
        const orderId = decodeURIComponent(path.replace("/api/admin/invoice/", "").replace("/email", ""));
        const invoice = await getInvoice(env, orderId);
        if (!invoice) return errResp("Fatura bulunamadı", 404);

        const body = await request.json();
        const customSubject = body.subject;
        const customMessage = body.message;
        const recipient = body.recipient || (invoice.customer && invoice.customer.email);

        if (!recipient) return errResp("Alıcı e-posta yok");
        if (!env.RESEND_API_KEY) return errResp("Resend API key yapılandırılmamış");
        if (!env.BROWSER_RENDERING_TOKEN) return errResp("Browser Rendering token yapılandırılmamış");

        // 1) PDF'i Cloudflare Browser Rendering API ile üret (helper kullan — kompakt margin)
        let pdfBuffer;
        try {
          pdfBuffer = await generateInvoicePDF(env, invoice);
        } catch (e) {
          return errResp("PDF üretim hatası: " + e.message);
        }

        // ArrayBuffer → base64
        const bytes = new Uint8Array(pdfBuffer);
        let binary = "";
        const chunkSize = 8192;
        for (let i = 0; i < bytes.length; i += chunkSize) {
          binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize));
        }
        const pdfBase64 = btoa(binary);

        // 2) Resend ile mail gönder
        const subject = customSubject || `Ihre Rechnung ${invoice.number} – Keby`;
        const customerName = (invoice.customer && invoice.customer.name) || "Kundin/Kunde";
        const message = customMessage || `Liebe/r ${customerName},

vielen Dank für Ihre Bestellung bei Keby!

Im Anhang finden Sie Ihre Rechnung Nr. ${invoice.number} als PDF.

Bei Fragen erreichen Sie uns jederzeit unter info@keby.shop oder +49 159 06342354.

Mit besten Grüßen
Kader Kayihan
Keby — Premium Olivenöl
https://keby.shop`;

        const messageHtml = message.replace(/\n/g, '<br>');

        const resendBody = {
          from: "Keby <noreply@machbar24.com>",
          reply_to: "info@keby.shop",
          to: [recipient],
          bcc: ["info@keby.shop"],
          subject,
          text: message,
          html: `<div style="font-family:Inter,Arial,sans-serif;color:#2a2a22;line-height:1.6;max-width:560px">${messageHtml}</div>`,
          attachments: [
            {
              filename: `Rechnung_${invoice.number}.pdf`,
              content: pdfBase64
            }
          ]
        };

        const res = await fetch("https://api.resend.com/emails", {
          method: "POST",
          headers: {
            Authorization: "Bearer " + env.RESEND_API_KEY,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(resendBody)
        });
        const data = await res.json();
        if (!res.ok || data.error) {
          return errResp("Resend: " + (data.error?.message || data.message || JSON.stringify(data)));
        }

        // Sipariş kaydına email log ekle
        const orders = await getOrders(env);
        const order = orders.find(o => o.id === orderId);
        if (order) {
          if (!order.invoice_emails) order.invoice_emails = [];
          order.invoice_emails.push({
            recipient,
            sent_at: new Date().toISOString(),
            email_id: data.id,
            pdf_size: pdfBuffer.byteLength
          });
          await putOrders(env, orders);
        }

        return jsonResp({
          success: true,
          email_id: data.id,
          recipient,
          pdf_size: pdfBuffer.byteLength
        });
      } catch (e) {
        return errResp("E-mail hatası: " + e.message);
      }
    }

    // ============ PAYPAL ============
    if (request.method === "POST" && path === "/api/paypal/capture") {
      try {
        const { orderID } = await request.json();
        const authRes = await fetch(PAYPAL_API + "/v1/oauth2/token", {
          method: "POST",
          headers: {
            Authorization: "Basic " + btoa(env.PAYPAL_CLIENT_ID + ":" + env.PAYPAL_SECRET),
            "Content-Type": "application/x-www-form-urlencoded"
          },
          body: "grant_type=client_credentials"
        });
        const authData = await authRes.json();
        const captureRes = await fetch(PAYPAL_API + "/v2/checkout/orders/" + orderID + "/capture", {
          method: "POST",
          headers: {
            Authorization: "Bearer " + authData.access_token,
            "Content-Type": "application/json"
          }
        });
        const result = await captureRes.json();
        if (result.status === "COMPLETED") {
          const orders = await getOrders(env);
          if (!orders.find(o => o.paypalOrderId === orderID)) {
            orders.unshift({
              id: "ord_pp_" + orderID,
              ref: "KEBY-" + orderID.slice(-6).toUpperCase(),
              name: result.payer?.name?.given_name + " " + (result.payer?.name?.surname || ""),
              email: result.payer?.email_address || "",
              payment: "paypal",
              paypalOrderId: orderID,
              status: "onaylandi",
              items: [],
              total: parseFloat(result.purchase_units?.[0]?.payments?.captures?.[0]?.amount?.value || 0),
              date: new Date().toISOString()
            });
            await putOrders(env, orders);
          }
        }
        return jsonResp({ success: true, result });
      } catch (e) {
        return errResp(e.message, 500);
      }
    }

    // ============ DEFAULT ============
    return new Response("Keby API v4", { status: 200, headers: CORS });
  }
};

// ============ INVOICE HTML TEMPLATE (Kleinunternehmer §19 UStG) ============
function renderInvoiceHTML(inv) {
  const c = inv.customer || {};
  const addr = c.address || {};
  const addrLines = [
    c.name,
    addr.line1,
    addr.line2,
    [addr.postal_code, addr.city].filter(Boolean).join(' '),
    addr.country && addr.country !== 'DE' ? addr.country : null
  ].filter(Boolean);

  const issuedDate = new Date(inv.issued_at);
  const orderDate = new Date(inv.order_date);
  const fmtDate = (d) => d.toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit', year: 'numeric' });

  const items = inv.items || [];
  const subtotal = items.reduce((s, it) => s + (it.price * it.qty), 0);
  const total = inv.total || subtotal;
  const shipping = inv.shipping_fee || 0;

  const paymentLabels = {
    stripe: 'Kreditkarte (Stripe)',
    paypal: 'PayPal',
    transfer: 'Banküberweisung',
    invoice: 'Rechnung'
  };
  const paymentLabel = paymentLabels[inv.payment] || inv.payment || 'Bezahlt';

  const escape = (s) => String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

  return `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Rechnung ${escape(inv.number)} – Keby</title>
<meta name="robots" content="noindex,nofollow">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --olive:#3d6b2a;--olive-dark:#2a4a1a;--gold:#c4a24a;
  --text:#2a2a22;--muted:#6b6b5a;--border:#e8e3d6;--cream:#faf7f0;--white:#fff;
}
body{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:var(--text);background:#f5f1ea;-webkit-print-color-adjust:exact;print-color-adjust:exact}

.actions{position:sticky;top:0;background:var(--olive-dark);color:white;padding:0.7rem 1rem;display:flex;justify-content:space-between;align-items:center;z-index:100;box-shadow:0 2px 8px rgba(0,0,0,0.15)}
.actions h2{font-size:0.95rem;font-weight:500;margin:0}
.actions .btns{display:flex;gap:0.5rem;flex-wrap:wrap}
.actions button,.actions a{background:white;color:var(--olive-dark);border:none;padding:0.45rem 0.9rem;border-radius:5px;cursor:pointer;font-size:0.82rem;font-weight:500;text-decoration:none;display:inline-flex;align-items:center;gap:0.3rem;transition:transform 0.1s}
.actions button:hover,.actions a:hover{transform:translateY(-1px)}

.page{max-width:800px;margin:1.5rem auto;background:white;padding:3rem 3.5rem;box-shadow:0 4px 20px rgba(0,0,0,0.08);border-radius:4px;min-height:1100px}

/* Header */
.header{display:flex;justify-content:space-between;align-items:flex-start;border-bottom:2px solid var(--olive);padding-bottom:1.5rem;margin-bottom:2rem}
.brand{display:flex;align-items:center;gap:0.8rem}
.brand img{width:64px;height:64px;object-fit:contain}
.brand-name{font-family:'Cormorant Garamond',Georgia,serif;font-size:1.8rem;font-weight:600;color:var(--olive-dark);letter-spacing:0.02em;line-height:1}
.brand-tag{color:var(--muted);font-size:0.78rem;letter-spacing:0.1em;text-transform:uppercase;margin-top:0.2rem}
.invoice-info{text-align:right;font-size:0.85rem;color:var(--muted);line-height:1.7}
.invoice-info strong{color:var(--olive-dark);font-size:0.9rem}

/* Title */
h1{font-family:'Cormorant Garamond',Georgia,serif;font-size:2.1rem;font-weight:400;color:var(--olive-dark);margin-bottom:0.3rem}
.invoice-meta{display:grid;grid-template-columns:1fr 1fr;gap:2rem;margin-bottom:2rem;font-size:0.85rem}
.meta-block{padding:0.8rem 1rem;background:var(--cream);border-left:3px solid var(--olive);border-radius:0 4px 4px 0}
.meta-label{color:var(--muted);font-size:0.7rem;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:0.3rem}
.meta-value{color:var(--text);font-weight:500;line-height:1.5}

/* Addresses */
.addresses{display:grid;grid-template-columns:1fr 1fr;gap:2rem;margin-bottom:2rem;font-size:0.85rem}
.addr-block .addr-label{color:var(--muted);font-size:0.7rem;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:0.5rem;font-weight:500}
.addr-lines{line-height:1.6}
.addr-lines .name{font-weight:600;color:var(--olive-dark);margin-bottom:0.2rem}

/* Table */
table{width:100%;border-collapse:collapse;margin-bottom:1.5rem;font-size:0.88rem}
thead{background:var(--olive-dark);color:white}
thead th{padding:0.7rem 0.6rem;text-align:left;font-weight:500;font-size:0.78rem;letter-spacing:0.05em;text-transform:uppercase}
thead th.num{text-align:right}
thead th.center{text-align:center}
tbody td{padding:0.8rem 0.6rem;border-bottom:1px solid var(--border)}
tbody td.num{text-align:right;font-variant-numeric:tabular-nums}
tbody td.center{text-align:center}
tbody tr:last-child td{border-bottom:1px solid var(--olive)}
.item-name{font-weight:500;color:var(--olive-dark)}

/* Totals */
.totals{margin-left:auto;width:280px;font-size:0.9rem}
.totals .row{display:flex;justify-content:space-between;padding:0.5rem 0.8rem;border-bottom:1px solid var(--border)}
.totals .row.grand{background:var(--olive-dark);color:white;font-weight:600;font-size:1rem;border-radius:4px;margin-top:0.4rem;border:none;padding:0.8rem 0.8rem}
.totals .lbl{color:var(--muted)}
.totals .grand .lbl{color:white}
.totals .val{font-weight:500;font-variant-numeric:tabular-nums}

/* Tax notice (Kleinunternehmer) */
.tax-notice{background:#fff8e7;border-left:3px solid var(--gold);padding:0.8rem 1rem;margin:1.5rem 0;font-size:0.82rem;color:#5a4a1a;border-radius:0 4px 4px 0}

/* Payment + footer */
.payment-info{margin:1.5rem 0;padding:0.8rem 1rem;background:var(--cream);border-radius:4px;font-size:0.85rem}
.payment-info strong{color:var(--olive-dark)}

footer.invoice-footer{margin-top:3rem;padding-top:1.5rem;border-top:1px solid var(--border);font-size:0.72rem;color:var(--muted);line-height:1.7}
footer.invoice-footer .grid{display:grid;grid-template-columns:repeat(3,1fr);gap:1.5rem}
footer.invoice-footer strong{color:var(--olive-dark);font-size:0.78rem;display:block;margin-bottom:0.3rem;text-transform:uppercase;letter-spacing:0.05em}
footer.invoice-footer a{color:var(--olive)}

.thank-you{text-align:center;font-family:'Cormorant Garamond',Georgia,serif;font-style:italic;color:var(--olive-dark);font-size:1.15rem;margin:2rem 0 1rem}

/* Print */
@media print{
  body{background:white}
  .actions{display:none!important}
  .page{box-shadow:none;margin:0;padding:1.5cm 2cm;max-width:none;border-radius:0}
  @page{size:A4;margin:0}
}
@media (max-width:700px){
  .page{padding:1.5rem 1.2rem;margin:0.5rem;border-radius:6px;min-height:auto}
  .header{flex-direction:column;gap:1rem;align-items:stretch}
  .invoice-info{text-align:left}
  .invoice-meta,.addresses{grid-template-columns:1fr;gap:1rem}
  footer.invoice-footer .grid{grid-template-columns:1fr;gap:1rem}
  .totals{width:100%}
  table{font-size:0.8rem}
  thead th,tbody td{padding:0.5rem 0.4rem}
}
</style>
</head>
<body>

<div class="actions">
  <h2>Rechnung ${escape(inv.number)}</h2>
  <div class="btns">
    <button onclick="window.print()">🖨 Drucken / PDF</button>
    <a href="mailto:${escape(c.email)}?subject=${encodeURIComponent('Ihre Rechnung ' + inv.number + ' – Keby')}&body=${encodeURIComponent('Liebe Kundin, lieber Kunde,\\n\\nim Anhang erhalten Sie Ihre Rechnung Nr. ' + inv.number + '.\\n\\nVielen Dank für Ihre Bestellung bei Keby!\\n\\nMit besten Grüßen,\\nKader Kayihan\\n\\nhttps://keby.shop/api/invoice/' + inv.order_id)}">📧 Per E-Mail senden</a>
  </div>
</div>

<div class="page">

  <div class="header">
    <div class="brand">
      <img src="https://keby-api.hguencavdi.workers.dev/img/keby/logo-new.png" alt="Keby">
      <div>
        <div class="brand-name">Keby</div>
        <div class="brand-tag">Premium Olivenöl</div>
      </div>
    </div>
    <div class="invoice-info">
      <div><strong>Rechnungs-Nr.</strong></div>
      <div style="font-size:1.05rem;color:var(--text);font-weight:500">${escape(inv.number)}</div>
      <div style="margin-top:0.5rem">Datum: ${fmtDate(issuedDate)}</div>
    </div>
  </div>

  <h1>Rechnung</h1>

  <div class="invoice-meta">
    <div class="meta-block">
      <div class="meta-label">Bestellnummer</div>
      <div class="meta-value">${escape(inv.order_ref)}</div>
    </div>
    <div class="meta-block">
      <div class="meta-label">Bestelldatum / Lieferdatum</div>
      <div class="meta-value">${fmtDate(orderDate)}</div>
    </div>
  </div>

  <div class="addresses">
    <div class="addr-block">
      <div class="addr-label">Verkäuferin</div>
      <div class="addr-lines">
        <div class="name">Keby — Kader Kayihan</div>
        <div>Stuttgarter Str. 65</div>
        <div>71665 Vaihingen an der Enz</div>
        <div>Deutschland</div>
        <div style="margin-top:0.5rem;color:var(--muted);font-size:0.8rem">
          info@keby.shop · +49 159 06342354<br>
          USt-IdNr: DE358426450
        </div>
      </div>
    </div>
    <div class="addr-block">
      <div class="addr-label">Rechnungsempfänger</div>
      <div class="addr-lines">
        ${addrLines.length > 0
          ? addrLines.map((l, i) => `<div${i===0 ? ' class="name"' : ''}>${escape(l)}</div>`).join('')
          : '<div style="color:var(--muted);font-style:italic">Keine Adressdaten</div>'}
        ${c.email ? `<div style="margin-top:0.5rem;color:var(--muted);font-size:0.8rem">${escape(c.email)}</div>` : ''}
      </div>
    </div>
  </div>

  <table>
    <thead>
      <tr>
        <th style="width:14%">Art-Nr</th>
        <th style="width:38%">Artikel</th>
        <th class="center" style="width:10%">Menge</th>
        <th class="num" style="width:19%">Einzelpreis</th>
        <th class="num" style="width:19%">Gesamt</th>
      </tr>
    </thead>
    <tbody>
      ${items.map(it => `<tr>
        <td style="font-family:monospace;color:#6b6b5a;font-size:0.88em">${escape(it.artikel_nr || '—')}</td>
        <td><div class="item-name">${escape(it.name)}</div></td>
        <td class="center">${it.qty}</td>
        <td class="num">€ ${it.price.toFixed(2).replace('.', ',')}</td>
        <td class="num">€ ${(it.price * it.qty).toFixed(2).replace('.', ',')}</td>
      </tr>`).join('')}
    </tbody>
  </table>

  <div class="totals">
    <div class="row">
      <span class="lbl">Zwischensumme</span>
      <span class="val">€ ${subtotal.toFixed(2).replace('.', ',')}</span>
    </div>
    ${shipping > 0 ? `<div class="row">
      <span class="lbl">Versand</span>
      <span class="val">€ ${shipping.toFixed(2).replace('.', ',')}</span>
    </div>` : ''}
    <div class="row grand">
      <span class="lbl">Gesamtbetrag</span>
      <span class="val">€ ${total.toFixed(2).replace('.', ',')}</span>
    </div>
  </div>

  <div class="tax-notice">
    <strong>Hinweis zur Umsatzsteuer:</strong> ${escape(inv.tax_note)}
  </div>

  <div class="payment-info">
    <strong>Zahlungsart:</strong> ${escape(paymentLabel)} — <span style="color:var(--olive)">✓ Bezahlt</span>
    ${inv.payment_id ? `<br><small style="color:var(--muted)">Transaktions-ID: <code style="font-size:0.78rem">${escape(inv.payment_id)}</code></small>` : ''}
  </div>

  <div class="thank-you">Vielen Dank für Ihre Bestellung!</div>

  <footer class="invoice-footer">
    <div class="grid">
      <div>
        <strong>Anschrift</strong>
        Keby — Kader Kayihan<br>
        Stuttgarter Str. 65<br>
        71665 Vaihingen an der Enz<br>
        Deutschland
      </div>
      <div>
        <strong>Kontakt</strong>
        <a href="mailto:info@keby.shop">info@keby.shop</a><br>
        Tel: +49 159 06342354<br>
        Web: <a href="https://keby.shop">keby.shop</a>
      </div>
      <div>
        <strong>Bankverbindung</strong>
        Inhaber: Kader Kayihan<br>
        IBAN: DE91 6045 0050 0030 2309 81<br>
        USt-IdNr: DE358426450
      </div>
    </div>
  </footer>
</div>

</body>
</html>`;
}

// ============ PDF-FRIENDLY INVOICE (html2canvas-compatible) ============
// No @page rule, no external fonts, basic CSS only
function renderInvoicePDFHTML(inv) {
  const c = inv.customer || {};
  const addr = c.address || {};
  const addrLines = [
    c.name,
    addr.line1,
    addr.line2,
    [addr.postal_code, addr.city].filter(Boolean).join(' '),
    addr.country && addr.country !== 'DE' ? addr.country : null
  ].filter(Boolean);

  const issuedDate = new Date(inv.issued_at);
  const orderDate = new Date(inv.order_date);
  const fmtDate = (d) => d.toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit', year: 'numeric' });

  const items = inv.items || [];
  const subtotal = items.reduce((s, it) => s + (it.price * it.qty), 0);
  const total = inv.total || subtotal;
  const shipping = inv.shipping_fee || 0;

  const paymentLabels = {
    stripe: 'Kreditkarte (Stripe)',
    paypal: 'PayPal',
    transfer: 'Banküberweisung',
    invoice: 'Rechnung'
  };
  const paymentLabel = paymentLabels[inv.payment] || inv.payment || 'Bezahlt';

  const escape = (s) => String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

  // INLINE CSS — no @page, no external fonts, no modern features
  return `<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<title>Rechnung ${escape(inv.number)}</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: Georgia, 'Times New Roman', serif;
  color: #2a2a22;
  background: white;
  padding: 0;
  font-size: 10pt;
  line-height: 1.35;
}
.page {
  width: 800px;
  background: white;
  padding: 28px 42px;
  margin: 0 auto;
}

/* Header */
.header {
  border-bottom: 1.5px solid #3d6b2a;
  padding-bottom: 12px;
  margin-bottom: 16px;
  overflow: hidden;
}
.brand { float: left; }
.brand-name {
  font-size: 22px;
  font-weight: bold;
  color: #2a4a1a;
  line-height: 1;
}
.brand-tag {
  color: #6b6b5a;
  font-size: 9px;
  letter-spacing: 1.5px;
  text-transform: uppercase;
  margin-top: 3px;
}
.invoice-info {
  float: right;
  text-align: right;
  font-size: 10px;
  color: #6b6b5a;
  line-height: 1.5;
}
.invoice-info strong {
  color: #2a4a1a;
  font-size: 11px;
}
.invoice-info .num {
  font-size: 14px;
  color: #2a2a22;
  font-weight: bold;
  display: block;
  margin: 1px 0;
}

/* Title */
h1 {
  font-size: 22px;
  font-weight: normal;
  color: #2a4a1a;
  margin-bottom: 2px;
  font-family: Georgia, serif;
}

/* Meta blocks */
.meta-row {
  overflow: hidden;
  margin-bottom: 12px;
}
.meta-block {
  float: left;
  width: 48%;
  padding: 6px 10px;
  background: #faf7f0;
  border-left: 2px solid #3d6b2a;
  margin-right: 4%;
  font-size: 10px;
}
.meta-block:last-child { margin-right: 0; }
.meta-label {
  color: #6b6b5a;
  font-size: 8px;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  margin-bottom: 2px;
}
.meta-value {
  color: #2a2a22;
  font-weight: bold;
}

/* Addresses */
.addr-row {
  overflow: hidden;
  margin-bottom: 14px;
}
.addr-block {
  float: left;
  width: 48%;
  margin-right: 4%;
  font-size: 10px;
  line-height: 1.45;
}
.addr-block:last-child { margin-right: 0; }
.addr-label {
  color: #6b6b5a;
  font-size: 8px;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  margin-bottom: 3px;
  font-weight: bold;
}
.addr-name {
  font-weight: bold;
  color: #2a4a1a;
  margin-bottom: 1px;
}
.addr-meta {
  margin-top: 3px;
  color: #6b6b5a;
  font-size: 9px;
}

/* Table */
table {
  width: 100%;
  border-collapse: collapse;
  margin-bottom: 10px;
  font-size: 10px;
}
thead th {
  background: #2a4a1a;
  color: white;
  padding: 5px 6px;
  text-align: left;
  font-weight: bold;
  font-size: 8.5px;
  letter-spacing: 0.6px;
  text-transform: uppercase;
}
thead th.num { text-align: right; }
thead th.center { text-align: center; }
tbody td {
  padding: 6px 6px;
  border-bottom: 1px solid #e8e3d6;
}
tbody td.num { text-align: right; }
tbody td.center { text-align: center; }
tbody tr:last-child td { border-bottom: 1px solid #3d6b2a; }
.item-name { font-weight: bold; color: #2a4a1a; }

/* Totals */
.totals-wrap { overflow: hidden; margin-bottom: 10px; }
.totals {
  float: right;
  width: 260px;
  font-size: 10px;
}
.totals .row {
  overflow: hidden;
  padding: 4px 8px;
  border-bottom: 1px solid #e8e3d6;
}
.totals .row.grand {
  background: #2a4a1a;
  color: white;
  font-weight: bold;
  font-size: 12px;
  margin-top: 2px;
  padding: 7px 10px;
}
.totals .lbl { float: left; color: #6b6b5a; }
.totals .grand .lbl { color: white; }
.totals .val {
  float: right;
  font-weight: bold;
}

/* Tax notice */
.tax-notice {
  background: #fff8e7;
  border-left: 2px solid #c4a24a;
  padding: 6px 10px;
  margin: 10px 0;
  font-size: 9px;
  color: #5a4a1a;
  clear: both;
}

/* Payment */
.payment-info {
  margin: 10px 0;
  padding: 6px 10px;
  background: #faf7f0;
  font-size: 10px;
}
.payment-info strong { color: #2a4a1a; }
.payment-info code {
  font-family: 'Courier New', monospace;
  font-size: 9px;
  background: white;
  padding: 1px 3px;
}

.thank-you {
  text-align: center;
  font-style: italic;
  color: #2a4a1a;
  font-size: 12px;
  margin: 12px 0 8px;
}

/* Footer */
footer {
  margin-top: 14px;
  padding-top: 10px;
  border-top: 1px solid #e8e3d6;
  font-size: 8.5px;
  color: #6b6b5a;
  line-height: 1.45;
  overflow: hidden;
}
footer .col {
  float: left;
  width: 32%;
  margin-right: 2%;
}
footer .col:last-child { margin-right: 0; }
footer .col strong {
  color: #2a4a1a;
  font-size: 8.5px;
  display: block;
  margin-bottom: 2px;
  text-transform: uppercase;
  letter-spacing: 0.8px;
}

.clearfix { clear: both; }
</style>
</head>
<body>
<div class="page">

  <div class="header">
    <div class="brand">
      <img src="https://keby-api.hguencavdi.workers.dev/img/keby/logo-new.png" alt="Keby" style="height:44px;display:block;margin-bottom:3px">
      <div class="brand-tag">Premium Olivenöl</div>
    </div>
    <div class="invoice-info">
      <strong>${inv.number === 'VORSCHAU' ? 'VORSCHAU' : 'Rechnungs-Nr.'}</strong>
      <span class="num">${escape(inv.number)}</span>
      Datum: ${fmtDate(issuedDate)}
    </div>
    <div class="clearfix"></div>
  </div>

  <h1>${inv.number === 'VORSCHAU' ? 'Rechnung (Vorschau)' : 'Rechnung'}</h1>
  <div style="height:10px"></div>

  <div class="meta-row">
    <div class="meta-block">
      <div class="meta-label">Bestellnummer</div>
      <div class="meta-value">${escape(inv.order_ref)}</div>
    </div>
    <div class="meta-block">
      <div class="meta-label">Bestelldatum / Lieferdatum</div>
      <div class="meta-value">${fmtDate(orderDate)}</div>
    </div>
  </div>

  <div class="addr-row">
    <div class="addr-block">
      <div class="addr-label">Verkäuferin</div>
      <div class="addr-name">Keby — Kader Kayihan</div>
      <div>Stuttgarter Str. 65</div>
      <div>71665 Vaihingen an der Enz</div>
      <div>Deutschland</div>
      <div class="addr-meta">
        info@keby.shop · +49 159 06342354<br>
        USt-IdNr: DE358426450
      </div>
    </div>
    <div class="addr-block">
      <div class="addr-label">Rechnungsempfänger</div>
      ${addrLines.length > 0
        ? addrLines.map((l, i) => `<div${i===0 ? ' class="addr-name"' : ''}>${escape(l)}</div>`).join('')
        : '<div style="color:#999;font-style:italic">Keine Adressdaten</div>'}
      ${c.email ? `<div class="addr-meta">${escape(c.email)}</div>` : ''}
    </div>
  </div>

  <table>
    <thead>
      <tr>
        <th style="width:14%">Art-Nr</th>
        <th style="width:38%">Artikel</th>
        <th class="center" style="width:10%">Menge</th>
        <th class="num" style="width:19%">Einzelpreis</th>
        <th class="num" style="width:19%">Gesamt</th>
      </tr>
    </thead>
    <tbody>
      ${items.map(it => `<tr>
        <td style="font-family:'Courier New',monospace;color:#6b6b5a;font-size:10pt">${escape(it.artikel_nr || '—')}</td>
        <td><span class="item-name">${escape(it.name)}</span></td>
        <td class="center">${it.qty}</td>
        <td class="num">€ ${it.price.toFixed(2).replace('.', ',')}</td>
        <td class="num">€ ${(it.price * it.qty).toFixed(2).replace('.', ',')}</td>
      </tr>`).join('')}
    </tbody>
  </table>

  <div class="totals-wrap">
    <div class="totals">
      <div class="row">
        <span class="lbl">Zwischensumme</span>
        <span class="val">€ ${subtotal.toFixed(2).replace('.', ',')}</span>
      </div>
      ${shipping > 0 ? `<div class="row">
        <span class="lbl">Versand</span>
        <span class="val">€ ${shipping.toFixed(2).replace('.', ',')}</span>
      </div>` : ''}
      <div class="row grand">
        <span class="lbl">Gesamtbetrag</span>
        <span class="val">€ ${total.toFixed(2).replace('.', ',')}</span>
      </div>
    </div>
  </div>

  <div class="tax-notice">
    <strong>Hinweis zur Umsatzsteuer:</strong> ${escape(inv.tax_note)}
  </div>

  <div class="payment-info">
    <strong>Zahlungsart:</strong> ${escape(paymentLabel)} — <span style="color:#3d6b2a">✓ Bezahlt</span>
    ${inv.payment_id ? `<br><span style="color:#6b6b5a;font-size:10px">Transaktions-ID: <code>${escape(inv.payment_id)}</code></span>` : ''}
  </div>

  <div class="thank-you">Vielen Dank für Ihre Bestellung!</div>

  <footer>
    <div class="col">
      <strong>Anschrift</strong>
      Keby — Kader Kayihan<br>
      Stuttgarter Str. 65<br>
      71665 Vaihingen an der Enz<br>
      Deutschland
    </div>
    <div class="col">
      <strong>Kontakt</strong>
      info@keby.shop<br>
      Tel: +49 159 06342354<br>
      Web: keby.shop
    </div>
    <div class="col">
      <strong>Bankverbindung</strong>
      Inhaber: Kader Kayihan<br>
      IBAN: DE91 6045 0050 0030 2309 81<br>
      USt-IdNr: DE358426450
    </div>
    <div class="clearfix"></div>
  </footer>

</div>
</body>
</html>`;
}

export { worker_default as default };
