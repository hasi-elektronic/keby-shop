// Keby Index Sync — patches all product cards on homepage from products.json
// Finds <a class="prod-card" data-product-id="..."> and rewrites images + texts
(function() {
  const API = 'https://keby-api.hguencavdi.workers.dev';
  const IMG = API + '/img/';
  const lang = (document.documentElement.lang || 'tr').toLowerCase().startsWith('de') ? 'de' : 'tr';

  // Add cache-buster suffix (timestamp once per page load) to force-bypass any stale cache
  const V = '?v=' + Date.now();

  function patchCard(card, p) {
    const imgs = (p.images || []);
    if (imgs.length === 0) return;

    // Find carousel track — <div class="prod-track" id="pcN-track">
    const track = card.querySelector('.prod-track');
    if (track) {
      // Replace all images inside with the product's images
      track.innerHTML = imgs.map((key, i) =>
        `<img src="${IMG}${key}${V}" alt="${(p.name_tr||'').replace(/"/g,'&quot;')}" ` +
        `loading="${i === 0 ? 'eager' : 'lazy'}">`
      ).join('');
    }

    // Reset dots
    const trackId = track ? track.id : '';
    const dotsId = trackId.replace('-track', '-dots');
    const dots = document.getElementById(dotsId);
    if (dots) {
      dots.innerHTML = imgs.map((_, i) =>
        `<span${i === 0 ? ' class="active"' : ''}></span>`
      ).join('');
    }

    // Update title
    const title = card.querySelector('.prod-title');
    if (title) {
      title.setAttribute('data-tr', p.name_tr || '');
      title.setAttribute('data-de', p.name_de || '');
      title.textContent = lang === 'de' ? (p.name_de || p.name_tr) : (p.name_tr || p.name_de);
    }

    // Update short description
    const desc = card.querySelector('.prod-desc');
    if (desc && (p.short_tr || p.short_de)) {
      desc.setAttribute('data-tr', p.short_tr || '');
      desc.setAttribute('data-de', p.short_de || '');
      desc.textContent = lang === 'de' ? (p.short_de || p.short_tr) : (p.short_tr || p.short_de);
    }

    // Update price if present
    const priceEl = card.querySelector('.prod-price, .price, .price-main');
    if (priceEl && p.price) {
      const formatted = p.price.toFixed(2).replace('.', ',') + ' €';
      // Preserve any child span (like currency) — just replace text node
      const firstText = Array.from(priceEl.childNodes).find(n => n.nodeType === 3);
      if (firstText) firstText.nodeValue = formatted + ' ';
      else priceEl.textContent = formatted;
    }

    // Update add-to-cart button onclick
    const addBtn = card.querySelector('.btn-addcart, [onclick*="quickAdd"]');
    if (addBtn && p.price && imgs[0]) {
      const name = (p.name_tr || p.id).replace(/'/g, "\\'");
      addBtn.setAttribute('onclick',
        `event.preventDefault();event.stopPropagation();quickAdd('${name}',${p.price},'${IMG}${imgs[0]}${V}')`
      );
    }

    // Stock / sold-out handling
    const carousel = card.querySelector('.prod-carousel');
    const existingBadge = carousel ? carousel.querySelector('.prod-soldout-badge') : null;
    if ((p.stock || 0) === 0) {
      card.classList.add('is-soldout');
      if (carousel && !existingBadge) {
        const sb = document.createElement('span');
        sb.className = 'prod-soldout-badge';
        sb.setAttribute('data-tr', 'TÜKENDİ');
        sb.setAttribute('data-de', 'AUSVERKAUFT');
        sb.textContent = lang === 'de' ? 'AUSVERKAUFT' : 'TÜKENDİ';
        carousel.appendChild(sb);
      }
      if (addBtn) {
        addBtn.setAttribute('onclick', 'event.preventDefault();event.stopPropagation();');
        addBtn.classList.add('is-disabled');
        addBtn.setAttribute('data-tr', 'Tükendi');
        addBtn.setAttribute('data-de', 'Ausverkauft');
        addBtn.textContent = lang === 'de' ? 'Ausverkauft' : 'Tükendi';
      }
    } else {
      card.classList.remove('is-soldout');
      if (existingBadge) existingBadge.remove();
      if (addBtn) {
        addBtn.classList.remove('is-disabled');
        addBtn.setAttribute('data-tr', '+ Sepet');
        addBtn.setAttribute('data-de', '+ Warenkorb');
        addBtn.textContent = lang === 'de' ? '+ Warenkorb' : '+ Sepet';
      }
    }
  }

  async function run() {
    try {
      const r = await fetch(API + '/api/products' + V, { cache: 'no-store' });
      const d = await r.json();
      if (!d.success) return;
      document.querySelectorAll('[data-product-id]').forEach(card => {
        const id = card.dataset.productId;
        const p = d.products.find(x => x.id === id);
        if (p) patchCard(card, p);
      });
      // Re-init any carousel the page uses (if function exists)
      if (typeof initCarousels === 'function') initCarousels();
    } catch (err) {
      console.warn('[index-sync]', err);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', run);
  } else {
    run();
  }
})();
