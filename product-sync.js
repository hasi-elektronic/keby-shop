// Keby Product Sync — syncs product page content from admin products.json
// Reads <meta name="product-id"> and patches DOM with latest data
(function() {
  const API = 'https://keby-api.hguencavdi.workers.dev';
  const IMG = API + '/img/';
  const meta = document.querySelector('meta[name="product-id"]');
  if (!meta) return;
  const productId = meta.content;
  const lang = (document.documentElement.lang || 'tr').toLowerCase().startsWith('de') ? 'de' : 'tr';

  // Helper: get text for language with fallback
  function text(p, key) {
    return p[key + '_' + lang] || p[key + '_tr'] || p[key + '_de'] || '';
  }

  function patchDOM(p) {
    const imgs = (p.images || []).map(k => IMG + k);
    if (imgs.length === 0) return;

    // Update main gallery image
    const mainImg = document.getElementById('mainImg');
    if (mainImg) mainImg.src = imgs[0];

    // Update thumbnails
    const thumbWrap = document.querySelector('.gallery-thumbs');
    if (thumbWrap) {
      thumbWrap.innerHTML = imgs.map((url, i) => `
        <img class="gallery-thumb${i === 0 ? ' active' : ''}"
             src="${url}"
             onclick="setImg(this,'${url}')">
      `).join('');
    }

    // Update title
    const title = document.querySelector('.prod-title');
    if (title) {
      const tr = p.name_tr || '';
      const de = p.name_de || '';
      title.setAttribute('data-tr', tr);
      title.setAttribute('data-de', de);
      title.textContent = lang === 'de' ? de : tr;
    }

    // Update tagline (short desc)
    const tagline = document.querySelector('.prod-tagline');
    if (tagline && (p.short_tr || p.short_de)) {
      const tr = p.short_tr || '';
      const de = p.short_de || '';
      tagline.setAttribute('data-tr', tr);
      tagline.setAttribute('data-de', de);
      tagline.textContent = lang === 'de' ? de : tr;
    }

    // Update price
    const priceMain = document.querySelector('.price-main');
    if (priceMain && p.price) {
      const priceText = p.price.toFixed(2).replace('.', ',');
      const eurSpan = priceMain.querySelector('span');
      priceMain.textContent = priceText + ' ';
      if (eurSpan) priceMain.appendChild(eurSpan);
      else {
        const s = document.createElement('span');
        s.textContent = 'EUR';
        priceMain.appendChild(s);
      }
    }

    // Update Add-to-cart button: name, price, first image
    const addBtn = document.querySelector('.btn-add');
    if (addBtn && p.price) {
      const name = p.name_tr || p.id;
      addBtn.setAttribute('onclick',
        `addFromPage('${name.replace(/'/g, "\\'")}',${p.price},'${imgs[0]}')`);
    }

    // Update PayPal button
    const ppBtn = document.querySelector('.btn-pp');
    if (ppBtn && p.price) {
      ppBtn.setAttribute('onclick', `paypalDirect(${p.price})`);
    }

    // Update long description
    const descBody = document.querySelector('.desc-body, .desc-text, #desc-content');
    if (descBody && (p.desc_tr || p.desc_de)) {
      const html = lang === 'de' ? p.desc_de : p.desc_tr;
      if (html && html.length > 20) {
        // only replace if admin has real content (>20 chars)
        descBody.setAttribute('data-tr', p.desc_tr || '');
        descBody.setAttribute('data-de', p.desc_de || '');
        descBody.textContent = html;
      }
    }
  }

  // Fetch + patch
  fetch(API + '/api/products')
    .then(r => r.json())
    .then(d => {
      if (!d.success) return;
      const p = (d.products || []).find(x => x.id === productId);
      if (p) patchDOM(p);
    })
    .catch(err => console.warn('[product-sync]', err));
})();
