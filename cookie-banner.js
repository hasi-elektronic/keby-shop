// Keby Cookie/DSGVO Info Banner
// Shows once, dismissible. No tracking cookies → only info, no consent needed.
// TDDDG §25: notwendig-only → Einwilligung nicht erforderlich, Information ausreichend.
(function() {
  const STORAGE_KEY = 'keby_cookie_info_dismissed';
  const lang = (document.documentElement.lang || 'tr').toLowerCase().startsWith('de') ? 'de' : 'tr';

  // Show only if not dismissed before
  if (localStorage.getItem(STORAGE_KEY) === '1') return;

  const texts = {
    tr: {
      title: '🍪 Teknik Çerezler',
      body: 'Keby sitesinde sadece <strong>teknik olarak gerekli</strong> depolama kullanılır: dil tercihi ve alışveriş sepeti. <strong>Takip çerezi, analitik veya reklam çerezi kullanmıyoruz.</strong>',
      details: 'Detaylar:',
      link: 'Gizlilik Politikası',
      ok: 'Anladım'
    },
    de: {
      title: '🍪 Technisch notwendige Speicherung',
      body: 'Auf keby.shop verwenden wir ausschließlich <strong>technisch notwendige Speicherung</strong> (Sprachauswahl, Warenkorb). <strong>Keine Tracking-Cookies, keine Analytics, keine Werbung.</strong>',
      details: 'Details:',
      link: 'Datenschutzerklärung',
      ok: 'Verstanden'
    }
  };
  const t = texts[lang];

  // Styles injected
  const style = document.createElement('style');
  style.textContent = `
    #keby-cookie-banner{
      position:fixed;bottom:1rem;left:1rem;right:1rem;max-width:520px;margin:0 auto;
      background:#fdfcf9;border:1px solid rgba(61,107,42,0.2);border-radius:10px;
      box-shadow:0 10px 40px rgba(42,74,26,0.15);padding:1.1rem 1.3rem;
      font-family:'Jost','Inter',system-ui,sans-serif;font-size:0.85rem;color:#2a2a22;
      z-index:9998;animation:kbc-in 0.35s ease-out;
    }
    @keyframes kbc-in{from{transform:translateY(20px);opacity:0}to{transform:translateY(0);opacity:1}}
    @keyframes kbc-out{to{transform:translateY(20px);opacity:0}}
    #keby-cookie-banner.hide{animation:kbc-out 0.25s ease-in forwards}
    #keby-cookie-banner .kbc-title{font-weight:500;margin-bottom:0.4rem;color:#2a4a1a;font-size:0.92rem}
    #keby-cookie-banner .kbc-body{line-height:1.55;color:#4a4a3a;margin-bottom:0.7rem}
    #keby-cookie-banner .kbc-body strong{color:#2a2a22;font-weight:500}
    #keby-cookie-banner .kbc-link{color:#3d6b2a;text-decoration:underline;font-size:0.8rem}
    #keby-cookie-banner .kbc-actions{display:flex;align-items:center;justify-content:space-between;gap:1rem;margin-top:0.6rem}
    #keby-cookie-banner .kbc-ok{background:#2a4a1a;color:white;border:none;padding:0.55rem 1.4rem;border-radius:6px;
      cursor:pointer;font-family:inherit;font-size:0.85rem;font-weight:500;transition:background 0.15s}
    #keby-cookie-banner .kbc-ok:hover{background:#3d6b2a}
    @media(max-width:500px){
      #keby-cookie-banner{left:0.75rem;right:0.75rem;bottom:0.75rem;padding:1rem}
      #keby-cookie-banner .kbc-actions{flex-direction:column-reverse;align-items:stretch}
      #keby-cookie-banner .kbc-ok{width:100%}
    }
  `;
  document.head.appendChild(style);

  const banner = document.createElement('div');
  banner.id = 'keby-cookie-banner';
  banner.setAttribute('role', 'complementary');
  banner.setAttribute('aria-label', t.title);
  banner.innerHTML = `
    <div class="kbc-title">${t.title}</div>
    <div class="kbc-body">${t.body}</div>
    <div class="kbc-actions">
      <a href="datenschutz.html" class="kbc-link">${t.link} →</a>
      <button type="button" class="kbc-ok">${t.ok}</button>
    </div>
  `;
  document.body.appendChild(banner);

  banner.querySelector('.kbc-ok').addEventListener('click', () => {
    localStorage.setItem(STORAGE_KEY, '1');
    banner.classList.add('hide');
    setTimeout(() => banner.remove(), 300);
  });
})();
