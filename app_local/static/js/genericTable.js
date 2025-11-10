(function(global){
  function createGenericTable(root, options){
    if(!root) return;
    const cfg = Object.assign({
      columns: [], // array of column names
      rows: [],    // array of objects {col:value}
      pageSizeOptions: [10, 25, 50, 100],
      defaultPageSize: 10,
      onRowClick: null, // function(rowData, rowIndex)
      title: '',
      // Optional: built-in row-click edit modal support
      // rowClickEditModal: { selector: '#modalId', prefill: (modal,row)=>{}, onOpen:(modal,row)=>{} }
      rowClickEditModal: null
    }, options || {});

    // Basic layout
    root.classList.add('gt-container');
    root.innerHTML = '';

    const header = document.createElement('div');
    header.className = 'gt-header';

    const titleEl = document.createElement('div');
    titleEl.className = 'gt-title';
    titleEl.textContent = cfg.title || '';

    const controlsRight = document.createElement('div');
    controlsRight.className = 'gt-controls';

    const searchInput = document.createElement('input');
    searchInput.type = 'search';
    searchInput.placeholder = 'Search...';
    searchInput.className = 'gt-search';

    const pageSizeSel = document.createElement('select');
    pageSizeSel.className = 'gt-page-size';
    (cfg.pageSizeOptions || [10,25,50,100]).forEach(function(n){
      const opt = document.createElement('option');
      opt.value = String(n);
      opt.textContent = n + ' rows';
      if(n === cfg.defaultPageSize) opt.selected = true;
      pageSizeSel.appendChild(opt);
    });

    controlsRight.appendChild(searchInput);
    controlsRight.appendChild(pageSizeSel);
    header.appendChild(titleEl);
    header.appendChild(controlsRight);

    const tableWrap = document.createElement('div');
    tableWrap.className = 'gt-table-wrap';

    const table = document.createElement('table');
    table.className = 'gt-table';

    const thead = document.createElement('thead');
    const groupRow = document.createElement('tr'); // optional top grouping row
    const headerRow = document.createElement('tr');
    const filterRow = document.createElement('tr');

    // Helpers to update sort button icons based on state
    function updateSortIcon(th){
      try {
        const btn = th.querySelector('button.gt-sort');
        if(!btn) return;
        const st = th.dataset.sortState;
        btn.textContent = (st === 'asc') ? '↑' : (st === 'desc' ? '↓' : '⇅');
      } catch(e) {}
    }
    function updateAllSortIcons(){
      Array.from(headerRow.children).forEach(function(th){ updateSortIcon(th); });
    }

    // Column filter state
    const filters = {}; // colName -> selected value (or null)

    cfg.columns.forEach(function(col){
      const th = document.createElement('th');
      const label = document.createElement('span');
      label.textContent = col;
      label.className = 'gt-col-label';

      const sortBtn = document.createElement('button');
      sortBtn.type = 'button';
      sortBtn.className = 'gt-sort';
      sortBtn.textContent = '⇅';
      sortBtn.title = 'Sort';

  const filterSel = document.createElement('select');
  filterSel.className = 'gt-filter';
  const anyOpt = document.createElement('option');
  anyOpt.value = '';
  anyOpt.textContent = 'All';
  filterSel.appendChild(anyOpt);

  // header cell (label + sort)
  th.appendChild(label);
  th.appendChild(sortBtn);
  headerRow.appendChild(th);

  // second-row cell for the filter select (keeps filters on their own row)
  const thFilter = document.createElement('th');
  thFilter.appendChild(filterSel);
  filterRow.appendChild(thFilter);

      // Sorting state: null | 'asc' | 'desc'
      th.dataset.sortState = '';
      updateSortIcon(th);
      sortBtn.addEventListener('click', function(){
        const state = th.dataset.sortState;
        th.dataset.sortState = state === 'asc' ? 'desc' : (state === 'desc' ? '' : 'asc');
        // clear sort on other columns
        Array.from(headerRow.children).forEach(function(otherTh){
          if(otherTh !== th) otherTh.dataset.sortState = '';
        });
        updateAllSortIcons();
        refresh();
      });

      filterSel.addEventListener('change', function(){
        filters[col] = filterSel.value || null;
        refresh();
      });
    });
    // If grouping requested, add a grouping row spanning the grouped columns.
    // cfg.groups structure: { GroupLabel: [col1,col2,...] }
    if (cfg.groups && typeof cfg.groups === 'object') {
      // For now support single group (Personnel Cost). Additional groups can be added similarly.
      Object.keys(cfg.groups).forEach(function(gLabel){
        const colsInGroup = cfg.groups[gLabel] || [];
        if(!Array.isArray(colsInGroup) || !colsInGroup.length) return;
        // Determine contiguous span indices
        const indices = colsInGroup.map(function(c){ return cfg.columns.indexOf(c); }).filter(function(i){ return i >= 0; }).sort(function(a,b){return a-b;});
        if(!indices.length) return;
        const first = indices[0];
        const last = indices[indices.length - 1];
        // Build cells before group (empty placeholders)
        for (let i = 0; i < first; i++) {
          const emptyTh = document.createElement('th');
          emptyTh.className = 'gt-group-spacer';
          emptyTh.style.background = '#f5f5f5';
          emptyTh.style.borderBottom = 'none';
          groupRow.appendChild(emptyTh);
        }
        // Group header cell
        const gth = document.createElement('th');
        gth.className = 'gt-group-header';
        gth.textContent = gLabel;
        gth.colSpan = (last - first + 1);
        gth.style.textAlign = 'center';
        gth.style.background = '#e9ecef';
        gth.style.fontWeight = '600';
        groupRow.appendChild(gth);
        // Remaining columns after group
        for (let i = last + 1; i < cfg.columns.length; i++) {
          const emptyTh = document.createElement('th');
          emptyTh.className = 'gt-group-spacer';
          emptyTh.style.background = '#f5f5f5';
          emptyTh.style.borderBottom = 'none';
          groupRow.appendChild(emptyTh);
        }
      });
      // Append grouping row only if it has children
      if(groupRow.children.length){ thead.appendChild(groupRow); }
    }

    thead.appendChild(headerRow);
    thead.appendChild(filterRow);

    const tbody = document.createElement('tbody');

    const footer = document.createElement('div');
    footer.className = 'gt-footer';
    const pageInfo = document.createElement('div');
    pageInfo.className = 'gt-page-info';
    const pager = document.createElement('div');
    pager.className = 'gt-pager';

    footer.appendChild(pageInfo);
    footer.appendChild(pager);

    table.appendChild(thead);
    table.appendChild(tbody);
    tableWrap.appendChild(table);

    root.appendChild(header);
    root.appendChild(tableWrap);
    root.appendChild(footer);

    // Internal state
    let pageSize = parseInt(pageSizeSel.value,10) || cfg.defaultPageSize || 10;
    let currentPage = 1;

    // Build column filters from current data
    function rebuildFilters(values){
      // values: filtered dataset currently considered for filter population
      const uniques = {};
      cfg.columns.forEach(function(c){ uniques[c] = new Set(); });
      (values || cfg.rows).forEach(function(row){
        cfg.columns.forEach(function(c){
          const v = (row[c] != null ? String(row[c]) : '').trim();
          if(v !== '') uniques[c].add(v);
        });
      });
      // update selects located in the filterRow
      cfg.columns.forEach(function(col, idx){
        const th = filterRow.children[idx];
        const sel = th ? th.querySelector('select.gt-filter') : null;
        if(!sel) return;
        const current = sel.value;
        sel.innerHTML = '';
        const anyOpt = document.createElement('option');
        anyOpt.value = '';
        anyOpt.textContent = 'All';
        sel.appendChild(anyOpt);
        Array.from(uniques[col]).sort().forEach(function(v){
          const opt = document.createElement('option');
          opt.value = v;
          opt.textContent = v;
          if (current && current === v) opt.selected = true;
          sel.appendChild(opt);
        });
      });
    }

    // Apply search, filters, and sorting to rows
    function computeView(){
      const q = (searchInput.value || '').toLowerCase();
      let rows = cfg.rows.slice();
      if(q){
        rows = rows.filter(function(r){
          return cfg.columns.some(function(c){
            const v = r[c];
            return v != null && String(v).toLowerCase().includes(q);
          });
        });
      }
      // column filters
      Object.keys(filters).forEach(function(col){
        const val = filters[col];
        if(val){
          rows = rows.filter(function(r){ return String(r[col]) === String(val); });
        }
      });
      // sorting
      let sortIdx = -1, sortDir = '';
      Array.from(headerRow.children).forEach(function(th, idx){
        if(th.dataset.sortState){ sortIdx = idx; sortDir = th.dataset.sortState; }
      });
      if(sortIdx >= 0 && sortDir){
        const col = cfg.columns[sortIdx];
        rows.sort(function(a,b){
          const av = a[col];
          const bv = b[col];
          if(av == null && bv == null) return 0;
          if(av == null) return sortDir === 'asc' ? -1 : 1;
          if(bv == null) return sortDir === 'asc' ? 1 : -1;
          const an = parseFloat(av); const bn = parseFloat(bv);
          const bothNum = !Number.isNaN(an) && !Number.isNaN(bn);
          if(bothNum){
            return sortDir === 'asc' ? (an - bn) : (bn - an);
          }
          const as = String(av).toLowerCase();
          const bs = String(bv).toLowerCase();
          if(as < bs) return sortDir === 'asc' ? -1 : 1;
          if(as > bs) return sortDir === 'asc' ? 1 : -1;
          return 0;
        });
      }
      return rows;
    }

    function openModal(modal){
      if(!modal) return;
      modal.style.display = 'block';
      modal.setAttribute('aria-hidden','false');
      try { document.body.style.overflow = 'hidden'; } catch(e) {}
      const first = modal.querySelector('input,select,button,textarea');
      if(first) first.focus();
    }
    function closeModal(modal){
      if(!modal) return;
      modal.style.display = 'none';
      modal.setAttribute('aria-hidden','true');
      try { document.body.style.overflow = ''; } catch(e) {}
    }
    function defaultPrefill(modal, row){
      if(!modal || !row) return;
      const els = modal.querySelectorAll('input,select,textarea');
      const keys = Object.keys(row || {});
      els.forEach(function(el){
        const candidates = [el.name, el.id].filter(Boolean);
        let matched;
        for(let i=0;i<candidates.length && !matched;i++){
          const cand = candidates[i];
          matched = keys.find(function(k){ return String(k).toLowerCase() === String(cand).toLowerCase(); });
        }
        if(matched != null){
          const val = row[matched];
          try {
            if(el.tagName === 'SELECT'){
              // try exact, then stringified
              el.value = val;
              if(el.value !== String(val)) el.value = String(val);
            } else if(el.type === 'checkbox' || el.type === 'radio') {
              el.checked = !!val && (String(val).toLowerCase() === 'true' || String(val) === '1');
            } else {
              el.value = val != null ? val : '';
            }
          } catch(e) {}
        }
      });
    }

    function handleRowClick(row, rowIndex){
      if(cfg && cfg.rowClickEditModal && cfg.rowClickEditModal.selector){
        const modal = document.querySelector(cfg.rowClickEditModal.selector);
        if(modal){
          try {
            const prefill = (cfg.rowClickEditModal.prefill || defaultPrefill);
            prefill(modal, row);
          } catch(e) {}
          try {
            if(typeof cfg.rowClickEditModal.onOpen === 'function'){
              cfg.rowClickEditModal.onOpen(modal, row);
            }
          } catch(e) {}
          openModal(modal);
          // wire close buttons if present (one-time best-effort)
          try {
            const overlay = modal.querySelector('.modal-overlay');
            const closeBtn = modal.querySelector('.modal-close');
            const cancelBtn = modal.querySelector('#modalCancel, [data-modal-cancel="true"]');
            [overlay, closeBtn, cancelBtn].forEach(function(el){ if(el && !el.__gtBound){ el.__gtBound = true; el.addEventListener('click', function(){ closeModal(modal); }); }});
          } catch(e) {}
          return; // handled by built-in modal feature
        }
      }
      if(typeof cfg.onRowClick === 'function'){
        cfg.onRowClick(row, rowIndex);
      }
    }

    function renderBody(rows){
      tbody.innerHTML = '';
      const start = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(start, start + pageSize);
      pageRows.forEach(function(r, i){
        const tr = document.createElement('tr');
        cfg.columns.forEach(function(c){
          const td = document.createElement('td');
          td.textContent = r[c] != null ? r[c] : '';
          tr.appendChild(td);
        });
        tr.addEventListener('click', function(){ handleRowClick(r, start + i); });
        tbody.appendChild(tr);
      });
    }

    function renderPager(total){
      pager.innerHTML = '';
      const totalPages = Math.max(1, Math.ceil(total / pageSize));
      function addBtn(label, go){
        const b = document.createElement('button');
        b.type = 'button';
        b.textContent = label;
        b.disabled = (go < 1 || go > totalPages || go === currentPage);
        b.addEventListener('click', function(){ currentPage = go; refresh(); });
        pager.appendChild(b);
      }
      addBtn('⏮', 1);
      addBtn('◀', currentPage - 1);
      const span = document.createElement('span');
      span.className = 'gt-page';
      span.textContent = 'Page ' + currentPage + ' / ' + totalPages;
      pager.appendChild(span);
      addBtn('▶', currentPage + 1);
      addBtn('⏭', totalPages);

      pageInfo.textContent = total + ' rows';
    }

    function refresh(){
      const view = computeView();
      // rebuild filters based on currently viewable set to keep options meaningful
      rebuildFilters(view);
      const total = view.length;
      // clamp current page
      const maxPage = Math.max(1, Math.ceil(total / pageSize));
      if(currentPage > maxPage) currentPage = maxPage;
      renderBody(view);
      renderPager(total);
    }

    // wire events
    searchInput.addEventListener('input', function(){ currentPage = 1; refresh(); });
    pageSizeSel.addEventListener('change', function(){ pageSize = parseInt(pageSizeSel.value,10)||10; currentPage = 1; refresh(); });

    // Initial
    rebuildFilters(cfg.rows);
    refresh();

    // Minimal styles (scoped)
    if(!document.getElementById('gt-styles')){
      const st = document.createElement('style');
      st.id = 'gt-styles';
      st.textContent = `
      .gt-container{display:flex;flex-direction:column;gap:8px;height:100%;}
      .gt-header{display:flex;justify-content:space-between;align-items:center;}
      .gt-title{font-weight:600;font-size:1.1em;}
      .gt-controls{display:flex;gap:8px;align-items:center;}
      .gt-search{padding:6px 8px;}
      .gt-page-size{padding:6px 8px;}
      .gt-table-wrap{overflow:auto;flex:1 1 auto;}
      .gt-table{border-collapse:collapse;width:100%;}
  .gt-table th,.gt-table td{border:1px solid #ddd;padding:6px 8px;}
  .gt-table th{position:sticky;top:0;background:#f5f5f5;white-space:nowrap;}
  .gt-col-label{margin-right:6px;display:inline-block;vertical-align:middle;}
  .gt-sort{margin:0 6px 0 0;padding:2px 6px;width:1.5em;display:inline-flex;justify-content:center;align-items:center;line-height:1;box-sizing:content-box;white-space:nowrap;overflow:hidden;}
  .gt-group-header{position:static !important;}
  .gt-group-spacer{position:static !important;}
      .gt-filter{padding:2px 6px;}
      .gt-footer{display:flex;justify-content:space-between;align-items:center;gap:8px;}
      .gt-pager button{margin:0 4px;}
      `;
      document.head.appendChild(st);
    }

    return {
      refresh: refresh,
      setData: function(columns, rows){
        cfg.columns = columns || [];
        cfg.rows = (rows || []).map(function(r){
          if(Array.isArray(r)){
            const obj = {}; cfg.columns.forEach(function(c, i){ obj[c] = r[i]; });
            return obj;
          }
          return r;
        });
        // rebuild header
        // Re-create table if columns changed
        // For simplicity, recreate the component
        const prevCfg = Object.assign({}, cfg);
        root.innerHTML = '';
        createGenericTable(root, Object.assign({}, prevCfg, { columns: cfg.columns, rows: cfg.rows }));
      }
    };
  }

  global.GenericTable = { create: createGenericTable };
})(window);
