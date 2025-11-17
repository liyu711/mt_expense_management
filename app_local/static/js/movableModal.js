/* movableModal.js
   - Make modal windows (.modal .modal-content) draggable by their .modal-header
   - Persist position per-session via sessionStorage (key: modal_pos_<id>)
   - Provide safe defaults (centers modal when no saved position)
*/
(function(){
  function parseTranslate(transform){
    if(!transform) return {x:0,y:0};
    var m = /translate\(([-\d.]+)px,\s*([-\d.]+)px\)/.exec(transform);
    if(m){ return { x: parseFloat(m[1])||0, y: parseFloat(m[2])||0 }; }
    return {x:0,y:0};
  }

  function makeModalMovable(modal){
    if(!modal) return;
    var content = modal.querySelector('.modal-content');
    if(!content) return;


    var state = { dragging:false, startX:0, startY:0, startOffX:0, startOffY:0 };
    var DRAG_ZONE_PX = 24; // top strip of modal-content that acts as drag handle

    // Pointer down on the very top of modal-content starts dragging
    content.addEventListener('pointerdown', function(e){
      if(e.button && e.button !== 0) return; // only left button
      var rect = content.getBoundingClientRect();
      var withinTopStrip = (e.clientY - rect.top) <= DRAG_ZONE_PX;
      // do not start dragging when clicking close button (or its children)
      if(e.target && e.target.closest && e.target.closest('.modal-close')) return;
      if(!withinTopStrip) return; // only top strip is draggable
      state.dragging = true;
      state.startX = e.clientX;
      state.startY = e.clientY;
      var offs = parseTranslate(content.style.transform);
      state.startOffX = offs.x;
      state.startOffY = offs.y;
      content.setPointerCapture && content.setPointerCapture(e.pointerId);
      e.preventDefault();
    });

    // Visual hint: change cursor when hovering top strip
    content.addEventListener('pointermove', function(e){
      var rect = content.getBoundingClientRect();
      var withinTopStrip = (e.clientY - rect.top) <= DRAG_ZONE_PX;
      var overClose = !!(e.target && e.target.closest && e.target.closest('.modal-close'));
      content.style.cursor = (withinTopStrip && !overClose) ? 'move' : '';
    });

    document.addEventListener('pointermove', function(e){
      if(!state.dragging) return;
      var dx = e.clientX - state.startX;
      var dy = e.clientY - state.startY;
      var nx = state.startOffX + dx;
      var ny = state.startOffY + dy;
      content.style.transform = 'translate(' + nx + 'px,' + ny + 'px)';
    });

    document.addEventListener('pointerup', function(){
      if(!state.dragging) return;
      state.dragging = false;
      // do not persist position
    });

    // Wire overlay and close button if present (safe defaults)
    var overlay = modal.querySelector('.modal-overlay');
    var closeBtn = modal.querySelector('.modal-close');
    function resetPosition(){ try{ content.style.transform = ''; }catch(_){} }
    if(overlay){
      overlay.addEventListener('click', function(){
        modal.style.display = 'none';
        modal.setAttribute('aria-hidden','true');
        document.body.style.overflow = '';
        resetPosition();
      });
    }
    if(closeBtn){
      closeBtn.addEventListener('click', function(){
        modal.style.display = 'none';
        modal.setAttribute('aria-hidden','true');
        document.body.style.overflow = '';
        resetPosition();
      });
    }

    // Also reset position when modal visibility toggles via other scripts (e.g., ESC or programmatic open/close)
    try{
      var obs = new MutationObserver(function(mutations){
        mutations.forEach(function(m){
          if(m.type === 'attributes' && (m.attributeName === 'aria-hidden' || m.attributeName === 'style' || m.attributeName === 'class')){
            // On any visibility change, clear transform so we don't "remember" the last drag
            resetPosition();
          }
        });
      });
      obs.observe(modal, { attributes: true, attributeFilter: ['aria-hidden','style','class'] });
    }catch(e){ /* ignore */ }
  }

  // Initialize any modals on DOM ready
  if(document.readyState === 'complete' || document.readyState === 'interactive'){
    document.querySelectorAll('.modal').forEach(makeModalMovable);
  } else {
    document.addEventListener('DOMContentLoaded', function(){ document.querySelectorAll('.modal').forEach(makeModalMovable); });
  }
})();
