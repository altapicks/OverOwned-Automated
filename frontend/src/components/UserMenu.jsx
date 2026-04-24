// UserMenu v5.11 — rebuilt user dropdown.
//
// Pill trigger (in topbar):
//   - Avatar circle with the first letter of display name (or email)
//   - Display name if set, otherwise email (truncated)
//   - Chevron
//
// Panel (desktop dropdown / mobile bottom-sheet):
//   - Identity: large avatar, display name (inline-editable), email, badge
//   - Subscription block:
//       * Admin        → "Admin access · all features unlocked"
//       * Subscribed   → tier + renewal date + Manage button
//       * Free + user  → "Subscribe to unlock" CTA
//   - Sign out
//
// Mobile: panel becomes a bottom-sheet (full-width, slides up, dim backdrop)
// on narrow screens. Same content, re-laid-out for thumbs.

import React, { useState, useRef, useEffect, useCallback } from 'react';
import { useAuth } from '../lib/auth-context';
import { startCheckout, openBillingPortal } from '../lib/checkout';

// Small responsive hook — true when viewport is < 640px.
function useIsMobile() {
  const [isMobile, setIsMobile] = useState(() =>
    typeof window !== 'undefined' && window.matchMedia('(max-width: 639px)').matches
  );
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const mq = window.matchMedia('(max-width: 639px)');
    const handler = (e) => setIsMobile(e.matches);
    if (mq.addEventListener) mq.addEventListener('change', handler);
    else mq.addListener(handler);
    return () => {
      if (mq.removeEventListener) mq.removeEventListener('change', handler);
      else mq.removeListener(handler);
    };
  }, []);
  return isMobile;
}

function initialFor(name, email) {
  const s = (name || email || '?').trim();
  return s.charAt(0).toUpperCase();
}

function formatDate(iso) {
  if (!iso) return null;
  try {
    const d = new Date(iso);
    if (isNaN(d)) return null;
    return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
  } catch { return null; }
}

// ─── Display name inline editor ─────────────────────────────────────
function DisplayNameEditor({ displayName, email, onSave }) {
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(displayName || '');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const inputRef = useRef(null);

  useEffect(() => { setValue(displayName || ''); }, [displayName]);
  useEffect(() => { if (editing && inputRef.current) inputRef.current.select(); }, [editing]);

  const commit = useCallback(async () => {
    if (saving) return;
    const trimmed = value.trim();
    if (trimmed === (displayName || '')) { setEditing(false); setError(''); return; }
    if (!trimmed) { setError('Cannot be empty.'); return; }
    setSaving(true);
    const result = await onSave(trimmed);
    setSaving(false);
    if (result?.ok) {
      setEditing(false);
      setError('');
    } else {
      setError(result?.error || 'Save failed.');
    }
  }, [value, displayName, onSave, saving]);

  if (!editing) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <div style={{
          fontSize: 15, fontWeight: 700, color: '#E8ECF4',
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 220,
        }}>
          {displayName || email}
        </div>
        <button
          onClick={() => setEditing(true)}
          title="Edit display name"
          aria-label="Edit display name"
          style={{
            padding: 4, background: 'transparent', border: 'none', cursor: 'pointer',
            color: '#8B9ABA', display: 'inline-flex', alignItems: 'center',
            borderRadius: 4,
          }}
          onMouseEnter={(e) => { e.currentTarget.style.color = '#F5C518'; }}
          onMouseLeave={(e) => { e.currentTarget.style.color = '#8B9ABA'; }}
        >
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M17 3a2.85 2.85 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5L17 3z"/>
          </svg>
        </button>
      </div>
    );
  }

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <input
          ref={inputRef}
          type="text"
          value={value}
          onChange={(e) => { setValue(e.target.value); if (error) setError(''); }}
          onKeyDown={(e) => {
            if (e.key === 'Enter') { e.preventDefault(); commit(); }
            if (e.key === 'Escape') { setEditing(false); setValue(displayName || ''); setError(''); }
          }}
          disabled={saving}
          maxLength={40}
          placeholder="Display name"
          style={{
            flex: 1, minWidth: 0, padding: '5px 9px', background: '#0A1628',
            border: `1px solid ${error ? '#EF4444' : 'rgba(245,197,24,0.4)'}`, borderRadius: 5,
            color: '#E8ECF4', fontSize: 14, fontWeight: 700, outline: 'none',
            fontFamily: 'inherit',
          }}
        />
        <button onClick={commit} disabled={saving}
          style={{ padding: '6px 10px', fontSize: 11, fontWeight: 700,
                   background: '#F5C518', color: '#0A1628', border: 'none',
                   borderRadius: 5, cursor: saving ? 'wait' : 'pointer', fontFamily: 'inherit' }}>
          {saving ? '…' : 'Save'}
        </button>
        <button onClick={() => { setEditing(false); setValue(displayName || ''); setError(''); }}
          disabled={saving}
          style={{ padding: '6px 10px', fontSize: 11, fontWeight: 600,
                   background: 'transparent', color: '#8B9ABA', border: '1px solid rgba(42,61,95,0.5)',
                   borderRadius: 5, cursor: saving ? 'wait' : 'pointer', fontFamily: 'inherit' }}>
          Cancel
        </button>
      </div>
      {error && <div style={{ marginTop: 6, fontSize: 11, color: '#EF4444' }}>{error}</div>}
    </div>
  );
}

// ─── Status badge (admin / subscription tier / free) ────────────────
function StatusBadge({ isAdmin, isSubscribed, subscription }) {
  if (isAdmin) {
    return (
      <span style={{
        display: 'inline-flex', alignItems: 'center', gap: 5,
        padding: '2px 8px', borderRadius: 4,
        background: 'rgba(245,197,24,0.14)',
        border: '1px solid rgba(245,197,24,0.4)',
        fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
        letterSpacing: '0.06em', color: '#F5C518',
      }}>
        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 2l9 4v6c0 5-4 9-9 10-5-1-9-5-9-10V6l9-4z"/>
        </svg>
        Admin
      </span>
    );
  }
  if (isSubscribed) {
    const tierLabel = subscription?.tier
      ? subscription.tier.charAt(0).toUpperCase() + subscription.tier.slice(1)
      : 'Active';
    return (
      <span style={{
        display: 'inline-flex', alignItems: 'center', gap: 5,
        padding: '2px 8px', borderRadius: 4,
        background: 'rgba(74,222,128,0.12)', border: '1px solid rgba(74,222,128,0.35)',
        fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
        letterSpacing: '0.06em', color: '#4ADE80',
      }}>
        <span style={{ width: 5, height: 5, borderRadius: '50%', background: '#4ADE80',
                       boxShadow: '0 0 4px #4ADE80' }} />
        {tierLabel}
      </span>
    );
  }
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 5,
      padding: '2px 8px', borderRadius: 4,
      background: 'rgba(245,158,11,0.12)', border: '1px solid rgba(245,158,11,0.35)',
      fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
      letterSpacing: '0.06em', color: '#F59E0B',
    }}>
      Free
    </span>
  );
}

// ─── Subscription section (varies by state) ─────────────────────────
function SubscriptionBlock({ isAdmin, isSubscribed, subscription, onSubscribe, onManage, busy }) {
  const blockStyle = {
    margin: '10px 6px 4px',
    padding: '12px 14px',
    background: 'rgba(10,22,40,0.5)',
    border: '1px solid rgba(42,61,95,0.5)',
    borderRadius: 8,
  };

  if (isAdmin) {
    return (
      <div style={blockStyle}>
        <div style={{ fontSize: 11, color: '#F5C518', textTransform: 'uppercase',
                      letterSpacing: '0.08em', fontWeight: 700, marginBottom: 4 }}>
          Admin access
        </div>
        <div style={{ fontSize: 12, color: '#8B9ABA', lineHeight: 1.5 }}>
          All features unlocked. No subscription required.
        </div>
      </div>
    );
  }

  if (isSubscribed) {
    const renews = formatDate(subscription?.current_period_end);
    const tierLabel = subscription?.tier
      ? subscription.tier.charAt(0).toUpperCase() + subscription.tier.slice(1)
      : 'Active subscriber';
    return (
      <div style={blockStyle}>
        <div style={{ fontSize: 11, color: '#4ADE80', textTransform: 'uppercase',
                      letterSpacing: '0.08em', fontWeight: 700, marginBottom: 4 }}>
          {tierLabel} plan
        </div>
        {renews && (
          <div style={{ fontSize: 12, color: '#8B9ABA', marginBottom: 10, lineHeight: 1.5 }}>
            {subscription?.status === 'trialing' ? 'Trial ends' : 'Renews'} {renews}
          </div>
        )}
        <button onClick={onManage} disabled={busy}
          style={{ display: 'block', width: '100%', padding: '9px 12px',
                   background: 'transparent', color: '#E8ECF4',
                   border: '1px solid rgba(245,197,24,0.35)',
                   borderRadius: 6, fontSize: 12, fontWeight: 600,
                   cursor: busy ? 'wait' : 'pointer', fontFamily: 'inherit' }}>
          {busy ? 'Opening billing portal…' : 'Manage subscription'}
        </button>
      </div>
    );
  }

  return (
    <div style={blockStyle}>
      <div style={{ fontSize: 11, color: '#F59E0B', textTransform: 'uppercase',
                    letterSpacing: '0.08em', fontWeight: 700, marginBottom: 4 }}>
        No active subscription
      </div>
      <div style={{ fontSize: 12, color: '#8B9ABA', marginBottom: 10, lineHeight: 1.5 }}>
        Unlock OverOwned Mode, Live Leverage Tracker, and more.
      </div>
      <button onClick={onSubscribe} disabled={busy}
        style={{ display: 'block', width: '100%', padding: '10px 12px',
                 background: busy ? '#8B9ABA' : 'linear-gradient(135deg, #D4A912, #F5C518)',
                 color: '#0A1628', border: 'none', borderRadius: 6,
                 fontSize: 13, fontWeight: 700,
                 cursor: busy ? 'wait' : 'pointer', fontFamily: 'inherit' }}>
        {busy ? 'Opening checkout…' : 'Subscribe →'}
      </button>
    </div>
  );
}

// ─── Main UserMenu ──────────────────────────────────────────────────
export function UserMenu() {
  const { user, isSubscribed, subscription, isAdmin, displayName, signOut, status, updateDisplayName } = useAuth();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const isMobile = useIsMobile();
  const ref = useRef(null);

  useEffect(() => {
    function handleClick(e) {
      if (isMobile) return;
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    function handleKey(e) {
      if (e.key === 'Escape') setOpen(false);
    }
    document.addEventListener('mousedown', handleClick);
    document.addEventListener('keydown', handleKey);
    return () => {
      document.removeEventListener('mousedown', handleClick);
      document.removeEventListener('keydown', handleKey);
    };
  }, [isMobile]);

  useEffect(() => {
    if (!isMobile || !open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => { document.body.style.overflow = prev; };
  }, [isMobile, open]);

  async function handleSubscribe() {
    if (busy) return;
    setBusy(true);
    try {
      await startCheckout({ tier: 'monthly', email: user?.email, userId: user?.id });
    } catch (err) {
      console.error(err);
      alert('Checkout is temporarily unavailable. Please try again in a moment.');
      setBusy(false);
    }
  }

  async function handleManage() {
    if (busy) return;
    setBusy(true);
    try {
      await openBillingPortal(user.id);
    } catch (err) {
      console.error(err);
      alert('Unable to open billing portal. Please try again.');
      setBusy(false);
    }
  }

  if (status === 'loading') {
    return <div style={{ width: 32, height: 32 }} />;
  }

  if (status === 'unauthenticated') {
    return (
      <a href="#signin" onClick={(e) => { e.preventDefault(); window.location.hash = 'signin'; window.location.reload(); }}
         style={{ padding: '6px 12px', borderRadius: 6, border: '1px solid rgba(245,197,24,0.4)',
                  color: '#F5C518', fontSize: 12, fontWeight: 600, textDecoration: 'none',
                  background: 'rgba(245,197,24,0.08)', cursor: 'pointer' }}>
        Sign In
      </a>
    );
  }

  const initial = initialFor(displayName, user.email);
  const primaryLabel = displayName || user.email;

  const triggerButton = (
    <button onClick={() => setOpen(o => !o)}
            aria-label="Account menu"
            aria-expanded={open}
            style={{
              display: 'flex', alignItems: 'center', gap: 8, padding: '4px 10px 4px 4px',
              borderRadius: 999,
              border: '1px solid rgba(42,61,95,0.6)',
              background: 'rgba(10,22,40,0.5)',
              color: '#E8ECF4', cursor: 'pointer',
              fontSize: 12, fontFamily: 'inherit', flexShrink: 0,
            }}>
      <span style={{
        width: 24, height: 24, borderRadius: '50%',
        background: 'linear-gradient(135deg, #D4A912, #F5C518)',
        color: '#0A1628', fontSize: 12, fontWeight: 800,
        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
        flexShrink: 0,
      }}>
        {initial}
      </span>
      <span style={{
        maxWidth: isMobile ? 100 : 160,
        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
      }}>
        {primaryLabel}
      </span>
      <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#8B9ABA" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <polyline points="6 9 12 15 18 9"/>
      </svg>
    </button>
  );

  const panelContents = (
    <>
      <div style={{
        padding: isMobile ? '16px 18px' : '14px 14px',
        borderBottom: '1px solid rgba(42,61,95,0.5)',
      }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start', marginBottom: 10 }}>
          <div style={{
            width: isMobile ? 48 : 40, height: isMobile ? 48 : 40, borderRadius: '50%',
            background: 'linear-gradient(135deg, #D4A912, #F5C518)',
            color: '#0A1628',
            fontSize: isMobile ? 20 : 17, fontWeight: 800,
            display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
            flexShrink: 0,
            boxShadow: '0 4px 14px rgba(245,197,24,0.25)',
          }}>
            {initial}
          </div>
          <div style={{ flex: 1, minWidth: 0 }}>
            <DisplayNameEditor
              displayName={displayName}
              email={user.email}
              onSave={updateDisplayName}
            />
            <div style={{
              fontSize: 11, color: '#8B9ABA', marginTop: 4,
              overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
            }}>
              {user.email}
            </div>
          </div>
        </div>
        <StatusBadge isAdmin={isAdmin} isSubscribed={isSubscribed} subscription={subscription} />
      </div>

      <SubscriptionBlock
        isAdmin={isAdmin}
        isSubscribed={isSubscribed}
        subscription={subscription}
        onSubscribe={handleSubscribe}
        onManage={handleManage}
        busy={busy}
      />

      <div style={{ padding: isMobile ? '6px 10px 14px' : '4px 6px 6px' }}>
        <button onClick={() => { signOut(); setOpen(false); }}
                style={{ width: '100%', padding: '11px 12px', textAlign: 'left',
                         background: 'transparent', border: 'none', borderRadius: 6,
                         color: '#E8ECF4', fontSize: 13, cursor: 'pointer',
                         fontFamily: 'inherit', display: 'flex', alignItems: 'center', gap: 8 }}
                onMouseEnter={e => e.currentTarget.style.background = 'rgba(42,61,95,0.4)'}
                onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/>
            <polyline points="16 17 21 12 16 7"/>
            <line x1="21" y1="12" x2="9" y2="12"/>
          </svg>
          Sign out
        </button>
      </div>
    </>
  );

  if (isMobile) {
    return (
      <div ref={ref} style={{ position: 'relative' }}>
        {triggerButton}
        {open && (
          <>
            <div
              onClick={() => setOpen(false)}
              style={{
                position: 'fixed', inset: 0,
                background: 'rgba(5,10,20,0.6)',
                backdropFilter: 'blur(2px)',
                WebkitBackdropFilter: 'blur(2px)',
                zIndex: 200,
                animation: 'oo-fade-in 0.15s ease-out',
              }}
            />
            <div
              role="dialog"
              aria-label="Account menu"
              style={{
                position: 'fixed', left: 0, right: 0, bottom: 0,
                background: '#0F1D33',
                borderTop: '1px solid rgba(245,197,24,0.25)',
                borderRadius: '16px 16px 0 0',
                boxShadow: '0 -20px 60px rgba(0,0,0,0.6)',
                zIndex: 201,
                paddingBottom: 'max(14px, env(safe-area-inset-bottom))',
                animation: 'oo-slide-up 0.22s cubic-bezier(0.2, 0.8, 0.3, 1)',
              }}
              onClick={(e) => e.stopPropagation()}
            >
              <div style={{ display: 'flex', justifyContent: 'center', padding: '8px 0 2px' }}>
                <div style={{ width: 36, height: 4, borderRadius: 2,
                              background: 'rgba(139,154,186,0.4)' }} />
              </div>
              {panelContents}
            </div>
            <style>{`
              @keyframes oo-fade-in { from { opacity: 0; } to { opacity: 1; } }
              @keyframes oo-slide-up {
                from { transform: translateY(100%); }
                to   { transform: translateY(0); }
              }
            `}</style>
          </>
        )}
      </div>
    );
  }

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      {triggerButton}
      {open && (
        <div style={{
          position: 'absolute', top: 'calc(100% + 6px)', right: 0,
          width: 300, background: '#0F1D33',
          border: '1px solid rgba(245,197,24,0.25)',
          borderRadius: 10, padding: 0,
          boxShadow: '0 12px 40px rgba(0,0,0,0.5)',
          zIndex: 100,
          animation: 'oo-dropdown-in 0.12s ease-out',
        }}>
          {panelContents}
          <style>{`
            @keyframes oo-dropdown-in {
              from { opacity: 0; transform: translateY(-4px); }
              to   { opacity: 1; transform: translateY(0); }
            }
          `}</style>
        </div>
      )}
    </div>
  );
}
