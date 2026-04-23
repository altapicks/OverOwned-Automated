// UserMenu — rendered in the topbar. Shows the user's sign-in state:
//   - Signed in + subscribed → green dot + email + "Manage subscription" + Sign Out
//   - Signed in + NOT subscribed → amber dot + "Subscribe to unlock" (Stripe Checkout)
//   - Signed out → "Sign In" button

import React, { useState, useRef, useEffect } from 'react';
import { useAuth } from '../lib/auth-context';
import { startCheckout, openBillingPortal } from '../lib/checkout';

export function UserMenu() {
  const { user, isSubscribed, subscription, signOut, status } = useAuth();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const ref = useRef(null);

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClick(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  async function handleSubscribe(tier) {
    if (busy) return;
    setBusy(true);
    try {
      await startCheckout({ tier, email: user?.email, userId: user?.id });
      // Browser redirects away; no need to reset busy.
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

  // Authenticated
  const dotColor = isSubscribed ? '#4ADE80' : '#F59E0B';
  const statusLabel = isSubscribed
    ? (subscription?.tier ? subscription.tier.charAt(0).toUpperCase() + subscription.tier.slice(1) : 'Active')
    : 'No subscription';

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button onClick={() => setOpen(o => !o)}
              style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '6px 10px',
                       borderRadius: 6, border: '1px solid rgba(42,61,95,0.6)',
                       background: 'rgba(10,22,40,0.5)', color: '#E8ECF4', cursor: 'pointer',
                       fontSize: 12, fontFamily: 'inherit' }}>
        <span style={{ width: 8, height: 8, borderRadius: '50%', background: dotColor,
                       boxShadow: isSubscribed ? `0 0 6px ${dotColor}` : 'none' }} />
        <span style={{ maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {user.email}
        </span>
        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#8B9ABA" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <polyline points="6 9 12 15 18 9"/>
        </svg>
      </button>

      {open && (
        <div style={{ position: 'absolute', top: 'calc(100% + 6px)', right: 0, minWidth: 240,
                      background: '#0F1D33', border: '1px solid rgba(245,197,24,0.25)',
                      borderRadius: 8, padding: 6, boxShadow: '0 12px 40px rgba(0,0,0,0.5)',
                      zIndex: 100 }}>
          <div style={{ padding: '10px 12px', borderBottom: '1px solid rgba(42,61,95,0.5)' }}>
            <div style={{ fontSize: 11, color: '#8B9ABA', textTransform: 'uppercase',
                          letterSpacing: '0.08em', fontWeight: 600, marginBottom: 4 }}>Signed in as</div>
            <div style={{ fontSize: 13, color: '#E8ECF4', fontWeight: 500, overflowWrap: 'anywhere' }}>
              {user.email}
            </div>
            <div style={{ marginTop: 6, display: 'inline-flex', alignItems: 'center', gap: 6,
                          padding: '2px 8px', borderRadius: 4,
                          background: isSubscribed ? 'rgba(74,222,128,0.12)' : 'rgba(245,158,11,0.12)',
                          border: `1px solid ${isSubscribed ? 'rgba(74,222,128,0.35)' : 'rgba(245,158,11,0.35)'}`,
                          fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
                          letterSpacing: '0.06em', color: isSubscribed ? '#4ADE80' : '#F59E0B' }}>
              {statusLabel}
            </div>
          </div>

          {!isSubscribed && (
            <button onClick={() => handleSubscribe('monthly')} disabled={busy}
                    style={{ display: 'block', width: '100%', padding: '10px 12px', margin: '4px 0',
                             borderRadius: 6, border: 'none',
                             background: busy ? '#8B9ABA' : 'linear-gradient(135deg, #D4A912, #F5C518)',
                             color: '#0A1628', fontSize: 13, fontWeight: 700, textAlign: 'center',
                             cursor: busy ? 'wait' : 'pointer', fontFamily: 'inherit' }}>
              {busy ? 'Opening checkout…' : 'Subscribe to unlock →'}
            </button>
          )}

          {isSubscribed && (
            <button onClick={handleManage} disabled={busy}
                    style={{ display: 'block', width: '100%', padding: '10px 12px', margin: '4px 0',
                             borderRadius: 6, border: '1px solid rgba(42,61,95,0.5)',
                             background: 'transparent', color: '#E8ECF4', fontSize: 13,
                             cursor: busy ? 'wait' : 'pointer', fontFamily: 'inherit',
                             textAlign: 'left' }}>
              {busy ? 'Opening…' : 'Manage subscription'}
            </button>
          )}

          <button onClick={() => { signOut(); setOpen(false); }}
                  style={{ width: '100%', padding: '10px 12px', textAlign: 'left',
                           background: 'transparent', border: 'none', borderRadius: 6,
                           color: '#E8ECF4', fontSize: 13, cursor: 'pointer',
                           fontFamily: 'inherit' }}
                  onMouseEnter={e => e.currentTarget.style.background = 'rgba(42,61,95,0.4)'}
                  onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
            Sign out
          </button>
        </div>
      )}
    </div>
  );
}
