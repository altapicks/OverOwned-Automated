// AccountPage v5.12 — full-screen account view, no dropdown.
//
// Accessed by clicking the user pill in the topbar, which sets
// window.location.hash = '#account'. App.jsx then renders this
// component in place of the main view (same pattern as SignInPrompt).
//
// Layout:
//   - Back button (top-left) → returns to previous tab
//   - Header: display name (inline-editable) + email + status badge
//   - Subscription card (admin / subscribed / free, varies by state)
//   - Sign out (bottom, muted)
//
// Single-column layout that scales from mobile to desktop. No avatar
// icons — clean typography only.

import React, { useState, useRef, useEffect, useCallback } from 'react';
import { useAuth } from '../lib/auth-context';
import { startCheckout, openBillingPortal } from '../lib/checkout';

function formatDate(iso) {
  if (!iso) return null;
  try {
    const d = new Date(iso);
    if (isNaN(d)) return null;
    return d.toLocaleDateString(undefined, { month: 'long', day: 'numeric', year: 'numeric' });
  } catch { return null; }
}

function goBack() {
  // If the user came from another page with hash, just clear hash.
  // History.back would also work if they navigated here via click, but
  // direct-URL visits (bookmark, shared link) shouldn't take them off-site.
  if (window.location.hash === '#account') {
    history.replaceState(null, '', window.location.pathname + window.location.search);
    // Force re-render by dispatching the hashchange event our App listens to
    window.dispatchEvent(new HashChangeEvent('hashchange'));
  } else {
    history.back();
  }
}

// ── Display name inline editor ──────────────────────────────────────
function DisplayNameRow({ displayName, email, onSave }) {
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
    if (!trimmed) { setError('Display name cannot be empty.'); return; }
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

  return (
    <div style={{ marginBottom: 32 }}>
      <div style={{
        fontSize: 11, color: '#8B9ABA', textTransform: 'uppercase',
        letterSpacing: '0.1em', fontWeight: 700, marginBottom: 10,
      }}>
        Display Name
      </div>
      {!editing ? (
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 12, flexWrap: 'wrap' }}>
          <div style={{
            fontSize: 26, fontWeight: 700, color: '#E8ECF4',
            letterSpacing: '-0.01em', lineHeight: 1.2,
            wordBreak: 'break-word',
          }}>
            {displayName || <span style={{ color: '#8B9ABA', fontStyle: 'italic', fontWeight: 500 }}>Not set</span>}
          </div>
          <button
            onClick={() => setEditing(true)}
            style={{
              padding: '5px 12px', fontSize: 12, fontWeight: 600,
              background: 'transparent', color: '#F5C518',
              border: '1px solid rgba(245,197,24,0.4)', borderRadius: 5,
              cursor: 'pointer', fontFamily: 'inherit',
            }}
          >
            {displayName ? 'Edit' : 'Set display name'}
          </button>
        </div>
      ) : (
        <>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
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
              placeholder="e.g. Alta"
              style={{
                flex: '1 1 260px', minWidth: 180, padding: '11px 14px',
                background: '#0A1628',
                border: `1px solid ${error ? '#EF4444' : 'rgba(245,197,24,0.4)'}`,
                borderRadius: 7, color: '#E8ECF4', fontSize: 16, fontWeight: 600,
                outline: 'none', fontFamily: 'inherit',
              }}
            />
            <button onClick={commit} disabled={saving}
              style={{
                padding: '11px 22px', fontSize: 13, fontWeight: 700,
                background: saving ? '#8B9ABA' : 'linear-gradient(135deg, #D4A912, #F5C518)',
                color: '#0A1628', border: 'none', borderRadius: 7,
                cursor: saving ? 'wait' : 'pointer', fontFamily: 'inherit',
              }}>
              {saving ? 'Saving…' : 'Save'}
            </button>
            <button onClick={() => { setEditing(false); setValue(displayName || ''); setError(''); }}
              disabled={saving}
              style={{
                padding: '11px 18px', fontSize: 13, fontWeight: 600,
                background: 'transparent', color: '#8B9ABA',
                border: '1px solid rgba(42,61,95,0.5)', borderRadius: 7,
                cursor: saving ? 'wait' : 'pointer', fontFamily: 'inherit',
              }}>
              Cancel
            </button>
          </div>
          {error && <div style={{ marginTop: 8, fontSize: 12, color: '#EF4444' }}>{error}</div>}
          <div style={{ marginTop: 8, fontSize: 11, color: '#8B9ABA' }}>
            {value.length}/40 characters
          </div>
        </>
      )}
    </div>
  );
}

// ── Status badge ─────────────────────────────────────────────────────
function StatusBadge({ isAdmin, isSubscribed, subscription }) {
  if (isAdmin) {
    return (
      <span style={{
        display: 'inline-flex', alignItems: 'center', gap: 6,
        padding: '5px 12px', borderRadius: 5,
        background: 'rgba(245,197,24,0.12)',
        border: '1px solid rgba(245,197,24,0.4)',
        fontSize: 11, fontWeight: 700, textTransform: 'uppercase',
        letterSpacing: '0.08em', color: '#F5C518',
      }}>
        <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
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
        display: 'inline-flex', alignItems: 'center', gap: 6,
        padding: '5px 12px', borderRadius: 5,
        background: 'rgba(74,222,128,0.12)', border: '1px solid rgba(74,222,128,0.35)',
        fontSize: 11, fontWeight: 700, textTransform: 'uppercase',
        letterSpacing: '0.08em', color: '#4ADE80',
      }}>
        <span style={{
          width: 6, height: 6, borderRadius: '50%', background: '#4ADE80',
          boxShadow: '0 0 6px #4ADE80',
        }} />
        {tierLabel}
      </span>
    );
  }
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 6,
      padding: '5px 12px', borderRadius: 5,
      background: 'rgba(245,158,11,0.12)', border: '1px solid rgba(245,158,11,0.35)',
      fontSize: 11, fontWeight: 700, textTransform: 'uppercase',
      letterSpacing: '0.08em', color: '#F59E0B',
    }}>
      Free
    </span>
  );
}

// ── Subscription card ────────────────────────────────────────────────
function SubscriptionCard({ isAdmin, isSubscribed, subscription, onSubscribe, onManage, busy }) {
  const card = {
    padding: '22px 24px',
    background: 'rgba(15,29,51,0.85)',
    border: '1px solid rgba(42,61,95,0.5)',
    borderRadius: 12,
    marginBottom: 20,
  };

  if (isAdmin) {
    return (
      <div style={card}>
        <div style={{
          display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10,
        }}>
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#F5C518" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M12 2l9 4v6c0 5-4 9-9 10-5-1-9-5-9-10V6l9-4z"/>
          </svg>
          <div style={{
            fontSize: 16, fontWeight: 700, color: '#E8ECF4', letterSpacing: '-0.01em',
          }}>
            Admin access
          </div>
        </div>
        <div style={{ fontSize: 14, color: '#8B9ABA', lineHeight: 1.6 }}>
          All features unlocked — OverOwned Mode, Live Leverage Tracker, contest
          CSV uploads, and admin SQL tools. No subscription required.
        </div>
      </div>
    );
  }

  if (isSubscribed) {
    const renews = formatDate(subscription?.current_period_end);
    const tierLabel = subscription?.tier
      ? subscription.tier.charAt(0).toUpperCase() + subscription.tier.slice(1)
      : 'Active';
    return (
      <div style={card}>
        <div style={{
          fontSize: 11, color: '#4ADE80', textTransform: 'uppercase',
          letterSpacing: '0.1em', fontWeight: 700, marginBottom: 6,
        }}>
          Current plan
        </div>
        <div style={{
          fontSize: 22, fontWeight: 700, color: '#E8ECF4',
          letterSpacing: '-0.01em', marginBottom: 8,
        }}>
          {tierLabel}
        </div>
        {renews && (
          <div style={{ fontSize: 13, color: '#8B9ABA', marginBottom: 18 }}>
            {subscription?.status === 'trialing' ? 'Trial ends' : 'Next billing'} {renews}
          </div>
        )}
        <button onClick={onManage} disabled={busy}
          style={{
            padding: '11px 22px', fontSize: 13, fontWeight: 600,
            background: 'transparent', color: '#E8ECF4',
            border: '1px solid rgba(245,197,24,0.35)', borderRadius: 7,
            cursor: busy ? 'wait' : 'pointer', fontFamily: 'inherit',
          }}>
          {busy ? 'Opening billing portal…' : 'Manage subscription'}
        </button>
      </div>
    );
  }

  return (
    <div style={card}>
      <div style={{
        fontSize: 11, color: '#F59E0B', textTransform: 'uppercase',
        letterSpacing: '0.1em', fontWeight: 700, marginBottom: 6,
      }}>
        No active subscription
      </div>
      <div style={{
        fontSize: 18, fontWeight: 700, color: '#E8ECF4',
        letterSpacing: '-0.01em', marginBottom: 8,
      }}>
        Upgrade to unlock
      </div>
      <div style={{ fontSize: 13, color: '#8B9ABA', marginBottom: 18, lineHeight: 1.6 }}>
        OverOwned Mode, Live Leverage Tracker, projection sharing,
        and all future features.
      </div>
      <button onClick={onSubscribe} disabled={busy}
        style={{
          display: 'inline-flex', alignItems: 'center', gap: 8,
          padding: '11px 22px', fontSize: 13, fontWeight: 700,
          background: busy ? '#8B9ABA' : 'linear-gradient(135deg, #D4A912, #F5C518)',
          color: '#0A1628', border: 'none', borderRadius: 7,
          cursor: busy ? 'wait' : 'pointer', fontFamily: 'inherit',
        }}>
        {busy ? 'Opening checkout…' : <>Subscribe <span style={{ marginLeft: 2 }}>→</span></>}
      </button>
    </div>
  );
}

// ── Main AccountPage ─────────────────────────────────────────────────
export function AccountPage() {
  const {
    user, isSubscribed, subscription, isAdmin, displayName,
    signOut, updateDisplayName, status,
  } = useAuth();
  const [busy, setBusy] = useState(false);

  if (status === 'loading') {
    return (
      <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{ color: '#8B9ABA', fontSize: 13 }}>Loading…</div>
      </div>
    );
  }

  if (status === 'unauthenticated' || !user) {
    // If the user lands here unauthenticated, bounce them to sign-in
    window.location.hash = '#signin';
    window.location.reload();
    return null;
  }

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

  async function handleSignOut() {
    await signOut();
    // Bounce to main app root
    history.replaceState(null, '', window.location.pathname + window.location.search);
    window.location.reload();
  }

  return (
    <div style={{
      minHeight: '100vh',
      background: 'radial-gradient(ellipse at 50% 0%, #0F1D35 0%, #0A1628 40%, #060F1F 100%)',
      color: '#E8ECF4', fontFamily: 'inherit',
    }}>
      {/* Topbar with back button */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 10,
        padding: '14px 16px 10px', maxWidth: 720, margin: '0 auto',
      }}>
        <button
          onClick={goBack}
          aria-label="Back"
          style={{
            padding: '6px 12px 6px 8px', display: 'inline-flex', alignItems: 'center', gap: 4,
            background: 'transparent', color: '#8B9ABA',
            border: '1px solid rgba(42,61,95,0.6)', borderRadius: 6,
            fontSize: 13, fontWeight: 500, cursor: 'pointer', fontFamily: 'inherit',
          }}
          onMouseEnter={(e) => { e.currentTarget.style.color = '#E8ECF4'; e.currentTarget.style.borderColor = 'rgba(245,197,24,0.4)'; }}
          onMouseLeave={(e) => { e.currentTarget.style.color = '#8B9ABA'; e.currentTarget.style.borderColor = 'rgba(42,61,95,0.6)'; }}
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="15 18 9 12 15 6"/>
          </svg>
          Back
        </button>
      </div>

      {/* Content */}
      <div style={{
        maxWidth: 720, margin: '0 auto',
        padding: '10px 20px 80px',
      }}>
        <h1 style={{
          fontSize: 28, fontWeight: 800, color: '#E8ECF4',
          letterSpacing: '-0.02em', margin: '10px 0 30px',
        }}>
          Account
        </h1>

        {/* Display name section */}
        <DisplayNameRow
          displayName={displayName}
          email={user.email}
          onSave={updateDisplayName}
        />

        {/* Email + status badge */}
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          gap: 12, padding: '16px 0', borderTop: '1px solid rgba(42,61,95,0.5)',
          borderBottom: '1px solid rgba(42,61,95,0.5)', marginBottom: 28, flexWrap: 'wrap',
        }}>
          <div>
            <div style={{
              fontSize: 11, color: '#8B9ABA', textTransform: 'uppercase',
              letterSpacing: '0.1em', fontWeight: 700, marginBottom: 4,
            }}>
              Email
            </div>
            <div style={{
              fontSize: 14, color: '#E8ECF4', fontWeight: 500, wordBreak: 'break-all',
            }}>
              {user.email}
            </div>
          </div>
          <StatusBadge isAdmin={isAdmin} isSubscribed={isSubscribed} subscription={subscription} />
        </div>

        {/* Subscription section */}
        <div style={{
          fontSize: 11, color: '#8B9ABA', textTransform: 'uppercase',
          letterSpacing: '0.1em', fontWeight: 700, marginBottom: 10,
        }}>
          Subscription
        </div>
        <SubscriptionCard
          isAdmin={isAdmin}
          isSubscribed={isSubscribed}
          subscription={subscription}
          onSubscribe={handleSubscribe}
          onManage={handleManage}
          busy={busy}
        />

        {/* Sign out at bottom */}
        <div style={{
          marginTop: 40, paddingTop: 20,
          borderTop: '1px solid rgba(42,61,95,0.5)',
          textAlign: 'center',
        }}>
          <button onClick={handleSignOut}
            style={{
              padding: '10px 20px', fontSize: 13, fontWeight: 600,
              background: 'transparent', color: '#8B9ABA',
              border: '1px solid rgba(42,61,95,0.5)', borderRadius: 7,
              cursor: 'pointer', fontFamily: 'inherit',
              display: 'inline-flex', alignItems: 'center', gap: 8,
            }}
            onMouseEnter={(e) => { e.currentTarget.style.color = '#EF4444'; e.currentTarget.style.borderColor = 'rgba(239,68,68,0.4)'; }}
            onMouseLeave={(e) => { e.currentTarget.style.color = '#8B9ABA'; e.currentTarget.style.borderColor = 'rgba(42,61,95,0.5)'; }}
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/>
              <polyline points="16 17 21 12 16 7"/>
              <line x1="21" y1="12" x2="9" y2="12"/>
            </svg>
            Sign out
          </button>
        </div>
      </div>
    </div>
  );
}
