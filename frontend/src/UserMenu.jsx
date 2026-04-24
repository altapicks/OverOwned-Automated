// UserMenu v5.12 — simple text pill in the topbar.
//
// Shows the user's display name (or email if not set). Clicking it
// navigates to the full Account page via window.location.hash = '#account'.
// No dropdown, no avatar, no icons — just a clickable label.
//
// Unauthenticated users see a "Sign In" button that navigates to #signin.

import React from 'react';
import { useAuth } from '../lib/auth-context';

function goToAccount(e) {
  e.preventDefault();
  window.location.hash = 'account';
  // App.jsx listens for hashchange to re-render the route
  window.dispatchEvent(new HashChangeEvent('hashchange'));
}

function goToSignIn(e) {
  e.preventDefault();
  window.location.hash = 'signin';
  window.location.reload();
}

export function UserMenu() {
  const { user, displayName, status } = useAuth();

  if (status === 'loading') {
    // Invisible placeholder to avoid layout shift
    return <div style={{ width: 60, height: 28 }} />;
  }

  if (status === 'unauthenticated' || !user) {
    return (
      <a href="#signin" onClick={goToSignIn}
         style={{
           padding: '6px 14px', borderRadius: 6,
           border: '1px solid rgba(245,197,24,0.4)',
           color: '#F5C518', fontSize: 12, fontWeight: 600,
           textDecoration: 'none',
           background: 'rgba(245,197,24,0.08)',
           cursor: 'pointer',
         }}>
        Sign In
      </a>
    );
  }

  const label = displayName || user.email;

  return (
    <a href="#account" onClick={goToAccount}
       title="Account settings"
       style={{
         display: 'inline-flex', alignItems: 'center', gap: 6,
         padding: '6px 12px', borderRadius: 6,
         border: '1px solid rgba(42,61,95,0.6)',
         background: 'rgba(10,22,40,0.5)',
         color: '#E8ECF4', fontSize: 12, fontWeight: 500,
         textDecoration: 'none', cursor: 'pointer',
         maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis',
         whiteSpace: 'nowrap', flexShrink: 0,
         transition: 'border-color 0.15s, color 0.15s',
       }}
       onMouseEnter={(e) => { e.currentTarget.style.borderColor = 'rgba(245,197,24,0.4)'; }}
       onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'rgba(42,61,95,0.6)'; }}
    >
      <span style={{
        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        maxWidth: 160,
      }}>
        {label}
      </span>
      <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#8B9ABA"
           strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
        <polyline points="9 18 15 12 9 6"/>
      </svg>
    </a>
  );
}
