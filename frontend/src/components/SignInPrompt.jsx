// Sign-in prompt shown when user is unauthenticated.
// Uses Supabase magic-link (passwordless) auth — user enters email,
// receives a link, one click to sign in.
//
// Styled to match OverOwned brand (gold/navy). No passwords, no social login yet.

import React, { useState } from 'react';
import { useAuth } from '../lib/auth-context';

export function SignInPrompt() {
  const { signIn } = useAuth();
  const [email, setEmail] = useState('');
  const [state, setState] = useState('idle');   // 'idle' | 'sending' | 'sent' | 'error'
  const [errorMsg, setErrorMsg] = useState('');

  async function handleSubmit(e) {
    e.preventDefault();
    if (!email || !email.includes('@')) { setState('error'); setErrorMsg('Please enter a valid email'); return; }
    setState('sending');
    try {
      await signIn(email);
      setState('sent');
    } catch (err) {
      setState('error');
      setErrorMsg(err.message || 'Sign-in failed. Please try again.');
    }
  }

  return (
    <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center',
                  background: 'radial-gradient(ellipse at 50% 40%, #0F1D35 0%, #0A1628 40%, #060F1F 100%)',
                  padding: '40px 20px' }}>
      <div style={{ maxWidth: 420, width: '100%', background: 'rgba(15,29,51,0.85)',
                    border: '1px solid rgba(245,197,24,0.25)', borderRadius: 14,
                    padding: '32px 28px', boxShadow: '0 20px 60px rgba(0,0,0,0.5)' }}>

        {/* Logo + brand */}
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', marginBottom: 22 }}>
          <img src="/logo.png" alt="OverOwned" style={{ width: 56, height: 56, borderRadius: '50%', marginBottom: 10 }} />
          <div style={{ fontSize: 20, fontWeight: 800, letterSpacing: '-0.5px' }}>
            <span style={{ color: '#E8ECF4' }}>OVER</span>
            <span style={{ color: '#F5C518' }}>OWNED</span>
          </div>
        </div>

        {state === 'sent' ? (
          <div style={{ textAlign: 'center', padding: '20px 0' }}>
            <div style={{ width: 48, height: 48, borderRadius: '50%', background: 'rgba(74,222,128,0.15)',
                          border: '1px solid rgba(74,222,128,0.4)', display: 'inline-flex',
                          alignItems: 'center', justifyContent: 'center', marginBottom: 14 }}>
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#4ADE80" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="20 6 9 17 4 12"/>
              </svg>
            </div>
            <div style={{ fontSize: 16, fontWeight: 700, color: '#E8ECF4', marginBottom: 6 }}>Check your email</div>
            <div style={{ fontSize: 13, color: '#8B9ABA', lineHeight: 1.5 }}>
              We sent a sign-in link to <strong style={{ color: '#E8ECF4' }}>{email}</strong>.<br/>
              Click it to continue — takes a second.
            </div>
            <button onClick={() => { setState('idle'); setEmail(''); }}
                    style={{ marginTop: 18, background: 'none', border: 'none', color: '#8B9ABA',
                             fontSize: 12, cursor: 'pointer', textDecoration: 'underline' }}>
              Use a different email
            </button>
          </div>
        ) : (
          <form onSubmit={handleSubmit}>
            <p style={{ fontSize: 13, color: '#8B9ABA', marginBottom: 18, textAlign: 'center', lineHeight: 1.5 }}>
              We'll email you a one-click link.<br/>No password needed.
            </p>

            <label style={{ fontSize: 11, fontWeight: 600, color: '#8B9ABA', textTransform: 'uppercase',
                            letterSpacing: '0.08em', display: 'block', marginBottom: 6 }}>
              Email address
            </label>
            <input type="email" value={email} onChange={e => { setEmail(e.target.value); if (state === 'error') setState('idle'); }}
                   placeholder="you@example.com" autoFocus
                   style={{ width: '100%', padding: '11px 13px', background: '#0A1628',
                            border: `1px solid ${state === 'error' ? '#EF4444' : 'rgba(245,197,24,0.25)'}`,
                            borderRadius: 7, color: '#E8ECF4', fontSize: 14, outline: 'none',
                            fontFamily: 'inherit' }} />

            {state === 'error' && (
              <div style={{ fontSize: 11, color: '#EF4444', marginTop: 6 }}>{errorMsg}</div>
            )}

            <button type="submit" disabled={state === 'sending'}
                    style={{ width: '100%', padding: '11px 16px', marginTop: 16,
                             background: state === 'sending' ? '#8B9ABA' : 'linear-gradient(135deg, #D4A912, #F5C518)',
                             color: '#0A1628', border: 'none', borderRadius: 7, fontSize: 14,
                             fontWeight: 700, cursor: state === 'sending' ? 'wait' : 'pointer',
                             transition: 'all 0.15s' }}>
              {state === 'sending' ? 'Sending link…' : 'Email me a sign-in link'}
            </button>

            <div style={{ marginTop: 18, paddingTop: 16, borderTop: '1px solid rgba(245,197,24,0.15)',
                          textAlign: 'center', fontSize: 12, color: '#8B9ABA' }}>
              New here?{' '}
              <a href="https://overowned.io/#pricing" style={{ color: '#F5C518', fontWeight: 600, textDecoration: 'none' }}>
                View subscription options →
              </a>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}
