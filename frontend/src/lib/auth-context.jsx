// React auth context — exposes { user, subscription, isSubscribed, signIn, signOut }
// to the entire app. Wraps <App /> in main.jsx via <AuthProvider>.
//
// How it works:
//  1. On mount, reads the persisted Supabase session from localStorage.
//  2. Subscribes to auth state changes (sign in, sign out, token refresh).
//  3. Whenever the user changes, fetches their row from the `subscriptions` table
//     and exposes the subscription tier + active status.
//  4. `isSubscribed` is true only if status === 'active' AND current_period_end is in the future.

import React, { createContext, useContext, useEffect, useState, useCallback } from 'react';
import { supabase } from './supabase';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [subscription, setSubscription] = useState(null);
  // v5.11: admin status derived from admin_users table. Non-admins see
  // 0 rows due to RLS; admins see their own row. Used to hide subscribe
  // prompts from admin accounts (who have full access without paying).
  const [isAdmin, setIsAdmin] = useState(false);
  // 'loading' until we've checked session + subscription. Prevents UI flash of "not signed in"
  // when a returning user is actually still authenticated.
  const [status, setStatus] = useState('loading');

  // Fetch subscription row for a given user. Called whenever the user changes.
  // v5.13: hardened with a 4s timeout — a slow/stuck query must not block
  // the UI. If it times out, we just leave subscription null; user can refresh.
  const loadSubscription = useCallback(async (userId) => {
    if (!userId) { setSubscription(null); return; }
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), 4000);
    try {
      const { data, error } = await supabase
        .from('subscriptions')
        .select('*')
        .eq('user_id', userId)
        .order('current_period_end', { ascending: false })
        .limit(1)
        .abortSignal(abort.signal)
        .maybeSingle();
      clearTimeout(timer);
      if (error) {
        console.warn('[auth] subscription fetch error:', error.message);
        setSubscription(null);
        return;
      }
      setSubscription(data || null);
    } catch (e) {
      clearTimeout(timer);
      console.warn('[auth] subscription fetch aborted/failed:', e?.message || e);
      setSubscription(null);
    }
  }, []);

  // Check admin status against admin_users. RLS policy uses is_admin() which
  // is security-definer, so this is a fast indexed PK lookup.
  // v5.13: 4s timeout — must not block authentication status.
  const loadAdminStatus = useCallback(async (userId) => {
    if (!userId) { setIsAdmin(false); return; }
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), 4000);
    try {
      const { data, error } = await supabase
        .from('admin_users')
        .select('user_id')
        .eq('user_id', userId)
        .abortSignal(abort.signal)
        .maybeSingle();
      clearTimeout(timer);
      if (error) {
        console.warn('[auth] admin status check failed:', error.message);
        setIsAdmin(false);
        return;
      }
      setIsAdmin(!!data);
    } catch (e) {
      clearTimeout(timer);
      console.warn('[auth] admin status aborted/failed:', e?.message || e);
      setIsAdmin(false);
    }
  }, []);

  // On mount: check existing session + listen for auth state changes.
  // v5.13: status is set based purely on whether a session exists.
  // Subscription + admin status load asynchronously in the background —
  // they must NEVER block the authentication state transition. Otherwise
  // a slow or hung aux query leaves the UI stuck on "Loading…" forever.
  useEffect(() => {
    let mounted = true;

    supabase.auth.getSession().then(({ data: { session } }) => {
      if (!mounted) return;
      const u = session?.user ?? null;
      setUser(u);
      setStatus(u ? 'authenticated' : 'unauthenticated');
      if (u) {
        // Fire-and-forget — errors handled inside each load function
        loadSubscription(u.id);
        loadAdminStatus(u.id);
      }
    });

    const { data: { subscription: authSub } } = supabase.auth.onAuthStateChange((event, session) => {
      if (!mounted) return;
      const u = session?.user ?? null;
      setUser(u);
      if (u) {
        setStatus('authenticated');
        loadSubscription(u.id);
        loadAdminStatus(u.id);
      } else {
        setSubscription(null);
        setIsAdmin(false);
        setStatus('unauthenticated');
      }
    });

    return () => { mounted = false; authSub.unsubscribe(); };
  }, [loadSubscription, loadAdminStatus]);

  // Magic-link sign-in. Sends an email with a one-click login link.
  const signIn = useCallback(async (email) => {
    const { error } = await supabase.auth.signInWithOtp({
      email,
      options: {
        // Where the user lands after clicking the magic link.
        // Site URL in Supabase dashboard must allow this.
        emailRedirectTo: window.location.origin,
      },
    });
    if (error) throw error;
    return { ok: true, email };
  }, []);

  const signOut = useCallback(async () => {
    await supabase.auth.signOut();
    setUser(null);
    setSubscription(null);
    setIsAdmin(false);
    setStatus('unauthenticated');
  }, []);

  // v5.11: update display_name in auth.users.user_metadata.
  // Returns { ok, error? } — the component handles UI feedback.
  // v5.13: 6s timeout so the Save button never gets stuck indefinitely.
  const updateDisplayName = useCallback(async (name) => {
    const trimmed = (name || '').trim();
    if (!trimmed) return { ok: false, error: 'Display name cannot be empty.' };
    if (trimmed.length > 40) return { ok: false, error: 'Maximum 40 characters.' };
    const timeoutPromise = new Promise((resolve) =>
      setTimeout(() => resolve({ data: null, error: { message: 'Request timed out. Please try again.' } }), 6000)
    );
    const updatePromise = supabase.auth.updateUser({ data: { display_name: trimmed } });
    const { data, error } = await Promise.race([updatePromise, timeoutPromise]);
    if (error) return { ok: false, error: error.message };
    if (data?.user) setUser(data.user);
    return { ok: true };
  }, []);

  // Re-fetch subscription manually (used after returning from Stripe Checkout, to pick up
  // the webhook-written subscription row without requiring a full sign-out/in cycle).
  const refreshSubscription = useCallback(async () => {
    if (user) await loadSubscription(user.id);
  }, [user, loadSubscription]);

  // An active subscription is one where status is 'active' or 'trialing' AND the period end
  // is in the future (or null for legacy reasons — we trust the status field in that case).
  const isSubscribed = !!(subscription &&
    (subscription.status === 'active' || subscription.status === 'trialing') &&
    (!subscription.current_period_end || new Date(subscription.current_period_end) > new Date())
  );

  const value = {
    user,
    subscription,
    isSubscribed,
    isAdmin,
    // v5.11: convenience — prefer display_name from user_metadata, fall back
    // to email. Used by UserMenu and anywhere else we want a friendly label.
    displayName: (user?.user_metadata?.display_name && user.user_metadata.display_name.trim()) || null,
    status,           // 'loading' | 'authenticated' | 'unauthenticated'
    signIn,
    signOut,
    updateDisplayName,
    refreshSubscription,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within <AuthProvider>');
  return ctx;
}
