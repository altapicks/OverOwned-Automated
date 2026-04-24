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
  const loadSubscription = useCallback(async (userId) => {
    if (!userId) { setSubscription(null); return; }
    const { data, error } = await supabase
      .from('subscriptions')
      .select('*')
      .eq('user_id', userId)
      .order('current_period_end', { ascending: false })  // latest/active one first
      .limit(1)
      .maybeSingle();
    if (error) {
      // Not a fatal error — user may simply have no subscription row yet.
      // Log for debugging but don't block the app.
      console.warn('[auth] subscription fetch error:', error.message);
      setSubscription(null);
      return;
    }
    setSubscription(data || null);
  }, []);

  // Check admin status against admin_users. RLS policy on that table uses
  // is_admin() which means non-admins see 0 rows, admins see their own row.
  // Either way the query succeeds — we just count rows.
  const loadAdminStatus = useCallback(async (userId) => {
    if (!userId) { setIsAdmin(false); return; }
    const { data, error } = await supabase
      .from('admin_users')
      .select('user_id')
      .eq('user_id', userId)
      .maybeSingle();
    if (error) {
      console.warn('[auth] admin status check failed:', error.message);
      setIsAdmin(false);
      return;
    }
    setIsAdmin(!!data);
  }, []);

  // On mount: check existing session + listen for auth state changes.
  useEffect(() => {
    let mounted = true;

    // Initial session check (reads from localStorage via Supabase SDK).
    supabase.auth.getSession().then(async ({ data: { session } }) => {
      if (!mounted) return;
      const u = session?.user ?? null;
      setUser(u);
      if (u) {
        await Promise.all([loadSubscription(u.id), loadAdminStatus(u.id)]);
      }
      setStatus(u ? 'authenticated' : 'unauthenticated');
    });

    // Subscribe to future auth changes (sign-in, sign-out, token refresh).
    const { data: { subscription: authSub } } = supabase.auth.onAuthStateChange(async (event, session) => {
      if (!mounted) return;
      const u = session?.user ?? null;
      setUser(u);
      if (u) {
        await Promise.all([loadSubscription(u.id), loadAdminStatus(u.id)]);
        setStatus('authenticated');
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
  const updateDisplayName = useCallback(async (name) => {
    const trimmed = (name || '').trim();
    if (!trimmed) return { ok: false, error: 'Display name cannot be empty.' };
    if (trimmed.length > 40) return { ok: false, error: 'Maximum 40 characters.' };
    const { data, error } = await supabase.auth.updateUser({
      data: { display_name: trimmed },
    });
    if (error) return { ok: false, error: error.message };
    // Refresh local user object so UI picks up the change immediately.
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
