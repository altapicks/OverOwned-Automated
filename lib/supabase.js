// Supabase client singleton.
// Reads credentials from Vite env vars set in Netlify dashboard:
//   VITE_SUPABASE_URL
//   VITE_SUPABASE_ANON_KEY
// Both are exposed to frontend — this is fine (anon/publishable keys are designed for it).
// Secret key (service_role) is NEVER used here; only in serverless functions.

import { createClient } from '@supabase/supabase-js';

const url = import.meta.env.VITE_SUPABASE_URL;
const anonKey = import.meta.env.VITE_SUPABASE_ANON_KEY;

if (!url || !anonKey) {
  // Surfacing this early avoids silent "nothing works" debugging later.
  // In prod this should never fire — Netlify should always have these set.
  console.error('[supabase] Missing VITE_SUPABASE_URL or VITE_SUPABASE_ANON_KEY env vars. ' +
    'Check Netlify → Site settings → Environment variables.');
}

export const supabase = createClient(url, anonKey, {
  auth: {
    // Persist session in localStorage so users stay signed in across tab closes / refreshes.
    persistSession: true,
    // Auto-refresh the JWT silently before it expires — this is what makes "already signed in"
    // feel seamless for 30+ days of return visits without explicit login.
    autoRefreshToken: true,
    // Detect session in URL after magic-link callback.
    detectSessionInUrl: true,
    // Implicit flow — carries the session directly in the URL hash after
    // the magic link redirects. Unlike PKCE, it does NOT require a code
    // verifier to be present in the clicking browser's localStorage, so
    // cross-device sign-in works (request link on desktop, click on phone
    // or vice versa). Slightly less secure than PKCE in theory because
    // hashes are visible in server logs, but for email magic-links this
    // is the industry-standard trade-off (auth proof is already the fact
    // that the user accessed their own inbox).
    flowType: 'implicit',
  },
});
