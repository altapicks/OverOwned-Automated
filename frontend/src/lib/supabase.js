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
    // Use PKCE flow (more secure, standard for SPAs).
    flowType: 'pkce',
  },
});
