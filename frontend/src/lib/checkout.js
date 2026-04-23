// Checkout helper for the app side.
// When a signed-in user clicks "Subscribe to unlock" inside the app, we call
// the landing page's create-checkout-session function (on overowned.io) with
// the user's email + id so Stripe pre-fills and the webhook can match them.
//
// Uses cross-origin fetch (app.overowned.io → overowned.io). The function
// already has CORS headers allowing this.

const CHECKOUT_ENDPOINT = 'https://overowned.io/.netlify/functions/create-checkout-session';

export async function startCheckout({ tier = 'monthly', email, userId } = {}) {
  const res = await fetch(CHECKOUT_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tier, email, user_id: userId }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Checkout failed: ${res.status} ${text}`);
  }
  const { url } = await res.json();
  if (!url) throw new Error('No checkout URL returned');
  window.location.href = url;
}

const PORTAL_ENDPOINT = 'https://overowned.io/.netlify/functions/create-portal-session';

export async function openBillingPortal(userId) {
  const res = await fetch(PORTAL_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user_id: userId }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Portal failed: ${res.status} ${text}`);
  }
  const { url } = await res.json();
  if (!url) throw new Error('No portal URL returned');
  window.location.href = url;
}
