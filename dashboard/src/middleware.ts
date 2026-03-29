import { defineMiddleware } from 'astro:middleware';
import { getAuth } from 'firebase-admin/auth';
import './lib/firebase-admin'; // Ensure admin is initialized

export const onRequest = defineMiddleware(async (context, next) => {
  // Debug log to confirm request entry and check headers for CSRF issues
  console.log(
    `[Middleware] Method: ${context.request.method} Path: ${context.url.pathname}`,
  );
  console.log(`[Middleware] Origin: ${context.request.headers.get('origin')}`);
  console.log(`[Middleware] Host: ${context.request.headers.get('host')}`);

  const sessionCookie = context.cookies.get('__session')?.value;

  if (sessionCookie) {
    try {
      const auth = getAuth();
      // Verify the session cookie.
      // Note: Setting the second parameter to 'true' (checkRevoked) requires the
      // App Hosting service account to have the 'Firebase Authentication Admin' role.
      // We'll set it to false temporarily to verify if IAM permissions are the bottleneck.
      const decodedClaims = await auth.verifySessionCookie(
        sessionCookie,
        false,
      );
      context.locals.user = decodedClaims;
    } catch (error) {
      // More descriptive error logging
      if (error instanceof Error) {
        console.error(`[Middleware] Firebase Auth Error: ${error.message}`);
      } else {
        console.error('[Middleware] Firebase Auth Error:', error);
      }
      // If invalid, expired, or permission error, clear the cookie
      context.cookies.delete('__session', { path: '/' });
    }
  }

  // Redirect logic for protected pages
  const isProtected = context.url.pathname.startsWith('/top-lists');

  if (isProtected && !context.locals.user) {
    return context.redirect('/login');
  }

  // If trying to access login while already logged in
  if (context.url.pathname === '/login' && context.locals.user) {
    return context.redirect('/');
  }

  return next();
});
