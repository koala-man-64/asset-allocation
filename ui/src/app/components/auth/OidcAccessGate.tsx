import type { ReactNode } from 'react';
import { useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';

import { Button } from '@/app/components/ui/button';
import { config } from '@/config';
import { consumePostLoginRedirectPath, useAuth } from '@/contexts/AuthContext';
import { DataService } from '@/services/DataService';

type AccessState = 'idle' | 'checking' | 'allowed' | 'forbidden' | 'error';

const DEPLOYMENT_AUTH_MISCONFIGURED_BODY =
  'This deployment requires browser OIDC before the UI can call protected API routes. Set UI_OIDC_CLIENT_ID, UI_OIDC_AUTHORITY, UI_OIDC_SCOPES, and UI_OIDC_REDIRECT_URI. The deployed UI only supports OIDC.';

function AuthPanel({
  title,
  body,
  actionLabel,
  onAction,
  secondaryActionLabel,
  onSecondaryAction
}: {
  title: string;
  body: string;
  actionLabel?: string;
  onAction?: () => void;
  secondaryActionLabel?: string;
  onSecondaryAction?: () => void;
}) {
  return (
    <div className="flex min-h-[calc(100vh-8rem)] items-center justify-center px-6 py-12">
      <div className="w-full max-w-xl rounded-2xl border border-border bg-card p-8 shadow-sm">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">
          Asset Allocation
        </p>
        <h2 className="mt-3 text-3xl font-semibold text-foreground">{title}</h2>
        <p className="mt-3 text-sm leading-6 text-muted-foreground">{body}</p>
        {(actionLabel || secondaryActionLabel) && (
          <div className="mt-6 flex flex-wrap gap-3">
            {actionLabel && onAction && <Button onClick={onAction}>{actionLabel}</Button>}
            {secondaryActionLabel && onSecondaryAction && (
              <Button variant="outline" onClick={onSecondaryAction}>
                {secondaryActionLabel}
              </Button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export function OidcCallbackPage() {
  const auth = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    if (!config.oidcEnabled) {
      navigate('/', { replace: true });
      return;
    }
    if (!auth.ready || !auth.authenticated) {
      return;
    }
    navigate(consumePostLoginRedirectPath(), { replace: true });
  }, [auth.authenticated, auth.ready, navigate]);

  if (!config.oidcEnabled) {
    return null;
  }
  if (!auth.ready) {
    return (
      <AuthPanel
        title="Signing you in"
        body="Completing the Microsoft Entra sign-in flow and restoring your session."
      />
    );
  }
  if (auth.authenticated) {
    return (
      <AuthPanel
        title="Redirecting"
        body="Your session is ready. Sending you back into the application now."
      />
    );
  }
  return (
    <AuthPanel
      title="Sign-in could not be completed"
      body={
        auth.error || 'Authentication did not complete successfully. Start the sign-in flow again.'
      }
      actionLabel="Try again"
      onAction={() => auth.signIn('/system-status')}
    />
  );
}

export function OidcAccessGate({ children }: { children: ReactNode }) {
  const auth = useAuth();
  const location = useLocation();
  const [accessState, setAccessState] = useState<AccessState>('idle');
  const [errorMessage, setErrorMessage] = useState<string>('');
  const [retryNonce, setRetryNonce] = useState(0);
  const browserOidcMisconfigured = config.authRequired && (!config.oidcEnabled || !auth.enabled);

  useEffect(() => {
    if (browserOidcMisconfigured) {
      setAccessState('idle');
      setErrorMessage('');
      return;
    }
    if (!config.oidcEnabled || !config.authRequired) {
      setAccessState('allowed');
      setErrorMessage('');
      return;
    }
    if (!auth.ready) {
      setAccessState('idle');
      setErrorMessage('');
      return;
    }
    if (!auth.authenticated) {
      setAccessState('idle');
      setErrorMessage('');
      return;
    }

    let cancelled = false;
    setAccessState('checking');
    setErrorMessage('');

    void DataService.getSystemHealthWithMeta()
      .then(() => {
        if (!cancelled) {
          setAccessState('allowed');
        }
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        const message = error instanceof Error ? error.message : String(error ?? 'Unknown error');
        if (/API Error:\s*403\b/.test(message)) {
          setAccessState('forbidden');
          return;
        }
        setAccessState('error');
        setErrorMessage(message);
      });

    return () => {
      cancelled = true;
    };
  }, [auth.authenticated, auth.ready, browserOidcMisconfigured, retryNonce]);

  if (browserOidcMisconfigured) {
    return (
      <AuthPanel title="Deployment auth misconfigured" body={DEPLOYMENT_AUTH_MISCONFIGURED_BODY} />
    );
  }

  if (!config.oidcEnabled || !config.authRequired) {
    return <>{children}</>;
  }

  if (!auth.ready) {
    return (
      <AuthPanel
        title="Preparing secure access"
        body="Loading the Microsoft Entra session before protected application data is requested."
      />
    );
  }

  if (!auth.authenticated) {
    return (
      <AuthPanel
        title="Sign in required"
        body={
          auth.error ||
          'This deployment requires Microsoft Entra authentication before the UI can load protected API data.'
        }
        actionLabel={auth.error ? 'Try again' : 'Sign in'}
        onAction={() => auth.signIn(`${location.pathname}${location.search}${location.hash}`)}
      />
    );
  }

  if (accessState === 'checking' || accessState === 'idle') {
    return (
      <AuthPanel
        title="Checking access"
        body="Your session is valid. Verifying that your Microsoft Entra assignment is allowed to use the API."
      />
    );
  }

  if (accessState === 'forbidden') {
    return (
      <AuthPanel
        title="Access denied"
        body="You signed in successfully, but your Microsoft Entra account is not assigned the required application role for this API. Ask an administrator to grant the AssetAllocation.Access role."
        actionLabel="Sign out"
        onAction={auth.signOut}
      />
    );
  }

  if (accessState === 'error') {
    return (
      <AuthPanel
        title="Access check failed"
        body={
          errorMessage ||
          'The application could not verify your access against the API. Retry the check or sign out.'
        }
        actionLabel="Retry"
        onAction={() => {
          setAccessState('idle');
          setRetryNonce((value) => value + 1);
        }}
        secondaryActionLabel="Sign out"
        onSecondaryAction={auth.signOut}
      />
    );
  }

  return <>{children}</>;
}
